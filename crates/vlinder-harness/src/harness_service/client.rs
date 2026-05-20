//! gRPC client implementing the Harness trait (bidi streaming).

use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

use super::proto::{
    self, harness_client::HarnessClient, run_agent_client_msg, run_agent_server_msg,
};
use vlinder_core::domain::{
    ApprovalDecision, BranchId, DagNodeId, ExternalSessionId, ForkParams, Harness, HarnessEvent,
    HarnessType, Message, PromoteParams, ResourceId, RunCtl, RunResult, SessionId,
};

pub async fn ping_harness_async(addr: &str) -> Option<(u32, u32, u32)> {
    let Ok(mut client) = HarnessClient::connect(addr.to_string()).await else {
        return None;
    };
    client.ping(proto::PingRequest {}).await.ok().map(|r| {
        let v = r.into_inner();
        (v.major, v.minor, v.patch)
    })
}

/// Harness implementation that makes gRPC calls to a remote server.
pub struct GrpcHarnessClient {
    client: HarnessClient<Channel>,
}

impl GrpcHarnessClient {
    /// Connect to a harness server.
    pub async fn connect(addr: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let client = HarnessClient::connect(addr.to_string()).await?;

        Ok(Self { client })
    }
}

#[async_trait]
impl Harness for GrpcHarnessClient {
    fn harness_type(&self) -> HarnessType {
        HarnessType::Grpc
    }

    async fn start_session(
        &self,
        agent_name: &str,
        external_id: ExternalSessionId,
    ) -> (SessionId, BranchId) {
        let request = proto::StartSessionRequest {
            agent_name: agent_name.to_string(),
            external_id: external_id.as_str().to_string(),
        };

        let mut client = self.client.clone();
        let response = client
            .start_session(request)
            .await
            .expect("failed to start session via gRPC")
            .into_inner();
        let session_id =
            SessionId::try_from(response.session_id).expect("server returned invalid session_id");
        let branch_id = BranchId::from(response.default_branch_id);
        (session_id, branch_id)
    }

    async fn run_agent(
        &self,
        agent_id: &ResourceId,
        input: &str,
        session_id: SessionId,
        timeline: BranchId,
        sealed: bool,
        initial_state: Option<String>,
        dag_parent: DagNodeId,
        ctl: &RunCtl,
    ) -> Result<RunResult, String> {
        let mut client = self.client.clone();

        let start = proto::RunAgentStart {
            agent_id: agent_id.to_string(),
            input: input.to_string(),
            session_id: session_id.to_string(),
            branch_id: timeline.as_i64().to_string(),
            sealed,
            initial_state,
            dag_parent: dag_parent.to_string(),
            approval_mode: if ctl.requires_approval() {
                proto::ApprovalModeProto::RequireApproval.into()
            } else {
                proto::ApprovalModeProto::Autonomous.into()
            },
            messages_json: String::new(), // run_agent path
        };

        run_agent_bidi(&mut client, "run_agent", start, ctl).await
    }

    async fn fork_timeline(
        &self,
        params: ForkParams,
        session_id: SessionId,
        timeline: BranchId,
    ) -> Result<(), String> {
        let request = proto::ForkTimelineRequest {
            agent_name: params.agent_name.to_string(),
            branch_name: params.branch_name,
            fork_point: params.fork_point.to_string(),
            branch_id: timeline.to_string(),
            session_id: session_id.as_str().to_string(),
        };

        let mut client = self.client.clone();
        let response = client
            .fork_timeline(request)
            .await
            .map_err(|e| format!("gRPC error: {e}"))?;

        let resp = response.into_inner();
        if let Some(error) = resp.error {
            Err(error)
        } else {
            Ok(())
        }
    }

    async fn promote_timeline(
        &self,
        params: PromoteParams,
        session_id: SessionId,
        timeline: BranchId,
    ) -> Result<(), String> {
        let request = proto::PromoteTimelineRequest {
            agent_name: params.agent_name.to_string(),
            branch_id: timeline.to_string(),
            session_id: session_id.as_str().to_string(),
        };

        let mut client = self.client.clone();
        let response = client
            .promote_timeline(request)
            .await
            .map_err(|e| format!("gRPC error: {e}"))?;

        let resp = response.into_inner();
        if let Some(error) = resp.error {
            Err(error)
        } else {
            Ok(())
        }
    }

    async fn run_agent_with_messages(
        &self,
        agent_id: &ResourceId,
        messages: Vec<Message>,
        session_id: SessionId,
        ctl: &RunCtl,
    ) -> Result<RunResult, String> {
        let mut client = self.client.clone();

        let messages_json = serde_json::to_string(&messages)
            .map_err(|e| format!("failed to serialize messages: {e}"))?;

        let start = proto::RunAgentStart {
            agent_id: agent_id.to_string(),
            input: String::new(), // not used for run_agent_with_messages
            session_id: session_id.to_string(),
            branch_id: String::new(),
            sealed: false,
            initial_state: None,
            dag_parent: String::new(),
            approval_mode: if ctl.requires_approval() {
                proto::ApprovalModeProto::RequireApproval.into()
            } else {
                proto::ApprovalModeProto::Autonomous.into()
            },
            messages_json,
        };

        run_agent_bidi(&mut client, "run_agent_with_messages", start, ctl).await
    }
}

/// Shared bidi streaming logic for `run_agent` and `run_agent_with_messages`.
///
/// Opens a bidi stream, sends the `RunAgentStart` message, then drains the
/// server stream, forwarding events to the caller's `RunCtl` channel. For
/// `ToolCallApprovalRequired` events, creates a local oneshot channel,
/// includes the sender in the in-process `HarnessEvent`, and spawns a task
/// that awaits the decision and sends it back over the wire keyed by the
/// original `approval_id`.
async fn run_agent_bidi(
    client: &mut HarnessClient<Channel>,
    rpc_name: &str,
    start: proto::RunAgentStart,
    ctl: &RunCtl,
) -> Result<RunResult, String> {
    use tokio::sync::mpsc;

    // Queue the start message *before* opening the stream. The server reads
    // the first message synchronously inside its handler before returning a
    // Response, and `client.method(...).await` does not return until those
    // response headers arrive — so awaiting the call first and then sending
    // the start would deadlock both sides.
    let (in_tx, in_rx) = mpsc::channel::<proto::RunAgentClientMsg>(32);
    in_tx
        .send(proto::RunAgentClientMsg {
            kind: Some(run_agent_client_msg::Kind::Start(start)),
        })
        .await
        .map_err(|_| "failed to queue start message".to_string())?;

    let in_stream = ReceiverStream::new(in_rx);
    let out_stream = match rpc_name {
        "run_agent" => client.run_agent(tonic::Request::new(in_stream)).await,
        "run_agent_with_messages" => {
            client
                .run_agent_with_messages(tonic::Request::new(in_stream))
                .await
        }
        _ => return Err(format!("unknown RPC: {rpc_name}")),
    }
    .map_err(|e| format!("gRPC error opening {rpc_name}: {e}"))?
    .into_inner();

    // Pin the output stream for iteration.
    tokio::pin!(out_stream);

    // Drain the server stream.
    loop {
        match out_stream.message().await {
            Ok(Some(msg)) => match msg.kind {
                Some(run_agent_server_msg::Kind::Event(evt)) => {
                    if let Some(event) = proto_event_to_harness_event(evt, &in_tx) {
                        ctl.emit(event).await;
                    }
                }
                Some(run_agent_server_msg::Kind::Result(r)) => {
                    return Ok(r.into());
                }
                Some(run_agent_server_msg::Kind::Error(e)) => {
                    return Err(e);
                }
                None => return Err("stream ended without result".to_string()),
            },
            Ok(None) => return Err("stream ended prematurely".to_string()),
            Err(e) => return Err(format!("gRPC stream error: {e}")),
        }
    }
}

/// Translate a proto `HarnessEventProto` to a domain `HarnessEvent`.
///
/// For `ToolCallApprovalRequiredEvt`, creates a local oneshot channel and
/// spawns a task that awaits the decision and sends it back over the wire
/// via the input stream.
fn proto_event_to_harness_event(
    evt: proto::HarnessEventProto,
    in_tx: &mpsc::Sender<proto::RunAgentClientMsg>,
) -> Option<HarnessEvent> {
    use proto::harness_event_proto::Kind;

    match evt.kind? {
        Kind::RunStarted(e) => Some(HarnessEvent::RunStarted {
            agent_id: ResourceId::new(&e.agent_id),
            session_id: SessionId::try_from(e.session_id).unwrap_or_else(|_| SessionId::new()),
            timeline: BranchId::from(e.branch_id.parse::<i64>().unwrap_or(0)),
        }),
        Kind::TurnStarted(e) => Some(HarnessEvent::TurnStarted {
            turn_index: e.turn_index,
        }),
        Kind::AssistantContent(e) => Some(HarnessEvent::AssistantContent {
            turn_index: e.turn_index,
            text: e.text,
        }),
        Kind::ApprovalRequired(e) => {
            let approval_id = e.approval_id.clone();
            let tool_call: vlinder_core::domain::ToolCall = e.tool_call?.into();

            // Create a local oneshot channel for the decision.
            let (decision_tx, decision_rx) = tokio::sync::oneshot::channel();

            // Spawn a task that awaits the decision and sends it back over the wire.
            let in_tx_clone = in_tx.clone();
            tokio::spawn(async move {
                match decision_rx.await {
                    Ok(ApprovalDecision::Approve) => {
                        let _ = in_tx_clone
                            .send(proto::RunAgentClientMsg {
                                kind: Some(run_agent_client_msg::Kind::Approval(
                                    proto::ApprovalDecisionMsg {
                                        approval_id,
                                        decision: Some(
                                            proto::approval_decision_msg::Decision::Approve(()),
                                        ),
                                    },
                                )),
                            })
                            .await;
                    }
                    Ok(ApprovalDecision::Reject { reason }) => {
                        let _ = in_tx_clone
                            .send(proto::RunAgentClientMsg {
                                kind: Some(run_agent_client_msg::Kind::Approval(
                                    proto::ApprovalDecisionMsg {
                                        approval_id,
                                        decision: Some(
                                            proto::approval_decision_msg::Decision::RejectReason(
                                                reason,
                                            ),
                                        ),
                                    },
                                )),
                            })
                            .await;
                    }
                    Err(_) => {
                        // Decision channel closed — the consumer hung up.
                        // The server will time out and return an error.
                    }
                }
            });

            Some(HarnessEvent::ToolCallApprovalRequired {
                tool_call,
                decision: decision_tx,
            })
        }
        Kind::ToolCallStarted(e) => {
            let tool_call = e.tool_call?.into();
            Some(HarnessEvent::ToolCallStarted { tool_call })
        }
        Kind::ToolCallCompleted(e) => {
            let trace = e.trace?.into();
            Some(HarnessEvent::ToolCallCompleted { trace })
        }
        Kind::TurnCompleted(e) => {
            let turn = e.turn?.into();
            Some(HarnessEvent::TurnCompleted {
                turn_index: e.turn_index,
                turn,
            })
        }
    }
}
