//! gRPC server wrapping the Harness trait (bidi streaming for `RunAgent` /
//! `RunAgentWithMessages`).

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use super::proto::{
    self, harness_server::Harness as HarnessService, run_agent_client_msg, run_agent_server_msg,
    ForkTimelineRequest, ForkTimelineResponse, PingRequest, PromoteTimelineRequest,
    PromoteTimelineResponse, RunAgentClientMsg, RunAgentServerMsg, SemVer, StartSessionRequest,
    StartSessionResponse,
};
use vlinder_core::domain::{
    AgentName, ApprovalDecision, ApprovalMode, BranchId, DagNodeId, ForkParams, Harness,
    HarnessEvent, PromoteParams, ResourceId, RunCtl, SessionId,
};

/// gRPC server that wraps a Harness implementation.
pub struct HarnessServer {
    harness: Arc<Box<dyn Harness + Send + Sync>>,
}

impl HarnessServer {
    pub fn new(harness: Box<dyn Harness + Send + Sync>) -> Self {
        Self {
            harness: Arc::new(harness),
        }
    }

    /// Create a tonic service from this server.
    pub fn into_service(self) -> proto::harness_server::HarnessServer<Self> {
        proto::harness_server::HarnessServer::new(self)
    }
}

#[tonic::async_trait]
#[allow(clippy::too_many_lines)]
impl HarnessService for HarnessServer {
    type RunAgentStream = ReceiverStream<Result<RunAgentServerMsg, Status>>;
    type RunAgentWithMessagesStream = ReceiverStream<Result<RunAgentServerMsg, Status>>;

    async fn ping(&self, _request: Request<PingRequest>) -> Result<Response<SemVer>, Status> {
        Ok(Response::new(SemVer {
            major: 0,
            minor: 0,
            patch: 1,
        }))
    }

    async fn start_session(
        &self,
        request: Request<StartSessionRequest>,
    ) -> Result<Response<StartSessionResponse>, Status> {
        let req = request.into_inner();
        let external_id = vlinder_core::domain::ExternalSessionId::new(&req.external_id)
            .map_err(|e| Status::invalid_argument(format!("invalid external_id: {e}")))?;
        let (session_id, branch_id) = self
            .harness
            .start_session(&req.agent_name, external_id)
            .await;
        Ok(Response::new(StartSessionResponse {
            session_id: session_id.as_str().to_string(),
            default_branch_id: branch_id.as_i64(),
        }))
    }

    async fn run_agent(
        &self,
        request: Request<tonic::Streaming<RunAgentClientMsg>>,
    ) -> Result<Response<Self::RunAgentStream>, Status> {
        let mut client_stream = request.into_inner();
        let start = expect_start_msg(&mut client_stream).await?;

        let (server_tx, server_rx) = mpsc::channel(32);
        let (event_tx, mut event_rx) = mpsc::channel::<HarnessEvent>(32);
        let approvals: Arc<Mutex<HashMap<String, oneshot::Sender<ApprovalDecision>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // Pump 1: client → approvals map. Drain remaining client messages,
        // forwarding approval decisions to the in-process oneshot channels.
        let approvals_for_pump = approvals.clone();
        tokio::spawn(async move {
            while let Ok(Some(msg)) = client_stream.message().await {
                if let Some(run_agent_client_msg::Kind::Approval(d)) = msg.kind {
                    let mut map = approvals_for_pump.lock().await;
                    if let Some(tx) = map.remove(&d.approval_id) {
                        let decision = match d.decision {
                            Some(proto::approval_decision_msg::Decision::Approve(())) => {
                                ApprovalDecision::Approve
                            }
                            Some(proto::approval_decision_msg::Decision::RejectReason(r)) => {
                                ApprovalDecision::Reject { reason: r }
                            }
                            None => continue,
                        };
                        let _ = tx.send(decision);
                    }
                }
            }
        });

        // Pump 2: harness events → server stream (translate to proto events).
        let server_tx_for_events = server_tx.clone();
        let approvals_for_translate = approvals.clone();
        tokio::spawn(async move {
            while let Some(evt) = event_rx.recv().await {
                if let Some(proto_evt) = harness_event_to_proto(evt, &approvals_for_translate).await
                {
                    let _ = server_tx_for_events
                        .send(Ok(RunAgentServerMsg {
                            kind: Some(run_agent_server_msg::Kind::Event(proto_evt)),
                        }))
                        .await;
                }
            }
        });

        // The run task: build RunCtl, invoke harness, send terminal message.
        let harness = Arc::clone(&self.harness);
        tokio::spawn(async move {
            let ctl = ctl_from_start(&start, event_tx);

            let agent_id = ResourceId::new(&start.agent_id);
            let session_id = match SessionId::try_from(start.session_id.clone()) {
                Ok(s) => s,
                Err(e) => {
                    let _ = server_tx
                        .send(Ok(RunAgentServerMsg {
                            kind: Some(run_agent_server_msg::Kind::Error(format!(
                                "invalid session_id: {e}"
                            ))),
                        }))
                        .await;
                    return;
                }
            };
            let timeline = BranchId::from(start.branch_id.parse::<i64>().unwrap_or(0));
            let dag_parent = DagNodeId::from(start.dag_parent.clone());
            let initial_state = start.initial_state;
            let input = start.input;

            // Route to run_agent or run_agent_with_messages.
            let outcome = if start.messages_json.is_empty() {
                harness
                    .run_agent(
                        &agent_id,
                        &input,
                        session_id,
                        timeline,
                        start.sealed,
                        initial_state,
                        dag_parent,
                        &ctl,
                    )
                    .await
            } else {
                // Deserialize messages from JSON.
                let messages: Vec<vlinder_core::domain::Message> =
                    match serde_json::from_str(&start.messages_json) {
                        Ok(m) => m,
                        Err(e) => {
                            let _ = server_tx
                                .send(Ok(RunAgentServerMsg {
                                    kind: Some(run_agent_server_msg::Kind::Error(format!(
                                        "invalid messages_json: {e}"
                                    ))),
                                }))
                                .await;
                            return;
                        }
                    };
                harness
                    .run_agent_with_messages(&agent_id, messages, session_id, &ctl)
                    .await
            };

            let terminal = match outcome {
                Ok(r) => run_agent_server_msg::Kind::Result(r.into()),
                Err(e) => run_agent_server_msg::Kind::Error(e),
            };
            let _ = server_tx
                .send(Ok(RunAgentServerMsg {
                    kind: Some(terminal),
                }))
                .await;
        });

        Ok(Response::new(ReceiverStream::new(server_rx)))
    }

    async fn run_agent_with_messages(
        &self,
        request: Request<tonic::Streaming<RunAgentClientMsg>>,
    ) -> Result<Response<Self::RunAgentWithMessagesStream>, Status> {
        // Delegate to run_agent — the dispatch inside run_agent checks
        // messages_json and routes to the correct method internally.
        // The only difference is the stream type; the logic is identical.
        let mut client_stream = request.into_inner();
        let start = expect_start_msg(&mut client_stream).await?;

        let (server_tx, server_rx) = mpsc::channel(32);
        let (event_tx, mut event_rx) = mpsc::channel::<HarnessEvent>(32);
        let approvals: Arc<Mutex<HashMap<String, oneshot::Sender<ApprovalDecision>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // Pump 1: client → approvals map.
        let approvals_for_pump = approvals.clone();
        tokio::spawn(async move {
            while let Ok(Some(msg)) = client_stream.message().await {
                if let Some(run_agent_client_msg::Kind::Approval(d)) = msg.kind {
                    let mut map = approvals_for_pump.lock().await;
                    if let Some(tx) = map.remove(&d.approval_id) {
                        let decision = match d.decision {
                            Some(proto::approval_decision_msg::Decision::Approve(())) => {
                                ApprovalDecision::Approve
                            }
                            Some(proto::approval_decision_msg::Decision::RejectReason(r)) => {
                                ApprovalDecision::Reject { reason: r }
                            }
                            None => continue,
                        };
                        let _ = tx.send(decision);
                    }
                }
            }
        });

        // Pump 2: events → server stream.
        let server_tx_for_events = server_tx.clone();
        let approvals_for_translate = approvals.clone();
        tokio::spawn(async move {
            while let Some(evt) = event_rx.recv().await {
                if let Some(proto_evt) = harness_event_to_proto(evt, &approvals_for_translate).await
                {
                    let _ = server_tx_for_events
                        .send(Ok(RunAgentServerMsg {
                            kind: Some(run_agent_server_msg::Kind::Event(proto_evt)),
                        }))
                        .await;
                }
            }
        });

        // Spawn the run.
        let harness = Arc::clone(&self.harness);
        tokio::spawn(async move {
            let ctl = ctl_from_start(&start, event_tx);

            let agent_id = ResourceId::new(&start.agent_id);
            let session_id = match SessionId::try_from(start.session_id.clone()) {
                Ok(s) => s,
                Err(e) => {
                    let _ = server_tx
                        .send(Ok(RunAgentServerMsg {
                            kind: Some(run_agent_server_msg::Kind::Error(format!(
                                "invalid session_id: {e}"
                            ))),
                        }))
                        .await;
                    return;
                }
            };

            if start.messages_json.is_empty() {
                // No messages provided — treat as error; the user called
                // run_agent_with_messages but didn't provide messages.
                let _ = server_tx
                    .send(Ok(RunAgentServerMsg {
                        kind: Some(run_agent_server_msg::Kind::Error(
                            "run_agent_with_messages requires messages_json".to_string(),
                        )),
                    }))
                    .await;
                return;
            }

            let messages: Vec<vlinder_core::domain::Message> =
                match serde_json::from_str(&start.messages_json) {
                    Ok(m) => m,
                    Err(e) => {
                        let _ = server_tx
                            .send(Ok(RunAgentServerMsg {
                                kind: Some(run_agent_server_msg::Kind::Error(format!(
                                    "invalid messages_json: {e}"
                                ))),
                            }))
                            .await;
                        return;
                    }
                };

            let outcome = harness
                .run_agent_with_messages(&agent_id, messages, session_id, &ctl)
                .await;

            let terminal = match outcome {
                Ok(r) => run_agent_server_msg::Kind::Result(r.into()),
                Err(e) => run_agent_server_msg::Kind::Error(e),
            };
            let _ = server_tx
                .send(Ok(RunAgentServerMsg {
                    kind: Some(terminal),
                }))
                .await;
        });

        Ok(Response::new(ReceiverStream::new(server_rx)))
    }

    async fn fork_timeline(
        &self,
        request: Request<ForkTimelineRequest>,
    ) -> Result<Response<ForkTimelineResponse>, Status> {
        let req = request.into_inner();
        let harness = Arc::clone(&self.harness);

        let params = ForkParams {
            agent_name: AgentName::new(req.agent_name),
            branch_name: req.branch_name,
            fork_point: DagNodeId::from(req.fork_point),
        };
        let session_id = SessionId::try_from(req.session_id)
            .map_err(|e| Status::invalid_argument(format!("invalid session_id: {e}")))?;
        let timeline = BranchId::from(req.branch_id.parse::<i64>().unwrap_or(0));
        let result = harness.fork_timeline(params, session_id, timeline).await;

        match result {
            Ok(()) => Ok(Response::new(ForkTimelineResponse { error: None })),
            Err(e) => Ok(Response::new(ForkTimelineResponse { error: Some(e) })),
        }
    }

    async fn promote_timeline(
        &self,
        request: Request<PromoteTimelineRequest>,
    ) -> Result<Response<PromoteTimelineResponse>, Status> {
        let req = request.into_inner();
        let params = PromoteParams {
            agent_name: AgentName::new(req.agent_name),
        };
        let session_id = SessionId::try_from(req.session_id)
            .map_err(|e| Status::invalid_argument(format!("invalid session_id: {e}")))?;
        let timeline = BranchId::from(req.branch_id.parse::<i64>().unwrap_or(0));

        match self
            .harness
            .promote_timeline(params, session_id, timeline)
            .await
        {
            Ok(()) => Ok(Response::new(PromoteTimelineResponse { error: None })),
            Err(e) => Ok(Response::new(PromoteTimelineResponse { error: Some(e) })),
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────

/// Read the first message from a client stream and assert it's a
/// `RunAgentStart`. Returns `Status::invalid_argument` on protocol violation.
async fn expect_start_msg(
    client_stream: &mut tonic::Streaming<RunAgentClientMsg>,
) -> Result<proto::RunAgentStart, Status> {
    match client_stream.message().await {
        Ok(Some(msg)) => match msg.kind {
            Some(run_agent_client_msg::Kind::Start(start)) => Ok(start),
            Some(kind) => Err(Status::invalid_argument(format!(
                "expected start message, got {kind:?}"
            ))),
            None => Err(Status::invalid_argument(
                "client stream ended before start message".to_string(),
            )),
        },
        Ok(None) => Err(Status::invalid_argument(
            "client stream ended before start message".to_string(),
        )),
        Err(e) => Err(Status::internal(format!(
            "failed to read start message: {e}"
        ))),
    }
}

/// Build a `RunCtl` from a `RunAgentStart` proto message and an event sender.
fn ctl_from_start(start: &proto::RunAgentStart, event_tx: mpsc::Sender<HarnessEvent>) -> RunCtl {
    let proto_mode = proto::ApprovalModeProto::try_from(start.approval_mode)
        .unwrap_or(proto::ApprovalModeProto::Autonomous);
    let mode: ApprovalMode = proto_mode.into();
    match mode {
        ApprovalMode::Autonomous => RunCtl::streaming(event_tx),
        ApprovalMode::RequireApproval => RunCtl::streaming_with_approval(event_tx),
    }
}

/// Translate a domain `HarnessEvent` to a proto `HarnessEventProto`.
///
/// For `ToolCallApprovalRequired`, the oneshot sender is consumed and stored
/// in the approvals map under a freshly generated UUID `approval_id`. The
/// resulting proto carries the same id so the client can correlate its
/// `ApprovalDecisionMsg` response.
///
/// Returns `None` for `RunCompleted` — that event is communicated implicitly
/// via the terminal `RunResultProto` message sent by the run task.
async fn harness_event_to_proto(
    event: HarnessEvent,
    approvals: &Arc<Mutex<HashMap<String, oneshot::Sender<ApprovalDecision>>>>,
) -> Option<proto::HarnessEventProto> {
    use proto::harness_event_proto;

    match event {
        HarnessEvent::RunStarted {
            agent_id,
            session_id,
            timeline,
        } => Some(proto::HarnessEventProto {
            kind: Some(harness_event_proto::Kind::RunStarted(
                proto::RunStartedEvt {
                    agent_id: agent_id.to_string(),
                    session_id: session_id.to_string(),
                    branch_id: timeline.as_i64().to_string(),
                },
            )),
        }),
        HarnessEvent::TurnStarted { turn_index } => Some(proto::HarnessEventProto {
            kind: Some(harness_event_proto::Kind::TurnStarted(
                proto::TurnStartedEvt { turn_index },
            )),
        }),
        HarnessEvent::AssistantContent { turn_index, text } => Some(proto::HarnessEventProto {
            kind: Some(harness_event_proto::Kind::AssistantContent(
                proto::AssistantContentEvt { turn_index, text },
            )),
        }),
        HarnessEvent::ToolCallApprovalRequired {
            tool_call,
            decision,
        } => {
            let approval_id = Uuid::new_v4().to_string();
            approvals.lock().await.insert(approval_id.clone(), decision);
            Some(proto::HarnessEventProto {
                kind: Some(harness_event_proto::Kind::ApprovalRequired(
                    proto::ToolCallApprovalRequiredEvt {
                        approval_id,
                        tool_call: Some(tool_call.into()),
                    },
                )),
            })
        }
        HarnessEvent::ToolCallStarted { tool_call } => Some(proto::HarnessEventProto {
            kind: Some(harness_event_proto::Kind::ToolCallStarted(
                proto::ToolCallStartedEvt {
                    tool_call: Some(tool_call.into()),
                },
            )),
        }),
        HarnessEvent::ToolCallCompleted { trace } => Some(proto::HarnessEventProto {
            kind: Some(harness_event_proto::Kind::ToolCallCompleted(
                proto::ToolCallCompletedEvt {
                    trace: Some(trace.into()),
                },
            )),
        }),
        HarnessEvent::TurnCompleted { turn_index, turn } => Some(proto::HarnessEventProto {
            kind: Some(harness_event_proto::Kind::TurnCompleted(
                proto::TurnCompletedEvt {
                    turn_index,
                    turn: Some(turn.into()),
                },
            )),
        }),
        HarnessEvent::RunCompleted { .. } => {
            // RunCompleted is communicated via the terminal RunResultProto
            // message sent by the run task. Do not emit it as an event.
            None
        }
    }
}
