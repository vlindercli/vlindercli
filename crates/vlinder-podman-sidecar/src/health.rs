//! Agent container health observation.
//!
//! Captures health check results as `HealthSnapshots` in a sliding window.
//! The sidecar uses this during startup (`wait_for_agent`) and can poll
//! between invocations.

use std::time::{Duration, Instant};

use vlinder_core::domain::{
    ContainerId, HealthSnapshot, HealthWindow, ImageDigest, ImageRef, RuntimeDiagnostics,
    RuntimeInfo,
};

/// Check the agent's health endpoint once and record the observation.
pub fn check_once(url: &str, health: &mut HealthWindow) -> Option<u16> {
    let client = ureq::Agent::new();
    let check_start = Instant::now();
    let timestamp_ms = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX);

    let status_code = match client.get(url).call() {
        Ok(_) => 200,
        Err(ureq::Error::Status(code, _)) => code,
        Err(_) => 0,
    };

    health.push(HealthSnapshot {
        timestamp_ms,
        latency_ms: u64::try_from(check_start.elapsed().as_millis()).unwrap_or(u64::MAX),
        status_code,
    });

    if status_code == 200 {
        Some(status_code)
    } else {
        None
    }
}

/// Build runtime diagnostics for a completed invocation.
///
/// Performs a health check at completion time and includes the snapshot
/// in the diagnostics.
pub fn build_diagnostics(
    health: &mut HealthWindow,
    port: u16,
    duration_ms: u64,
    container_id: &ContainerId,
    image_ref: Option<&ImageRef>,
    image_digest: Option<&ImageDigest>,
) -> RuntimeDiagnostics {
    let url = format!("http://127.0.0.1:{port}/health");
    check_once(&url, health);
    let snapshot = health.latest().cloned();

    RuntimeDiagnostics {
        stderr: Vec::new(),
        runtime: RuntimeInfo::Container {
            engine_version: "sidecar".to_string(),
            image_ref: image_ref.cloned(),
            image_digest: image_digest.cloned(),
            container_id: container_id.clone(),
        },
        duration_ms,
        health: snapshot,
    }
}

/// Poll the agent's health endpoint until it returns 200 or the deadline passes.
///
/// Every poll attempt is recorded as a `HealthSnapshot` — including
/// failures before the container is ready.
pub async fn wait_for_ready(
    health: &mut HealthWindow,
    port: u16,
    agent_name: &str,
) -> Result<(), String> {
    let url = format!("http://127.0.0.1:{port}/health");
    let deadline = Instant::now() + Duration::from_mins(1);

    tracing::info!(
        event = "sidecar.waiting",
        agent = %agent_name,
        port = port,
        "Waiting for agent container to become ready"
    );

    loop {
        if Instant::now() > deadline {
            return Err(format!(
                "agent container did not become ready within 60 seconds (port {port})"
            ));
        }

        if check_once(&url, health).is_some() {
            tracing::info!(
                event = "sidecar.agent_ready",
                agent = %agent_name,
                "Agent container is ready"
            );
            return Ok(());
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
