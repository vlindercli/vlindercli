//! SQL worker — receives SQL requests from the queue and sends responses back.

use std::sync::Arc;

use tokio_postgres::types::ToSql;
use tokio_postgres::NoTls;
use vlinder_core::domain::{
    MessageQueue, Operation, RequestMessage, ResponseMessage, ServiceBackend, ServiceDiagnostics,
};

use crate::types::{SqlQueryRequest, SqlQueryResponse};

pub struct SqlWorker {
    queue: Arc<dyn MessageQueue + Send + Sync>,
    service: ServiceBackend,
    rt: tokio::runtime::Runtime,
}

impl SqlWorker {
    pub fn new(queue: Arc<dyn MessageQueue + Send + Sync>, service: ServiceBackend) -> Self {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        Self { queue, service, rt }
    }

    /// Process one message if available. Returns true if processed.
    pub fn tick(&self) -> bool {
        self.try_execute()
    }

    fn try_execute(&self) -> bool {
        match self.queue.receive_request(self.service, Operation::Execute) {
            Ok((request, ack)) => {
                let start = std::time::Instant::now();
                let (response_payload, new_state) = self.handle_execute(&request);
                let duration_ms = start.elapsed().as_millis() as u64;

                let diag = ServiceDiagnostics::storage(
                    self.service.service_type(),
                    self.service.backend_str(),
                    Operation::Execute,
                    response_payload.len() as u64,
                    duration_ms,
                );

                let mut response =
                    ResponseMessage::from_request_with_diagnostics(&request, response_payload, diag);
                response.state = new_state.or_else(|| request.state.clone());

                let _ = self.queue.send_response(response);
                let _ = ack();
                true
            }
            Err(_) => false,
        }
    }

    fn handle_execute(&self, request: &RequestMessage) -> (Vec<u8>, Option<String>) {
        let req: SqlQueryRequest = match serde_json::from_slice(request.payload.as_slice()) {
            Ok(r) => r,
            Err(e) => {
                let resp = SqlQueryResponse {
                    columns: Vec::new(),
                    rows: Vec::new(),
                    rows_affected: 0,
                    error: Some(format!("invalid request: {}", e)),
                };
                return (
                    serde_json::to_vec(&resp).unwrap_or_else(|e| format!("[error] {}", e).into_bytes()),
                    None,
                );
            }
        };

        self.rt.block_on(async {
            let mut new_state: Option<String> = None;

            let (client, connection) = match tokio_postgres::connect(
                "host=localhost port=15432 user=root dbname=doltgres",
                NoTls,
            )
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    let resp = SqlQueryResponse {
                        columns: Vec::new(),
                        rows: Vec::new(),
                        rows_affected: 0,
                        error: Some(e.to_string()),
                    };
                    return (
                        serde_json::to_vec(&resp)
                            .unwrap_or_else(|e| format!("[error] {}", e).into_bytes()),
                        None,
                    );
                }
            };

            // Crucial: drive the connection future or the client won't handshake.
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("doltgres connection error: {e}");
                }
            });

            // Time-travel checkout based on request.state.
            if let Some(ref hash) = request.state {
                if !hash.is_empty() {
                    if let Err(e) = client.execute("SELECT DOLT_CHECKOUT($1)", &[hash]).await {
                        let resp = SqlQueryResponse {
                            columns: Vec::new(),
                            rows: Vec::new(),
                            rows_affected: 0,
                            error: Some(e.to_string()),
                        };
                        return (
                            serde_json::to_vec(&resp)
                                .unwrap_or_else(|e| format!("[error] {}", e).into_bytes()),
                            None,
                        );
                    }
                }
            }

            let sql = req.sql.clone();
            let is_mutation = is_mutation_sql(&sql);

            let mut param_boxes: Vec<Box<dyn ToSql + Sync>> = Vec::new();
            for p in req.params.iter() {
                param_boxes.push(param_from_json(p));
            }
            let params: Vec<&(dyn ToSql + Sync)> = param_boxes.iter().map(|b| b.as_ref()).collect();

            let mut columns: Vec<String> = Vec::new();
            let mut rows_json: Vec<Vec<serde_json::Value>> = Vec::new();
            let mut rows_affected: u64 = 0;

            if is_mutation {
                match client.execute(sql.as_str(), &params).await {
                    Ok(n) => rows_affected = n,
                    Err(e) => {
                        let resp = SqlQueryResponse {
                            columns: Vec::new(),
                            rows: Vec::new(),
                            rows_affected: 0,
                            error: Some(e.to_string()),
                        };
                        return (
                            serde_json::to_vec(&resp)
                                .unwrap_or_else(|e| format!("[error] {}", e).into_bytes()),
                            None,
                        );
                    }
                }

                // Commit for mutations and capture returned hash.
                match client
                    .query_one("SELECT dolt_commit('-Am', 'vlinder agent query')", &[])
                    .await
                {
                    Ok(row) => {
                        let hash: Option<String> = row.try_get(0).ok();
                        if let Some(h) = hash {
                            new_state = Some(h);
                        }
                    }
                    Err(e) => {
                        let resp = SqlQueryResponse {
                            columns: Vec::new(),
                            rows: Vec::new(),
                            rows_affected,
                            error: Some(e.to_string()),
                        };
                        return (
                            serde_json::to_vec(&resp)
                                .unwrap_or_else(|e| format!("[error] {}", e).into_bytes()),
                            None,
                        );
                    }
                }
            } else {
                match client.query(sql.as_str(), &params).await {
                    Ok(rows) => {
                        if let Some(first) = rows.first() {
                            columns = first
                                .columns()
                                .iter()
                                .map(|c| c.name().to_string())
                                .collect();
                        }
                        for row in rows {
                            rows_json.push(row_to_json_values(&row));
                        }
                    }
                    Err(e) => {
                        let resp = SqlQueryResponse {
                            columns: Vec::new(),
                            rows: Vec::new(),
                            rows_affected: 0,
                            error: Some(e.to_string()),
                        };
                        return (
                            serde_json::to_vec(&resp)
                                .unwrap_or_else(|e| format!("[error] {}", e).into_bytes()),
                            None,
                        );
                    }
                }
            }

            let resp = SqlQueryResponse {
                columns,
                rows: rows_json,
                rows_affected,
                error: None,
            };

            (
                serde_json::to_vec(&resp).unwrap_or_else(|e| format!("[error] {}", e).into_bytes()),
                new_state,
            )
        })
    }
}

fn is_mutation_sql(sql: &str) -> bool {
    let s = sql.trim_start();
    let upper = s
        .chars()
        .take(32)
        .collect::<String>()
        .to_ascii_uppercase();
    upper.starts_with("INSERT")
        || upper.starts_with("UPDATE")
        || upper.starts_with("DELETE")
        || upper.starts_with("CREATE")
}

fn row_to_json_values(row: &tokio_postgres::Row) -> Vec<serde_json::Value> {
    row.columns()
        .iter()
        .enumerate()
        .map(|(idx, col)| {
            let tname = col.type_().name();
            match tname {
                "text" | "varchar" => match row.try_get::<_, Option<String>>(idx) {
                    Ok(Some(v)) => serde_json::Value::String(v),
                    Ok(None) => serde_json::Value::Null,
                    Err(_) => serde_json::Value::Null,
                },
                "int4" => match row.try_get::<_, Option<i32>>(idx) {
                    Ok(Some(v)) => serde_json::Value::Number(v.into()),
                    Ok(None) => serde_json::Value::Null,
                    Err(_) => serde_json::Value::Null,
                },
                "int8" => match row.try_get::<_, Option<i64>>(idx) {
                    Ok(Some(v)) => serde_json::Value::Number(v.into()),
                    Ok(None) => serde_json::Value::Null,
                    Err(_) => serde_json::Value::Null,
                },
                "bool" => match row.try_get::<_, Option<bool>>(idx) {
                    Ok(Some(v)) => serde_json::Value::Bool(v),
                    Ok(None) => serde_json::Value::Null,
                    Err(_) => serde_json::Value::Null,
                },
                "jsonb" => match row.try_get::<_, Option<Vec<u8>>>(idx) {
                    Ok(Some(bytes)) => jsonb_bytes_to_value(&bytes).unwrap_or(serde_json::Value::Null),
                    Ok(None) => serde_json::Value::Null,
                    Err(_) => serde_json::Value::Null,
                },
                _ => match row.try_get::<_, Option<String>>(idx) {
                    Ok(Some(v)) => serde_json::Value::String(v),
                    Ok(None) => serde_json::Value::Null,
                    Err(_) => serde_json::Value::Null,
                },
            }
        })
        .collect()
}

fn param_from_json(v: &serde_json::Value) -> Box<dyn ToSql + Sync> {
    match v {
        serde_json::Value::Null => Box::new(Option::<String>::None),
        serde_json::Value::Bool(b) => Box::new(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Box::new(i)
            } else if let Some(f) = n.as_f64() {
                Box::new(f)
            } else {
                Box::new(n.to_string())
            }
        }
        serde_json::Value::String(s) => Box::new(s.clone()),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => Box::new(v.to_string()),
    }
}

fn jsonb_bytes_to_value(bytes: &[u8]) -> Option<serde_json::Value> {
    if bytes.is_empty() {
        return None;
    }
    let payload = if bytes[0] == 1 { &bytes[1..] } else { bytes };
    let s = std::str::from_utf8(payload).ok()?;
    serde_json::from_str(s).ok()
}
