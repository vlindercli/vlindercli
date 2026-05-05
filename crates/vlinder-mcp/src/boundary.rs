//! Serialization and deserialization at the protocol boundary.
//!
//! Converts between protocol-specific types (``CallToolRequestParams``,
//! ``CallToolResult``) and opaque `Vec<u8>` payload bytes on the queue.
//! The harness never sees MCP types; the worker never sees harness types.

use anyhow::Result;
use rmcp::model::CallToolRequestParams;
use serde_json::{json, Value};

/// Serialize tool name and arguments into request payload bytes.
pub fn serialize_request(name: &str, arguments: &Value) -> Vec<u8> {
    serde_json::to_vec(&json!({ "name": name, "arguments": arguments })).unwrap_or_default()
}

/// Deserialize queue bytes into tool name and ``CallToolRequestParams``.
pub fn deserialize_request(payload: &[u8]) -> Result<(String, CallToolRequestParams)> {
    let obj: Value = serde_json::from_slice(payload)?;
    let name = obj
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("missing 'name' field"))?
        .to_string();
    let arguments = obj
        .get("arguments")
        .and_then(|v| v.as_object().cloned())
        .unwrap_or_default();
    Ok((
        name.clone(),
        CallToolRequestParams::new(name).with_arguments(arguments),
    ))
}

/// Serialize a ``CallToolResult`` into bytes for the queue.
pub fn serialize_response(result: &rmcp::model::CallToolResult) -> Vec<u8> {
    serde_json::to_vec(result).unwrap_or_default()
}

/// Build a ``CallToolResult`` from text content (for error cases).
pub fn make_text_result(text: &str, is_error: bool) -> rmcp::model::CallToolResult {
    serde_json::from_value(serde_json::json!({
        "content": [{"type": "text", "text": text}],
        "isError": is_error
    }))
    .unwrap_or_else(|_| {
        // Fallback: a minimal valid CallToolResult
        serde_json::from_value(serde_json::json!({
            "content": [],
            "isError": true
        }))
        .expect("fallback CallToolResult must deserialize")
    })
}

/// Extract text content from ``CallToolResult`` bytes.
pub fn extract_tool_content(payload: &[u8]) -> String {
    use rmcp::model::CallToolResult;
    let result: CallToolResult = match serde_json::from_slice(payload) {
        Ok(r) => r,
        Err(_) => return String::from_utf8_lossy(payload).into_owned(),
    };
    result
        .content
        .into_iter()
        .filter_map(|c| match c.raw {
            rmcp::model::RawContent::Text(tc) => Some(tc.text),
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn serialize_request_round_trip() {
        let bytes = serialize_request("echo", &json!({"message": "hello"}));
        let (name, params) = deserialize_request(&bytes).unwrap();
        assert_eq!(name, "echo");
        assert_eq!(
            params.arguments.unwrap().get("message"),
            Some(&json!("hello"))
        );
    }

    #[test]
    fn deserialize_request_missing_name() {
        let bytes = serde_json::to_vec(&json!({"arguments": {}})).unwrap();
        let result = deserialize_request(&bytes);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("missing 'name'"));
    }

    #[test]
    fn deserialize_request_invalid_json() {
        let result = deserialize_request(b"not json");
        assert!(result.is_err());
    }

    #[test]
    fn serialize_response_success() {
        let result = make_text_result("hello world", false);
        let serialized = serialize_response(&result);
        let text = extract_tool_content(&serialized);
        assert_eq!(text, "hello world");
    }

    #[test]
    fn serialize_response_with_error_flag() {
        let result = make_text_result("error occurred", true);
        let serialized = serialize_response(&result);
        let text = extract_tool_content(&serialized);
        assert_eq!(text, "error occurred");
    }

    #[test]
    fn extract_tool_content_fallback_on_invalid_json() {
        let text = extract_tool_content(b"raw text fallback");
        assert_eq!(text, "raw text fallback");
    }
}
