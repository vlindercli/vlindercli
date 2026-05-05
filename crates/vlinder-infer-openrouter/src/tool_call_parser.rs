use async_openai::types::chat::{ChatCompletionMessageToolCalls, CreateChatCompletionResponse};
use serde_json::Value;
use vlinder_core::domain::{Message, ParseError, ParsedResponse, ToolCall, ToolCallParser};

/// OpenAI‑compatible tool‑call parser.
///
/// Handles the wire format used by `OpenAI`, `OpenRouter`, and `Ollama`.
#[derive(Clone, Debug, Default)]
pub struct OpenAiToolCallParser;

impl ToolCallParser for OpenAiToolCallParser {
    fn parse_response(&self, payload: &[u8]) -> Result<ParsedResponse, ParseError> {
        let response: CreateChatCompletionResponse =
            serde_json::from_slice(payload).map_err(ParseError::Json)?;

        let message = response
            .choices
            .first()
            .ok_or_else(|| ParseError::InvalidStructure("no choices".into()))?
            .message
            .clone();

        let content = message.content;

        let tool_calls = if let Some(tc) = message.tool_calls {
            let mut parsed = Vec::with_capacity(tc.len());
            for call in tc {
                match call {
                    ChatCompletionMessageToolCalls::Function(inner) => {
                        let args = inner.function.arguments.parse::<Value>().map_err(|e| {
                            ParseError::InvalidStructure(format!("invalid JSON arguments: {e}"))
                        })?;
                        parsed.push(ToolCall {
                            id: inner.id.into(),
                            name: inner.function.name,
                            arguments: args,
                        });
                    }
                    ChatCompletionMessageToolCalls::Custom(_) => {
                        // ignore custom tool calls for now
                    }
                }
            }
            Some(parsed)
        } else {
            None
        };

        Ok(ParsedResponse {
            content,
            tool_calls,
        })
    }

    fn serialize_conversation(&self, messages: &[Message]) -> Vec<u8> {
        use serde_json::json;
        let wire_messages: Vec<serde_json::Value> = messages
            .iter()
            .map(|m| match m {
                Message::User { content } => json!({
                    "role": "user",
                    "content": content,
                }),
                Message::Agent {
                    content,
                    tool_calls,
                } => {
                    let mut msg = json!({ "role": "assistant" });
                    if let Some(c) = content {
                        msg["content"] = json!(c);
                    }
                    if let Some(tcs) = tool_calls {
                        let tool_calls_json: Vec<_> = tcs
                            .iter()
                            .map(|tc| {
                                json!({
                                    "id": tc.id,
                                    "type": "function",
                                    "function": {
                                        "name": tc.name,
                                        "arguments": tc.arguments.to_string(),
                                    },
                                })
                            })
                            .collect();
                        msg["tool_calls"] = json!(tool_calls_json);
                    }
                    msg
                }
                Message::Tool {
                    tool_call_id,
                    content,
                } => json!({
                    "role": "tool",
                    "content": String::from_utf8_lossy(content).into_owned(),
                    "tool_call_id": tool_call_id,
                }),
            })
            .collect();

        let body = json!({ "messages": wire_messages });
        serde_json::to_vec(&body).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use vlinder_core::domain::{Message, ToolCall};

    #[test]
    fn parse_response_plain_text() {
        let payload = json!({
            "id": "cmpl-123",
            "object": "chat.completion",
            "created": 1_234_567_890,
            "model": "gpt-3.5-turbo",
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 5,
                "total_tokens": 15
            },
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": "Hello there"
                    },
                    "finish_reason": "stop"
                }
            ]
        });
        let parser = OpenAiToolCallParser;
        let result = parser
            .parse_response(&serde_json::to_vec(&payload).unwrap())
            .unwrap();
        assert_eq!(result.content, Some("Hello there".into()));
        assert_eq!(result.tool_calls, None);
    }

    #[test]
    fn parse_response_with_tool_calls() {
        let payload = json!({
            "id": "cmpl-456",
            "object": "chat.completion",
            "created": 1_234_567_890,
            "model": "gpt-3.5-turbo",
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 5,
                "total_tokens": 15
            },
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": null,
                        "tool_calls": [
                            {
                                "id": "call_123",
                                "type": "function",
                                "function": {
                                    "name": "delegate_agent",
                                    "arguments": "{\"agent\": \"worker\"}"
                                }
                            }
                        ]
                    },
                    "finish_reason": "tool_calls"
                }
            ]
        });
        let parser = OpenAiToolCallParser;
        let result = parser
            .parse_response(&serde_json::to_vec(&payload).unwrap())
            .unwrap();
        assert_eq!(result.content, None);
        let tcs = result.tool_calls.unwrap();
        assert_eq!(tcs.len(), 1);
        assert_eq!(tcs[0].id.as_str(), "call_123");
        assert_eq!(tcs[0].name, "delegate_agent");
        assert_eq!(tcs[0].arguments, json!({"agent": "worker"}));
    }

    #[test]
    fn parse_response_with_content_and_tool_calls() {
        let payload = json!({
            "id": "cmpl-789",
            "object": "chat.completion",
            "created": 1_234_567_890,
            "model": "gpt-3.5-turbo",
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 5,
                "total_tokens": 15
            },
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": "Let me delegate that.",
                        "tool_calls": [
                            {
                                "id": "call_456",
                                "type": "function",
                                "function": {
                                    "name": "delegate_agent",
                                    "arguments": "{}"
                                }
                            }
                        ]
                    },
                    "finish_reason": "tool_calls"
                }
            ]
        });
        let parser = OpenAiToolCallParser;
        let result = parser
            .parse_response(&serde_json::to_vec(&payload).unwrap())
            .unwrap();
        assert_eq!(result.content, Some("Let me delegate that.".into()));
        let tcs = result.tool_calls.unwrap();
        assert_eq!(tcs.len(), 1);
        assert_eq!(tcs[0].id.as_str(), "call_456");
        assert_eq!(tcs[0].name, "delegate_agent");
        assert_eq!(tcs[0].arguments, json!({}));
    }

    #[test]
    fn parse_response_invalid_json() {
        let parser = OpenAiToolCallParser;
        let result = parser.parse_response(b"not json");
        assert!(matches!(result, Err(ParseError::Json(_))));
    }

    #[test]
    fn parse_response_missing_choices() {
        let payload = json!({});
        let parser = OpenAiToolCallParser;
        let result = parser.parse_response(&serde_json::to_vec(&payload).unwrap());
        assert!(matches!(result, Err(ParseError::Json(_))));
    }

    #[test]
    fn serialize_conversation_user_only() {
        let parser = OpenAiToolCallParser;
        let messages = vec![Message::User {
            content: "hello".into(),
        }];
        let bytes = parser.serialize_conversation(&messages);
        let parsed: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed["messages"][0]["role"], "user");
        assert_eq!(parsed["messages"][0]["content"], "hello");
    }

    #[test]
    fn serialize_conversation_user_agent() {
        let parser = OpenAiToolCallParser;
        let messages = vec![
            Message::User {
                content: "hi".into(),
            },
            Message::Agent {
                content: Some("response".into()),
                tool_calls: None,
            },
        ];
        let bytes = parser.serialize_conversation(&messages);
        let parsed: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed["messages"][0]["role"], "user");
        assert_eq!(parsed["messages"][1]["role"], "assistant");
        assert_eq!(parsed["messages"][1]["content"], "response");
    }

    #[test]
    fn serialize_conversation_with_tool_calls() {
        let parser = OpenAiToolCallParser;
        let messages = vec![Message::Agent {
            content: None,
            tool_calls: Some(vec![ToolCall {
                id: "call_1".to_string().into(),
                name: "test".into(),
                arguments: json!({"arg": 1}),
            }]),
        }];
        let bytes = parser.serialize_conversation(&messages);
        let parsed: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed["messages"][0]["role"], "assistant");
        assert_eq!(parsed["messages"][0]["tool_calls"][0]["id"], "call_1");
        assert_eq!(
            parsed["messages"][0]["tool_calls"][0]["function"]["name"],
            "test"
        );
        assert_eq!(
            parsed["messages"][0]["tool_calls"][0]["function"]["arguments"],
            "{\"arg\":1}"
        );
    }
    #[test]
    fn parse_response_with_multiple_tool_calls() {
        let payload = json!({
            "id": "cmpl-multi",
            "object": "chat.completion",
            "created": 1_234_567_890,
            "model": "gpt-3.5-turbo",
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 5,
                "total_tokens": 15
            },
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": null,
                        "tool_calls": [
                            {
                                "id": "call_1",
                                "type": "function",
                                "function": {
                                    "name": "delegate_agent",
                                    "arguments": "{\"agent\": \"worker-a\"}"
                                }
                            },
                            {
                                "id": "call_2",
                                "type": "function",
                                "function": {
                                    "name": "delegate_agent",
                                    "arguments": "{\"agent\": \"worker-b\"}"
                                }
                            }
                        ]
                    },
                    "finish_reason": "tool_calls"
                }
            ]
        });
        let parser = OpenAiToolCallParser;
        let result = parser
            .parse_response(&serde_json::to_vec(&payload).unwrap())
            .unwrap();
        assert_eq!(result.content, None);
        let tcs = result.tool_calls.unwrap();
        assert_eq!(tcs.len(), 2);
        assert_eq!(tcs[0].id.as_str(), "call_1");
        assert_eq!(tcs[0].name, "delegate_agent");
        assert_eq!(tcs[0].arguments, json!({"agent": "worker-a"}));
        assert_eq!(tcs[1].id.as_str(), "call_2");
        assert_eq!(tcs[1].name, "delegate_agent");
        assert_eq!(tcs[1].arguments, json!({"agent": "worker-b"}));
    }

    #[test]
    fn parse_response_empty_choices() {
        let payload = json!({
            "id": "cmpl-empty",
            "object": "chat.completion",
            "created": 1_234_567_890,
            "model": "gpt-3.5-turbo",
            "choices": []
        });
        let parser = OpenAiToolCallParser;
        let result = parser.parse_response(&serde_json::to_vec(&payload).unwrap());
        assert!(
            matches!(result, Err(ParseError::InvalidStructure(_))),
            "empty choices should be InvalidStructure"
        );
    }
}
