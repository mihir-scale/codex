use serde_json::Value;

use super::types::MessageSendParams;
use super::types::TaskMessageContent;
use crate::error::ProxyError;

/// Translate a Responses API request body into Agentex `MessageSendParams`.
///
/// Agentex `message/send` accepts a single `content` item per call, so we
/// build a composite text from the full conversation history. The last user
/// message is used as the primary content; earlier messages and instructions
/// are folded into a context preamble.
pub fn translate_request(
    body: &Value,
    is_first_turn: bool,
    task_id: Option<&str>,
    task_name: Option<&str>,
) -> Result<MessageSendParams, ProxyError> {
    let input = body
        .get("input")
        .and_then(Value::as_array)
        .ok_or_else(|| ProxyError::RequestParse("missing or invalid 'input' array".into()))?;

    let wants_stream = body
        .get("stream")
        .and_then(Value::as_bool)
        .unwrap_or(true);

    // Find the last user-role message to use as the primary content.
    // Also collect any function_call_output items — those need to be sent as
    // tool_response content instead.
    let mut last_user_text: Option<String> = None;
    let mut last_tool_response: Option<TaskMessageContent> = None;
    let mut context_parts: Vec<String> = Vec::new();

    // Inject instructions as context on first turn.
    if is_first_turn
        && let Some(instructions) = body.get("instructions").and_then(Value::as_str)
        && !instructions.is_empty()
    {
        context_parts.push(format!("[System Instructions]\n{instructions}"));
    }

    for item in input {
        let item_type = item.get("type").and_then(Value::as_str).unwrap_or("");

        match item_type {
            "message" => {
                let role = item.get("role").and_then(Value::as_str).unwrap_or("user");

                if let Some(content_arr) = item.get("content").and_then(Value::as_array) {
                    for c in content_arr {
                        let c_type = c.get("type").and_then(Value::as_str).unwrap_or("");
                        if matches!(c_type, "input_text" | "output_text")
                            && let Some(text) = c.get("text").and_then(Value::as_str)
                        {
                            if role == "user" || role == "developer" {
                                if let Some(prev) = last_user_text.take() {
                                    context_parts.push(format!("[User]\n{prev}"));
                                }
                                last_user_text = Some(text.to_string());
                            } else {
                                context_parts.push(format!("[Assistant]\n{text}"));
                            }
                        }
                    }
                }
            }

            "function_call" => {
                let name = item
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown");
                let call_id = item
                    .get("call_id")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let arguments = item
                    .get("arguments")
                    .and_then(Value::as_str)
                    .unwrap_or("{}");
                context_parts.push(format!(
                    "[Tool Call: {name} ({call_id})]\n{arguments}"
                ));
            }

            "function_call_output" => {
                let call_id = item
                    .get("call_id")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();

                let output = if let Some(s) = item.get("output").and_then(Value::as_str) {
                    s.to_string()
                } else if let Some(obj) = item.get("output") {
                    serde_json::to_string(obj).unwrap_or_default()
                } else {
                    String::new()
                };

                // The most recent function_call_output becomes the primary
                // content (as a tool_response), since the agent needs to
                // continue from it.
                last_tool_response = Some(TaskMessageContent::ToolResponse {
                    author: "user".to_string(),
                    tool_call_id: call_id,
                    name: String::new(), // resolved by caller from session state
                    content: serde_json::Value::String(output),
                });
            }

            "reasoning" => {
                if let Some(summary_arr) = item.get("summary").and_then(Value::as_array) {
                    let texts: Vec<&str> = summary_arr
                        .iter()
                        .filter_map(|s| s.get("text").and_then(Value::as_str))
                        .collect();
                    if !texts.is_empty() {
                        context_parts
                            .push(format!("[Reasoning]\n{}", texts.join("\n")));
                    }
                }
            }

            _ => {} // Skip local_shell_call, web_search_call, etc.
        }
    }

    // If we have a tool_response, use that as the primary content (the agent
    // is waiting for tool results). Otherwise, use the last user text.
    let content = if let Some(tool_resp) = last_tool_response {
        tool_resp
    } else {
        let text = last_user_text.unwrap_or_default();
        let full_text = if context_parts.is_empty() {
            text
        } else {
            // Prepend conversation context before the latest user message.
            context_parts.push(format!("[User]\n{text}"));
            context_parts.join("\n\n")
        };

        TaskMessageContent::Text {
            author: "user".to_string(),
            content: full_text,
            format: Some("plain".to_string()),
            style: None,
            attachments: Some(vec![]),
        }
    };

    Ok(MessageSendParams {
        task_id: task_id.map(String::from),
        task_name: task_name.map(String::from),
        content,
        stream: Some(wants_stream),
        task_params: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_translate_simple_user_message() {
        let body = json!({
            "instructions": "You are a helpful assistant.",
            "input": [
                {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": "Hello"}]
                }
            ],
            "stream": true
        });

        let params = translate_request(&body, true, Some("task-1"), None).unwrap();
        assert_eq!(params.task_id.as_deref(), Some("task-1"));
        assert_eq!(params.stream, Some(true));
        match &params.content {
            TaskMessageContent::Text {
                content, author, ..
            } => {
                assert_eq!(author, "user");
                assert!(content.contains("Hello"));
                assert!(content.contains("[System Instructions]"));
            }
            _ => panic!("expected Text content"),
        }
    }

    #[test]
    fn test_translate_no_instructions_on_subsequent_turn() {
        let body = json!({
            "instructions": "You are a helpful assistant.",
            "input": [
                {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": "Hello"}]
                }
            ]
        });

        let params = translate_request(&body, false, Some("task-1"), None).unwrap();
        match &params.content {
            TaskMessageContent::Text { content, .. } => {
                assert!(!content.contains("[System Instructions]"));
                assert_eq!(content, "Hello");
            }
            _ => panic!("expected Text content"),
        }
    }

    #[test]
    fn test_translate_function_call_output() {
        let body = json!({
            "input": [
                {
                    "type": "function_call",
                    "call_id": "call_1",
                    "name": "read_file",
                    "arguments": "{\"path\":\"/tmp/foo\"}"
                },
                {
                    "type": "function_call_output",
                    "call_id": "call_1",
                    "output": "file contents here"
                }
            ]
        });

        let params = translate_request(&body, false, Some("task-1"), None).unwrap();
        match &params.content {
            TaskMessageContent::ToolResponse {
                tool_call_id,
                content,
                ..
            } => {
                assert_eq!(tool_call_id, "call_1");
                assert_eq!(content, &serde_json::Value::String("file contents here".into()));
            }
            _ => panic!("expected ToolResponse content"),
        }
    }

    #[test]
    fn test_translate_with_task_name() {
        let body = json!({
            "input": [
                {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": "hi"}]
                }
            ]
        });

        let params = translate_request(&body, true, None, Some("my-task")).unwrap();
        assert!(params.task_id.is_none());
        assert_eq!(params.task_name.as_deref(), Some("my-task"));
    }
}
