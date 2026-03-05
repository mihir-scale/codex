use std::collections::HashMap;
use std::collections::HashSet;

use super::types::MessageSendResult;
use super::types::StreamDelta;
use super::types::TaskMessageContent;
use super::types::TaskMessageUpdate;
use crate::sse_writer::SseEvent;
use crate::tool_routing::ToolRoute;
use crate::tool_routing::route_tool;

/// State buffer for accumulating streamed tool-call deltas until the full
/// tool request is available.
#[derive(Debug, Default)]
pub struct ToolDeltaBuffer {
    /// tool_call_id -> (name, arguments_so_far)
    pub pending: HashMap<String, (String, String)>,
    /// The currently active item type, used to emit `output_item.added`
    /// before the first delta and `output_item.done` when the item ends.
    active_item: Option<ActiveItem>,
}

#[derive(Debug, Clone)]
enum ActiveItem {
    Message { id: String, text: String },
    Reasoning { id: String },
}


/// Translate a complete (non-streaming) Agentex response into SSE events.
pub fn translate_message_result(
    result: &MessageSendResult,
    agent_tools: &HashSet<String>,
    response_id: &str,
) -> Vec<SseEvent> {
    let mut events = Vec::new();
    let mut item_index: u32 = 0;

    // Collect content items from either the single `content` field or the
    // `contents` array.
    let mut items: Vec<&TaskMessageContent> = Vec::new();
    if let Some(c) = &result.content {
        items.push(c);
    }
    if let Some(cs) = &result.contents {
        items.extend(cs.iter());
    }

    for content in items {
        translate_content_item(content, agent_tools, response_id, &mut item_index, &mut events);
    }

    events
}

/// Translate a single streaming update into zero or more SSE events.
pub fn translate_stream_event(
    update: &TaskMessageUpdate,
    agent_tools: &HashSet<String>,
    buffer: &mut ToolDeltaBuffer,
    response_id: &str,
    item_index: &mut u32,
) -> Vec<SseEvent> {
    match update {
        TaskMessageUpdate::Start { content, .. } => {
            // Close any previous active item.
            let mut events = close_active_item(buffer, response_id);

            // Determine what kind of item is starting based on the content type.
            // The Agentex `start` event for text with reasoning deltas means a
            // reasoning item; a `start` followed by text deltas means a message.
            // We don't know yet which deltas will come, so we defer the `added`
            // event to when we see the first delta. However, if the start event
            // has content that's already non-empty text, emit immediately.
            if let Some(TaskMessageContent::Text { content: text, .. }) = content {
                if !text.is_empty() {
                    let item_id = format!("{response_id}_item_{item_index}");
                    events.push(SseEvent::output_item_added_message(&item_id));
                    buffer.active_item = Some(ActiveItem::Message {
                        id: item_id,
                        text: text.clone(),
                    });
                    *item_index += 1;
                }
            }
            // Otherwise, we'll detect the item type on the first delta.
            events
        }

        TaskMessageUpdate::Delta { delta, .. } => {
            let mut events = Vec::new();

            match delta {
                StreamDelta::Text { text_delta } => {
                    // Ensure we have an active message item.
                    if buffer.active_item.is_none()
                        || matches!(&buffer.active_item, Some(ActiveItem::Reasoning { .. }))
                    {
                        // Close reasoning if active, start message.
                        events.extend(close_active_item(buffer, response_id));
                        let item_id = format!("{response_id}_item_{item_index}");
                        events.push(SseEvent::output_item_added_message(&item_id));
                        buffer.active_item = Some(ActiveItem::Message {
                            id: item_id,
                            text: String::new(),
                        });
                        *item_index += 1;
                    }
                    if let Some(t) = text_delta {
                        if let Some(ActiveItem::Message { text, .. }) = &mut buffer.active_item {
                            text.push_str(t);
                        }
                        events.push(SseEvent::output_text_delta(t));
                    }
                }

                StreamDelta::ReasoningContent {
                    content_delta,
                    content_index,
                } => {
                    // Ensure we have an active reasoning item.
                    if buffer.active_item.is_none()
                        || matches!(&buffer.active_item, Some(ActiveItem::Message { .. }))
                    {
                        events.extend(close_active_item(buffer, response_id));
                        let item_id = format!("{response_id}_item_{item_index}");
                        events.push(SseEvent::output_item_added_reasoning(&item_id));
                        buffer.active_item = Some(ActiveItem::Reasoning { id: item_id });
                        *item_index += 1;
                    }
                    if let Some(t) = content_delta {
                        events.push(SseEvent::reasoning_text_delta(t, *content_index));
                    }
                }

                StreamDelta::ReasoningSummary {
                    summary_delta,
                    summary_index,
                } => {
                    // Ensure we have an active reasoning item.
                    if buffer.active_item.is_none()
                        || matches!(&buffer.active_item, Some(ActiveItem::Message { .. }))
                    {
                        events.extend(close_active_item(buffer, response_id));
                        let item_id = format!("{response_id}_item_{item_index}");
                        events.push(SseEvent::output_item_added_reasoning(&item_id));
                        buffer.active_item = Some(ActiveItem::Reasoning { id: item_id });
                        *item_index += 1;
                    }
                    if let Some(t) = summary_delta {
                        events.push(SseEvent::reasoning_summary_text_delta(t, *summary_index));
                    }
                }

                StreamDelta::Arguments {
                    tool_call_id,
                    name,
                    arguments_delta,
                } => {
                    if let Some(call_id) = tool_call_id {
                        let entry = buffer
                            .pending
                            .entry(call_id.clone())
                            .or_insert_with(|| (String::new(), String::new()));
                        if let Some(n) = name {
                            entry.0.clone_from(n);
                        }
                        if let Some(args) = arguments_delta {
                            entry.1.push_str(args);
                        }
                    }
                }
            }

            events
        }

        TaskMessageUpdate::Full { content, .. } => {
            let mut events = close_active_item(buffer, response_id);
            let mut item_idx_local = *item_index;
            translate_content_item(
                content,
                agent_tools,
                response_id,
                &mut item_idx_local,
                &mut events,
            );
            *item_index = item_idx_local;
            events
        }

        TaskMessageUpdate::Done { content, .. } => {
            let mut events = Vec::new();

            // Emit done for any active item.
            events.extend(close_active_item(buffer, response_id));

            // Done may include content.
            if let Some(c) = content {
                let mut item_idx_local = *item_index;
                translate_content_item(
                    c,
                    agent_tools,
                    response_id,
                    &mut item_idx_local,
                    &mut events,
                );
                *item_index = item_idx_local;
            }

            // Flush any remaining buffered tool-call deltas on Done.
            for (call_id, (name, arguments)) in buffer.pending.drain() {
                match route_tool(&name, agent_tools) {
                    ToolRoute::CodexLocal => {
                        events.push(SseEvent::output_item_done_function_call(
                            &call_id,
                            &name,
                            &arguments,
                        ));
                        *item_index += 1;
                    }
                    ToolRoute::Agent => {}
                }
            }

            events
        }

        TaskMessageUpdate::Error { message } => {
            vec![SseEvent::response_failed("proxy_error", message)]
        }
    }
}

/// Close the currently active item by emitting an `output_item.done` event.
fn close_active_item(buffer: &mut ToolDeltaBuffer, _response_id: &str) -> Vec<SseEvent> {
    let mut events = Vec::new();

    if let Some(active) = buffer.active_item.take() {
        match active {
            ActiveItem::Message { id, text } => {
                events.push(SseEvent::output_item_done_message(&id, "assistant", &text));
            }
            ActiveItem::Reasoning { id } => {
                // Emit a reasoning done with empty summary/content — the deltas
                // already delivered the actual content incrementally.
                events.push(SseEvent::output_item_done_reasoning(&id, &[], &[]));
            }
        }
    }

    events
}

fn translate_content_item(
    content: &TaskMessageContent,
    agent_tools: &HashSet<String>,
    response_id: &str,
    item_index: &mut u32,
    events: &mut Vec<SseEvent>,
) {
    match content {
        TaskMessageContent::Text {
            content: text,
            author,
            ..
        } => {
            if author == "agent" || author == "assistant" {
                let item_id = format!("{response_id}_item_{item_index}");
                events.push(SseEvent::output_item_done_message(&item_id, "assistant", text));
                *item_index += 1;
            }
        }

        TaskMessageContent::ToolRequest {
            tool_call_id,
            name,
            arguments,
            ..
        } => match route_tool(name, agent_tools) {
            ToolRoute::CodexLocal => {
                let args_str = serde_json::to_string(arguments).unwrap_or_default();
                events.push(SseEvent::output_item_done_function_call(
                    tool_call_id,
                    name,
                    &args_str,
                ));
                *item_index += 1;
            }
            ToolRoute::Agent => {}
        },

        TaskMessageContent::Reasoning {
            summary, content, ..
        } => {
            let item_id = format!("{response_id}_item_{item_index}");
            let summary_entries: Vec<crate::translate::types::ReasoningSummaryEntry> = summary
                .iter()
                .map(|s| crate::translate::types::ReasoningSummaryEntry { text: s.clone() })
                .collect();
            let content_entries: Vec<crate::translate::types::ReasoningContentEntry> = content
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|c| crate::translate::types::ReasoningContentEntry { text: c.clone() })
                .collect();
            events.push(SseEvent::output_item_done_reasoning(
                &item_id,
                &summary_entries,
                &content_entries,
            ));
            *item_index += 1;
        }

        TaskMessageContent::ToolResponse { .. } | TaskMessageContent::Data { .. } => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_translate_text_content() {
        let result = MessageSendResult {
            task: None,
            content: Some(TaskMessageContent::Text {
                author: "agent".to_string(),
                content: "Hello!".to_string(),
                format: None,
                style: None,
                attachments: None,
            }),
            contents: None,
        };

        let events = translate_message_result(&result, &HashSet::new(), "resp_1");
        assert_eq!(events.len(), 1);
        let data = events[0].data_json();
        assert_eq!(data["type"], "response.output_item.done");
        assert_eq!(data["item"]["content"][0]["text"], "Hello!");
    }

    #[test]
    fn test_translate_tool_request_codex_local() {
        let result = MessageSendResult {
            task: None,
            content: Some(TaskMessageContent::ToolRequest {
                author: "agent".to_string(),
                tool_call_id: "call_1".to_string(),
                name: "read_file".to_string(),
                arguments: json!({"path": "/tmp"}),
            }),
            contents: None,
        };

        let events = translate_message_result(&result, &HashSet::new(), "resp_1");
        assert_eq!(events.len(), 1);
        let data = events[0].data_json();
        assert_eq!(data["item"]["type"], "function_call");
        assert_eq!(data["item"]["call_id"], "call_1");
    }

    #[test]
    fn test_agent_tool_suppressed() {
        let mut agent_tools = HashSet::new();
        agent_tools.insert("search".to_string());

        let result = MessageSendResult {
            task: None,
            content: Some(TaskMessageContent::ToolRequest {
                author: "agent".to_string(),
                tool_call_id: "call_1".to_string(),
                name: "search".to_string(),
                arguments: json!({}),
            }),
            contents: None,
        };

        let events = translate_message_result(&result, &agent_tools, "resp_1");
        assert_eq!(events.len(), 0);
    }

    #[test]
    fn test_stream_text_delta_emits_added_first() {
        let update = TaskMessageUpdate::Delta {
            delta: StreamDelta::Text {
                text_delta: Some("Hello".to_string()),
            },
            parent_task_message: None,
        };

        let mut buffer = ToolDeltaBuffer::default();
        let mut idx = 0;
        let events =
            translate_stream_event(&update, &HashSet::new(), &mut buffer, "resp_1", &mut idx);
        // Should emit: output_item.added + output_text.delta
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].data_json()["type"], "response.output_item.added");
        assert_eq!(events[0].data_json()["item"]["type"], "message");
        assert_eq!(events[1].data_json()["type"], "response.output_text.delta");
        assert_eq!(events[1].data_json()["delta"], "Hello");
    }

    #[test]
    fn test_stream_reasoning_delta_emits_added_first() {
        let update = TaskMessageUpdate::Delta {
            delta: StreamDelta::ReasoningContent {
                content_index: 0,
                content_delta: Some("thinking".to_string()),
            },
            parent_task_message: None,
        };

        let mut buffer = ToolDeltaBuffer::default();
        let mut idx = 0;
        let events =
            translate_stream_event(&update, &HashSet::new(), &mut buffer, "resp_1", &mut idx);
        // Should emit: output_item.added (reasoning) + reasoning_text.delta
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].data_json()["type"], "response.output_item.added");
        assert_eq!(events[0].data_json()["item"]["type"], "reasoning");
        assert_eq!(events[1].data_json()["type"], "response.reasoning_text.delta");
        assert_eq!(events[1].data_json()["delta"], "thinking");
    }

    #[test]
    fn test_stream_reasoning_then_text_closes_previous() {
        let mut buffer = ToolDeltaBuffer::default();
        let mut idx = 0;

        // First: reasoning delta
        let events = translate_stream_event(
            &TaskMessageUpdate::Delta {
                delta: StreamDelta::ReasoningContent {
                    content_index: 0,
                    content_delta: Some("think".to_string()),
                },
                parent_task_message: None,
            },
            &HashSet::new(),
            &mut buffer,
            "resp_1",
            &mut idx,
        );
        assert_eq!(events.len(), 2); // added + delta

        // Second: text delta — should close reasoning, open message
        let events = translate_stream_event(
            &TaskMessageUpdate::Delta {
                delta: StreamDelta::Text {
                    text_delta: Some("Hello".to_string()),
                },
                parent_task_message: None,
            },
            &HashSet::new(),
            &mut buffer,
            "resp_1",
            &mut idx,
        );
        // reasoning.done + message.added + text.delta
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].data_json()["type"], "response.output_item.done");
        assert_eq!(events[0].data_json()["item"]["type"], "reasoning");
        assert_eq!(events[1].data_json()["type"], "response.output_item.added");
        assert_eq!(events[1].data_json()["item"]["type"], "message");
        assert_eq!(events[2].data_json()["type"], "response.output_text.delta");
    }

    #[test]
    fn test_stream_done_closes_active() {
        let mut buffer = ToolDeltaBuffer::default();
        let mut idx = 0;

        // Start text
        translate_stream_event(
            &TaskMessageUpdate::Delta {
                delta: StreamDelta::Text {
                    text_delta: Some("hi".to_string()),
                },
                parent_task_message: None,
            },
            &HashSet::new(),
            &mut buffer,
            "resp_1",
            &mut idx,
        );

        // Done
        let events = translate_stream_event(
            &TaskMessageUpdate::Done {
                content: None,
                parent_task_message: None,
            },
            &HashSet::new(),
            &mut buffer,
            "resp_1",
            &mut idx,
        );
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data_json()["type"], "response.output_item.done");
        assert_eq!(events[0].data_json()["item"]["content"][0]["text"], "hi");
    }
}
