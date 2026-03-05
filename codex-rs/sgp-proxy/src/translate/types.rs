use serde::Deserialize;
use serde::Serialize;

// ---------------------------------------------------------------------------
// JSON-RPC 2.0 envelope
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct JsonRpcRequest<T: Serialize> {
    pub jsonrpc: &'static str,
    pub id: String,
    pub method: String,
    pub params: T,
}

impl<T: Serialize> JsonRpcRequest<T> {
    pub fn new(id: impl Into<String>, method: impl Into<String>, params: T) -> Self {
        Self {
            jsonrpc: "2.0",
            id: id.into(),
            method: method.into(),
            params,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct JsonRpcResponse<T> {
    #[allow(dead_code)]
    pub jsonrpc: Option<String>,
    #[allow(dead_code)]
    pub id: Option<serde_json::Value>,
    pub result: Option<T>,
    pub error: Option<JsonRpcError>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
}

// ---------------------------------------------------------------------------
// task/create
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct CreateTaskParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Task {
    pub id: String,
    #[allow(dead_code)]
    #[serde(default)]
    pub name: Option<String>,
    #[allow(dead_code)]
    #[serde(default)]
    pub status: Option<String>,
}

// ---------------------------------------------------------------------------
// message/send
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct MessageSendParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_name: Option<String>,
    pub content: TaskMessageContent,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_params: Option<serde_json::Value>,
}

// ---------------------------------------------------------------------------
// Message content types (discriminated by "type" field)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TaskMessageContent {
    Text {
        author: String,
        content: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        format: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        style: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        attachments: Option<Vec<serde_json::Value>>,
    },
    ToolRequest {
        author: String,
        tool_call_id: String,
        name: String,
        /// Agentex expects arguments as a JSON object, not a string.
        arguments: serde_json::Value,
    },
    ToolResponse {
        author: String,
        tool_call_id: String,
        name: String,
        content: serde_json::Value,
    },
    Reasoning {
        author: String,
        summary: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content: Option<Vec<String>>,
    },
    Data {
        author: String,
        data: serde_json::Value,
    },
}

// ---------------------------------------------------------------------------
// Streaming response types (NDJSON lines from Agentex)
// ---------------------------------------------------------------------------

/// Each NDJSON line from the streaming endpoint is a JSON-RPC envelope whose
/// `result` field contains the actual `TaskMessageUpdate`.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TaskMessageUpdate {
    Start {
        #[serde(default)]
        content: Option<TaskMessageContent>,
        #[serde(default)]
        parent_task_message: Option<serde_json::Value>,
    },
    Delta {
        delta: StreamDelta,
        #[serde(default)]
        parent_task_message: Option<serde_json::Value>,
    },
    Full {
        content: TaskMessageContent,
        #[serde(default)]
        parent_task_message: Option<serde_json::Value>,
    },
    Done {
        #[serde(default)]
        content: Option<TaskMessageContent>,
        #[serde(default)]
        parent_task_message: Option<serde_json::Value>,
    },
    Error {
        #[serde(default)]
        message: String,
    },
}

/// Stream deltas discriminated by `type`. Field names match the actual
/// Agentex wire format discovered via live testing.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamDelta {
    /// Text content delta: `{"type":"text","text_delta":"..."}`
    Text {
        #[serde(default)]
        text_delta: Option<String>,
    },
    /// Reasoning content delta:
    /// `{"type":"reasoning_content","content_index":0,"content_delta":"..."}`
    ReasoningContent {
        #[serde(default)]
        content_index: u32,
        #[serde(default)]
        content_delta: Option<String>,
    },
    /// Reasoning summary delta:
    /// `{"type":"reasoning_summary","summary_index":0,"summary_delta":"..."}`
    ReasoningSummary {
        #[serde(default)]
        summary_index: u32,
        #[serde(default)]
        summary_delta: Option<String>,
    },
    /// Tool-call arguments delta:
    /// `{"type":"arguments","tool_call_id":"...","name":"...","arguments_delta":"..."}`
    Arguments {
        #[serde(default)]
        tool_call_id: Option<String>,
        #[serde(default)]
        name: Option<String>,
        #[serde(default)]
        arguments_delta: Option<String>,
    },
}

// ---------------------------------------------------------------------------
// Non-streaming message/send response
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Helper types for SSE writer (Codex-side reasoning format)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ReasoningSummaryEntry {
    pub text: String,
}

#[derive(Debug, Clone)]
pub struct ReasoningContentEntry {
    pub text: String,
}

// ---------------------------------------------------------------------------
// Non-streaming message/send response
// ---------------------------------------------------------------------------

/// The result of a non-streaming message/send call wraps the task plus content.
#[derive(Debug, Clone, Deserialize)]
pub struct MessageSendResult {
    #[serde(default)]
    pub task: Option<Task>,
    #[serde(default)]
    pub content: Option<TaskMessageContent>,
    /// Some responses return an array of content items.
    #[serde(default)]
    pub contents: Option<Vec<TaskMessageContent>>,
}
