use futures::Stream;
use futures::StreamExt;
use reqwest::Client;
use reqwest::header::HeaderName;
use reqwest::header::HeaderValue;
use tokio_stream::wrappers::ReceiverStream;

use crate::error::AgentexError;
use crate::translate::types::CreateTaskParams;
use crate::translate::types::JsonRpcRequest;
use crate::translate::types::JsonRpcResponse;
use crate::translate::types::MessageSendParams;
use crate::translate::types::MessageSendResult;
use crate::translate::types::Task;
use crate::translate::types::TaskMessageUpdate;

static X_API_KEY: HeaderName = HeaderName::from_static("x-api-key");
static X_SELECTED_ACCOUNT_ID: HeaderName = HeaderName::from_static("x-selected-account-id");

/// HTTP client for Agentex JSON-RPC endpoints.
pub struct AgentexClient {
    client: Client,
    base_url: String,
    agent_id: String,
    /// The raw API key for the `x-api-key` header.
    api_key: &'static str,
    /// Optional account ID for the `x-selected-account-id` header.
    account_id: Option<&'static str>,
}

impl AgentexClient {
    pub fn new(
        base_url: String,
        agent_id: String,
        auth_header: &'static str,
        account_id: Option<String>,
    ) -> Self {
        let client = Client::builder()
            .build()
            .unwrap_or_default();

        // auth_header comes in as "Bearer <key>" from stdin — strip the
        // prefix to get the raw key for the x-api-key header.
        let api_key_str = auth_header
            .strip_prefix("Bearer ")
            .unwrap_or(auth_header);
        let api_key: &'static str = String::from(api_key_str).leak();

        let account_id: Option<&'static str> =
            account_id.map(|s| &*Box::leak(s.into_boxed_str()));

        Self {
            client,
            base_url,
            agent_id,
            api_key,
            account_id,
        }
    }

    fn rpc_url(&self) -> String {
        format!(
            "{}/agents/name/{}/rpc",
            self.base_url.trim_end_matches('/'),
            self.agent_id,
        )
    }

    fn api_key_value(&self) -> HeaderValue {
        let mut v = HeaderValue::from_static(self.api_key);
        v.set_sensitive(true);
        v
    }

    /// Apply auth headers (`x-api-key` and optionally `x-selected-account-id`)
    /// to a request builder.
    fn authed(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let req = req.header(X_API_KEY.clone(), self.api_key_value());
        if let Some(acct) = self.account_id {
            req.header(X_SELECTED_ACCOUNT_ID.clone(), HeaderValue::from_static(acct))
        } else {
            req
        }
    }

    /// Create a new task on the Agentex agent.
    pub async fn task_create(
        &self,
        name: &str,
    ) -> Result<String, AgentexError> {
        let rpc = JsonRpcRequest::new(
            uuid::Uuid::new_v4().to_string(),
            "task/create",
            CreateTaskParams {
                name: Some(name.to_string()),
                params: None,
            },
        );

        let resp = self
            .authed(self.client.post(self.rpc_url()))
            .json(&rpc)
            .send()
            .await?;

        let body: JsonRpcResponse<Task> = resp.json().await?;

        if let Some(err) = body.error {
            return Err(AgentexError::Rpc {
                code: err.code,
                message: err.message,
            });
        }

        body.result
            .map(|t| t.id)
            .ok_or_else(|| AgentexError::Parse("missing result in task/create response".into()))
    }

    /// Send a message and receive the full response (non-streaming).
    pub async fn message_send(
        &self,
        params: MessageSendParams,
    ) -> Result<MessageSendResult, AgentexError> {
        let rpc = JsonRpcRequest::new(
            uuid::Uuid::new_v4().to_string(),
            "message/send",
            params,
        );

        let resp = self
            .authed(self.client.post(self.rpc_url()))
            .json(&rpc)
            .send()
            .await?;

        let body: JsonRpcResponse<MessageSendResult> = resp.json().await?;

        if let Some(err) = body.error {
            return Err(AgentexError::Rpc {
                code: err.code,
                message: err.message,
            });
        }

        body.result.ok_or_else(|| {
            AgentexError::Parse("missing result in message/send response".into())
        })
    }

    /// Send a message and receive a stream of `TaskMessageUpdate` events.
    ///
    /// The implementation handles both NDJSON and SSE-style `data:` prefixed
    /// lines from the response body, accommodating either Agentex wire format.
    pub async fn message_send_stream(
        &self,
        params: MessageSendParams,
    ) -> Result<impl Stream<Item = Result<TaskMessageUpdate, AgentexError>> + Send + 'static, AgentexError> {
        let rpc = JsonRpcRequest::new(
            uuid::Uuid::new_v4().to_string(),
            "message/send",
            params,
        );

        let resp = self
            .authed(self.client.post(self.rpc_url()))
            .header("accept", "application/x-ndjson")
            .json(&rpc)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(AgentexError::Stream(format!(
                "HTTP {status}: {text}"
            )));
        }

        let mut byte_stream = resp.bytes_stream();
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<TaskMessageUpdate, AgentexError>>(64);

        tokio::spawn(async move {
            let mut line_buf = String::new();

            while let Some(chunk_result) = byte_stream.next().await {
                let chunk = match chunk_result {
                    Ok(c) => c,
                    Err(e) => {
                        let _ = tx.send(Err(AgentexError::Http(e))).await;
                        return;
                    }
                };

                let text = match std::str::from_utf8(&chunk) {
                    Ok(t) => t,
                    Err(e) => {
                        let _ = tx
                            .send(Err(AgentexError::Parse(format!("invalid UTF-8: {e}"))))
                            .await;
                        return;
                    }
                };

                line_buf.push_str(text);

                // Process complete lines.
                while let Some(newline_pos) = line_buf.find('\n') {
                    let line: String = line_buf.drain(..=newline_pos).collect();
                    let line = line.trim();

                    if line.is_empty() {
                        continue;
                    }

                    // Handle SSE `data: {...}` or raw NDJSON `{...}`.
                    let json_str = line.strip_prefix("data:").map(str::trim).unwrap_or(line);

                    // Skip SSE event-type lines like `event: ...`
                    if json_str.starts_with("event:") {
                        continue;
                    }

                    // Each NDJSON line is a JSON-RPC envelope:
                    // {"jsonrpc":"2.0","result":<TaskMessageUpdate>,...}
                    // Try to unwrap the envelope first; fall back to parsing
                    // the line directly as a TaskMessageUpdate.
                    let parsed = serde_json::from_str::<serde_json::Value>(json_str)
                        .ok()
                        .and_then(|val| {
                            // If there's a "result" key, extract it.
                            if let Some(result) = val.get("result") {
                                serde_json::from_value::<TaskMessageUpdate>(result.clone()).ok()
                            } else {
                                // Try parsing the whole value directly.
                                serde_json::from_value::<TaskMessageUpdate>(val).ok()
                            }
                        });

                    match parsed {
                        Some(update) => {
                            if tx.send(Ok(update)).await.is_err() {
                                return;
                            }
                        }
                        None => {
                            tracing::debug!("skipping unparseable stream line: {json_str}");
                        }
                    }
                }
            }

            // Process any remaining data in the buffer.
            let remaining = line_buf.trim();
            if !remaining.is_empty() {
                let json_str = remaining
                    .strip_prefix("data:")
                    .map(str::trim)
                    .unwrap_or(remaining);
                let parsed = serde_json::from_str::<serde_json::Value>(json_str)
                    .ok()
                    .and_then(|val| {
                        if let Some(result) = val.get("result") {
                            serde_json::from_value::<TaskMessageUpdate>(result.clone()).ok()
                        } else {
                            serde_json::from_value::<TaskMessageUpdate>(val).ok()
                        }
                    });
                if let Some(update) = parsed {
                    let _ = tx.send(Ok(update)).await;
                }
            }
        });

        Ok(ReceiverStream::new(rx))
    }
}
