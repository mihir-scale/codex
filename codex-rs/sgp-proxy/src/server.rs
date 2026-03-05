use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::get;
use axum::routing::post;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream;
use tokio_stream::wrappers::ReceiverStream;

use crate::config::TaskLifecycleMode;
use crate::error::ProxyError;
use crate::sse_writer::SseEvent;
use crate::state::ProxyState;
use crate::state::SessionContext;
use crate::translate::request::translate_request;
use crate::translate::response::ToolDeltaBuffer;
use crate::translate::response::translate_message_result;
use crate::translate::response::translate_stream_event;
use crate::translate::types::TaskMessageContent;

/// Build the Axum router.
pub fn build_router(state: Arc<ProxyState>) -> Router {
    Router::new()
        .route("/v1/responses", post(handle_responses))
        .route("/shutdown", get(handle_shutdown))
        .with_state(state)
}

async fn handle_shutdown(State(state): State<Arc<ProxyState>>) -> impl IntoResponse {
    if state.http_shutdown {
        tokio::spawn(async {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            std::process::exit(0);
        });
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

async fn handle_responses(
    State(state): State<Arc<ProxyState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    match handle_responses_inner(state, headers, body).await {
        Ok(response) => response,
        Err(err) => {
            eprintln!("sgp-proxy error: {err}");
            let event = SseEvent::response_failed("proxy_error", &err.to_string());
            let body_bytes = event.to_bytes();
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/event-stream")
                .header("cache-control", "no-cache")
                .body(Body::from(body_bytes))
                .unwrap_or_else(|_| {
                    Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Body::empty())
                        .unwrap_or_default()
                })
        }
    }
}

async fn handle_responses_inner(
    state: Arc<ProxyState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ProxyError> {
    let request_body: serde_json::Value = serde_json::from_slice(&body)
        .map_err(|e| ProxyError::RequestParse(format!("invalid JSON: {e}")))?;

    let wants_stream = request_body
        .get("stream")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(true);

    // Resolve session.
    let session_id = headers
        .get("x-session-id")
        .or_else(|| headers.get("session-id"))
        .and_then(|v| v.to_str().ok())
        .unwrap_or("default")
        .to_string();

    let (task_id, task_name, is_first_turn) = resolve_session(&state, &session_id).await?;

    // Translate request.
    let mut params = translate_request(
        &request_body,
        is_first_turn,
        task_id.as_deref(),
        task_name.as_deref(),
    )?;

    // Fill in tool names for ToolResponse content from session state.
    if let TaskMessageContent::ToolResponse {
        tool_call_id,
        name,
        ..
    } = &mut params.content
        && name.is_empty()
    {
        let sessions = state.sessions.read().await;
        if let Some(ctx) = sessions.get(&session_id)
            && let Some(resolved) = ctx.tool_name_by_call_id.get(tool_call_id)
        {
            name.clone_from(resolved);
        }
    }

    let response_id = format!("resp_{}", uuid::Uuid::new_v4());

    if wants_stream {
        build_streaming_response(state, session_id, params, response_id).await
    } else {
        build_non_streaming_response(state, session_id, params, response_id).await
    }
}

async fn resolve_session(
    state: &ProxyState,
    session_id: &str,
) -> Result<(Option<String>, Option<String>, bool), ProxyError> {
    match state.task_lifecycle {
        TaskLifecycleMode::PerSession => {
            // Check if session already exists.
            {
                let sessions = state.sessions.read().await;
                if let Some(ctx) = sessions.get(session_id) {
                    return Ok((Some(ctx.task_id.clone()), None, false));
                }
            }

            // Create a new task.
            let task_name = format!("codex-session-{session_id}");
            let task_id = state
                .client
                .task_create(&task_name)
                .await
                .map_err(ProxyError::Agentex)?;

            let ctx = SessionContext {
                task_id: task_id.clone(),
                tool_name_by_call_id: std::collections::HashMap::new(),
                is_first_turn: true,
            };

            state
                .sessions
                .write()
                .await
                .insert(session_id.to_string(), ctx);

            Ok((Some(task_id), None, true))
        }

        TaskLifecycleMode::PerRequest => {
            // Don't create a task up front; let message/send auto-create via
            // task_name.
            let task_name = format!("codex-request-{}", uuid::Uuid::new_v4());
            Ok((None, Some(task_name), true))
        }
    }
}

async fn build_streaming_response(
    state: Arc<ProxyState>,
    session_id: String,
    params: crate::translate::types::MessageSendParams,
    response_id: String,
) -> Result<Response, ProxyError> {
    let agentex_stream = {
        let stream = state
            .client
            .message_send_stream(params)
            .await
            .map_err(ProxyError::Agentex)?;
        Box::pin(stream)
    };

    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::convert::Infallible>>(64);

    let created_bytes = SseEvent::response_created(&response_id).to_bytes();
    let _ = tx.send(Ok(created_bytes)).await;

    let agent_tools = state.agent_tools.clone();
    let rid = response_id.clone();

    tokio::spawn(async move {
        let mut buffer = ToolDeltaBuffer::default();
        let mut item_index: u32 = 0;
        let mut agentex_stream = agentex_stream;

        while let Some(update_result) = agentex_stream.next().await {
            match update_result {
                Ok(update) => {
                    track_tool_names(&state, &session_id, &update).await;

                    let events = translate_stream_event(
                        &update,
                        &agent_tools,
                        &mut buffer,
                        &rid,
                        &mut item_index,
                    );

                    for event in events {
                        if tx.send(Ok(event.to_bytes())).await.is_err() {
                            return;
                        }
                    }
                }
                Err(e) => {
                    eprintln!("sgp-proxy stream error: {e}");
                    let event = SseEvent::response_failed("agentex_error", &e.to_string());
                    let _ = tx.send(Ok(event.to_bytes())).await;
                    return;
                }
            }
        }

        mark_not_first_turn(&state, &session_id).await;

        let completed = SseEvent::response_completed(&rid).to_bytes();
        let _ = tx.send(Ok(completed)).await;
    });

    let body_stream = ReceiverStream::new(rx);
    let response = Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/event-stream")
        .header("cache-control", "no-cache")
        .body(Body::from_stream(body_stream))
        .map_err(|e| ProxyError::Internal(format!("building response: {e}")))?;

    Ok(response)
}

async fn build_non_streaming_response(
    state: Arc<ProxyState>,
    session_id: String,
    params: crate::translate::types::MessageSendParams,
    response_id: String,
) -> Result<Response, ProxyError> {
    let result = state
        .client
        .message_send(params)
        .await
        .map_err(ProxyError::Agentex)?;

    // Track tool names from response.
    let items: Vec<&TaskMessageContent> = result
        .content
        .iter()
        .chain(result.contents.iter().flatten())
        .collect();
    for content in &items {
        if let TaskMessageContent::ToolRequest {
            tool_call_id,
            name,
            ..
        } = content
        {
            let mut sessions = state.sessions.write().await;
            if let Some(ctx) = sessions.get_mut(&session_id) {
                ctx.tool_name_by_call_id
                    .insert(tool_call_id.clone(), name.clone());
            }
        }
    }

    // If the result contains a task, update the session.
    if let Some(task) = &result.task {
        let mut sessions = state.sessions.write().await;
        if let Some(ctx) = sessions.get_mut(&session_id) {
            ctx.task_id.clone_from(&task.id);
        }
    }

    mark_not_first_turn(&state, &session_id).await;

    let mut events = vec![SseEvent::response_created(&response_id)];
    events.extend(translate_message_result(
        &result,
        &state.agent_tools,
        &response_id,
    ));
    events.push(SseEvent::response_completed(&response_id));

    let body_stream =
        stream::iter(events.into_iter().map(|e| Ok::<_, std::convert::Infallible>(e.to_bytes())));

    let response = Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/event-stream")
        .header("cache-control", "no-cache")
        .body(Body::from_stream(body_stream))
        .map_err(|e| ProxyError::Internal(format!("building response: {e}")))?;

    Ok(response)
}

async fn track_tool_names(
    state: &ProxyState,
    session_id: &str,
    update: &crate::translate::types::TaskMessageUpdate,
) {
    let content = match update {
        crate::translate::types::TaskMessageUpdate::Full { content, .. } => Some(content),
        crate::translate::types::TaskMessageUpdate::Done { content, .. } => content.as_ref(),
        _ => return,
    };

    if let Some(TaskMessageContent::ToolRequest {
        tool_call_id,
        name,
        ..
    }) = content
    {
        let mut sessions = state.sessions.write().await;
        if let Some(ctx) = sessions.get_mut(session_id) {
            ctx.tool_name_by_call_id
                .insert(tool_call_id.clone(), name.clone());
        }
    }
}

async fn mark_not_first_turn(state: &ProxyState, session_id: &str) {
    let mut sessions = state.sessions.write().await;
    if let Some(ctx) = sessions.get_mut(session_id) {
        ctx.is_first_turn = false;
    }
}
