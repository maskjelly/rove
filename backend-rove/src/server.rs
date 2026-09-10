use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use backend_rove::{MAX_BYTES, MAX_MESSAGES};
use serde::Deserialize;
use serde_json::json;
use std::{convert::Infallible, sync::Arc, time::Duration};
use tokio::sync::Semaphore;

#[derive(Clone)]
struct AppState {
    http: reqwest::Client,
    key: Option<String>,
    model: String,
    endpoint: String,
    slots: Arc<Semaphore>,
}

#[derive(Deserialize)]
struct Message {
    role: String,
    content: String,
}

#[derive(Deserialize)]
struct ChatRequest {
    messages: Vec<Message>,
}

fn error(status: StatusCode, message: &str) -> Response {
    (status, Json(json!({"error": message}))).into_response()
}

async fn chat(State(state): State<AppState>, Json(request): Json<ChatRequest>) -> Response {
    let Some(key) = &state.key else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "OPENAI_API_KEY is not configured on the server.",
        );
    };
    if request.messages.is_empty()
        || request.messages.len() > MAX_MESSAGES
        || request.messages.iter().any(|m| {
            !matches!(m.role.as_str(), "user" | "assistant") || m.content.trim().is_empty()
        })
        || request
            .messages
            .iter()
            .map(|m| m.content.len())
            .sum::<usize>()
            > MAX_BYTES
        || request.messages.last().map(|m| m.role.as_str()) != Some("user")
    {
        return error(
            StatusCode::BAD_REQUEST,
            "Send 1–21 user/assistant messages, up to 24 KB, ending with a user message.",
        );
    }
    let Ok(permit) = state.slots.clone().try_acquire_owned() else {
        return (
            [(header::RETRY_AFTER, "2")],
            error(
                StatusCode::TOO_MANY_REQUESTS,
                "The server is busy. Try again shortly.",
            ),
        )
            .into_response();
    };
    let input: Vec<_> = request
        .messages
        .iter()
        .map(|m| json!({"role": m.role, "content": m.content}))
        .collect();
    let upstream = state
        .http
        .post(&state.endpoint)
        .bearer_auth(key)
        .json(&json!({
            "model": state.model,
            "instructions": "You are Rove, a helpful assistant. Be concise and practical. You have no shell or file tools; never claim to have executed commands or edited files.",
            "input": input,
            "stream": true,
            "store": false,
            "reasoning": {"effort": "low", "summary": "auto"},
            "max_output_tokens": 2048
        }))
        .send()
        .await;
    let mut upstream = match upstream {
        Ok(response) => response,
        Err(_) => {
            return error(
                StatusCode::BAD_GATEWAY,
                "Cannot reach OpenAI. Please retry.",
            );
        }
    };
    if !upstream.status().is_success() {
        let status = upstream.status().as_u16();
        // Do not forward upstream error bodies: authentication errors can include key fragments.
        let message = match status {
            401 => "OpenAI rejected the server API key.",
            403 => {
                "OpenAI denied access. Check model access and organization verification for reasoning summaries."
            }
            429 => "OpenAI quota or rate limit reached. Check API billing or retry later.",
            400 | 404 => {
                "OpenAI rejected the model or request settings. Check server configuration."
            }
            _ => "OpenAI returned an error. Please retry.",
        };
        return error(StatusCode::BAD_GATEWAY, message);
    }
    let stream = async_stream::stream! {
        // Held until completion or client disconnect; dropping this stream closes upstream.
        let _permit = permit;
        // Idle keep-alive: without traffic, NATs and middleboxes can kill a
        // slow reasoning stream before the first token arrives.
        let mut heartbeat = tokio::time::interval(Duration::from_secs(15));
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        heartbeat.tick().await;
        loop {
            tokio::select! {
                _ = heartbeat.tick() => yield Ok::<_, Infallible>(axum::body::Bytes::from_static(b": ping\n\n")),
                chunk = upstream.chunk() => match chunk {
                    Ok(Some(bytes)) => yield Ok(bytes),
                    Ok(None) => break,
                    Err(_) => {
                        yield Ok(axum::body::Bytes::from_static(b"event: error\ndata: {\"type\":\"error\",\"message\":\"Upstream stream interrupted; please retry.\"}\n\n"));
                        break;
                    }
                },
            }
        }
    };
    (
        [
            (header::CONTENT_TYPE, "text/event-stream"),
            (header::CACHE_CONTROL, "no-cache, no-transform"),
        ],
        Body::from_stream(stream),
    )
        .into_response()
}

#[tokio::main]
async fn main() {
    let state = AppState {
        http: reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(15))
            .timeout(Duration::from_secs(180))
            .build()
            .expect("HTTP client"),
        key: std::env::var("OPENAI_API_KEY")
            .ok()
            .filter(|s| !s.is_empty()),
        model: std::env::var("OPENAI_MODEL").unwrap_or_else(|_| "gpt-5-mini".into()),
        endpoint: format!(
            "{}/responses",
            std::env::var("OPENAI_BASE_URL")
                .unwrap_or_else(|_| "https://api.openai.com/v1".into())
                .trim_end_matches('/')
        ),
        slots: Arc::new(Semaphore::new(8)),
    };
    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/chat", post(chat).layer(DefaultBodyLimit::max(64 * 1024)))
        .with_state(state);
    let addr = std::env::var("ROVE_BIND").unwrap_or_else(|_| "127.0.0.1:3000".into());
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("bind server");
    println!("Rove listening on {addr}");
    axum::serve(listener, app.into_make_service())
        .await
        .expect("serve requests");
}
