mod voice;
use axum::{
    Json, Router,
    body::Body,
    extract::{ConnectInfo, DefaultBodyLimit, State},
    http::{StatusCode, header},
    response::{Html, IntoResponse, Response, Sse, sse::Event},
    routing::{get, post},
};
use chrono::Utc;
use serde::Deserialize;
use serde_json::json;
use std::{convert::Infallible, net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::Semaphore;
use tokio_stream::{Stream, StreamExt, wrappers::IntervalStream};

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

async fn chat(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Json(request): Json<ChatRequest>,
) -> Response {
    // SSH and the public HTTPS reverse proxy connect from loopback.
    // Direct plain-HTTP chat on port 3000 stays disabled; never trust forwarded headers.
    if !peer.ip().is_loopback() {
        return error(
            StatusCode::FORBIDDEN,
            "Open https://45.196.196.251 or connect through the SSH client.",
        );
    }
    let Some(key) = &state.key else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "OPENAI_API_KEY is not configured on the server.",
        );
    };
    if request.messages.is_empty()
        || request.messages.len() > 21
        || request.messages.iter().any(|m| {
            !matches!(m.role.as_str(), "user" | "assistant") || m.content.trim().is_empty()
        })
        || request
            .messages
            .iter()
            .map(|m| m.content.len())
            .sum::<usize>()
            > 24_000
        || request.messages.last().map(|m| m.role.as_str()) != Some("user")
    {
        return error(
            StatusCode::BAD_REQUEST,
            "Send 1–21 user/assistant messages, up to 24 KB, ending with a user message.",
        );
    }
    let Ok(permit) = state.slots.clone().try_acquire_owned() else {
        return error(
            StatusCode::TOO_MANY_REQUESTS,
            "The server is busy. Try again shortly.",
        );
    };
    let input: Vec<_> = request
        .messages
        .iter()
        .map(|m| json!({"role": m.role, "content": m.content}))
        .collect();
    let upstream = state.http.post(&state.endpoint)
        .bearer_auth(key)
        .json(&json!({
            "model": state.model,
            "instructions": "You are Rove, a helpful assistant. Be concise and practical. You can discuss code, but you have no shell or file tools; never claim to have executed commands or edited files.",
            "input": input,
            "stream": true,
            "store": false,
            "reasoning": {"effort": "low", "summary": "auto"},
            "max_output_tokens": 2048
        }))
        .send().await;
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
        loop {
            match upstream.chunk().await {
                Ok(Some(bytes)) => yield Ok::<_, Infallible>(bytes),
                Ok(None) => break,
                Err(_) => {
                    yield Ok(axum::body::Bytes::from_static(b"event: error\ndata: {\"type\":\"error\",\"message\":\"Upstream stream interrupted; please retry.\"}\n\n"));
                    break;
                }
            }
        }
    };
    (
        [
            (header::CONTENT_TYPE, "text/event-stream"),
            (header::CACHE_CONTROL, "no-cache, no-transform"),
            (header::HeaderName::from_static("x-accel-buffering"), "no"),
        ],
        Body::from_stream(stream),
    )
        .into_response()
}

async fn sse_event_handler() -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let stream = IntervalStream::new(tokio::time::interval(Duration::from_secs(5))).map(|_| {
        Ok(Event::default().data(format!(
            "New data from the server at: {}",
            Utc::now().format("%d/%m/%Y %H:%M:%S")
        )))
    });
    Sse::new(stream)
        .keep_alive(axum::response::sse::KeepAlive::new().interval(Duration::from_secs(5)))
}

/// Frontend bundle with content-hash cache busting.
///
/// `index.html` and `app.js` carry a `{{ASSET_HASH}}` placeholder that is
/// stamped with a hash of the bundle at startup. HTML is served `no-store`
/// so browsers always pick up the newest asset URLs; the hashed JS/CSS
/// URLs are `immutable` and safe to cache forever.
fn frontend_app(state: AppState) -> Router {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let raw_html = include_str!("../../frontend-rove/index.html");
    let raw_css = include_str!("../../frontend-rove/app.css");
    let raw_js = include_str!("../../frontend-rove/app.js");
    let raw_stream = include_str!("../../frontend-rove/stream.mjs");
    let raw_voice = include_str!("../../frontend-rove/voice.mjs");
    let mut hasher = DefaultHasher::new();
    raw_html.hash(&mut hasher);
    raw_css.hash(&mut hasher);
    raw_js.hash(&mut hasher);
    raw_stream.hash(&mut hasher);
    raw_voice.hash(&mut hasher);
    let hash = format!("{:016x}", hasher.finish());
    let html = Html(raw_html.replace("{{ASSET_HASH}}", &hash));
    let js: &'static str = Box::leak(raw_js.replace("{{ASSET_HASH}}", &hash).into_boxed_str());
    let css: &'static str = raw_css;
    let stream: &'static str = raw_stream;
    let voice: &'static str = raw_voice;
    let asset = |content_type: &'static str, body: &'static str| {
        (
            [
                (header::CONTENT_TYPE, content_type),
                (
                    header::CACHE_CONTROL,
                    "public, max-age=31536000, immutable",
                ),
            ],
            body,
        )
    };
    Router::new()
        .route(
            "/",
            get(move || {
                let html = html.clone();
                async move {
                    (
                        [(header::CACHE_CONTROL, "no-store")],
                        html,
                    )
                }
            }),
        )
        .route(
            "/app.css",
            get(move || async move { asset("text/css; charset=utf-8", css) }),
        )
        .route(
            "/app.js",
            get(move || async move { asset("text/javascript; charset=utf-8", js) }),
        )
        .route(
            "/stream.mjs",
            get(move || async move { asset("text/javascript; charset=utf-8", stream) }),
        )
        .route("/health", get(|| async { "ok" }))
        .route("/events", get(sse_event_handler))
        .route(
            "/chat",
            post(chat).layer(DefaultBodyLimit::max(128 * 1024)),
        )
        .route("/voice", post(voice::voice).layer(DefaultBodyLimit::max(8 * 1024 * 1024)))
        .route(
            "/voice.mjs",
            get(move || async move { asset("text/javascript; charset=utf-8", voice) }),
        )
        .layer(DefaultBodyLimit::max(8 * 1024 * 1024))
        .with_state(state)
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
    let app = frontend_app(state);
    let addr = std::env::var("ROVE_BIND").unwrap_or_else(|_| "0.0.0.0:3000".into());
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("bind server");
    println!("Rove listening on {addr}");
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .expect("serve requests");
}
