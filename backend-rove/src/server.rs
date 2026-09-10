use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use backend_rove::{MAX_BYTES, MAX_MESSAGES, SseDecoder};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, VecDeque},
    convert::Infallible,
    sync::Arc,
    time::Duration,
};
use tokio::sync::{Semaphore, broadcast};

/// Live observer tap: every streamed line is teed here. `GET /tap` replays
/// recent history then tails live, so any device (e.g. `curl` on a phone)
/// can watch what flows between clients and the server.
#[derive(Clone)]
struct Tap {
    tx: broadcast::Sender<String>,
    buf: Arc<std::sync::Mutex<VecDeque<String>>>,
}

impl Tap {
    fn new() -> Self {
        let (tx, _) = broadcast::channel(512);
        Self {
            tx,
            buf: Arc::new(std::sync::Mutex::new(VecDeque::new())),
        }
    }

    fn push(&self, line: String) {
        if let Ok(mut buf) = self.buf.lock() {
            buf.push_back(line.clone());
            while buf.len() > 500 {
                buf.pop_front();
            }
        }
        let _ = self.tx.send(line);
    }

    fn snapshot(&self) -> Vec<String> {
        self.buf
            .lock()
            .map(|buf| buf.iter().cloned().collect())
            .unwrap_or_default()
    }
}

fn stamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let day = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
        % 86400;
    format!("{:02}:{:02}:{:02}", day / 3600, day % 3600 / 60, day % 60)
}

fn tap_line(kind: &str, text: &str) -> String {
    let short: String = text.chars().take(300).collect();
    format!("[{}] {kind} {short}", stamp())
}

/// Human-readable one-liner for a streamed event. Protocol noise
/// (item added/done bookkeeping, empty frames) renders to nothing.
fn render_tap_event(value: &Value) -> Option<String> {
    let short = |text: &str| {
        let short: String = text.chars().take(300).collect();
        short
    };
    match value.get("type")?.as_str()? {
        "response.created" => Some(format!(
            "· {} thinking…",
            value
                .get("response")?
                .get("model")?
                .as_str()
                .unwrap_or("AI")
        )),
        "response.reasoning_summary_text.delta" => value
            .get("delta")?
            .as_str()
            .map(|d| format!("~ {}", short(d))),
        "response.output_text.delta" | "response.refusal.delta" => {
            value.get("delta")?.as_str().map(short)
        }
        "response.completed" => {
            let usage = value.get("response")?.get("usage")?;
            Some(format!(
                "✓ done · {} in / {} out tokens",
                usage.get("input_tokens")?,
                usage.get("output_tokens")?
            ))
        }
        "response.incomplete" => Some("! stopped early (output limit)".into()),
        "response.failed" => Some("! model run failed".into()),
        "error" => Some(format!(
            "! {}",
            value
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("stream failed")
        )),
        _ => None,
    }
}

#[derive(Clone)]
struct AppState {
    http: reqwest::Client,
    key: Option<String>,
    model: String,
    endpoint: String,
    slots: Arc<Semaphore>,
    tap: Tap,
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

/// At most 8 model responses per chat request: enough for real tool chains,
/// bounded against runaway loops.
const MAX_TOOL_ROUNDS: u8 = 8;

fn exec_tool_def() -> Value {
    json!({
        "type": "function",
        "name": "run_command",
        "description": "Run a shell command on the server with sh -c as an unprivileged user. 30 second timeout, output truncated past ~12KB. Prefer read-only probing (ls, cat, df, uname, systemctl status) before changing anything, and report what each command showed.",
        "parameters": {
            "type": "object",
            "properties": {
                "command": {"type": "string", "description": "Shell command to run on the server"}
            },
            "required": ["command"],
            "additionalProperties": false
        }
    })
}

/// Run one shell command. Secrets are scrubbed from the child environment;
/// output is capped; overruns are killed.
async fn run_command(command: &str) -> (i32, String, u64) {
    let start = std::time::Instant::now();
    let mut cmd = tokio::process::Command::new("sh");
    cmd.arg("-c")
        .arg(command)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .kill_on_drop(true);
    let spawned = cmd.spawn();
    let ms = || start.elapsed().as_millis() as u64;
    let child = match spawned {
        Ok(child) => child,
        Err(e) => return (-1, format!("failed to start shell: {e}"), ms()),
    };
    match tokio::time::timeout(Duration::from_secs(30), child.wait_with_output()).await {
        Ok(Ok(output)) => {
            let code = output.status.code().unwrap_or(-1);
            let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
            let stderr = String::from_utf8_lossy(&output.stderr);
            if !stderr.trim().is_empty() {
                text.push_str("\nstderr:\n");
                text.push_str(&stderr);
            }
            if text.len() > 12_000 {
                text.truncate(12_000);
                text.push_str("\n…(output truncated)");
            }
            if text.trim().is_empty() {
                text = format!("<no output, exit {code}>");
            }
            (code, text, ms())
        }
        Ok(Err(e)) => (-1, format!("failed to read command output: {e}"), ms()),
        Err(_) => (
            -1,
            "command timed out after 30s and was killed".into(),
            ms(),
        ),
    }
}

fn sse_error(message: &str) -> axum::body::Bytes {
    axum::body::Bytes::from(format!(
        "event: error\ndata: {}\n\n",
        json!({"type": "error", "message": message})
    ))
}

fn sse_event(value: &Value) -> axum::body::Bytes {
    axum::body::Bytes::from(format!("data: {value}\n\n"))
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
    let tools = json!([exec_tool_def()]);
    let instructions = "You are Rove, a helpful assistant with a run_command tool that executes shell commands on this server as an unprivileged user (30s limit, output truncated past ~12KB). Use it whenever the user asks about the server or wants something done on it: probe with read-only commands first, then act. Be concise and practical.";
    let upstream = state
        .http
        .post(&state.endpoint)
        .bearer_auth(key)
        .json(&json!({
            "model": state.model,
            "instructions": instructions,
            "input": input,
            "tools": tools,
            "stream": true,
            "store": false,
            "reasoning": {"effort": "low", "summary": "auto"},
            "max_output_tokens": 2048
        }))
        .send()
        .await;
    let upstream = match upstream {
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
    // The SSE stream must be 'static: hand it owned copies of everything.
    let http = state.http.clone();
    let endpoint = state.endpoint.clone();
    let model = state.model.clone();
    let key: String = key.clone();
    let tap = state.tap.clone();
    if let Some(last) = request.messages.last() {
        tap.push(tap_line(">", &last.content));
    }
    let stream = async_stream::stream! {
        // Held until completion or client disconnect; dropping this stream closes upstream.
        let _permit = permit;
        // Idle keep-alive: without traffic, NATs and middleboxes can kill a
        // slow reasoning stream before the first token arrives.
        let mut heartbeat = tokio::time::interval(Duration::from_secs(15));
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        heartbeat.tick().await;
        // First round reuses the already-sent request above so its HTTP errors
        // keep their status codes; later rounds failed mid-stream become SSE
        // error events instead.
        let mut pending: Option<reqwest::Response> = Some(upstream);
        let mut turn_input = Vec::new();
        let mut round: u8 = 0;
        'rounds: loop {
            let mut upstream = match pending.take() {
                Some(response) => response,
                None => {
                    if round >= MAX_TOOL_ROUNDS {
                        tap.push(tap_line("!", "stopped after 8 tool steps"));
                        yield Ok::<_, Infallible>(sse_error("Stopped after 8 tool steps. Ask in smaller pieces."));
                        break 'rounds;
                    }
                    match http
                        .post(&endpoint)
                        .bearer_auth(&key)
                        .json(&json!({
                            "model": model,
                            "instructions": instructions,
                            "input": turn_input,
                            "tools": tools,
                            "stream": true,
                            "store": false,
                            "reasoning": {"effort": "low", "summary": "auto"},
                            "max_output_tokens": 2048
                        }))
                        .send()
                        .await
                    {
                        Ok(response) if response.status().is_success() => response,
                        _ => {
                            tap.push(tap_line("!", "follow-up model request failed"));
                            yield Ok::<_, Infallible>(sse_error("The model request failed. Please try again."));
                            break 'rounds;
                        }
                    }
                }
            };
            round += 1;
            let mut decoder = SseDecoder::default();
            let mut output_items: Vec<Value> = Vec::new();
            let mut completed = false;
            loop {
                tokio::select! {
                    _ = heartbeat.tick() => yield Ok::<_, Infallible>(axum::body::Bytes::from_static(b": ping\n\n")),
                    chunk = upstream.chunk() => match chunk {
                        Ok(Some(bytes)) => {
                            // Control-plane peek + observer tee; wire bytes go out verbatim.
                            for data in decoder.push(&bytes).unwrap_or_default() {
                                if data == "[DONE]" {
                                    continue;
                                }
                                let Ok(value): Result<Value, _> = serde_json::from_str(&data)
                                else {
                                    continue;
                                };
                                if let Some(line) = render_tap_event(&value)
                                    && !line.trim().is_empty()
                                {
                                    tap.push(format!("[{}] < {line}", stamp()));
                                }
                                if value.get("type").and_then(|t| t.as_str())
                                    == Some("response.completed")
                                {
                                    if let Some(output) = value
                                        .get("response")
                                        .and_then(|r| r.get("output"))
                                        .and_then(|o| o.as_array())
                                    {
                                        output_items = output.clone();
                                    }
                                    completed = true;
                                }
                            }
                            yield Ok(bytes);
                        }
                        Ok(None) => break,
                        Err(_) => {
                            tap.push(tap_line("!", "upstream stream interrupted"));
                            yield Ok::<_, Infallible>(sse_error("Upstream stream interrupted; please retry."));
                            break 'rounds;
                        }
                    },
                }
            }
            let calls: Vec<(String, String)> = output_items
                .iter()
                .filter(|item| item.get("type").and_then(|t| t.as_str()) == Some("function_call"))
                .filter_map(|item| {
                    Some((
                        item.get("call_id")?.as_str()?.to_string(),
                        item.get("arguments").and_then(|a| a.as_str()).unwrap_or("{}").to_string(),
                    ))
                })
                .collect();
            if calls.is_empty() || !completed {
                break 'rounds;
            }
            let mut turn = output_items;
            for (call_id, arguments) in calls {
                let command = serde_json::from_str::<Value>(&arguments)
                    .ok()
                    .and_then(|args| args.get("command")?.as_str().map(str::to_string))
                    .unwrap_or_default();
                yield Ok::<_, Infallible>(sse_event(&json!({"type": "exec.call", "command": command})));
                tap.push(format!("[{}] $ {command}", stamp()));
                let (code, output, ms) = run_command(&command).await;
                yield Ok::<_, Infallible>(sse_event(&json!({"type": "exec.done", "exit": code, "ms": ms})));
                tap.push(format!("[{0}] [exit {code} · {ms}ms]", stamp()));
                turn.push(json!({"type": "function_call_output", "call_id": call_id, "output": output}));
            }
            turn_input = turn;
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

async fn tap(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
) -> Response {
    // ?tail=N peeks at recent history and closes; plain GET streams live.
    if let Some(n) = params.get("tail").and_then(|v| v.parse::<usize>().ok()) {
        let lines = state.tap.snapshot();
        let from = lines.len().saturating_sub(n.min(500));
        return (
            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            lines[from..].join("\n") + "\n",
        )
            .into_response();
    }
    let tap = state.tap.clone();
    // Plain-text streaming (not SSE framing): observers watch with plain
    // `curl -N`, so lines arrive exactly as shown, with no `data:` noise.
    let stream = async_stream::stream! {
        for line in tap.snapshot() {
            yield Ok::<_, Infallible>(axum::body::Bytes::from(format!("{line}\n")));
        }
        let mut rx = tap.tx.subscribe();
        let mut heartbeat = tokio::time::interval(Duration::from_secs(15));
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        heartbeat.tick().await;
        loop {
            tokio::select! {
                _ = heartbeat.tick() => yield Ok::<_, Infallible>(axum::body::Bytes::from_static(b"\n")),
                got = rx.recv() => match got {
                    Ok(line) => yield Ok(axum::body::Bytes::from(format!("{line}\n"))),
                    Err(_) => break,
                },
            }
        }
    };
    (
        [
            (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
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
        tap: Tap::new(),
    };
    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/tap", get(tap))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn exec_runs_and_reports() {
        let (code, output, _) = run_command("echo hi").await;
        assert_eq!(code, 0);
        assert_eq!(output, "hi\n");
    }

    #[tokio::test]
    async fn exec_missing_command_fails_loudly() {
        let (code, output, _) = run_command("exit 3").await;
        assert_eq!(code, 3);
        assert!(output.contains("exit 3"));
    }
}
