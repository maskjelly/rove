//! Rove server: `POST /chat` relays text to OpenAI and streams the answer
//! back as SSE, with a `run_command` tool loop and a plain-text observer
//! tap (`GET /tap`). Read `lib.rs`, then `client.rs`, then this file.
//!
//! The two Rust ideas that unlock this file:
//! 1. *Ownership*: every value has one owner; passing it to another function
//!    *moves* it unless you borrow (`&`). The compiler rejects use-after-move.
//! 2. *Async*: `.await` pauses a task (not a thread) until I/O completes, so
//!    thousands of requests share a few threads via the tokio runtime.

use axum::{
    // axum maps HTTP to async functions: `Router` + `get`/`post` wire paths,
    // `Json` parses/serializes request/response bodies, `State` shares the
    // `AppState` below with every handler, `DefaultBodyLimit` caps uploads.
    Json,
    Router,
    body::Body,
    extract::{DefaultBodyLimit, State},
    http::{StatusCode, header},
    // `IntoResponse` converts many types (tuples, strings, JSON) into HTTP.
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
#[derive(Clone, Debug)]
struct TapLine {
    /// Wall-clock time (human reference; may stutter on sick VM clocks).
    srv_ms: u64,
    /// Monotonic ms since the previous pushed line: the trustworthy timeline.
    gap_ms: u64,
    text: String,
}

impl TapLine {
    fn now(text: String, gap_ms: u64) -> Self {
        let short: String = text.chars().take(300).collect();
        Self {
            srv_ms: now_ms(),
            gap_ms,
            text: short,
        }
    }
}

struct TapInner {
    buf: VecDeque<TapLine>,
    last_push: std::time::Instant,
}

/// The tap handle cloned into every request handler.
///
/// Rust lesson: `#[derive(Clone)]` here is *shallow* — cloning a `Tap`
/// copies the `Arc` pointer, not the data. `Arc` (atomic reference count)
/// lets many owners share one allocation across threads/tasks, and `Mutex`
/// allows only one task inside at a time. A plain `std` mutex (not tokio's)
/// is right because the critical section never awaits: lock, push, unlock.
#[derive(Clone)]
struct Tap {
    // `broadcast` is a multi-producer, multi-consumer channel: every `send`
    // reaches *all* live subscribers (each `/tap` tail holds one `Receiver`).
    // Slow readers get a `Lagged` error instead of slowing the sender.
    tx: broadcast::Sender<TapLine>,
    inner: Arc<std::sync::Mutex<TapInner>>,
}

impl Tap {
    fn new() -> Self {
        let (tx, _) = broadcast::channel(512);
        Self {
            tx,
            inner: Arc::new(std::sync::Mutex::new(TapInner {
                buf: VecDeque::new(),
                last_push: std::time::Instant::now(),
            })),
        }
    }

    fn push(&self, text: String) {
        // `&self` (not `&mut self`): interior mutability via the Mutex means
        // even a shared reference can update the buffer. `if let Ok(...)`
        // treats a poisoned lock as "drop the line" rather than panicking.
        let line = if let Ok(mut inner) = self.inner.lock() {
            let gap_ms = inner.last_push.elapsed().as_millis() as u64;
            inner.last_push = std::time::Instant::now();
            let line = TapLine::now(text, gap_ms);
            inner.buf.push_back(line.clone());
            while inner.buf.len() > 500 {
                inner.buf.pop_front();
            }
            line
        } else {
            return;
        };
        let _ = self.tx.send(line);
    }

    fn snapshot(&self) -> Vec<TapLine> {
        self.inner
            .lock()
            .map(|inner| inner.buf.iter().cloned().collect())
            .unwrap_or_default()
    }
}

fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        // `as u64` truncates; millis-since-epoch fits for millennia. Wall
        // time can jump (NTP), so intervals always use `Instant` instead.
        .unwrap_or(0)
}

fn stamp_ms(ms: u64) -> String {
    let day = ms / 1000 % 86400;
    format!(
        "{:02}:{:02}:{:02}.{:03}",
        day / 3600,
        day % 3600 / 60,
        day % 60,
        ms % 1000
    )
}

/// Delivery lag (or replay age) in human form: the gap between the server
/// handling a line and the observer receiving it.
fn age_str(ms: u64) -> String {
    if ms < 1000 {
        format!("{ms}ms")
    } else if ms < 60_000 {
        format!("{:.1}s", ms as f64 / 1000.0)
    } else if ms < 3_600_000 {
        format!("{}m", ms / 60_000)
    } else {
        format!("{}h", ms / 3_600_000)
    }
}

fn format_tap_line(line: &TapLine, now: u64) -> String {
    // Wall time for human reference, mono gap-since-previous-line for the
    // trustworthy timeline (wall clocks can stutter on sick VMs), delivery
    // age for observer lag.
    format!(
        "[{} (+{} mono) → +{}] {}",
        stamp_ms(line.srv_ms),
        age_str(line.gap_ms),
        age_str(now.saturating_sub(line.srv_ms)),
        line.text
    )
}

/// Human-readable one-liner for a streamed event. Protocol noise
/// (item added/done bookkeeping, empty frames, raw text deltas) renders to
/// nothing: text is coalesced into sentences elsewhere.
///
/// Rust lesson: returning `Option<String>` instead of `String` lets the
/// caller skip noise with `if let Some(line) = ...`. The `?` operator works
/// here too — inside an `Option`-returning function, `None` propagates just
/// like `Err` does in `Result` functions.
fn render_tap_event(value: &Value) -> Option<String> {
    match value.get("type")?.as_str()? {
        "response.created" => Some(format!(
            "· {} thinking…",
            value
                .get("response")?
                .get("model")?
                .as_str()
                .unwrap_or("AI")
        )),
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

/// Byte index where a sentence completes: punctuation followed by whitespace
/// or end of buffer (so "3.5" and "e.g." don't split).
///
/// Rust lesson: `char_indices()` yields `(byte_index, char)` pairs because
/// UTF-8 characters take 1–4 bytes — you must never slice a `&str` at an
/// arbitrary byte offset (it panics). `c.len_utf8()` advances past the
/// current character safely.
fn sentence_end(buf: &str) -> Option<usize> {
    let mut pending: Option<usize> = None;
    for (i, c) in buf.char_indices() {
        if let Some(end) = pending {
            if c.is_whitespace() {
                return Some(end);
            }
            pending = None;
        }
        if matches!(c, '.' | '!' | '?') {
            pending = Some(i + c.len_utf8());
            if i + c.len_utf8() == buf.len() {
                return Some(buf.len());
            }
        }
    }
    None
}

/// Soft cut for very long fragments: last space within 160 chars, else a
/// hard char-boundary cut.
fn soft_cut(buf: &str) -> usize {
    let cap = buf
        .char_indices()
        .take_while(|(i, _)| *i < 160)
        .last()
        .map(|(i, c)| i + c.len_utf8())
        .unwrap_or(0)
        .max(1)
        .min(buf.len());
    buf[..cap].rfind(' ').map(|i| i + 1).unwrap_or(cap).max(1)
}

/// Drain complete sentences (or long fragments) from the buffer as tap lines.
///
/// Rust lesson: `buf.drain(..end)` removes a range *and* hands back the
/// removed part — but the range end must sit on a character boundary or it
/// panics at runtime. Every index here comes from `char_indices` (or an
/// ASCII space from `rfind(' ')`, which is always boundary-safe), and the
/// `.min(buf.len())` is belt-and-braces.
fn flush_tap_lines(tap: &Tap, buf: &mut String, force: bool) {
    let rest = buf.trim_start().len();
    buf.drain(..buf.len() - rest);
    loop {
        if buf.is_empty() {
            break;
        }
        let end = sentence_end(buf).or_else(|| (force || buf.len() >= 160).then(|| soft_cut(buf)));
        let Some(end) = end else { break };
        let line: String = buf.drain(..end.min(buf.len())).collect();
        let rest = buf.trim_start().len();
        buf.drain(..buf.len() - rest);
        let line = line.trim().to_string();
        if !line.is_empty() {
            tap.push(format!("< {line}"));
        }
    }
}

#[derive(Clone)]
struct AppState {
    http: reqwest::Client,
    key: Option<String>,
    model: String,
    endpoint: String,
    // At most 8 turns run at once; the 9th gets HTTP 429 immediately instead
    // of queueing (fail fast beats slow death). `Arc` shares one semaphore
    // across all handler tasks.
    slots: Arc<Semaphore>,
    tap: Tap,
}

/// `#[derive(Deserialize)]` auto-generates JSON parsing for these structs:
/// a request body of `{"role": ..., "content": ...}` becomes a `Message`.
/// Unknown JSON fields are ignored; missing ones are a 400 error.
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
///
/// Rust lesson: `async fn` returning a plain tuple — no `Result` needed
/// because every failure mode is encoded *in* the tuple (negative exit
/// code + message). Callers never have to handle an error case here.
async fn run_command(command: &str) -> (i32, String, u64) {
    let start = std::time::Instant::now();
    let mut cmd = tokio::process::Command::new("sh");
    cmd.arg("-c")
        .arg(command)
        // Pipes, not inheritance: without these the child would write into
        // OUR stdout (we learned this the embarrassing way — a unit test
        // caught `echo hi` leaking into the test runner's output).
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        // `env_clear` wipes the parent environment (which holds the OpenAI
        // key) before adding back a minimal PATH.
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        // If our future is dropped (timeout below), take the child with it.
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
    // SSE framing by hand: an optional `event:` line, then `data:`, then a
    // blank line to dispatch. The client only looks at `data:` lines.
    axum::body::Bytes::from(format!(
        "event: error\ndata: {}\n\n",
        json!({"type": "error", "message": message})
    ))
}

fn sse_event(value: &Value) -> axum::body::Bytes {
    axum::body::Bytes::from(format!("data: {value}\n\n"))
}

/// First byte slower than this fires a hedged twin request; whichever
/// responds first wins. Kills tail latency at ~2x cost on slow starts only.
const HEDGE_DELAY_MS: u64 = 400;

struct HeadStart {
    response: reqwest::Response,
    stash: VecDeque<axum::body::Bytes>,
    hedged: bool,
}

fn stashed(
    response: reqwest::Response,
    first: Option<axum::body::Bytes>,
    hedged: bool,
) -> HeadStart {
    let mut stash = VecDeque::new();
    if let Some(bytes) = first {
        stash.push_back(bytes);
    }
    HeadStart {
        response,
        stash,
        hedged,
    }
}

/// Send with hedging across both stall phases: slow response headers AND
/// slow first body byte each trigger a twin race after the hedge delay.
/// Non-success statuses return immediately (a twin would fail identically).
///
/// Rust lesson: `Box::pin` puts a future at a stable memory address so
/// `select!` can poll it. Futures that borrow data can't be moved after
/// polling starts — pinning plus *owned* values (see `first_chunk` below)
/// is the standard escape hatch.
async fn hedge_send(
    http: &reqwest::Client,
    template: reqwest::Request,
) -> Result<HeadStart, reqwest::Error> {
    let mut twin_template = template.try_clone();
    let mut primary_fut = Box::pin(http.execute(template));
    // Phase 1: first response headers win.
    // `biased` polls branches top-to-bottom, so an already-ready primary
    // wins ties and we never waste a twin. The `if` guard disables the
    // sleep branch entirely when there is no twin to fire.
    let mut primary = tokio::select! {
        biased;
        result = &mut primary_fut => result?,
        _ = tokio::time::sleep(Duration::from_millis(HEDGE_DELAY_MS)), if twin_template.is_some() => {
            let request = twin_template.take().unwrap_or_else(|| unreachable!("guarded by is_some"));
            let mut twin_fut = Box::pin(http.execute(request));
            tokio::select! {
                result = &mut primary_fut => result?,
                result = &mut twin_fut => result?,
            }
        }
    };
    if !primary.status().is_success() {
        return Ok(stashed(primary, None, false));
    }
    // Phase 2: first body byte wins, if the twin is still unused.
    if twin_template.is_none() {
        return Ok(stashed(primary, None, true));
    }
    match tokio::time::timeout(Duration::from_millis(HEDGE_DELAY_MS), primary.chunk()).await {
        Ok(Ok(Some(first))) => Ok(stashed(primary, Some(first), false)),
        Ok(_) => Ok(stashed(primary, None, false)),
        Err(_) => {
            let request = twin_template
                .take()
                .unwrap_or_else(|| unreachable!("checked above"));
            match tokio::time::timeout(Duration::from_secs(2), http.execute(request)).await {
                Ok(Ok(secondary)) if secondary.status().is_success() => {
                    let mut primary_first = Box::pin(first_chunk(primary));
                    let mut twin_first = Box::pin(first_chunk(secondary));
                    tokio::select! {
                        (response, won) = &mut primary_first => match won {
                            Ok(Some(bytes)) => Ok(stashed(response, Some(bytes), true)),
                            _ => Ok(stashed(response, None, true)),
                        },
                        (response, won) = &mut twin_first => match won {
                            Ok(Some(bytes)) => Ok(stashed(response, Some(bytes), true)),
                            _ => Ok(stashed(response, None, true)),
                        },
                    }
                }
                _ => {
                    let (response, won) = first_chunk(primary).await;
                    match won {
                        Ok(Some(bytes)) => Ok(stashed(response, Some(bytes), true)),
                        _ => Ok(stashed(response, None, true)),
                    }
                }
            }
        }
    }
}

/// Own the response, return it with its first-chunk outcome. Lets select!
/// arms move winners while losers (response included) drop with their future.
async fn first_chunk(
    mut response: reqwest::Response,
) -> (
    reqwest::Response,
    Result<Option<axum::body::Bytes>, reqwest::Error>,
) {
    let won = response.chunk().await;
    (response, won)
}

/// Tap coalescing state for one upstream stream: fragments accumulate and
/// flush as readable sentence lines instead of one line per word.
///
/// Rust lesson: the `<'a>` is a *lifetime* — it tells the compiler "this
/// struct borrows a `Tap` that must outlive the struct". `&'a Tap` borrows
/// (no clone needed for a shared handle), while `answer`/`reason` are owned
/// `String`s because this struct builds them up itself. `Instant` is `Copy`,
/// so it moves by value with no ownership fuss.
struct TapFeed<'a> {
    tap: &'a Tap,
    answer: String,
    reason: String,
    first_text: bool,
    received: std::time::Instant,
}

impl TapFeed<'_> {
    fn feed(
        &mut self,
        decoder: &mut SseDecoder,
        bytes: &axum::body::Bytes,
        output_items: &mut Vec<Value>,
        completed: &mut bool,
    ) {
        for data in decoder.push(bytes).unwrap_or_default() {
            if data == "[DONE]" {
                continue;
            }
            let Ok(value): Result<Value, _> = serde_json::from_str(&data) else {
                continue;
            };
            match value.get("type").and_then(|t| t.as_str()) {
                Some("response.output_text.delta" | "response.refusal.delta") => {
                    if let Some(d) = value.get("delta").and_then(|d| d.as_str()) {
                        if !self.first_text {
                            self.first_text = true;
                            self.tap.push(format!(
                                "· first answer +{}ms after receipt",
                                self.received.elapsed().as_millis()
                            ));
                        }
                        self.answer.push_str(d);
                    }
                    flush_tap_lines(self.tap, &mut self.answer, false);
                }
                Some("response.reasoning_summary_text.delta") => {
                    if let Some(d) = value.get("delta").and_then(|d| d.as_str()) {
                        self.reason.push_str(d);
                    }
                }
                Some("response.reasoning_summary_text.done") => {
                    let text = self.reason.trim().to_string();
                    self.reason.clear();
                    if !text.is_empty() {
                        self.tap.push(format!("~ {text}"));
                    }
                }
                Some("response.completed") => {
                    self.finish();
                    if let Some(output) = value
                        .get("response")
                        .and_then(|r| r.get("output"))
                        .and_then(|o| o.as_array())
                    {
                        *output_items = output.clone();
                    }
                    *completed = true;
                    if let Some(usage) = value.get("response").and_then(|r| r.get("usage")) {
                        self.tap.push(format!(
                            "✓ done · {} in / {} out · {}ms server",
                            usage
                                .get("input_tokens")
                                .map(|v| v.to_string())
                                .as_deref()
                                .unwrap_or("?"),
                            usage
                                .get("output_tokens")
                                .map(|v| v.to_string())
                                .as_deref()
                                .unwrap_or("?"),
                            self.received.elapsed().as_millis()
                        ));
                    }
                }
                _ => {
                    if let Some(line) = render_tap_event(&value)
                        && !line.trim().is_empty()
                    {
                        self.tap.push(format!("< {line}"));
                    }
                }
            }
        }
    }

    fn finish(&mut self) {
        flush_tap_lines(self.tap, &mut self.answer, true);
        let reason = self.reason.trim().to_string();
        self.reason.clear();
        if !reason.is_empty() {
            self.tap.push(format!("~ {reason}"));
        }
    }
}

fn round_payload(model: &str, instructions: &str, input: &[Value], tools: &Value) -> Value {
    // `&[Value]` is a slice: works for `Vec` and arrays alike, borrowed.
    // `json!` is a macro that builds `serde_json::Value` from JSON-like syntax.
    json!({
        "model": model,
        "instructions": instructions,
        "input": input,
        "tools": tools,
        "stream": true,
        "store": false,
        "reasoning": {"effort": "low", "summary": "auto"},
        "max_output_tokens": 2048,
        "service_tier": "priority"
    })
}

async fn chat(State(state): State<AppState>, Json(request): Json<ChatRequest>) -> Response {
    // axum *extractors* in the signature do the HTTP plumbing: `State`
    // clones out the shared `AppState`, `Json` parses the body (rejecting
    // malformed JSON with 400/415 before this code runs).
    // `let ... else` (Rust 1.65+): unwrap the `Some` case into `key`, or
    // `return`/`break`/`continue` out of the function on `None`. It reads
    // like an early return and avoids one level of nesting.
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
    // `try_acquire_owned` never waits: at capacity it errors instantly and
    // the caller gets 429 + `Retry-After` instead of hanging in a queue.
    // The owned permit is moved into the stream below, so the slot frees
    // exactly when the response ends (or the client disconnects).
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
    let instructions = "You are Rove, a terse assistant with a run_command tool for operating this server (30s limit). Probe read-only first. Be concise.";
    // Stamp receipt BEFORE any upstream work: this is t=0 of the turn.
    // (No truncation here — `TapLine::now` already caps line length.)
    let tap = state.tap.clone();
    if let Some(last) = request.messages.last() {
        tap.push(format!("> {}", last.content));
    }
    let payload = round_payload(&state.model, instructions, &input, &tools);
    // `.build()` turns the request builder into a concrete `Request` *without*
    // sending it, so `try_clone()` can mint an identical twin for hedging.
    // (A body built from bytes is cloneable; a streaming body would not be.)
    let template = match state
        .http
        .post(&state.endpoint)
        .bearer_auth(key)
        .json(&payload)
        .build()
    {
        Ok(template) => template,
        Err(_) => {
            return error(
                StatusCode::BAD_GATEWAY,
                "Cannot reach OpenAI. Please retry.",
            );
        }
    };
    let hs_t0 = std::time::Instant::now();
    // Wall-clock stamp of the hedge duration goes on the tap line later, so
    // slow starts are visible per turn.
    let HeadStart {
        response: upstream,
        stash,
        hedged: hedged_round0,
    } = match hedge_send(&state.http, template).await {
        Ok(hedged) => hedged,
        Err(_) => {
            return error(
                StatusCode::BAD_GATEWAY,
                "Cannot reach OpenAI. Please retry.",
            );
        }
    };
    let hs_waited0 = hs_t0.elapsed().as_millis() as u64;
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
    // (`tap` was cloned above for the receipt stamp.) Anything merely
    // borrowed from this stack frame would dangle once the handler returns
    // while the stream is still being polled — the compiler rejects that.
    let http = state.http.clone();
    let endpoint = state.endpoint.clone();
    let model = state.model.clone();
    let key: String = key.clone();
    let stream = async_stream::stream! {
        // Held until completion or client disconnect; dropping this stream closes upstream.
        let _permit = permit;
        // t=0 for every server-side tap latency below.
        let received = std::time::Instant::now();
        // Idle keep-alive: without traffic, NATs and middleboxes can kill a
        // slow reasoning stream before the first token arrives.
        let mut heartbeat = tokio::time::interval(Duration::from_secs(15));
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        heartbeat.tick().await;
        // First round reuses the already-sent request above so its HTTP errors
        // keep their status codes; later rounds failed mid-stream become SSE
        // error events instead. (`Option::take` swaps in `None` and hands us
        // the value — the standard "move out and leave empty" idiom.)
        let mut pending: Option<HeadStart> = Some(HeadStart {
            response: upstream,
            stash,
            hedged: hedged_round0,
        });
        let mut turn_input = Vec::new();
        let mut round: u8 = 0;
        let mut hs_waited = hs_waited0;
        'rounds: loop {
            let head = match pending.take() {
                Some(head) => head,
                None => {
                    if round >= MAX_TOOL_ROUNDS {
                        tap.push("! stopped after 8 tool steps".into());
                        yield Ok::<_, Infallible>(sse_error("Stopped after 8 tool steps. Ask in smaller pieces."));
                        break 'rounds;
                    }
                    let payload = round_payload(&model, instructions, &turn_input, &tools);
                    let template = match http
                        .post(&endpoint)
                        .bearer_auth(&key)
                        .json(&payload)
                        .build()
                    {
                        Ok(template) => template,
                        Err(_) => {
                            tap.push("! follow-up model request failed".into());
                            yield Ok::<_, Infallible>(sse_error("The model request failed. Please try again."));
                            break 'rounds;
                        }
                    };
                    let hs_t0 = std::time::Instant::now();
                    match hedge_send(&http, template).await {
                        Ok(head) if head.response.status().is_success() => {
                            hs_waited = hs_t0.elapsed().as_millis() as u64;
                            head
                        }
                        _ => {
                            tap.push("! follow-up model request failed".into());
                            yield Ok::<_, Infallible>(sse_error("The model request failed. Please try again."));
                            break 'rounds;
                        }
                    }
                }
            };
            round += 1;
            let HeadStart {
                response: mut upstream,
                mut stash,
                hedged,
            } = head;
            if hedged {
                tap.push(format!("⇄ hedged retry fired (+{hs_waited}ms hedge)"));
            }
            let mut decoder = SseDecoder::default();
            let mut output_items: Vec<Value> = Vec::new();
            let mut completed = false;
            let mut feed = TapFeed {
                tap: &tap,
                answer: String::new(),
                reason: String::new(),
                first_text: false,
                received,
            };
            while let Some(bytes) = stash.pop_front() {
                feed.feed(&mut decoder, &bytes, &mut output_items, &mut completed);
                yield Ok::<_, Infallible>(bytes);
            }
            loop {
                // `completed` ends the round: anything after it is just
                // [DONE]/keepalive. Never wait on upstream EOF — a peer that
                // holds the connection open would wedge the turn (and the
                // client's read) until timeouts fire.
                if completed {
                    break;
                }
                tokio::select! {
                    _ = heartbeat.tick() => yield Ok::<_, Infallible>(axum::body::Bytes::from_static(b": ping\n\n")),
                    chunk = upstream.chunk() => match chunk {
                        Ok(Some(bytes)) => {
                            // Control-plane peek + observer tee; wire bytes go out verbatim.
                            feed.feed(&mut decoder, &bytes, &mut output_items, &mut completed);
                            yield Ok(bytes);
                        }
                        Ok(None) => break,
                        Err(_) => {
                            tap.push("! upstream stream interrupted".into());
                            yield Ok::<_, Infallible>(sse_error("Upstream stream interrupted; please retry."));
                            break 'rounds;
                        }
                    },
                }
            }
            feed.finish();
            // Collect this round's function calls, if any. `filter` keeps
            // matching items; `filter_map` keeps *and* transforms, dropping
            // `None`s — here a missing `call_id` discards a malformed item.
            // The `?`s work because the closure returns `Option`.
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
                // Parse the model's requested command out of its JSON arguments.
                let command = serde_json::from_str::<Value>(&arguments)
                    .ok()
                    .and_then(|args| args.get("command")?.as_str().map(str::to_string))
                    .unwrap_or_default();
                yield Ok::<_, Infallible>(sse_event(&json!({"type": "exec.call", "command": command})));
                tap.push(format!("$ {command}"));
                let (code, output, ms) = run_command(&command).await;
                yield Ok::<_, Infallible>(sse_event(&json!({"type": "exec.done", "exit": code, "ms": ms})));
                tap.push(format!("[exit {code} · {ms}ms]"));
                // Feed the result back so the model can continue with it.
                // This is the Responses API contract: every `function_call`
                // output item must be answered by a `function_call_output`
                // item carrying the same `call_id`.
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
    // `Query<HashMap<..>>` parses `?tail=50` into a map; a missing or
    // non-numeric value simply takes the streaming branch below.
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
) -> Response {
    // ?tail=N peeks at recent history and closes; plain GET streams live.
    if let Some(n) = params.get("tail").and_then(|v| v.parse::<usize>().ok()) {
        let lines = state.tap.snapshot();
        let from = lines.len().saturating_sub(n.min(500));
        let now = now_ms();
        let body: String = lines[from..]
            .iter()
            .map(|line| format_tap_line(line, now) + "\n")
            .collect();
        return ([(header::CONTENT_TYPE, "text/plain; charset=utf-8")], body).into_response();
    }
    let tap = state.tap.clone();
    // Plain-text streaming (not SSE framing): observers watch with plain
    // `curl -N`, so lines arrive exactly as shown, with no `data:` noise.
    let stream = async_stream::stream! {
        let now = now_ms();
        for line in tap.snapshot() {
            yield Ok::<_, Infallible>(axum::body::Bytes::from(format_tap_line(&line, now) + "\n"));
        }
        let mut rx = tap.tx.subscribe();
        let mut heartbeat = tokio::time::interval(Duration::from_secs(15));
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        heartbeat.tick().await;
        loop {
            tokio::select! {
                // Blank line, not an SSE comment: this stream is plain text,
                // so anything visible would show up in observers' terminals.
                _ = heartbeat.tick() => yield Ok::<_, Infallible>(axum::body::Bytes::from_static(b"\n")),
                got = rx.recv() => match got {
                    Ok(line) => yield Ok(axum::body::Bytes::from(format_tap_line(&line, now_ms()) + "\n")),
                    // `Lagged` (slow reader fell behind) and `Closed` both
                    // end the stream; the observer just reconnects.
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
    // One shared HTTP client for the process: connection pooling means the
    // second request to OpenAI reuses the warm TLS connection. The 180s
    // `timeout` is a *total* deadline per request, streaming included.
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

    // `#[tokio::test]` is the async version of `#[test]`: it builds a
    // runtime so the test body can `.await`. These spawn a real `sh`.
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

    // Pure functions get plain synchronous tests: no I/O, no runtime needed.
    #[test]
    fn sentence_boundaries() {
        // Punctuation + space ends a sentence; decimals and abbreviations don't.
        assert_eq!(sentence_end("Hi there. How are you?"), Some(9));
        assert_eq!(sentence_end("Pi is 3.5 today"), None);
        assert_eq!(sentence_end("Hi."), Some(3));
        assert_eq!(sentence_end("no punctuation"), None);
        // Long fragments cut at a space, never mid-character.
        let long = "word ".repeat(50);
        let cut = soft_cut(&long);
        assert!(cut <= 160 && long.is_char_boundary(cut));
        assert!(long[..cut].ends_with(' '));
    }
}
