//! Terminal chat client: sends text to the server, prints the SSE stream back.
//!
//! Read order for learning Rust: `lib.rs` first (small), then this file,
//! then `server.rs`. This file shows async I/O, pattern matching on JSON,
//! threads, channels, and graceful Ctrl-C handling.

use backend_rove::{MAX_BYTES, MAX_MESSAGES, SseDecoder};
use serde_json::{Value, json};
use std::{
    // `std::error::Error` is the base *trait* for all errors. `Box<dyn Error>`
    // below means "any error, heap-allocated, type erased" — one error type
    // for I/O failures, HTTP failures, JSON failures, everything.
    error::Error,
    io::{self, Write},
    time::Duration,
};

// A type alias so signatures stay short. `Result<T>` here is OUR Result
// (any error), not `std::result::Result<T, E>` — same enum, fixed error type.
type Result<T> = std::result::Result<T, Box<dyn Error>>;

/// Base URL is known up front, so there is no health-check round trip:
/// the first POST doubles as the reachability probe and saves ~200ms.
fn base_url() -> String {
    // `std::env::args()` yields the command-line words. `.nth(1)` is the
    // first real argument (`nth(0)` is the program name) as an `Option`.
    // `.map(...)` transforms it if present; `.unwrap_or_else(...)` supplies
    // the default otherwise. No `if` needed — this is idiomatic Option chaining.
    std::env::args()
        .nth(1)
        .map(|b| b.trim_end_matches('/').to_string())
        .unwrap_or_else(|| "https://45.196.196.251".to_string())
}

/// One full turn: POST the messages, print each streamed event, return the
/// complete answer text for the conversation history.
async fn response(http: &reqwest::Client, base: &str, messages: &[Value]) -> Result<String> {
    // `format!("{base}/chat")` interpolates variables directly into strings.
    // `.json(...)` serializes *and* sets Content-Type. `.await?` pauses this
    // function until the response headers arrive, propagating errors with `?`.
    let mut upstream = http
        .post(format!("{base}/chat"))
        .json(&json!({"messages": messages}))
        .send()
        .await?;
    if !upstream.status().is_success() {
        let status = upstream.status();
        // The server always answers errors as `{"error": "..."}` JSON.
        let details: Value = upstream.json().await.unwrap_or_default();
        return Err(format!(
            "{status}: {}",
            details["error"].as_str().unwrap_or("Request failed")
        )
        .into());
    }
    let mut parser = SseDecoder::default();
    let mut answer = String::new();
    // Tracks which heading was already printed so "AI" appears once.
    // `""` vs `"summary"` vs `"answer"` — a poor man's enum; `&str` keeps it tiny.
    let mut section = "";
    let mut completed = false;
    // `upstream.chunk().await?` yields each network chunk (`Some(bytes)`) and
    // `None` at end of stream. The `?` turns a transport failure into `Err`.
    while let Some(bytes) = upstream.chunk().await? {
        for data in parser.push(&bytes)? {
            if data == "[DONE]" {
                continue;
            }
            // Parse this event. A malformed one aborts the turn loudly
            // instead of silently printing garbage.
            let event: Value = serde_json::from_str(&data)?;
            // Indexing JSON with `event["type"]` never panics: a missing key
            // yields `Value::Null`, and `.as_str().unwrap_or("")` maps
            // anything non-string to `""`, which falls into `_ => {}` below.
            match event["type"].as_str().unwrap_or("") {
                "response.created" => {
                    println!("{} · generating...", event["response"]["model"].as_str().unwrap_or("AI"));
                }
                "response.reasoning_summary_text.delta" => {
                    if section != "summary" {
                        print!("\nReasoning summary\n");
                        section = "summary";
                    }
                    print!("{}", event["delta"].as_str().unwrap_or(""));
                }
                "response.output_text.delta" | "response.refusal.delta" => {
                    // `|` = or-pattern: two event types share one arm.
                    if section != "answer" {
                        print!("\nAI\n");
                        section = "answer";
                    }
                    let delta = event["delta"].as_str().unwrap_or("");
                    print!("{delta}");
                    answer.push_str(delta);
                }
                "response.reasoning_summary_text.done" => println!(),
                "response.completed" => {
                    completed = true;
                    let usage = &event["response"]["usage"];
                    println!("\n\n[{} input · {} output tokens, including reasoning]\n", usage["input_tokens"], usage["output_tokens"]);
                }
                "response.incomplete" => return Err(format!("Response stopped early: {}. Ask a shorter question or increase the server token limit.", event["response"]["incomplete_details"]["reason"].as_str().unwrap_or("unknown reason")).into()),
                "response.failed" | "error" => return Err("AI stream failed. Check API billing or retry; the partial answer was not saved to history.".into()),
                // Server tool events: the model ran a command over there.
                "exec.call" => {
                    println!("\n$ {}", event["command"].as_str().unwrap_or(""));
                }
                "exec.done" => {
                    println!(
                        "[exit {} · {}ms]",
                        event["exit"].as_i64().unwrap_or(-1),
                        event["ms"].as_u64().unwrap_or(0)
                    );
                }
                _ => {}
            }
            // `print!` buffers; flush so every token appears immediately.
            io::stdout().flush()?;
        }
    }
    // Read until the server closes the stream: one turn can span several
    // model responses when tools run (completed, exec events, completed...).
    if !completed {
        return Err(
            "Connection ended before completion. Please retry; partial output was not saved."
                .into(),
        );
    }
    if answer.is_empty() {
        return Err("No answer was returned. Please retry with a shorter question.".into());
    }
    Ok(answer)
}

async fn run() -> Result<()> {
    let http = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(200))
        .build()?;
    let base = base_url();
    println!("Rove chat · /new clears history · /exit quits · Ctrl-C cancels and exits");
    println!(
        "Conversation stays in this client session. Recent turns are sent with each message.\n"
    );
    let mut messages: Vec<Value> = Vec::new();
    loop {
        print!("You > ");
        io::stdout().flush()?;
        // `read_line` blocks its thread, which would swallow Ctrl-C. So input
        // runs on a detached OS thread and reports back over a oneshot
        // channel (single value, single receiver). The main task stays
        // responsive to the Ctrl-C handler in `main` below.
        let (sender, receiver) = tokio::sync::oneshot::channel();
        std::thread::spawn(move || {
            // `move` transfers ownership of `sender` into the thread.
            let mut input = String::new();
            let result = io::stdin()
                .read_line(&mut input)
                .map(|count| (count, input));
            // The receiver may be gone (we were cancelled); ignore that.
            let _ = sender.send(result);
        });
        // Two `?`s: the first unwraps channel delivery, the second the I/O result.
        let (count, input) = receiver.await??;
        if count == 0 {
            // EOF (piped stdin closed): quit quietly.
            break;
        }
        let input = input.trim();
        match input {
            "/exit" | "/quit" => break,
            "/new" => {
                messages.clear();
                println!("New conversation.\n");
                continue;
            }
            "" => continue,
            _ => {}
        }
        if input.len() > 16_000 {
            eprintln!("Keep each message under 16 KB.");
            continue;
        }
        messages.push(json!({"role": "user", "content": input}));
        // Mirror the server's budget locally so the server never rejects us.
        // `drain(..2)` drops the oldest user+assistant pair, keeping the
        // conversation alternating and ending with the new user message.
        let mut trimmed = false;
        while messages.len() > MAX_MESSAGES
            || messages
                .iter()
                .map(|m| m["content"].as_str().unwrap_or("").len())
                .sum::<usize>()
                > MAX_BYTES
        {
            messages.drain(..2);
            trimmed = true;
        }
        if trimmed {
            println!("[Older turns omitted to limit context cost]");
        }
        match response(&http, &base, &messages).await {
            Ok(answer) => messages.push(json!({"role": "assistant", "content": answer})),
            Err(error) => {
                eprintln!("\n{error}\n");
                // Drop the failed user message: only completed turns enter history.
                messages.pop();
            }
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    // `#[tokio::main]` starts the async runtime, so `main` can `.await`.
    // `select!` runs both branches concurrently and takes whichever finishes
    // first: a chat error exits(1), Ctrl-C prints and exits(0).
    tokio::select! {
        result = run() => if let Err(error) = result { eprintln!("{error}"); std::process::exit(1); },
        _ = tokio::signal::ctrl_c() => eprintln!("\nCancelled."),
    }
}
