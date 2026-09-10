use backend_rove::{MAX_BYTES, MAX_MESSAGES, SseDecoder};
use serde_json::{Value, json};
use std::{
    error::Error,
    io::{self, Write},
    time::Duration,
};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

async fn connect(http: &reqwest::Client) -> Result<String> {
    let base = std::env::args()
        .nth(1)
        .map(|b| b.trim_end_matches('/').to_string())
        .unwrap_or_else(|| "https://45.196.196.251".to_string());
    for _ in 0..100 {
        if let Ok(response) = http
            .get(format!("{base}/health"))
            .timeout(Duration::from_secs(1))
            .send()
            .await
            && response.status().is_success()
        {
            return Ok(base);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(format!("Server did not become reachable at {base}.").into())
}

async fn response(http: &reqwest::Client, base: &str, messages: &[Value]) -> Result<String> {
    let mut upstream = http
        .post(format!("{base}/chat"))
        .json(&json!({"messages": messages}))
        .send()
        .await?;
    if !upstream.status().is_success() {
        let status = upstream.status();
        let details: Value = upstream.json().await.unwrap_or_default();
        return Err(format!(
            "{status}: {}",
            details["error"].as_str().unwrap_or("Request failed")
        )
        .into());
    }
    let mut parser = SseDecoder::default();
    let mut answer = String::new();
    let mut section = "";
    let mut completed = false;
    while let Some(bytes) = upstream.chunk().await? {
        for data in parser.push(&bytes)? {
            if data == "[DONE]" {
                continue;
            }
            let event: Value = serde_json::from_str(&data)?;
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
                _ => {}
            }
            io::stdout().flush()?;
            if completed {
                break;
            }
        }
        if completed {
            break;
        }
    }
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
    let base = connect(&http).await?;
    println!("Rove chat · /new clears history · /exit quits · Ctrl-C cancels and exits");
    println!(
        "Conversation stays in this client session. Recent turns are sent with each message.\n"
    );
    let mut messages: Vec<Value> = Vec::new();
    loop {
        print!("You > ");
        io::stdout().flush()?;
        // A detached input thread lets Ctrl-C cancel even while waiting at the prompt.
        let (sender, receiver) = tokio::sync::oneshot::channel();
        std::thread::spawn(move || {
            let mut input = String::new();
            let result = io::stdin()
                .read_line(&mut input)
                .map(|count| (count, input));
            let _ = sender.send(result);
        });
        let (count, input) = receiver.await??;
        if count == 0 {
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
                messages.pop();
            }
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    tokio::select! {
        result = run() => if let Err(error) = result { eprintln!("{error}"); std::process::exit(1); },
        _ = tokio::signal::ctrl_c() => eprintln!("\nCancelled."),
    }
}
