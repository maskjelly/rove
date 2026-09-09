#[tokio::main]
async fn main() {
    let base = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "http://45.196.196.251:3000".to_string());
    let url = format!("{}/events", base.trim_end_matches('/'));

    println!("Connecting to {url} ...");

    let mut resp = match reqwest::get(&url).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("request failed: {e}");
            return;
        }
    };

    if !resp.status().is_success() {
        eprintln!("server returned: {}", resp.status());
        return;
    }

    println!("Connected, waiting for events (Ctrl-C to quit)...");

    let mut buf = String::new();
    loop {
        let chunk = match resp.chunk().await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("stream error: {e}");
                break;
            }
        };
        let Some(bytes) = chunk else {
            println!("server closed connection");
            break;
        };
        buf.push_str(&String::from_utf8_lossy(&bytes));

        // SSE events end with \n\n — print complete ones, keep partial in buf
        while let Some(pos) = buf.find("\n\n") {
            let event: String = buf.drain(..=pos + 1).collect();
            for line in event.lines() {
                if let Some(data) = line.strip_prefix("data:") {
                    println!("{}", data.trim());
                } else if !line.is_empty() && line != ":" {
                    println!("{line}");
                }
            }
        }
    }
}
