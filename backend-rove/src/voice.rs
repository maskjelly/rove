use super::*;
use axum::extract::Multipart;
use backend_rove::SseDecoder;
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::Value;
use tokio::{sync::mpsc, time::Instant};

struct Worker(tokio::task::JoinHandle<()>);
impl Drop for Worker {
    fn drop(&mut self) { self.0.abort(); }
}

fn event(value: Value) -> Result<Event, Infallible> {
    Ok(Event::default().data(value.to_string()))
}
fn failed(message: &str) -> Result<Event, Infallible> {
    event(json!({"type":"error","message":message}))
}

// Dispatch complete phrases early; keep each TTS request comfortably below its limit.
fn phrase(buffer: &mut String, finish: bool) -> Option<String> {
    let boundary = buffer.char_indices().find_map(|(index, character)| {
        if (index >= 35 && matches!(character, '.' | '!' | '?' | '\n'))
            || (index >= 180 && character.is_whitespace()) || index >= 400
        { Some(index + character.len_utf8()) } else { None }
    });
    let end = boundary.or_else(|| (finish && !buffer.is_empty()).then_some(buffer.len()))?;
    let text: String = buffer.drain(..end).collect();
    Some(text)
}

async fn speak(state: AppState, mut phrases: mpsc::UnboundedReceiver<String>, output: mpsc::Sender<Value>, started: Instant) {
    let base = state.endpoint.trim_end_matches("/responses");
    let mut first = true;
    let mut segment = 0;
    while let Some(text) = phrases.recv().await {
        if text.trim().is_empty() { continue; }
        let request = state.http.post(format!("{base}/audio/speech"))
            .bearer_auth(state.key.as_deref().unwrap_or(""))
            .json(&json!({"model":"gpt-4o-mini-tts","voice":"coral","input":text,
                "response_format":"pcm", "instructions":"Speak naturally and clearly, at a conversational pace."}))
            .send().await;
        let mut response = match request {
            Ok(response) if response.status().is_success() => response,
            _ => { let _ = output.send(json!({"type":"voice.error","message":"Speech generation failed. The text answer is still available."})).await; return; }
        };
        loop {
            match response.chunk().await {
                Ok(Some(bytes)) => {
                    if first {
                        first = false;
                        if output.send(json!({"type":"voice.first_audio","server_ms":started.elapsed().as_millis()})).await.is_err() { return; }
                    }
                    if output.send(json!({"type":"voice.audio.delta","audio":STANDARD.encode(&bytes),"sample_rate":24000,"segment":segment})).await.is_err() { return; }
                }
                Ok(None) => break,
                Err(_) => { let _ = output.send(json!({"type":"voice.error","message":"Speech stream interrupted."})).await; return; }
            }
        }
        segment += 1;
    }
}

pub(super) async fn voice(State(state): State<AppState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, mut form: Multipart) -> Response {
    if !peer.ip().is_loopback() { return error(StatusCode::FORBIDDEN, "Use the public HTTPS page."); }
    let Some(key) = state.key.clone() else { return error(StatusCode::SERVICE_UNAVAILABLE,"Server API key is not configured."); };
    let Ok(permit) = state.slots.clone().try_acquire_owned() else { return error(StatusCode::TOO_MANY_REQUESTS,"The server is busy. Try again shortly."); };
    let mut audio = None;
    let mut mime = String::new();
    let mut history: Vec<Message> = Vec::new();
    loop {
        let field = match form.next_field().await { Ok(Some(field)) => field, Ok(None) => break, Err(_) => return error(StatusCode::BAD_REQUEST,"Invalid audio upload.") };
        match field.name() {
            Some("audio") => {
                mime = field.content_type().unwrap_or("audio/webm").split(';').next().unwrap_or("").to_string();
                audio = match field.bytes().await { Ok(bytes) if !bytes.is_empty() && bytes.len() <= 5*1024*1024 => Some(bytes), _ => return error(StatusCode::BAD_REQUEST,"Use a recording under 5 MB.") };
            }
            Some("messages") => {
                let data = match field.text().await { Ok(text) if text.len() <= 128_000 => text, _ => return error(StatusCode::BAD_REQUEST,"Invalid history.") };
                history = match serde_json::from_str(&data) { Ok(value) => value, Err(_) => return error(StatusCode::BAD_REQUEST,"Invalid history.") };
            }
            _ => return error(StatusCode::BAD_REQUEST,"Unexpected upload field."),
        }
    }
    let Some(audio) = audio else { return error(StatusCode::BAD_REQUEST,"No audio was uploaded."); };
    let extension = match mime.as_str() { "audio/webm" | "video/webm" => "webm", "audio/mp4" | "video/mp4" => "mp4", "audio/wav" | "audio/x-wav" => "wav", "audio/ogg" => "ogg", "audio/mpeg" => "mp3", _ => return error(StatusCode::BAD_REQUEST,"Unsupported audio format.") };
    if history.len() > 20 || history.iter().any(|m| !matches!(m.role.as_str(),"user"|"assistant") || m.content.trim().is_empty()) || history.iter().map(|m|m.content.len()).sum::<usize>() > 20_000 {
        return error(StatusCode::BAD_REQUEST,"Conversation history is too large or invalid.");
    }
    let stream = async_stream::stream! {
        let _permit = permit;
        let started = Instant::now();
        let base = state.endpoint.trim_end_matches("/responses");
        yield event(json!({"type":"voice.stage","stage":"transcribing"}));
        let file = reqwest::multipart::Part::bytes(audio.to_vec()).file_name(format!("recording.{extension}")).mime_str(&mime).expect("validated MIME");
        let request = state.http.post(format!("{base}/audio/transcriptions")).bearer_auth(&key)
            .multipart(reqwest::multipart::Form::new().part("file",file).text("model","gpt-4o-mini-transcribe").text("response_format","json").text("stream","true"))
            .send().await;
        let mut asr = match request { Ok(response) if response.status().is_success() => response, _ => { yield failed("Transcription failed. Check audio model access or try again."); return; } };
        let mut decoder = SseDecoder::default();
        let mut transcript = String::new();
        let mut asr_done = false;
        let mut raw = Vec::new();
        while let Ok(Some(bytes)) = asr.chunk().await {
            if raw.len() < 512_000 { raw.extend_from_slice(&bytes); }
            let events = match decoder.push(&bytes) { Ok(events) => events, Err(_) => { yield failed("Invalid transcription stream."); return; } };
            for data in events {
                let value: Value = match serde_json::from_str(&data) { Ok(value) => value, Err(_) => continue };
                match value["type"].as_str() {
                    Some("transcript.text.delta") => { let delta = value["delta"].as_str().unwrap_or(""); transcript.push_str(delta); yield event(json!({"type":"voice.transcript.delta","delta":delta})); }
                    Some("transcript.text.done") => { if let Some(text) = value["text"].as_str() { transcript = text.into(); } asr_done = true; }
                    Some("error") => { yield failed("Transcription failed."); return; }
                    _ => {
                        // Non-streaming JSON fallback: {"text":"..."}.
                        if !asr_done {
                            if let Some(text) = value.get("text").and_then(|t| t.as_str()) {
                                transcript = text.into();
                                asr_done = true;
                            }
                        }
                    }
                }
            }
            if asr_done { break; }
        }
        if !asr_done {
            // Single-shot JSON body without SSE framing.
            if let Ok(value) = serde_json::from_slice::<Value>(&raw) {
                if let Some(text) = value.get("text").and_then(|t| t.as_str()) {
                    transcript = text.into();
                    asr_done = true;
                }
            }
        }
        if !asr_done || transcript.trim().is_empty() || transcript.len() > 4000 { yield failed("No usable speech was transcribed. Try a shorter, clear recording."); return; }
        yield event(json!({"type":"voice.transcribed","text":transcript,"asr_ms":started.elapsed().as_millis()}));
        let mut input: Vec<Value> = history.into_iter().map(|m| json!({"role":m.role,"content":m.content})).collect();
        input.push(json!({"role":"user","content":transcript}));
        let request = state.http.post(&state.endpoint).bearer_auth(&key).json(&json!({
            "model":state.model,"input":input,"stream":true,"store":false,"reasoning":{"effort":"low","summary":"auto"},"max_output_tokens":2048,
            "instructions":"You are Rove, a helpful voice assistant. Reply concisely in natural spoken sentences. Start with a short useful sentence. Avoid markdown and long lists. You have no shell or file tools; never claim to run commands."
        })).send().await;
        let mut llm = match request { Ok(response) if response.status().is_success() => response, _ => { yield failed("The model request failed. Please try again."); return; } };
        let (text_tx, text_rx) = mpsc::unbounded_channel();
        let (audio_tx, mut audio_rx) = mpsc::channel(32);
        let _worker = Worker(tokio::spawn(speak(state.clone(),text_rx,audio_tx,started)));
        let mut text_tx = Some(text_tx);
        let mut llm_done = false;
        let mut audio_done = false;
        let mut decoder = SseDecoder::default();
        let mut pending = String::new();
        let deadline = tokio::time::sleep(Duration::from_secs(180));
        tokio::pin!(deadline);
        loop {
            if llm_done && audio_done { break; }
            tokio::select! {
                _ = &mut deadline => { yield failed("Voice request timed out."); return; }
                chunk = llm.chunk(), if !llm_done => {
                    let bytes = match chunk { Ok(Some(bytes)) => bytes, _ => { yield failed("The model stream ended early."); return; } };
                    let events = match decoder.push(&bytes) { Ok(events) => events, Err(_) => { yield failed("Invalid model stream."); return; } };
                    for data in events {
                        let value: Value = match serde_json::from_str(&data) { Ok(value) => value, Err(_) => continue };
                        let kind = value["type"].as_str().unwrap_or("");
                        if matches!(kind,"response.output_text.delta"|"response.refusal.delta") {
                            pending.push_str(value["delta"].as_str().unwrap_or(""));
                            while let Some(part) = phrase(&mut pending,false) { if let Some(tx) = &text_tx { let _ = tx.send(part); } }
                        }
                        if kind == "response.completed" {
                            while let Some(part) = phrase(&mut pending,true) { if let Some(tx) = &text_tx { let _ = tx.send(part); } }
                            text_tx.take();
                            llm_done = true;
                        }
                        let failed = matches!(kind,"response.failed"|"response.incomplete"|"error");
                        yield event(value);
                        if failed { return; }
                    }
                }
                audio = audio_rx.recv(), if !audio_done => {
                    match audio { Some(value) => yield event(value), None => audio_done = true }
                }
            }
        }
        yield event(json!({"type":"voice.completed","server_ms":started.elapsed().as_millis()}));
    };
    Sse::new(stream).keep_alive(axum::response::sse::KeepAlive::new().interval(Duration::from_secs(5))).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn phrases_preserve_unicode_and_order() {
        let original = "Hello 🙂 this is a sufficiently long first sentence. Another short one.";
        let mut pending = original.to_string();
        let first = phrase(&mut pending,false).unwrap();
        assert!(first.ends_with('.'));
        let rest = phrase(&mut pending,true).unwrap();
        assert_eq!(first+&rest,original);
        assert!(phrase(&mut pending,true).is_none());
    }
}
