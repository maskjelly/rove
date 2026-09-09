use super::*;
use axum::extract::Multipart;
use backend_rove::SseDecoder;
use serde_json::Value;
use tokio::time::Instant;

const ASR_MODEL: &str = "gpt-4o-mini-transcribe";

fn event(value: Value) -> Result<Event, Infallible> {
    Ok(Event::default().data(value.to_string()))
}
fn failed(message: &str) -> Result<Event, Infallible> {
    event(json!({"type":"error","message":message}))
}

pub(super) async fn voice(State(state): State<AppState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, mut form: Multipart) -> Response {
    // Handler entry is t=0 for every server_ms below, so timings include
    // the multipart upload parse instead of hiding it.
    let received = Instant::now();
    if !peer.ip().is_loopback() { return error(StatusCode::FORBIDDEN, "Use the public HTTPS page."); }
    let Some(key) = state.key.clone() else { return error(StatusCode::SERVICE_UNAVAILABLE,"Server API key is not configured."); };
    let Ok(permit) = state.slots.clone().try_acquire_owned() else {
        return (
            [(header::RETRY_AFTER, "2")],
            error(StatusCode::TOO_MANY_REQUESTS, "The server is busy. Try again shortly."),
        )
            .into_response();
    };
    let mut audio = None;
    let mut mime = String::new();
    let mut history: Vec<Message> = Vec::new();
    let mut resume: Option<String> = None;
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
            Some("session") => {
                let data = match field.text().await { Ok(text) if text.len() <= 64 => text, _ => return error(StatusCode::BAD_REQUEST,"Invalid session.") };
                let id = data.trim().to_string();
                if !id.is_empty() {
                    if !sessions::valid_id(&id) { return error(StatusCode::BAD_REQUEST,"Invalid session."); }
                    resume = Some(id);
                }
            }
            _ => return error(StatusCode::BAD_REQUEST,"Unexpected upload field."),
        }
    }
    let Some(audio) = audio else { return error(StatusCode::BAD_REQUEST,"No audio was uploaded."); };
    let extension = match mime.as_str() { "audio/webm" | "video/webm" => "webm", "audio/mp4" | "video/mp4" => "mp4", "audio/wav" | "audio/x-wav" => "wav", "audio/ogg" => "ogg", "audio/mpeg" => "mp3", _ => return error(StatusCode::BAD_REQUEST,"Unsupported audio format.") };
    // The server copy is the source of truth when resuming: any device that
    // sends a session id continues the same conversation.
    let dir = state.sessions.clone();
    let mut session = match resume {
        Some(id) => sessions::load(&dir, &id).await.unwrap_or_else(|| sessions::blank(id)),
        None => sessions::blank(sessions::new_id()),
    };
    let base_messages: Vec<Message> = if session.messages.is_empty() {
        history
    } else {
        session.messages.iter().map(|m| Message { role: m.role.clone(), content: m.content.clone() }).collect()
    };
    if base_messages.len() > 20 || base_messages.iter().any(|m| !matches!(m.role.as_str(),"user"|"assistant") || m.content.trim().is_empty()) || base_messages.iter().map(|m|m.content.len()).sum::<usize>() > 20_000 {
        return error(StatusCode::BAD_REQUEST,"Conversation history is too large or invalid.");
    }
    let upload_bytes = audio.len();
    let upload_mime = mime.clone();
    let chat_model = state.model.clone();
    let parse_ms = received.elapsed().as_millis() as u64;
    // First turn in a session: seed the stored history with the context the
    // client sent, so prior text turns survive the resume.
    if session.messages.is_empty() {
        for m in &base_messages {
            session.messages.push(sessions::SessionMessage { role: m.role.clone(), content: m.content.clone() });
        }
    }
    let stream = async_stream::stream! {
        let _permit = permit;
        let started = received;
        let base = state.endpoint.trim_end_matches("/responses");
        yield event(json!({"type":"voice.started","session":session.id,"asr_model":ASR_MODEL,"chat_model":chat_model,"upload_bytes":upload_bytes,"upload_mime":upload_mime,"parse_ms":parse_ms}));
        yield event(json!({"type":"voice.stage","stage":"transcribing"}));
        let file = reqwest::multipart::Part::bytes(audio.to_vec()).file_name(format!("recording.{extension}")).mime_str(&mime).expect("validated MIME");
        let request = state.http.post(format!("{base}/audio/transcriptions")).bearer_auth(&key)
            .multipart(reqwest::multipart::Form::new().part("file",file).text("model",ASR_MODEL).text("response_format","json").text("stream","true"))
            .send().await;
        let mut asr = match request { Ok(response) if response.status().is_success() => response, _ => { yield failed("Transcription failed. Check audio model access or try again."); return; } };
        let mut decoder = SseDecoder::default();
        let mut transcript = String::new();
        let mut asr_done = false;
        let mut raw = Vec::new();
        // ASR gets its own 60s stall budget: a hung transcription must fail
        // loudly instead of wedging one of the 8 server slots forever.
        loop {
            let chunk = match tokio::time::timeout(Duration::from_secs(60), asr.chunk()).await {
                Ok(Ok(Some(bytes))) => bytes,
                Ok(Ok(None)) => break,
                Ok(Err(_)) => { yield failed("Transcription stream broke. Check your connection and try again."); return; }
                Err(_) => { yield failed("Transcription stalled. Try a shorter, clear recording."); return; }
            };
            if raw.len() < 512_000 { raw.extend_from_slice(&chunk); }
            let events = match decoder.push(&chunk) { Ok(events) => events, Err(_) => { yield failed("Invalid transcription stream."); return; } };
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
        let asr_ms = started.elapsed().as_millis() as u64;
        yield event(json!({"type":"voice.transcribed","text":transcript,"asr_ms":asr_ms}));
        let mut input: Vec<Value> = base_messages.into_iter().map(|m| json!({"role":m.role,"content":m.content})).collect();
        input.push(json!({"role":"user","content":transcript}));
        let request = state.http.post(&state.endpoint).bearer_auth(&key).json(&json!({
            "model":state.model,"input":input,"stream":true,"store":false,"reasoning":{"effort":"low","summary":"auto"},"max_output_tokens":2048,
            "instructions":"You are Rove, a helpful voice assistant. The user spoke; reply concisely in plain sentences. Avoid markdown and long lists. You have no shell or file tools; never claim to run commands."
        })).send().await;
        let mut llm = match request { Ok(response) if response.status().is_success() => response, _ => { yield failed("The model request failed. Please try again."); return; } };
        let mut llm_first_sent = false;
        let mut llm_first_ms: Option<u64> = None;
        let mut answer_chars: usize = 0;
        let mut answer_text = String::new();
        let mut in_tokens: Option<u64> = None;
        let mut out_tokens: Option<u64> = None;
        let mut decoder = SseDecoder::default();
        // The 180s LLM budget starts when the model request is sent, not at
        // handler entry: slow uploads and ASR must not steal model time.
        let deadline = tokio::time::sleep(Duration::from_secs(180));
        tokio::pin!(deadline);
        let mut llm_done = false;
        loop {
            if llm_done { break; }
            tokio::select! {
                _ = &mut deadline => { yield failed("Voice request timed out."); return; }
                chunk = llm.chunk() => {
                    let bytes = match chunk { Ok(Some(bytes)) => bytes, Ok(None) => { llm_done = true; continue; }, _ => { yield failed("The model stream ended early."); return; } };
                    let events = match decoder.push(&bytes) { Ok(events) => events, Err(_) => { yield failed("Invalid model stream."); return; } };
                    for data in events {
                        let value: Value = match serde_json::from_str(&data) { Ok(value) => value, Err(_) => continue };
                        let kind = value["type"].as_str().unwrap_or("");
                        if matches!(kind,"response.output_text.delta"|"response.refusal.delta") {
                            let delta = value["delta"].as_str().unwrap_or("");
                            answer_chars += delta.len();
                            answer_text.push_str(delta);
                            if !llm_first_sent {
                                llm_first_sent = true;
                                llm_first_ms = Some(started.elapsed().as_millis() as u64);
                                yield event(json!({"type":"voice.llm_first","server_ms":llm_first_ms}));
                            }
                        }
                        if kind == "response.completed" {
                            if let Some(usage) = value.get("response").and_then(|r| r.get("usage")) {
                                in_tokens = usage.get("input_tokens").and_then(|v| v.as_u64());
                                out_tokens = usage.get("output_tokens").and_then(|v| v.as_u64());
                            }
                            llm_done = true;
                        }
                        let failed = matches!(kind,"response.failed"|"response.incomplete"|"error");
                        yield event(value);
                        if failed { return; }
                    }
                }
            }
        }
        let done_ms = started.elapsed().as_millis() as u64;
        session.messages.push(sessions::SessionMessage { role: "user".into(), content: transcript.clone() });
        session.messages.push(sessions::SessionMessage { role: "assistant".into(), content: answer_text.clone() });
        session.turns.push(sessions::SessionTurn {
            transcript: transcript.clone(),
            answer: answer_text,
            asr_model: ASR_MODEL.into(),
            chat_model: chat_model.clone(),
            input_tokens: in_tokens,
            output_tokens: out_tokens,
            server_ms: done_ms,
            asr_ms: Some(asr_ms),
            llm_first_ms,
        });
        let persisted = sessions::save(&dir, &mut session).await;
        yield event(json!({"type":"voice.session","session":session.id}));
        yield event(json!({"type":"voice.completed","session":session.id,"server_ms":done_ms,"persisted":persisted,
            "chat_model":chat_model,"asr_model":ASR_MODEL,
            "upload_bytes":upload_bytes,"transcript_chars":transcript.len(),"answer_chars":answer_chars}));
    };
    (
        [(
            axum::http::HeaderName::from_static("x-accel-buffering"),
            "no",
        )],
        Sse::new(stream).keep_alive(axum::response::sse::KeepAlive::new().interval(Duration::from_secs(5))).into_response(),
    )
        .into_response()
}
