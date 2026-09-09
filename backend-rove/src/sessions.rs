//! Server-side voice session persistence.
//!
//! The browser stays thin: it only records audio. Every completed voice turn
//! (transcript + text answer + timings + models) is saved here under a short
//! client-chosen id, so the same conversation can be resumed from another
//! device via a `?s=<id>` link. One small JSON file per session; old
//! sessions are pruned.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Serialize, Deserialize)]
pub struct SessionMessage {
    pub role: String,
    pub content: String,
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct SessionTurn {
    #[serde(default)]
    pub transcript: String,
    #[serde(default)]
    pub answer: String,
    #[serde(default)]
    pub asr_model: String,
    #[serde(default)]
    pub chat_model: String,
    #[serde(default)]
    pub input_tokens: Option<u64>,
    #[serde(default)]
    pub output_tokens: Option<u64>,
    /// Server stopwatch (handler entry = 0).
    #[serde(default)]
    pub server_ms: u64,
    #[serde(default)]
    pub asr_ms: Option<u64>,
    #[serde(default)]
    pub llm_first_ms: Option<u64>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Session {
    pub id: String,
    pub updated_unix_ms: u64,
    pub messages: Vec<SessionMessage>,
    pub turns: Vec<SessionTurn>,
}

pub const MAX_TURNS: usize = 20;
const MAX_FILE_BYTES: usize = 12 * 1024 * 1024;

pub fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

fn path_for(dir: &Path, name: &str) -> PathBuf {
    dir.join(format!("{name}.json"))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Fallback id when the client did not supply one. 16 bytes from the OS
/// CSPRNG, hex-encoded: session links are bearer secrets (whoever holds the
/// id can read the transcript), so they must not be enumerable.
pub fn new_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    if let Ok(file) = std::fs::File::open("/dev/urandom") {
        use std::io::Read;
        let mut bytes = [0u8; 16];
        if file.take(16).read_exact(&mut bytes).is_ok() {
            return bytes.iter().map(|b| format!("{b:02x}")).collect();
        }
    }
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    (now_ms(), std::process::id(), n).hash(&mut h);
    format!("fb{:016x}", h.finish())
}

pub fn blank(id: String) -> Session {
    Session {
        id,
        updated_unix_ms: now_ms(),
        messages: Vec::new(),
        turns: Vec::new(),
    }
}

pub async fn load(dir: &Path, id: &str) -> Option<Session> {
    if !valid_id(id) {
        return None;
    }
    let data = tokio::fs::read(path_for(dir, id)).await.ok()?;
    if data.len() > MAX_FILE_BYTES {
        return None;
    }
    let session: Session = serde_json::from_slice(&data).ok()?;
    // A file whose stored id differs from its filename must never be trusted
    // (prevents cross-id overwrites on re-save).
    if session.id != id {
        return None;
    }
    Some(session)
}

pub async fn save(dir: &Path, session: &mut Session) -> bool {
    if !valid_id(&session.id) {
        return false;
    }
    session.updated_unix_ms = now_ms();
    if session.turns.len() > MAX_TURNS {
        let drop = session.turns.len() - MAX_TURNS;
        session.turns.drain(..drop);
    }
    if session.messages.len() > 41 {
        let drop = session.messages.len() - 41;
        session.messages.drain(..drop);
    }
    if tokio::fs::create_dir_all(dir).await.is_err() {
        eprintln!("sessions: cannot create dir {}", dir.display());
        return false;
    }
    let Ok(data) = serde_json::to_vec(session) else {
        return false;
    };
    if data.len() > MAX_FILE_BYTES {
        return false;
    }
    // Hidden tmp name without a .json extension: never matches the prune glob
    // and never reachable via GET /session/{id}.
    let tmp = dir.join(format!(".{}.tmp", session.id));
    if tokio::fs::write(&tmp, &data).await.is_err() {
        eprintln!("sessions: cannot write {}", tmp.display());
        return false;
    }
    if tokio::fs::rename(&tmp, path_for(dir, &session.id))
        .await
        .is_err()
    {
        let _ = tokio::fs::remove_file(&tmp).await;
        return false;
    }
    // Prune off the hot path: stale check runs at most ~1 in 20 saves.
    if now_ms() % 20 == 0 {
        prune(dir).await;
    }
    true
}

async fn prune(dir: &Path) {
    let Ok(mut entries) = tokio::fs::read_dir(dir).await else {
        return;
    };
    let mut files: Vec<(u64, PathBuf)> = Vec::new();
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        // Crash leftovers from atomic saves: hidden, no .json extension.
        if name.starts_with('.') && name.ends_with(".tmp") {
            let stale = tokio::fs::metadata(&path)
                .await
                .ok()
                .and_then(|m| m.modified().ok())
                .and_then(|t| t.elapsed().ok())
                .map(|d| d.as_secs())
                .unwrap_or(0);
            if stale > 3600 {
                let _ = tokio::fs::remove_file(&path).await;
            }
            continue;
        }
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let age = tokio::fs::metadata(&path)
            .await
            .ok()
            .and_then(|m| m.modified().ok())
            .and_then(|t| t.elapsed().ok())
            .map(|d| d.as_secs())
            .unwrap_or(0);
        // Drop sessions untouched for 7 days.
        if age > 7 * 24 * 3600 {
            let _ = tokio::fs::remove_file(&path).await;
            continue;
        }
        files.push((age, path));
    }
    // Keep at most 200 sessions, preferring recently touched ones:
    // larger age = older, so evict from the oldest end.
    if files.len() > 200 {
        files.sort_by_key(|(age, _)| std::cmp::Reverse(*age));
        for (_, path) in files.iter().take(files.len() - 200) {
            let _ = tokio::fs::remove_file(path).await;
        }
    }
}

/// Loopback-only: fetch a saved session to resume it on another device.
pub(super) async fn get(
    axum::extract::State(state): axum::extract::State<super::AppState>,
    axum::extract::ConnectInfo(peer): axum::extract::ConnectInfo<std::net::SocketAddr>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> super::Response {
    use axum::response::IntoResponse;
    if !peer.ip().is_loopback() {
        return super::error(
            axum::http::StatusCode::FORBIDDEN,
            "Open the resume link over HTTPS instead.",
        );
    }
    match load(&state.sessions, id.trim()).await {
        Some(session) => axum::Json(session).into_response(),
        None => super::error(
            axum::http::StatusCode::NOT_FOUND,
            "Unknown or expired session link.",
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn accepts_safe_ids_only() {
        assert!(valid_id("abc-123_XYZ"));
        assert!(!valid_id(""));
        assert!(!valid_id("../evil"));
        assert!(!valid_id("a/b"));
        assert!(!valid_id(&"x".repeat(65)));
    }
}
