//! Shared bits used by both binaries (`server` and `client`).
//!
//! Rust lesson: a `lib.rs` file becomes a *library crate* that the binaries
//! import with `use backend_rove::...` (the name comes from Cargo.toml's
//! `[package] name`). Code here is compiled once and shared.

/// Conversation budget, enforced by the server and mirrored by the terminal
/// client so valid input is never rejected.
///
/// Rust lesson: `pub const` is a compile-time constant — no memory address,
/// inlined wherever used. `usize` is the pointer-sized integer type, the
/// natural choice for counts and lengths.
pub const MAX_MESSAGES: usize = 21;
pub const MAX_BYTES: usize = 24_000;

/// Incremental SSE decoder. Buffer bytes until a complete event so UTF-8 split
/// across network chunks stays intact. Supports LF, CRLF and multiline data.
///
/// Rust lesson: `#[derive(Default)]` auto-generates `SseDecoder::default()`,
/// which fills every field with its type's default (`Vec::new()` here).
/// `Vec<u8>` is a growable byte buffer owned by the struct.
#[derive(Default)]
pub struct SseDecoder {
    // Bytes that arrived but don't form a complete line yet.
    pending: Vec<u8>,
    // `data:` lines of the event currently being assembled.
    lines: Vec<String>,
}

impl SseDecoder {
    /// Feed raw network bytes in; get back zero or more complete event payloads.
    ///
    /// Rust lesson: `&mut self` means this method may mutate the struct.
    /// `&[u8]` is a borrowed *slice* — the caller keeps ownership, we only
    /// look at the bytes. `Result<_, String>` forces callers to handle the
    /// error case; there are no exceptions in Rust.
    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<String>, String> {
        self.pending.extend_from_slice(bytes);
        if self.pending.len() > 2 * 1024 * 1024 {
            return Err("SSE line exceeded 2 MB".into());
        }
        let mut events = Vec::new();
        // Scan for `\n`. Each line is drained out of `pending` so the buffer
        // only ever holds an incomplete tail.
        while let Some(end) = self.pending.iter().position(|b| *b == b'\n') {
            // `..=end` is an *inclusive* range: it takes the `\n` too.
            // `drain` removes *and* returns those elements.
            let raw: Vec<u8> = self.pending.drain(..=end).collect();
            // A multi-byte character (e.g. emoji) may straddle two network
            // chunks, so only decode lines known to be complete. `&raw[..len-1]`
            // drops the trailing `\n` before validating UTF-8.
            let line = std::str::from_utf8(&raw[..raw.len() - 1])
                .map_err(|_| "Invalid UTF-8 in SSE stream")?
                // `?` returns early with `Err(...)` on failure, and unwraps
                // the `Ok` value otherwise. It only works in functions that
                // themselves return `Result` (or `Option`).
                .trim_end_matches('\r');
            if line.is_empty() {
                // A blank line terminates one SSE event: join any accumulated
                // `data:` lines with `\n` (the spec allows multi-line data).
                if !self.lines.is_empty() {
                    events.push(self.lines.join("\n"));
                    self.lines.clear();
                }
            } else if let Some(data) = line.strip_prefix("data:") {
                // `strip_prefix` returns `Some(rest)` or `None`; `if let`
                // runs its body only on `Some` and binds the inner value.
                // SSE allows an optional single space after the colon.
                self.lines
                    .push(data.strip_prefix(' ').unwrap_or(data).into());
            }
            // Any other line (`: comments`, `event:` fields) is ignored.
        }
        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    // `#[cfg(test)]` compiles this module only for `cargo test`, never into
    // the real binaries. `use super::*` imports the items under test.
    use super::*;

    #[test]
    fn decodes_split_unicode_crlf_and_multiline() {
        let input =
            ": keepalive\r\nevent: message\r\ndata: hé🙂\r\ndata: world\r\n\r\ndata: next\n\n";
        let mut parser = SseDecoder::default();
        let mut events = Vec::new();
        // Feed one byte at a time: the harshest possible network chunking.
        // `&[*byte]` builds a one-element slice from the loop variable.
        for byte in input.as_bytes() {
            events.extend(parser.push(&[*byte]).unwrap());
        }
        assert_eq!(events, vec!["hé🙂\nworld", "next"]);
    }
}
