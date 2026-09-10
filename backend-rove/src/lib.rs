/// Incremental SSE decoder. Buffer bytes until a complete event so UTF-8 split
/// across network chunks stays intact. Supports LF, CRLF and multiline data.
/// Shared conversation budget, enforced by the server and mirrored by the
/// terminal client so valid input is never rejected.
pub const MAX_MESSAGES: usize = 21;
pub const MAX_BYTES: usize = 24_000;

#[derive(Default)]
pub struct SseDecoder {
    pending: Vec<u8>,
    lines: Vec<String>,
}

impl SseDecoder {
    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<String>, String> {
        self.pending.extend_from_slice(bytes);
        if self.pending.len() > 2 * 1024 * 1024 {
            return Err("SSE line exceeded 2 MB".into());
        }
        let mut events = Vec::new();
        while let Some(end) = self.pending.iter().position(|b| *b == b'\n') {
            let raw: Vec<u8> = self.pending.drain(..=end).collect();
            let line = std::str::from_utf8(&raw[..raw.len() - 1])
                .map_err(|_| "Invalid UTF-8 in SSE stream")?
                .trim_end_matches('\r');
            if line.is_empty() {
                if !self.lines.is_empty() {
                    events.push(self.lines.join("\n"));
                    self.lines.clear();
                }
            } else if let Some(data) = line.strip_prefix("data:") {
                self.lines
                    .push(data.strip_prefix(' ').unwrap_or(data).into());
            }
        }
        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn decodes_split_unicode_crlf_and_multiline() {
        let input =
            ": keepalive\r\nevent: message\r\ndata: hé🙂\r\ndata: world\r\n\r\ndata: next\n\n";
        let mut parser = SseDecoder::default();
        let mut events = Vec::new();
        for byte in input.as_bytes() {
            events.extend(parser.push(&[*byte]).unwrap());
        }
        assert_eq!(events, vec!["hé🙂\nworld", "next"]);
    }
}
