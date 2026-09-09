import { readSse } from './stream.mjs?v={{ASSET_HASH}}';

// Server does ASR + LLM. Browser only records audio.
// All latency numbers are browser-observed unless marked server_ms.

// Best recording mime for POST /voice multipart upload.
export function pickMime() {
  if (typeof MediaRecorder === 'undefined' || !MediaRecorder.isTypeSupported) return '';
  for (const mime of ['audio/webm;codecs=opus', 'audio/webm', 'audio/mp4', 'audio/ogg;codecs=opus', 'audio/wav']) {
    try { if (MediaRecorder.isTypeSupported(mime)) return mime; } catch { /* keep trying */ }
  }
  return '';
}

export function extensionFor(mime) {
  if (mime.includes('mp4')) return 'mp4';
  if (mime.includes('wav')) return 'wav';
  if (mime.includes('ogg')) return 'ogg';
  if (mime.includes('mpeg') || mime.includes('mp3')) return 'mp3';
  return 'webm';
}

// POST multipart {audio, messages, session?} to /voice, yield parsed SSE events.
// onOpen fires when response headers arrive = upload fully received by server.
export async function postVoice(blob, messages, { signal, session, onEvent, onOpen }) {
  const form = new FormData();
  const mime = blob.type || 'audio/webm';
  form.append('audio', blob, `recording.${extensionFor(mime)}`);
  form.append('messages', JSON.stringify(messages));
  if (session) form.append('session', session);
  const response = await fetch('/voice', { method: 'POST', body: form, signal });
  if (!response.ok) {
    let detail = '';
    try { detail = (await response.json()).error || ''; } catch { /* fall through */ }
    throw new Error(detail || `Voice request failed (${response.status}). Please try again.`);
  }
  if (!response.body) throw new Error('Your browser could not open the voice stream.');
  onOpen?.();
  for await (const item of readSse(response.body)) {
    onEvent?.(item);
    if (item?.type === 'voice.completed' || item?.type === 'error') break;
    if (item?.type === 'response.completed') {
      // LLM done, but audio tail may still follow. Keep reading.
    }
  }
}

// Client-side stopwatch for the voice pipeline, anchored at the moment the
// recording is released (finger/mic-button up) — the latency the user feels.
// Server also reports server_ms per stage; this tracks the device side.
export class VoiceTimings {
  constructor(start = (typeof performance !== 'undefined' ? performance.now() : Date.now())) {
    this.start = start;
    this.uploadedAt = null;
    this.transcribedAt = null;
    this.firstTextAt = null;
    this.doneAt = null;
    this.textUpdates = 0;
  }
  mark(name, now) {
    const t = now ?? (typeof performance !== 'undefined' ? performance.now() : Date.now());
    if (name === 'uploaded' && this.uploadedAt === null) this.uploadedAt = t;
    if (name === 'transcribed' && this.transcribedAt === null) this.transcribedAt = t;
    if (name === 'text' && this.firstTextAt === null) this.firstTextAt = t;
    if (name === 'done' && this.doneAt === null) this.doneAt = t;
    if (name === 'text') this.textUpdates++;
  }
  elapsed(at) { return at === null ? null : at - this.start; }
  get uploaded() { return this.elapsed(this.uploadedAt); }
  get asr() { return this.elapsed(this.transcribedAt); }
  get firstText() { return this.elapsed(this.firstTextAt); }
  get total() { return this.elapsed(this.doneAt); }
}
