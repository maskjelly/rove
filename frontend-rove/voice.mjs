import { readSse } from './stream.mjs';

// Server does ASR + LLM + TTS. Browser only records and plays.
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

// 24 kHz 16-bit mono PCM (what gpt-4o-mini-tts returns as response_format pcm).
// Queues chunks so speech plays in order even as later text still streams.
export function createPcmPlayer(sampleRate = 24000) {
  let ctx = null;
  let nextTime = 0;
  let stopped = false;
  function ensure() {
    if (!ctx) {
      const AC = window.AudioContext || window.webkitAudioContext;
      ctx = new AC({ sampleRate });
    }
    if (ctx.state === 'suspended') void ctx.resume();
    return ctx;
  }
  function base64ToSamples(base64) {
    const binary = atob(base64);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
    return new Int16Array(bytes.buffer);
  }
  return {
    enqueue(base64, rate = sampleRate) {
      if (stopped || !base64) return -1;
      const audio = ensure();
      const samples = base64ToSamples(base64);
      if (!samples.length) return -1;
      const buffer = audio.createBuffer(1, samples.length, rate || sampleRate);
      const channel = buffer.getChannelData(0);
      for (let i = 0; i < samples.length; i++) channel[i] = samples[i] / 32768;
      const source = audio.createBufferSource();
      source.connect(audio.destination);
      const startAt = Math.max(audio.currentTime, nextTime);
      try { source.start(startAt); } catch { return -1; }
      nextTime = startAt + buffer.duration;
      return (startAt - audio.currentTime) * 1000;
    },
    stop() {
      stopped = true;
      nextTime = 0;
      if (ctx) void ctx.suspend().catch(() => {});
    },
    reset() { stopped = false; nextTime = 0; if (ctx && ctx.state === 'suspended') void ctx.resume().catch(() => {}); },
  };
}

// POST multipart {audio, messages} to /voice, yield parsed SSE events.
export async function postVoice(blob, messages, { signal, onEvent }) {
  const form = new FormData();
  const mime = blob.type || 'audio/webm';
  form.append('audio', blob, `recording.${extensionFor(mime)}`);
  form.append('messages', JSON.stringify(messages));
  const response = await fetch('/voice', { method: 'POST', body: form, signal });
  if (!response.ok) {
    let detail = '';
    try { detail = (await response.json()).error || ''; } catch { /* fall through */ }
    throw new Error(detail || `Voice request failed (${response.status}). Please try again.`);
  }
  if (!response.body) throw new Error('Your browser could not open the voice stream.');
  for await (const item of readSse(response.body)) {
    onEvent?.(item);
    if (item?.type === 'voice.completed' || item?.type === 'error') break;
    if (item?.type === 'response.completed') {
      // LLM done, but audio tail may still follow. Keep reading.
    }
  }
}

// Client-side stopwatch for the voice pipeline.
// Server also reports asr_ms / server_ms; this tracks what the user actually feels.
export class VoiceTimings {
  constructor(start = (typeof performance !== 'undefined' ? performance.now() : Date.now())) {
    this.start = start;
    this.transcribedAt = null;
    this.firstTextAt = null;
    this.firstAudioAt = null;
    this.doneAt = null;
    this.textUpdates = 0;
    this.audioChunks = 0;
  }
  mark(name, now) {
    const t = now ?? (typeof performance !== 'undefined' ? performance.now() : Date.now());
    if (name === 'transcribed' && this.transcribedAt === null) this.transcribedAt = t;
    if (name === 'text' && this.firstTextAt === null) this.firstTextAt = t;
    if (name === 'audio' && this.firstAudioAt === null) this.firstAudioAt = t;
    if (name === 'done' && this.doneAt === null) this.doneAt = t;
    if (name === 'text') this.textUpdates++;
    if (name === 'audio') this.audioChunks++;
  }
  elapsed(at) { return at === null ? null : at - this.start; }
  get asr() { return this.elapsed(this.transcribedAt); }
  get firstText() { return this.elapsed(this.firstTextAt); }
  get firstAudio() { return this.elapsed(this.firstAudioAt); }
  get total() { return this.elapsed(this.doneAt); }
}
