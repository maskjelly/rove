import { readSse, Timings } from './stream.mjs?v={{ASSET_HASH}}';
import { pickMime, createPcmPlayer, postVoice, VoiceTimings } from './voice.mjs?v={{ASSET_HASH}}';

const $ = (id) => document.getElementById(id);
const byteLength = (text) => new TextEncoder().encode(text).length;
const formatTime = (ms) => ms === null ? '—' : ms >= 1000 ? (ms / 1000).toFixed(2) + ' s' : Math.round(ms) + ' ms';
let history = [];
let controller = null;
const welcome = $('welcome');

function notice(text = '', kind = 'error') {
  const el = $('notice');
  el.textContent = text;
  el.hidden = !text;
  el.dataset.kind = kind;
}
function setState(text) {
  const el = $('state'));
  el.textContent = text;
  el.dataset.phase = text;
}
function scrollConversation() {
  const container = $('conversation');
  if (container.scrollHeight - container.scrollTop - container.clientHeight < 220) container.scrollTop = container.scrollHeight;
}
function message(role, text = '') {
  const item = document.createElement('article');
  item.className = 'message ' + role;
  const title = document.createElement('h3');
  title.textContent = role === 'you' ? 'YOU' : 'ROVE';
  const answer = document.createElement('p');
  answer.className = 'answer';
  answer.textContent = text;
  item.append(title, answer);
  $('conversation').append(item);
  return { item, answer };
}
function resetStats() {
  ['first', 'total', 'gap'].forEach((id) => { $(id).textContent = '—'; });
  $('updates').textContent = '0';
  $('event-count').textContent = '0';
  $('raw').textContent = '';
  $('rhythm').replaceChildren();
  $('signal-label').textContent = 'Waiting for the first answer';
}
function showTimings(timing) {
  $('first').textContent = formatTime(timing.firstAnswer);
  $('updates').textContent = String(timing.updates);
  $('gap').textContent = formatTime(timing.averageGap);
  $('signal-label').textContent = timing.updates + ' answer updates';
  const bar = document.createElement('i');
  const gap = timing.gaps.at(-1) ?? 0;
  bar.style.height = Math.min(40, 5 + Math.log2(gap + 1) * 4) + 'px';
  bar.title = formatTime(gap) + ' since previous answer update';
  $('rhythm').append(bar);
  if ($('rhythm').childElementCount > 48) $('rhythm').firstChild.remove();
}

$('chat-form').addEventListener('submit', async (event) => {
  event.preventDefault();
  if (controller) return;
  const prompt = $('prompt').value.trim();
  if (!prompt) return;
  if (byteLength(prompt) > 16_000) { notice('Please keep your message under 16 KB.'); return; }
  notice();
  welcome.remove();
  $('prompt').value = '';
  $('prompt').style.height = 'auto';
  message('you', prompt);
  const reply = message('ai');
  reply.item.classList.add('streaming');
  const summary = document.createElement('details');
  summary.className = 'reasoning';
  summary.hidden = true;
  const summaryTitle = document.createElement('summary');
  summaryTitle.textContent = 'Reasoning summary';
  const summaryText = document.createElement('p');
  summary.append(summaryTitle, summaryText);
  reply.item.insertBefore(summary, reply.answer);
  reply.answer.textContent = 'Waiting for the first words…';
  $('conversation').scrollTop = $('conversation').scrollHeight;
  const input = [...history, { role: 'user', content: prompt }];
  while (input.length > 21 || input.reduce((n, m) => n + byteLength(m.content), 0) > 24_000) {
    input.splice(0, 2);
    notice('Older turns were omitted to keep the conversation manageable.', 'info');
  }
  controller = new AbortController();
  $('send').hidden = true;
  $('stop').hidden = false;
  $('clear').disabled = true;
  $('prompt').disabled = true;
  setState('CONNECTING');
  resetStats();
  const started = performance.now();
  const timing = new Timings(started);
  const timer = setInterval(() => { $('total').textContent = formatTime(performance.now() - started); }, 50);
  const timeout = setTimeout(() => controller?.abort('timeout'), 190_000);
  let answer = '';
  let completed = false;
  let eventCount = 0;
  let rawEntries = [];
  try {
    const response = await fetch('/chat', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ messages: input }), signal: controller.signal,
    });
    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      throw new Error(body.error || 'Request failed (' + response.status + '). Please try again.');
    }
    if (!response.body) throw new Error('Your browser could not open the stream.');
    for await (const item of readSse(response.body)) {
      const now = performance.now();
      eventCount++;
      $('event-count').textContent = String(eventCount);
      rawEntries.push('+' + formatTime(now - started) + '  ' + (item.type || 'event') + '\n' + JSON.stringify(item, null, 2).slice(0, 5000));
      // Keep the readout bounded during long responses.
      if (rawEntries.length > 80) rawEntries.shift();
      const nearBottom = $('raw').scrollHeight - $('raw').scrollTop - $('raw').clientHeight < 80;
      $('raw').textContent = rawEntries.join('\n\n');
      if (nearBottom) $('raw').scrollTop = $('raw').scrollHeight;
      switch (item.type) {
        case 'response.created':
          setState('GENERATING');
          reply.item.querySelector('h3').textContent = item.response?.model || 'ROVE';
          break;
        case 'response.reasoning_summary_text.delta':
          setState('REASONING');
          if (summary.hidden) { summary.hidden = false; summary.open = true; }
          summaryText.textContent += item.delta || '';
          break;
        case 'response.output_text.delta':
        case 'response.refusal.delta':
          if (!item.delta) break;
          setState('STREAMING');
          answer += item.delta;
          reply.answer.textContent = answer;
          timing.add(now);
          showTimings(timing);
          break;
        case 'response.completed': {
          completed = true;
          setState('COMPLETE');
          const usage = document.createElement('div');
          usage.className = 'usage';
          const tokens = item.response?.usage;
          const model = item.response?.model ? ' · ' + item.response.model : '';
          if (tokens) usage.textContent = tokens.input_tokens + ' input · ' + tokens.output_tokens + ' output tokens (including reasoning) · ' + answer.length + ' chars' + model;
          else usage.textContent = 'Response complete · ' + answer.length + ' chars' + model;
          reply.item.append(usage);
          break;
        }
        case 'response.incomplete':
          throw new Error('The response reached its output limit. Try a shorter question.');
        case 'response.failed':
        case 'error':
          throw new Error('The AI stream failed. Please try again.');
      }
      scrollConversation();
      if (completed) break;
    }
    if (!completed) throw new Error('The connection ended early. Please try again.');
    if (!answer) throw new Error('No answer was returned. Please try a shorter question.');
    history = [...input, { role: 'assistant', content: answer }];
    notice('Response complete. Ask a follow-up or start a new conversation.', 'ok');
  } catch (error) {
    const stopped = controller.signal.aborted;
    setState(stopped ? 'STOPPED' : 'ERROR');
    const text = stopped ? (controller.signal.reason === 'timeout' ? 'The request timed out. Please try again.' : 'Stopped. Partial output was not saved to conversation history.') : error.message;
    notice(text);
    if (!answer) reply.answer.textContent = stopped ? 'Response stopped.' : 'Could not complete this response.';
    // Restore the failed prompt for an easy retry.
    $('prompt').value = prompt;
  } finally {
    clearInterval(timer);
    clearTimeout(timeout);
    $('total').textContent = formatTime(performance.now() - started);
    reply.item.classList.remove('streaming');
    controller = null;
    $('send').hidden = false;
    $('stop').hidden = true;
    $('clear').disabled = false;
    $('prompt').disabled = false;
    $('prompt').focus({ preventScroll: true });
  }
});

$('stop').addEventListener('click', () => { controller?.abort(); player?.stop(); });
$('clear').addEventListener('click', () => {
  if (controller) return;
  player?.stop();
  history = [];
  $('conversation').replaceChildren(welcome);
  $('prompt').value = '';
  $('prompt').style.height = 'auto';
  resetStats();
  $('raw').textContent = 'Send a message to inspect its event stream.';
  setState('READY');
  $('signal-label').textContent = 'Waiting for a message';
  notice();
  $('prompt').focus();
});
$('prompt').addEventListener('keydown', (event) => {
  if (event.key === 'Enter' && !event.shiftKey && !event.isComposing) {
    event.preventDefault();
    $('chat-form').requestSubmit();
  }
});
$('prompt').addEventListener('input', () => {
  const box = $('prompt');
  box.style.height = 'auto';
  box.style.height = Math.min(150, box.scrollHeight) + 'px';
});
document.querySelectorAll('[data-prompt]').forEach((button) => button.addEventListener('click', () => {
  $('prompt').value = button.dataset.prompt;
  $('chat-form').requestSubmit();
}));
$('share').addEventListener('click', async () => {
  try {
    await navigator.clipboard.writeText(location.origin + '/');
    $('share').textContent = 'Link copied ✓';
    setTimeout(() => { $('share').textContent = 'Copy link ↗'; }, 2000);
  } catch { notice('Copy this page’s address from your browser to share it.'); }
});
// Voice: hold Space outside the box, or hold the mic button. Server does ASR + LLM + TTS.
let recorder = null;
let recordChunks = [];
let recording = false;
let player = null;
let spaceHeld = false;

function setMicLabel(text) {
  const mic = $('mic');
  if (!mic) return;
  mic.textContent = text;
  mic.classList.toggle('live', recording);
  mic.setAttribute('aria-pressed', recording ? 'true' : 'false');
  mic.title = recording ? 'Release to send' : 'Hold Space or hold this button to talk';
}

async function startRecording() {
  if (recording || controller) return;
  if (!navigator.mediaDevices?.getUserMedia) { notice('This browser cannot record audio. Type instead.'); return; }
  try {
    const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
    const mime = pickMime();
    recordChunks = [];
    recorder = mime ? new MediaRecorder(stream, { mimeType: mime }) : new MediaRecorder(stream);
    recorder.ondataavailable = (event) => { if (event.data?.size) recordChunks.push(event.data); };
    recorder.onstop = () => {
      stream.getTracks().forEach((track) => track.stop());
      const type = recorder.mimeType || mime || 'audio/webm';
      const blob = new Blob(recordChunks, { type });
      recordChunks = [];
      if (blob.size > 0) void runVoice(blob);
      else notice('No audio captured. Hold a little longer and try again.');
    };
    player?.stop();
    recording = true;
    setMicLabel('●');
    notice('Recording… release to send. Server will transcribe, think, and speak back.', 'info');
    recorder.start();
  } catch { notice('Microphone blocked. Allow access and try again.'); }
}

function stopRecording() {
  if (!recording || !recorder) return;
  recording = false;
  setMicLabel('🎙');
  try { recorder.state !== 'inactive' ? recorder.stop() : null; } catch { /* already stopped */ }
}

async function runVoice(blob) {
  if (controller) return;
  if (blob.size > 5 * 1024 * 1024) { notice('Recording too large (5 MB max). Try a shorter message.'); return; }
  notice();
  try { welcome.remove(); } catch { /* already gone */ }
  const userBubble = message('you', '🎙 transcribing…');
  const reply = message('ai');
  reply.item.classList.add('streaming');
  const summary = document.createElement('details');
  summary.className = 'reasoning';
  summary.hidden = true;
  const summaryTitle = document.createElement('summary');
  summaryTitle.textContent = 'Reasoning summary';
  const summaryText = document.createElement('p');
  summary.append(summaryTitle, summaryText);
  reply.item.insertBefore(summary, reply.answer);
  reply.answer.textContent = 'Transcribing on the server…';
  $('conversation').scrollTop = $('conversation').scrollHeight;
  const input = [...history];
  while (input.length > 21 || input.reduce((n, m) => n + byteLength(m.content), 0) > 20_000) input.splice(0, 2);
  controller = new AbortController();
  player = player || createPcmPlayer(24000);
  player.reset();
  $('send').hidden = true;
  $('stop').hidden = false;
  $('clear').disabled = true;
  $('prompt').disabled = true;
  $('mic').disabled = true;
  setState('LISTENING');
  resetStats();
  const started = performance.now();
  const wall = new VoiceTimings(started);
  const textGap = new Timings(started);
  const timer = setInterval(() => { $('total').textContent = formatTime(performance.now() - started); }, 50);
  const timeout = setTimeout(() => controller?.abort('timeout'), 190_000);
  let transcript = '';
  let answer = '';
  let completed = false;
  let serverMs = null;
  const models = { asr: '—', chat: '—', tts: '—', voice: '', rate: 24000 };
  const serverAt = { asr: null, llm: null, audio: null, done: null };
  const counts = { upload: blob.size, audioBytes: 0, ttsChars: 0, ttsSegments: 0, inTokens: null, outTokens: null };
  const ttsRequests = [];
  let llmModel = '';
  let eventCount = 0;
  let rawEntries = [];
  const pushRaw = (item, now) => {
    eventCount++;
    $('event-count').textContent = String(eventCount);
    rawEntries.push('+' + formatTime(now - started) + '  ' + (item.type || 'event') + '\n' + JSON.stringify(item, null, 2).slice(0, 2000));
    if (rawEntries.length > 80) rawEntries.shift();
    const nearBottom = $('raw').scrollHeight - $('raw').scrollTop - $('raw').clientHeight < 80;
    $('raw').textContent = rawEntries.join('\n\n');
    if (nearBottom) $('raw').scrollTop = $('raw').scrollHeight;
  };
  try {
    await postVoice(blob, input, {
      signal: controller.signal,
      onEvent: (item) => {
        const now = performance.now();
        pushRaw(item, now);
        switch (item.type) {
          case 'voice.started':
            models.asr = item.asr_model || models.asr;
            models.chat = item.chat_model || models.chat;
            models.tts = item.tts_model || models.tts;
            models.voice = item.tts_voice || models.voice;
            models.rate = item.tts_sample_rate || 24000;
            counts.upload = item.upload_bytes ?? counts.upload;
            reply.item.querySelector('h3').textContent = 'ROVE · ' + (models.chat || 'voice');
            break;
          case 'voice.stage':
            setState('TRANSCRIBING');
            break;
          case 'voice.transcript.delta':
            transcript += item.delta || '';
            userBubble.answer.textContent = '🎙 ' + transcript;
            break;
          case 'voice.transcribed':
            wall.mark('transcribed', now);
            serverAt.asr = item.asr_ms ?? null;
            transcript = item.text || transcript;
            userBubble.answer.textContent = transcript;
            setState('GENERATING');
            reply.answer.textContent = 'Thinking… first words play as soon as they stream.';
            break;
          case 'voice.llm_first':
            serverAt.llm = item.server_ms ?? null;
            break;
          case 'response.created':
            setState('GENERATING');
            llmModel = item.response?.model || '';
            if (llmModel) models.chat = llmModel;
            reply.item.querySelector('h3').textContent = llmModel || 'ROVE';
            break;
          case 'response.reasoning_summary_text.delta':
            setState('REASONING');
            if (summary.hidden) { summary.hidden = false; summary.open = true; }
            summaryText.textContent += item.delta || '';
            break;
          case 'response.output_text.delta':
          case 'response.refusal.delta':
            if (!item.delta) break;
            wall.mark('text', now);
            setState('STREAMING');
            if (!answer) reply.answer.textContent = '';
            answer += item.delta;
            reply.answer.textContent = answer;
            textGap.add(now);
            showTimings(textGap);
            break;
          case 'voice.first_audio':
            wall.mark('audio', now);
            serverAt.audio = item.server_ms ?? serverAt.audio;
            serverMs = item.server_ms ?? serverMs;
            setState('SPEAKING');
            break;
          case 'voice.tts.request':
            ttsRequests.push({ segment: item.segment ?? ttsRequests.length, chars: item.chars ?? 0, at: now - started, serverMs: item.server_ms ?? null });
            counts.ttsSegments++;
            break;
          case 'voice.audio.delta':
            if (item.audio) {
              wall.mark('audio', now);
              counts.audioBytes += item.bytes ?? Math.floor(item.audio.length * 3 / 4);
              try { player.enqueue(item.audio, item.sample_rate || models.rate); } catch { /* keep text even if audio fails */ }
            }
            break;
          case 'voice.audio.totals':
            counts.ttsChars = item.tts_chars ?? counts.ttsChars;
            counts.ttsSegments = item.segments ?? counts.ttsSegments;
            counts.audioBytes = item.bytes ?? counts.audioBytes;
            break;
          case 'response.completed': {
            const usage = document.createElement('div');
            usage.className = 'usage';
            const tokens = item.response?.usage;
            if (tokens) {
              counts.inTokens = tokens.input_tokens ?? null;
              counts.outTokens = tokens.output_tokens ?? null;
              usage.textContent = tokens.input_tokens + ' input · ' + tokens.output_tokens + ' output tokens (including reasoning)';
            } else usage.textContent = 'Response complete.';
            reply.item.append(usage);
            break;
          }
          case 'voice.completed':
            completed = true;
            wall.mark('done', now);
            serverAt.done = item.server_ms ?? null;
            serverMs = item.server_ms ?? serverMs;
            if (item.chat_model) models.chat = item.chat_model;
            if (item.asr_model) models.asr = item.asr_model;
            if (item.tts_model) models.tts = item.tts_model;
            if (item.tts_voice) models.voice = item.tts_voice;
            counts.ttsChars = item.tts_chars ?? counts.ttsChars;
            counts.ttsSegments = item.tts_segments ?? counts.ttsSegments;
            counts.audioBytes = item.audio_bytes ?? counts.audioBytes;
            setState('COMPLETE');
            break;
          case 'response.incomplete':
            throw new Error('The response reached its output limit. Try a shorter recording.');
          case 'response.failed':
          case 'voice.error':
          case 'error':
            throw new Error(item.message || 'The voice stream failed. Please try again.');
        }
        scrollConversation();
      },
    });
    if (!completed) throw new Error('The connection ended early. Please try again.');
    if (!transcript.trim()) throw new Error('No speech was heard. Try a clearer recording.');
    if (!answer) throw new Error('No answer was returned. Please try again.');
    const fmt = (ms) => ms === null || ms === undefined ? '—' : formatTime(ms);
    const audioSec = counts.audioBytes ? (counts.audioBytes / 2 / models.rate) : 0;
    const esc = (text) => String(text).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
    const card = document.createElement('div');
    card.className = 'voice-analytics';
    const total = wall.total || 1;
    const row = (label, clientMs, serverMsValue) => {
      const pct = clientMs === null ? 0 : Math.min(100, (clientMs / total) * 100);
      const srv = serverMsValue !== null && serverMsValue !== undefined ? ` <small>srv ${fmt(serverMsValue)}</small>` : '';
      return `<div class="pipe"><span>${label}</span><b>${fmt(clientMs)}${srv}</b><div class="bar"><i style="width:${pct.toFixed(1)}%"></i></div></div>`;
    };
    const stat = (label, value) => `<div>${label} <b>${value}</b></div>`;
    card.innerHTML =
      `<div class="va-title">PIPELINE ANALYTICS</div>` +
      `<div class="va-models"><span class="chip">${esc(models.asr)}</span><span class="chip arrow">→</span><span class="chip">${esc(models.chat)}</span><span class="chip arrow">→</span><span class="chip voice">${esc(models.tts)}${models.voice ? ' · ' + esc(models.voice) : ''} · ${(models.rate / 1000).toFixed(0)}kHz</span></div>` +
      row('Transcribed · speech to text', wall.asr, serverAt.asr) +
      row('First words · LLM answer starts', wall.firstText, serverAt.llm) +
      row('First audio · TTS speaks', wall.firstAudio, serverAt.audio) +
      row('Done · all audio played', wall.total, serverAt.done ?? serverMs) +
      `<div class="va-stats">` +
      stat('Upload', (counts.upload / 1024).toFixed(1) + ' KB') +
      stat('Heard', transcript.length + ' chars') +
      stat('Answer', answer.length + ' chars') +
      (counts.inTokens !== null ? stat('Tokens', counts.inTokens + ' in / ' + counts.outTokens + ' out') : '') +
      stat('Speech', counts.ttsSegments + ' sentences / ' + counts.ttsChars + ' chars') +
      stat('Audio', wall.audioChunks + ' chunks / ' + (counts.audioBytes / 1024).toFixed(1) + ' KB ≈ ' + audioSec.toFixed(1) + 's') +
      stat('Text updates', textGap.updates) +
      `</div>` +
      `<div class="va-note">Server did ASR + AI + TTS. This device only recorded and played. Big times include network; srv = server stopwatch.</div>`;
    reply.item.append(card);
    history = [...input, { role: 'user', content: transcript }, { role: 'assistant', content: answer }];
    notice('Voice reply complete. Ask a follow-up by voice or text.', 'ok');
  } catch (error) {
    player?.stop();
    const stopped = controller?.signal.aborted;
    setState(stopped ? 'STOPPED' : 'ERROR');
    notice(stopped ? 'Stopped. Partial voice output was not saved.' : error.message);
    if (!answer) reply.answer.textContent = stopped ? 'Voice stopped.' : 'Could not complete this voice reply.';
    if (!transcript) userBubble.answer.textContent = '🎙 (no speech captured)';
  } finally {
    clearInterval(timer);
    clearTimeout(timeout);
    $('total').textContent = formatTime(performance.now() - started);
    reply.item.classList.remove('streaming');
    controller = null;
    $('send').hidden = false;
    $('stop').hidden = true;
    $('clear').disabled = false;
    $('prompt').disabled = false;
    $('mic').disabled = false;
    setMicLabel('🎙');
    $('prompt').focus({ preventScroll: true });
  }
}

$('mic')?.addEventListener('pointerdown', (event) => { event.preventDefault(); void startRecording(); });
window.addEventListener('pointerup', () => { if (recording) stopRecording(); });
window.addEventListener('pointercancel', () => { if (recording) stopRecording(); });
window.addEventListener('keydown', (event) => {
  if (event.code !== 'Space' || spaceHeld || event.repeat) return;
  const target = event.target;
  const typing = target instanceof HTMLTextAreaElement || target instanceof HTMLInputElement || target?.isContentEditable;
  if (typing || controller || recording) return;
  event.preventDefault();
  spaceHeld = true;
  void startRecording();
});
window.addEventListener('keyup', (event) => {
  if (event.code !== 'Space' || !spaceHeld) return;
  spaceHeld = false;
  if (recording) { event.preventDefault(); stopRecording(); }
});

fetch('/health', { cache: 'no-store' }).then((r) => {
  if (!r.ok) throw new Error();
  $('connection').textContent = 'Server online';
  $('connection-dot').classList.add('connected');
}).catch(() => { $('connection').textContent = 'Server unreachable'; });
