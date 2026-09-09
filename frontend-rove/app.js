import { readSse, Timings } from './stream.mjs?v={{ASSET_HASH}}';
import { pickMime, postVoice, VoiceTimings } from './voice.mjs?v={{ASSET_HASH}}';

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
  const el = $('state');
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
  try { welcome.remove(); } catch { /* already gone after resume */ }
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
  const timer = setInterval(() => { $('total').textContent = formatTime(performance.now() - started); }, 250);
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
      rawEntries.push('+' + formatTime(now - started) + '  ' + (item.type || 'event') + '\n' + JSON.stringify(item, null, 2).slice(0, 2000));
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
    const text = stopped ? (controller.signal.reason === 'timeout' ? 'The request timed out. Please try again.' : 'Stopped. Partial output was not saved to conversation history.') : (error instanceof SyntaxError ? 'The stream arrived garbled. Please try again.' : error.message);
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

$('stop').addEventListener('click', () => controller?.abort());
$('clear').addEventListener('click', () => {
  if (controller) return;
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
  // With a session, the link resumes the same conversation on any device.
  const link = sessionId ? location.origin + '/?s=' + encodeURIComponent(sessionId) : location.origin + '/';
  try {
    await navigator.clipboard.writeText(link);
    $('share').textContent = (sessionId ? 'Resume link copied ✓' : 'Link copied ✓');
    setTimeout(() => { $('share').textContent = 'Copy link ↗'; }, 2000);
  } catch { notice('Copy this page’s address from your browser to share it.'); }
});
// Voice: hold Space outside the box, or hold the mic button. Server transcribes and answers; this device only records.
let recorder = null;
let recordChunks = [];
let recording = false;
let spaceHeld = false;
let releasedAt = 0;
let sessionId = '';
try { sessionId = localStorage.getItem('rove.session') || ''; } catch { /* private mode */ }
function rememberSession(id) {
  if (!id) return;
  sessionId = id;
  try { localStorage.setItem('rove.session', id); } catch { /* private mode */ }
}

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
      const at = releasedAt;
      recordChunks = [];
      if (blob.size > 0) void runVoice(blob, at);
      else notice('No audio captured. Hold a little longer and try again.');
    };
    recording = true;
    setMicLabel('●');
    notice('Recording… release to send. The server transcribes it and streams back the answer.', 'info');
    recorder.start();
  } catch { notice('Microphone blocked. Allow access and try again.'); }
}

function stopRecording() {
  if (!recording || !recorder) return;
  releasedAt = performance.now();
  recording = false;
  setMicLabel('🎙');
  try { recorder.state !== 'inactive' ? recorder.stop() : null; } catch { /* already stopped */ }
}

function analyticsCardEl({ title, models, rows, stats, note }) {
  const fmt = (ms) => ms === null || ms === undefined ? '—' : formatTime(ms);
  const esc = (text) => String(text ?? '').replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
  const card = document.createElement('div');
  card.className = 'voice-analytics';
  const total = rows.map((r) => r[1]).filter((v) => typeof v === 'number').reduce((a, b) => Math.max(a, b), 0) || 1;
  const rowHtml = rows.map(([label, clientMs, serverMsValue]) => {
    const pct = clientMs === null || clientMs === undefined ? 0 : Math.min(100, (clientMs / total) * 100);
    const srv = serverMsValue !== null && serverMsValue !== undefined ? ` <small>srv ${fmt(serverMsValue)}</small>` : '';
    return `<div class="pipe"><span>${esc(label)}</span><b>${fmt(clientMs)}${srv}</b><div class="bar"><i style="width:${pct.toFixed(1)}%"></i></div></div>`;
  }).join('');
  const statHtml = stats.filter((s) => s[1] !== null && s[1] !== undefined && s[1] !== '').map(([label, value]) => `<div>${esc(label)} <b>${esc(value)}</b></div>`).join('');
  card.innerHTML =
    `<div class="va-title">${esc(title)}</div>` +
    `<div class="va-models"><span class="chip">${esc(models.asr)}</span><span class="chip arrow">→</span><span class="chip">${esc(models.chat)}</span></div>` +
    rowHtml +
    `<div class="va-stats">${statHtml}</div>` +
    `<div class="va-note">${esc(note)}</div>`;
  return card;
}

async function runVoice(blob, releasedAtParam) {
  if (controller) { notice('Still sending the previous recording — it was kept, yours was dropped.', 'info'); return; }
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
  $('send').hidden = true;
  $('stop').hidden = false;
  $('clear').disabled = true;
  $('prompt').disabled = true;
  $('mic').disabled = true;
  setState('LISTENING');
  resetStats();
  // Anchor every client timing at release (finger/mic up), not at fetch start,
  // so the numbers match what the speaker actually feels.
  const tRelease = releasedAtParam || performance.now();
  const started = tRelease;
  const wall = new VoiceTimings(tRelease);
  const textGap = new Timings(tRelease);
  const timer = setInterval(() => { $('total').textContent = formatTime(performance.now() - tRelease); }, 250);
  const timeout = setTimeout(() => controller?.abort('timeout'), 190_000);
  let transcript = '';
  let answer = '';
  let completed = false;
  let serverMs = null;
  let parseMs = null;
  const models = { asr: '—', chat: '—' };
  const serverAt = { asr: null, llm: null, done: null };
  const counts = { upload: blob.size, inTokens: null, outTokens: null };
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
      session: sessionId || undefined,
      onOpen: () => wall.mark('uploaded', performance.now()),
      onEvent: (item) => {
        const now = performance.now();
        pushRaw(item, now);
        switch (item.type) {
          case 'voice.started':
            models.asr = item.asr_model || models.asr;
            models.chat = item.chat_model || models.chat;
            counts.upload = item.upload_bytes ?? counts.upload;
            parseMs = item.parse_ms ?? null;
            reply.item.querySelector('h3').textContent = 'ROVE · ' + (models.chat || 'voice');
            break;
          case 'voice.session':
            rememberSession(item.session);
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
          case 'voice.tts.request':
          case 'voice.audio.delta':
          case 'voice.audio.totals':
            // Retired TTS events; older servers may still send them. Ignore.
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
    const card = analyticsCardEl({
      title: 'PIPELINE ANALYTICS · FROM RELEASE',
      models,
      rows: [
        ['Sent · release to upload done', wall.uploaded, null],
        ['Server parses audio', null, parseMs],
        ['Transcribed · speech to text', wall.asr, serverAt.asr],
        ['First words · AI answer starts', wall.firstText, serverAt.llm],
        ['Done · complete answer', wall.total, serverAt.done ?? serverMs],
      ],
      stats: [
        ['Upload', (counts.upload / 1024).toFixed(1) + ' KB'],
        ['Heard', transcript.length + ' chars'],
        ['Answer', answer.length + ' chars'],
        ['Tokens', counts.inTokens !== null ? counts.inTokens + ' in / ' + counts.outTokens + ' out' : null],
        ['Text updates', String(textGap.updates)],
        ['Session', sessionId || null],
      ],
      note: 'Client times run from the moment you released the recording; srv = server stopwatch from receipt. The server transcribes and answers — this device only records.',
    });
    reply.item.append(card);
    history = [...input, { role: 'user', content: transcript }, { role: 'assistant', content: answer }];
    notice('Voice reply complete. Share carries a resume link for your other devices.', 'ok');
  } catch (error) {
    const stopped = controller?.signal.aborted;
    setState(stopped ? 'STOPPED' : 'ERROR');
    notice(stopped ? 'Stopped. Partial voice output was not saved.' : (error instanceof SyntaxError ? 'The stream arrived garbled. Please try again.' : error.message));
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

// Resume a server-side session (?s=id): restores history plus the last turns
// with their analytics, on any device.
async function resumeSession(id) {
  let data;
  try {
    const response = await fetch('/session/' + encodeURIComponent(id), { cache: 'no-store' });
    if (!response.ok) throw new Error();
    data = await response.json();
  } catch { notice('That resume link is unknown or expired. Starting fresh.'); return; }
  if (!data || !Array.isArray(data.messages)) return;
  rememberSession(data.id || id);
  history = data.messages
    .filter((m) => m && (m.role === 'user' || m.role === 'assistant') && typeof m.content === 'string' && m.content.trim())
    .slice(-21);
  while (history.length > 21 || history.reduce((n, m) => n + byteLength(m.content), 0) > 20000) history.splice(0, 2);
  try { welcome.remove(); } catch { /* already gone */ }
  const turns = Array.isArray(data.turns) ? data.turns.slice(-3) : [];
  if (!turns.length) {
    for (const m of history.slice(-6)) message(m.role === 'user' ? 'you' : 'ai', m.content);
  } else {
    for (const turn of turns) {
      message('you', turn.transcript || '(voice message)');
      const reply = message('ai', turn.answer || '');
      reply.item.append(analyticsCardEl({
        title: 'PIPELINE ANALYTICS · RESUMED SESSION',
        models: { asr: turn.asr_model || '—', chat: turn.chat_model || '—' },
        rows: [
          ['Transcribed · speech to text', null, turn.asr_ms ?? null],
          ['First words · AI answer starts', null, turn.llm_first_ms ?? null],
          ['Done · complete answer', null, turn.server_ms ?? null],
        ],
        stats: [
          ['Heard', (turn.transcript || '').length + ' chars'],
          ['Answer', (turn.answer || '').length + ' chars'],
          ['Tokens', turn.input_tokens != null ? turn.input_tokens + ' in / ' + turn.output_tokens + ' out' : null],
        ],
        note: 'Restored from the server session. Client-side release timings belong to the original device; srv times are preserved.',
      }));
    }
  }
  setState('READY');
  notice('Resumed session with ' + history.length + ' messages. Hold Space or 🎙 to continue.', 'ok');
}
const resumeId = new URLSearchParams(location.search).get('s');
if (resumeId) void resumeSession(resumeId);

window.__roveBoot = true;
const healthCheck = new AbortController();
const healthTimer = setTimeout(() => healthCheck.abort(), 8000);
fetch('/health', { cache: 'no-store', signal: healthCheck.signal }).then((r) => {
  if (!r.ok) throw new Error();
  $('connection').textContent = 'Server online';
  $('connection-dot').classList.add('connected');
}).catch(() => { $('connection').textContent = 'Server unreachable'; })
  .finally(() => clearTimeout(healthTimer));
