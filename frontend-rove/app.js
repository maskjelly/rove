import { readSse, Timings } from './stream.mjs';
import { pickMime, createPcmPlayer, postVoice, VoiceTimings } from './voice.mjs';

const $ = (id) => document.getElementById(id);
const byteLength = (text) => new TextEncoder().encode(text).length;
const formatTime = (ms) => ms === null ? '—' : ms >= 1000 ? (ms / 1000).toFixed(2) + ' s' : Math.round(ms) + ' ms';
let history = [];
let controller = null;
const welcome = $('welcome');

function notice(text = '') {
  $('notice').textContent = text;
  $('notice').hidden = !text;
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
    notice('Older turns were omitted to keep the conversation manageable.');
  }
  controller = new AbortController();
  $('send').hidden = true;
  $('stop').hidden = false;
  $('clear').disabled = true;
  $('prompt').disabled = true;
  $('state').textContent = 'CONNECTING';
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
          $('state').textContent = 'GENERATING';
          reply.item.querySelector('h3').textContent = item.response?.model || 'ROVE';
          break;
        case 'response.reasoning_summary_text.delta':
          $('state').textContent = 'REASONING';
          if (summary.hidden) { summary.hidden = false; summary.open = true; }
          summaryText.textContent += item.delta || '';
          break;
        case 'response.output_text.delta':
        case 'response.refusal.delta':
          if (!item.delta) break;
          $('state').textContent = 'STREAMING';
          answer += item.delta;
          reply.answer.textContent = answer;
          timing.add(now);
          showTimings(timing);
          break;
        case 'response.completed': {
          completed = true;
          $('state').textContent = 'COMPLETE';
          const usage = document.createElement('div');
          usage.className = 'usage';
          const tokens = item.response?.usage;
          if (tokens) usage.textContent = tokens.input_tokens + ' input · ' + tokens.output_tokens + ' output tokens (including reasoning)';
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
    notice('Response complete. Ask a follow-up or start a new conversation.');
  } catch (error) {
    const stopped = controller.signal.aborted;
    $('state').textContent = stopped ? 'STOPPED' : 'ERROR';
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
  resetStats();
  $('raw').textContent = 'Send a message to inspect its event stream.';
  $('state').textContent = 'READY';
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

function setMicLabel(text) { const mic = $('mic'); if (mic) { mic.textContent = text; mic.classList.toggle('live', recording); } }

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
    notice('Recording… release to send. Server will transcribe, think, and speak back.');
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
  $('state').textContent = 'LISTENING';
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
          case 'voice.stage':
            $('state').textContent = 'TRANSCRIBING';
            break;
          case 'voice.transcript.delta':
            transcript += item.delta || '';
            userBubble.answer.textContent = '🎙 ' + transcript;
            break;
          case 'voice.transcribed':
            wall.mark('transcribed', now);
            transcript = item.text || transcript;
            userBubble.answer.textContent = transcript;
            $('state').textContent = 'GENERATING';
            reply.answer.textContent = 'Thinking… first words play as soon as they stream.';
            break;
          case 'response.created':
            $('state').textContent = 'GENERATING';
            reply.item.querySelector('h3').textContent = item.response?.model || 'ROVE';
            break;
          case 'response.reasoning_summary_text.delta':
            $('state').textContent = 'REASONING';
            if (summary.hidden) { summary.hidden = false; summary.open = true; }
            summaryText.textContent += item.delta || '';
            break;
          case 'response.output_text.delta':
          case 'response.refusal.delta':
            if (!item.delta) break;
            wall.mark('text', now);
            $('state').textContent = 'STREAMING';
            if (!answer) reply.answer.textContent = '';
            answer += item.delta;
            reply.answer.textContent = answer;
            textGap.add(now);
            showTimings(textGap);
            break;
          case 'voice.first_audio':
            wall.mark('audio', now);
            serverMs = item.server_ms ?? serverMs;
            $('state').textContent = 'SPEAKING';
            break;
          case 'voice.audio.delta':
            if (item.audio) {
              wall.mark('audio', now);
              try { player.enqueue(item.audio, item.sample_rate || 24000); } catch { /* keep text even if audio fails */ }
            }
            break;
          case 'response.completed': {
            const usage = document.createElement('div');
            usage.className = 'usage';
            const tokens = item.response?.usage;
            if (tokens) usage.textContent = tokens.input_tokens + ' input · ' + tokens.output_tokens + ' output tokens (including reasoning)';
            reply.item.append(usage);
            break;
          }
          case 'voice.completed':
            completed = true;
            wall.mark('done', now);
            serverMs = item.server_ms ?? serverMs;
            $('state').textContent = 'COMPLETE';
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
    const voiceLine = document.createElement('div');
    voiceLine.className = 'usage';
    voiceLine.textContent = `Heard in ${formatTime(wall.asr)} · first words ${formatTime(wall.firstText)} · first audio ${formatTime(wall.firstAudio)} · done ${formatTime(wall.total)} (server did ASR + AI + TTS; this device only recorded and played)`;
    reply.item.append(voiceLine);
    history = [...input, { role: 'user', content: transcript }, { role: 'assistant', content: answer }];
    notice('Voice reply complete. Ask a follow-up by voice or text.');
  } catch (error) {
    player?.stop();
    const stopped = controller?.signal.aborted;
    $('state').textContent = stopped ? 'STOPPED' : 'ERROR';
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
