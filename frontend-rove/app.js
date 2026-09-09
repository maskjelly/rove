import { readSse, Timings } from './stream.mjs';

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

$('stop').addEventListener('click', () => controller?.abort());
$('clear').addEventListener('click', () => {
  if (controller) return;
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
fetch('/health', { cache: 'no-store' }).then((r) => {
  if (!r.ok) throw new Error();
  $('connection').textContent = 'Server online';
  $('connection-dot').classList.add('connected');
}).catch(() => { $('connection').textContent = 'Server unreachable'; });
