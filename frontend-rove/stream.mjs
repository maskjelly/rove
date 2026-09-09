// Works in browsers and Node; TextDecoder preserves split UTF-8 network chunks.
export async function* readSse(stream) {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let pending = '';
  let lines = [];
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      pending += decoder.decode(value, { stream: true });
      if (pending.length > 2_000_000) throw new Error('An event exceeded the stream size limit.');
      let end;
      while ((end = pending.indexOf('\n')) !== -1) {
        const line = pending.slice(0, end).replace(/\r$/, '');
        pending = pending.slice(end + 1);
        if (line === '') {
          if (lines.length) {
            const data = lines.join('\n');
            lines = [];
            if (data !== '[DONE]') yield JSON.parse(data);
          }
        } else if (line.startsWith('data:')) {
          lines.push(line.slice(5).replace(/^ /, ''));
        }
      }
    }
  } finally {
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}

export class Timings {
  constructor(start) { this.start = start; this.first = null; this.last = null; this.updates = 0; this.gaps = []; }
  add(now) {
    if (this.first === null) this.first = now;
    if (this.last !== null) this.gaps.push(now - this.last);
    this.last = now;
    this.updates++;
  }
  get firstAnswer() { return this.first === null ? null : this.first - this.start; }
  get averageGap() { return this.gaps.length ? this.gaps.reduce((a, b) => a + b, 0) / this.gaps.length : null; }
}
