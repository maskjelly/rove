import test from 'node:test';
import assert from 'node:assert/strict';
import { readSse, Timings } from '../frontend-rove/stream.mjs';

test('decodes SSE across byte boundaries, including UTF-8, comments and CRLF', async () => {
  const source = ': heartbeat\r\nevent: response.output_text.delta\r\ndata: {"type":"response.output_text.delta",\r\ndata: "delta":"hé🙂"}\r\n\r\ndata: [DONE]\n\n';
  const bytes = new TextEncoder().encode(source);
  const stream = new ReadableStream({
    start(controller) {
      for (const byte of bytes) controller.enqueue(Uint8Array.of(byte));
      controller.close();
    },
  });
  const events = [];
  for await (const item of readSse(stream)) events.push(item);
  assert.deepEqual(events, [{ type: 'response.output_text.delta', delta: 'hé🙂' }]);
});

test('yields before upstream finishes and closes upstream when consumer stops', async () => {
  let cancelled = false;
  const stream = new ReadableStream({
    start(controller) {
      controller.enqueue(new TextEncoder().encode('data: {"delta":"first"}\n\n'));
      // Deliberately leave the stream open.
    },
    cancel() { cancelled = true; },
  });
  for await (const item of readSse(stream)) {
    assert.equal(item.delta, 'first');
    break;
  }
  assert.equal(cancelled, true);
});

test('propagates malformed data and transport failures', async () => {
  const stream = new ReadableStream({ start(c) { c.enqueue(new TextEncoder().encode('data: invalid\n\n')); c.close(); } });
  await assert.rejects(async () => { for await (const item of readSse(stream)) void item; }, SyntaxError);
  const broken = new ReadableStream({ start(c) { c.error(new Error('network closed')); } });
  await assert.rejects(async () => { for await (const item of readSse(broken)) void item; }, /network closed/);
});

test('separates first answer latency, answer updates and gaps; never calls them tokens', () => {
  const timing = new Timings(100);
  assert.equal(timing.firstAnswer, null);
  assert.equal(timing.averageGap, null);
  timing.add(500);
  assert.equal(timing.firstAnswer, 400);
  assert.equal(timing.averageGap, null);
  timing.add(520);
  timing.add(560);
  assert.equal(timing.updates, 3);
  assert.equal(timing.averageGap, 30);
});
