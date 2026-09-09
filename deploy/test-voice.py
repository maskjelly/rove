"""Voice pipeline tests use a local OpenAI mock; no credentials or API spend.

Pipeline: audio upload -> server transcription -> AI answer -> text stream.
Covers: release-to-first-response SSE stages, per-stage server timings,
session persistence, cross-device resume (GET /session/:id) and validation.
The server under test needs ROVE_SESSIONS pointed at a scratch dir (set below).
"""
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Mock(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(length) if length else b''
        if self.path == '/v1/audio/transcriptions':
            assert b'recording' in body, body[:200]
            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream')
            self.end_headers()

            def emit(value):
                self.wfile.write(('data: ' + json.dumps(value) + '\r\n\r\n').encode())
                self.wfile.flush()

            emit({'type': 'transcript.text.delta', 'delta': 'hello '})
            emit({'type': 'transcript.text.delta', 'delta': 'world'})
            emit({'type': 'transcript.text.done', 'text': 'hello world'})
            return
        if self.path == '/v1/responses':
            payload = json.loads(body.decode())
            assert payload['stream'] and not payload['store']
            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream')
            self.end_headers()

            def emit(value):
                self.wfile.write(('data: ' + json.dumps(value) + '\r\n\r\n').encode())
                self.wfile.flush()

            emit({'type': 'response.created', 'response': {'model': 'mock-mini'}})
            emit({'type': 'response.output_text.delta', 'delta': 'Hello! This is the streamed answer.'})
            emit({'type': 'response.completed',
                  'response': {'usage': {'input_tokens': 5, 'output_tokens': 7}}})
            return
        self.send_response(404)
        self.end_headers()


def sse_events(raw):
    events = []
    for chunk in raw.split('\n\n'):
        for line in chunk.splitlines():
            if line.startswith('data:'):
                events.append(json.loads(line[5:].strip()))
    return events


def post_voice(base, audio, messages, session=None):
    boundary = 'VOICETEST123'
    parts = []
    parts.append(
        f'--{boundary}\r\nContent-Disposition: form-data; name="messages"\r\n\r\n'.encode()
        + json.dumps(messages).encode() + b'\r\n')
    if session is not None:
        parts.append(
            f'--{boundary}\r\nContent-Disposition: form-data; name="session"\r\n\r\n'.encode()
            + session.encode() + b'\r\n')
    parts.append(
        f'--{boundary}\r\nContent-Disposition: form-data; name="audio"; filename="recording.webm"\r\n'
        f'Content-Type: audio/webm\r\n\r\n'.encode() + audio + b'\r\n')
    parts.append(f'--{boundary}--\r\n'.encode())
    req = urllib.request.Request(base + '/voice', data=b''.join(parts),
                                 headers={'Content-Type': f'multipart/form-data; boundary={boundary}'})
    with urllib.request.urlopen(req, timeout=30) as response:
        return sse_events(response.read().decode())


mock = ThreadingHTTPServer(('127.0.0.1', 0), Mock)
threading.Thread(target=mock.serve_forever, daemon=True).start()
with socket.socket() as s:
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
base = f'http://127.0.0.1:{port}'
tmp = tempfile.mkdtemp(prefix='rove-sessions-')
env = dict(os.environ, OPENAI_API_KEY='test-only',
           OPENAI_BASE_URL=f'http://127.0.0.1:{mock.server_port}/v1',
           ROVE_BIND=f'127.0.0.1:{port}', ROVE_SESSIONS=tmp)
server = subprocess.Popen([sys.argv[1]], env=env, stdout=subprocess.DEVNULL)
try:
    for _ in range(100):
        if server.poll() is not None:
            raise RuntimeError('Server exited')
        try:
            urllib.request.urlopen(base + '/health', timeout=1).close()
            break
        except urllib.error.URLError:
            time.sleep(.05)
    else:
        raise RuntimeError('Server did not start')

    audio = b'RIFF....fake-webm-audio-bytes'
    events = post_voice(base, audio, [], session='test-session-1')
    kinds = [e.get('type') for e in events]
    for marker in ['voice.started', 'voice.transcribed', 'voice.llm_first',
                   'voice.session', 'voice.completed']:
        assert marker in kinds, f'missing {marker} in {kinds}'
    for retired in ['voice.audio.delta', 'voice.tts.request', 'voice.first_audio',
                    'voice.audio.totals']:
        assert retired not in kinds, f'retired TTS event still emitted: {retired}'
    started = next(e for e in events if e['type'] == 'voice.started')
    assert started['session'] == 'test-session-1', started
    assert started['asr_model'] == 'gpt-4o-mini-transcribe', started
    assert 'tts_model' not in started, started
    assert isinstance(started['parse_ms'], int), started
    transcribed = next(e for e in events if e['type'] == 'voice.transcribed')
    assert transcribed['text'] == 'hello world', transcribed
    assert transcribed['asr_ms'] >= started['parse_ms'], (transcribed, started)
    completed = next(e for e in events if e['type'] == 'voice.completed')
    assert completed['session'] == 'test-session-1', completed
    assert completed['answer_chars'] > 0, completed
    assert completed['server_ms'] >= transcribed['asr_ms'], completed

    # Resume from "another device": history comes from the server.
    with urllib.request.urlopen(base + '/session/test-session-1', timeout=5) as response:
        session = json.load(response)
    assert [m['content'] for m in session['messages']] == [
        'hello world', session['messages'][1]['content']], session['messages']
    assert session['messages'][0]['role'] == 'user'
    assert session['messages'][1]['role'] == 'assistant'
    turn = session['turns'][0]
    assert 'streamed answer' in turn['answer'], turn['answer']
    assert 'audio_b64' not in turn, 'answer audio should not be persisted'

    # Second turn appends to the same server-side conversation.
    events2 = post_voice(base, audio, [], session='test-session-1')
    assert any(e.get('type') == 'voice.completed' for e in events2)
    with urllib.request.urlopen(base + '/session/test-session-1', timeout=5) as response:
        session2 = json.load(response)
    assert len(session2['messages']) == 4, session2['messages']
    assert len(session2['turns']) == 2, session2['turns']

    # Validation: traversal ids rejected, unknown sessions 404, audio required.
    for bad in ['../evil', 'a/b', 'x' * 65]:
        try:
            post_voice(base, audio, [], session=bad)
            raise AssertionError(f'accepted bad session id {bad!r}')
        except urllib.error.HTTPError as error:
            assert error.code == 400, error.code
    try:
        urllib.request.urlopen(base + '/session/nope-missing', timeout=5)
        raise AssertionError('unknown session was accepted')
    except urllib.error.HTTPError as error:
        assert error.code == 404, error.code
    print('PASS: voice stages, server timings, session persist/resume/validation (no TTS)')
finally:
    server.terminate()
    server.wait(timeout=5)
    mock.shutdown()
