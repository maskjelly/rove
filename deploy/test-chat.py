"""End-to-end tests use a local OpenAI mock; no credentials or API spend."""
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

requests = []
tool_calls = []

class Mock(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        assert self.path == '/v1/responses'
        assert self.headers['Authorization'] == 'Bearer test-only'
        assert body['stream'] and not body['store']
        assert body['max_output_tokens'] == 2048
        assert body['reasoning']['summary'] == 'auto'
        requests.append(body)
        last = body['input'][-1]
        prompt = last.get('content', '') if isinstance(last, dict) else ''
        followup = next((m for m in body['input'] if isinstance(m, dict) and m.get('type') == 'function_call_output'), None)
        if followup is not None:
            assert followup['call_id'] == 'call_1', followup
            assert 'tool-ok' in followup['output'], followup
            tool_calls.append(followup)
            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream')
            self.end_headers()
            def emit(value):
                data = ('data: ' + json.dumps(value, ensure_ascii=False) + chr(13) + chr(10) + chr(10)).encode()
                self.wfile.write(data)
                self.wfile.flush()
            emit({'type': 'response.created', 'response': {'model': 'mock-mini'}})
            emit({'type': 'response.output_text.delta', 'delta': 'Tool says: tool-ok. All good.'})
            emit({'type': 'response.completed', 'response': {'usage': {'input_tokens': 10, 'output_tokens': 8}}})
            return
        if prompt == 'runcmd':
            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream')
            self.end_headers()
            def emit(value):
                data = ('data: ' + json.dumps(value, ensure_ascii=False) + chr(13) + chr(10) + chr(10)).encode()
                self.wfile.write(data)
                self.wfile.flush()
            emit({'type': 'response.created', 'response': {'model': 'mock-mini'}})
            emit({'type': 'response.output_text.delta', 'delta': 'Checking. '})
            emit({'type': 'response.completed', 'response': {'usage': {'input_tokens': 10, 'output_tokens': 8}, 'output': [
                {'type': 'message', 'id': 'msg_1', 'role': 'assistant', 'content': [{'type': 'output_text', 'text': 'Checking. '}]},
                {'type': 'function_call', 'id': 'fc_1', 'call_id': 'call_1', 'name': 'run_command', 'arguments': '{' + chr(34) + 'command' + chr(34) + ': ' + chr(34) + 'echo tool-ok' + chr(34) + '}'}]}})
            return
        if prompt == 'quota':
            self.send_response(429)
            self.end_headers()
            self.wfile.write(b'{"error":{"message":"secret-should-not-leak"}}')
            return
        self.send_response(200)
        self.send_header('Content-Type', 'text/event-stream')
        self.end_headers()
        def emit(value):
            data = ('data: ' + json.dumps(value, ensure_ascii=False) + '\r\n\r\n').encode()
            # Force chunk boundaries through UTF-8 and SSE framing.
            for start in range(0, len(data), 3):
                self.wfile.write(data[start:start+3])
                self.wfile.flush()
        emit({'type': 'response.created', 'response': {'model': 'mock-mini'}})
        emit({'type': 'response.reasoning_summary_text.delta', 'delta': 'A brief summary.'})
        emit({'type': 'response.output_text.delta', 'delta': 'hé🙂 '})
        time.sleep(.15)
        if prompt == 'disconnect':
            return
        emit({'type': 'response.output_text.delta', 'delta': 'world'})
        emit({'type': 'response.completed', 'response': {'usage': {'input_tokens': 10, 'output_tokens': 8}}})

mock = ThreadingHTTPServer(('127.0.0.1', 0), Mock)
threading.Thread(target=mock.serve_forever, daemon=True).start()
with socket.socket() as sock:
    sock.bind(('127.0.0.1', 0))
    port = sock.getsockname()[1]
base = f'http://127.0.0.1:{port}'
env = dict(os.environ, OPENAI_API_KEY='test-only', OPENAI_BASE_URL=f'http://127.0.0.1:{mock.server_port}/v1', ROVE_BIND=f'127.0.0.1:{port}')
server = subprocess.Popen([sys.argv[1]], env=env, stdout=subprocess.DEVNULL)
try:
    for _ in range(100):
        if server.poll() is not None:
            raise RuntimeError('Server exited')
        try:
            urllib.request.urlopen(base+'/health', timeout=1).close()
            break
        except urllib.error.URLError:
            time.sleep(.05)
    else:
        raise RuntimeError('Server did not start')
    client = subprocess.run([sys.argv[2], base], input='hello\nfollow up\n/new\nnew chat\nquota\ndisconnect\nruncmd\n/exit\n', text=True, capture_output=True, timeout=20)
    assert client.returncode == 0, client.stderr
    assert client.stdout.count('hé🙂 world') == 3, client.stdout
    assert 'Reasoning summary' in client.stdout
    assert '10 input' in client.stdout
    assert len(requests[1]['input']) == 3, 'Follow-up must retain context'
    assert len(requests[2]['input']) == 1, '/new must clear context'
    assert 'quota or rate limit' in client.stderr
    assert 'Connection ended before completion' in client.stderr
    assert 'secret-should-not-leak' not in client.stderr
    disc = next(r for r in requests if isinstance(r['input'][-1], dict) and r['input'][-1].get('content') == 'disconnect')
    assert len(disc['input']) == 3, 'Failed turns must not enter history'
    assert chr(36) + ' echo tool-ok' in client.stdout, client.stdout
    assert 'Tool says: tool-ok' in client.stdout, client.stdout
    assert len(tool_calls) == 1 and tool_calls[0]['call_id'] == 'call_1', tool_calls
    before = len(requests)
    invalid = urllib.request.Request(base+'/chat', data=json.dumps({'messages':[{'role':'system','content':'invalid'}]}).encode(), headers={'Content-Type':'application/json'})
    try:
        urllib.request.urlopen(invalid)
        raise AssertionError('Invalid role was accepted')
    except urllib.error.HTTPError as error:
        assert error.code == 400
    assert len(requests) == before
    # Ctrl-C must exit even when stdin is still open and waiting for a line.
    idle = subprocess.Popen([sys.argv[2], base], stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    time.sleep(.2)
    idle.send_signal(signal.SIGINT)
    idle.wait(timeout=3)
    idle.stdin.close()
    print('PASS: summaries, split UTF-8, answers, history/reset, quota, disconnect, validation, idle cancellation, tool round-trip')
finally:
    server.terminate()
    server.wait(timeout=5)
    mock.shutdown()
