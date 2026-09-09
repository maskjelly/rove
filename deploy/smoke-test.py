"""Exercise the actual deployment binary's health and streaming endpoints."""
import subprocess
import sys
import time
import urllib.error
import urllib.request

process = subprocess.Popen([sys.argv[1]])
try:
    for attempt in range(50):
        if process.poll() is not None:
            raise RuntimeError("Server exited before becoming healthy")
        try:
            with urllib.request.urlopen("http://127.0.0.1:3000/health", timeout=1) as response:
                assert response.read() == b"ok"
            break
        except (urllib.error.URLError, TimeoutError):
            time.sleep(0.1)
    else:
        raise RuntimeError("Server did not become healthy")
    for path, content_type, marker in [
        ("/", "text/html", b'Talk. Watch it stream.'),
        ("/app.css", "text/css", b'.workspace'),
        ("/app.js", "text/javascript", b"fetch('/chat'"),
        ("/boot.js", "text/javascript", b'__roveBoot'),
        ("/stream.mjs", "text/javascript", b'export async function* readSse'),
        ("/voice.mjs", "text/javascript", b'postVoice'),
    ]:
        with urllib.request.urlopen("http://127.0.0.1:3000" + path, timeout=2) as response:
            assert response.headers.get_content_type() == content_type
            assert marker in response.read()
    with urllib.request.urlopen("http://127.0.0.1:3000/events", timeout=12) as response:
        assert response.headers.get_content_type() == "text/event-stream"
        events = []
        while len(events) < 2:
            line = response.readline()
            if not line:
                raise RuntimeError("Stream closed before two events arrived")
            if line.startswith(b"data:"):
                events.append(line)
        assert all(b"New data from the server at:" in event for event in events)
        assert events[0] != events[1], "Expected changing timestamps"
    print("PASS: health endpoint and two live SSE events")
finally:
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
