# Rove

A public browser chat client and a Rust terminal client, backed by the same
server-side OpenAI streaming relay.

## Share with friends

Open **https://45.196.196.251/**. No login, installation, SSH, or API key is needed.
Use **Copy link** to share. Anyone who can reach the public endpoint can use it.

The browser shows streamed answers, optional reasoning summaries, raw SSE events,
time to first answer, total elapsed time, answer update count, and average gap
between answer updates. These are browser-observed timings, not isolated network
latency; SSE updates are not individual tokens. The raw readout retains the last
80 events (up to 5,000 characters each). The Stop button closes the request.
Each tab has its own in-memory history. Reloading or New conversation clears it.

Caddy serves HTTPS, automatically renews the public IP certificate, redirects
HTTP to HTTPS, and streams `/chat` to the Rust server without buffering. Its
configuration is versioned in `deploy/Caddyfile` and installed at
`/etc/caddy/Caddyfile`. The API key stays in `/etc/rove.env`; it is never sent to
browsers. Direct plain-HTTP chat on port 3000 remains blocked.

## Chat

From this repository:

```sh
cargo run --manifest-path backend-rove/Cargo.toml --bin client
```

The client automatically opens an encrypted SSH tunnel using your existing
`ssh rove` configuration. No OpenAI key is needed on your laptop. The browser client is public over HTTPS; the terminal client can also use that
URL as its first argument instead of opening an SSH tunnel.
Use `ROVE_SSH_HOST=other-host` to select a different SSH alias.

Type a message and press Enter. Answers and supported reasoning summaries print
as they arrive. `/new` resets the conversation; `/exit` quits; Ctrl-C cancels and exits.
Reasoning summaries may be absent on simple requests; raw internal reasoning is
not available. The displayed output-token count includes reasoning tokens.

History lives in client memory, not on disk. Recent turns are sent each time;
older turns are dropped when the 21-message/24-KB context limit is reached.
Failed or interrupted turns are not saved. Restarting the client starts fresh.

The server calls OpenAI; the model does not run locally on the VPS. This version
is chat only: it has no shell, file editing, or autonomous tool execution.

## Server

Root-only `/etc/rove.env` supplies `OPENAI_API_KEY` and `OPENAI_MODEL` to systemd.
Default: `gpt-5-mini`, low reasoning effort, automatic reasoning summaries,
2,048 maximum output tokens (including reasoning), eight concurrent requests,
and a three-minute request timeout. Context and reasoning are also billable.
The limits bound individual calls; they are not a monthly spending cap.

Change environment settings on the server, then run `systemctl restart rove`.
Never commit the API key. Public `/health` and the original `/events` demo remain.
Chat is `POST /chat` with `{"messages":[{"role":"user","content":"Hello"}]}`,
accessible through public HTTPS or loopback/SSH. Streaming uses OpenAI Responses SSE events.
Disconnecting the client closes its upstream stream; tokens already generated
may still be billed.

For local development, set `OPENAI_API_KEY` in your shell, run the `server` binary,
and point the client at `http://127.0.0.1:3000` as its first argument. `ROVE_BIND`
overrides the listen address; `OPENAI_BASE_URL` is available for local mock tests.

## Checks and deployment

```sh
cargo test --locked --all-targets --manifest-path backend-rove/Cargo.toml
cargo build --locked --manifest-path backend-rove/Cargo.toml --bin server --bin client
python3 deploy/test-chat.py backend-rove/target/debug/server backend-rove/target/debug/client
node --test deploy/web-client.test.mjs
```

Pushes to `main` run tests, build the client/server, exercise streaming against a
local mock without spending API credits, then deploy to the VPS. See
[deployment details](deploy/README.md).

Official API references: [streaming](https://developers.openai.com/api/docs/guides/streaming-responses),
[reasoning summaries](https://developers.openai.com/api/docs/guides/reasoning#reasoning-summaries).
