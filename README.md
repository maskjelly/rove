# Rove

A small Rust terminal chat client with a server-side OpenAI streaming relay.

## Chat

From this repository:

```sh
cargo run --manifest-path backend-rove/Cargo.toml --bin client
```

The client automatically opens an encrypted SSH tunnel using your existing
`ssh rove` configuration. No OpenAI key is needed on your laptop. Only clients
with SSH access can reach the paid `/chat` endpoint; direct public access is denied.
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
2,048 maximum output tokens (including reasoning), two concurrent requests,
and a three-minute request timeout. Context and reasoning are also billable.
The limits bound individual calls; they are not a monthly spending cap.

Change environment settings on the server, then run `systemctl restart rove`.
Never commit the API key. Public `/health` and the original `/events` demo remain.
Chat is `POST /chat` with `{"messages":[{"role":"user","content":"Hello"}]}`,
accessible only via loopback/SSH. Streaming uses OpenAI Responses SSE events.
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
```

Pushes to `main` run tests, build the client/server, exercise streaming against a
local mock without spending API credits, then deploy to the VPS. See
[deployment details](deploy/README.md).

Official API references: [streaming](https://developers.openai.com/api/docs/guides/streaming-responses),
[reasoning summaries](https://developers.openai.com/api/docs/guides/reasoning#reasoning-summaries).
