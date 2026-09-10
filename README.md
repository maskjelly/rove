# Rove

A Rust terminal client backed by a server-side OpenAI streaming relay.
Text goes from the terminal to the server; the server streams SSE back.

## Chat

From this repository:

```sh
cargo run --manifest-path backend-rove/Cargo.toml --bin client
```

The client talks straight to the server. No SSH, no tunnel, no OpenAI key
on your laptop — it sends text and prints the streamed response.
Pass a base URL as the first argument to override the default
(`https://45.196.196.251`), e.g. `http://127.0.0.1:3000` for local development.

Type a message and press Enter. Answers and supported reasoning summaries print
as they arrive. `/new` resets the conversation; `/exit` (or `/quit`) quits; Ctrl-C cancels and exits.
Reasoning summaries may be absent on simple requests; raw internal reasoning is
not available. The displayed output-token count includes reasoning tokens.

History lives in client memory, not on disk. Recent turns are sent each time;
older turns are dropped when the 21-message/24-KB context limit is reached.
Failed or interrupted turns are not saved. Restarting the client starts fresh.

The server calls OpenAI; the model does not run locally on the VPS. This version
is chat only: it has no shell, file editing, or autonomous tool execution.

## Server

`POST /chat` takes `{"messages":[{"role":"user","content":"Hello"}]}` and
streams OpenAI Responses SSE events back. `GET /health` returns `ok`.
The server accepts chat requests directly and streams the answer back.

Root-only `/etc/rove.env` supplies `OPENAI_API_KEY` and `OPENAI_MODEL` to systemd.
Default: `gpt-5-mini`, low reasoning effort, automatic reasoning summaries,
2,048 maximum output tokens (including reasoning), eight concurrent requests,
and a three-minute request timeout. Context and reasoning are also billable.
The limits bound individual calls; they are not a monthly spending cap.

Change environment settings on the server, then run `systemctl restart rove`.
Never commit the API key. Disconnecting the client closes its upstream stream;
tokens already generated may still be billed.

For local development, set `OPENAI_API_KEY` in your shell, run the `server` binary,
and point the client at `http://127.0.0.1:3000` as its first argument. `ROVE_BIND`
overrides the listen address (default `127.0.0.1:3000`); `OPENAI_BASE_URL` is available for local mock tests.

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
