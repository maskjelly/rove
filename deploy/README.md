# Rove deployment

Push to `main` in `maskjelly/rove` to trigger `.github/workflows/deploy.yml`.
GitHub runs `cargo fmt --check`, locked clippy with warnings denied, locked Rust
tests for all targets and builds the `server` and `client` binaries. On success,
a restricted SSH key asks `ssh rove` to build and deploy that same commit.
The server rejects stale commits when main has advanced. PRs run checks only.

The server builds before stopping the old service. systemd stops the entire old
process group, starts the new executable, and restarts it after crashes/reboots.
Deployment requires `GET http://127.0.0.1:3000/health` to return `ok`; failure restores
the previous executable. There is a brief interruption during replacement.
No polling or GitHub webhook listener is installed.

GitHub secrets: `ROVE_DEPLOY_KEY`, `ROVE_KNOWN_HOSTS`.
The key only permits deployment commands, not an interactive root shell.
Builds and the backend run as the `rove` user.

Server paths:
- `/usr/local/sbin/rove-deploy`: deployment script
- `/usr/local/sbin/rove-deploy-ssh`: restricted SSH entry point
- `/etc/systemd/system/rove.service`: runtime service
- `/srv/rove/repository`: dedicated build checkout (never edit here)
- `/srv/rove/deployed-revision`: last healthy commit
- `/opt/rove/current`: active executable symlink
- `/opt/rove/releases`: current and previous executable
- `/etc/rove.env`: optional environment variables

Commands:
```sh
ssh rove 'systemctl status rove --no-pager'
ssh rove 'journalctl -u rove -n 100 --no-pager'
ssh rove 'journalctl -u "rove-deploy-*" -n 100 --no-pager'
```

The backend must keep its `server` Cargo binary name and `/health` endpoint.
Uncommitted code is never deployed. Push application code separately when ready.
Changes to the deployment scripts/service require reinstalling them on the server;
the workflow deploys application code only.

Caddy proxies https://45.196.196.251 to the server on port 3000.
`deploy/Caddyfile` is installed
at `/etc/caddy/Caddyfile`; install changes there, validate with
`caddy validate`, then `systemctl reload caddy`. Caddy automatically renews the
short-lived Let’s Encrypt IP certificate. Both services are enabled at boot.
