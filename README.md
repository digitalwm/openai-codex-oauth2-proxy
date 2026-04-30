# Codex OpenAI Proxy

Rust proxy that accepts OpenAI-style chat completion requests and forwards them to the ChatGPT Codex backend using credentials from one or more `auth.json` files.

This working tree has been adapted for self-hosting and is ready to be pushed to your own repository. Local credentials such as `auth/` contents and `.env` files are meant to stay untracked.
It started from the earlier `Securiteru/codex-openai-proxy` project and has been extended here with multi-account rotation, token refresh, quota tracking, Docker support, and proxy-side access control.

## What Changed

- Supports multiple auth files through repeated `--auth-path` flags or `AUTH_PATHS`.
- Rotates requests across accounts with round-robin selection.
- Temporarily backs off accounts that hit retryable upstream failures such as `401`, `403`, `429`, and `5xx`.
- Refreshes token-based ChatGPT auth before expiry and retries auth failures after refresh.
- Exposes account health on `/health`.
- Captures Codex quota headers on each upstream call and exposes the latest observed quota state on `/health`.
- Runs a background quota probe for each account on startup and once per hour, so quota fields stay populated even without user traffic.
- Supports an optional proxy-level bearer token to block unauthorized callers.
- Avoids logging request bodies and bearer tokens.
- Includes Docker and Compose examples.

## Safety Notes

- The proxy now logs only request method, path, and selected account name.
- Request bodies, prompts, and auth headers are not printed.
- `.dockerignore` excludes the common local `auth/` directory and JSON files so credentials are not copied into the image build context by default.
- Token refresh persistence writes updated credentials back to the mounted `auth.json` file, so Docker mounts must be writable if you want refreshes to survive restarts.
- If `PROXY_BEARER_TOKEN` is set, clients must send `Authorization: Bearer <token>` to reach the proxy.
- `.gitignore` excludes `auth/`, `.env`, and related secret files so you can publish this repository without committing live credentials.

## Local Run

Build:

```bash
cargo build --release
```

Run with one account:

```bash
./target/release/codex-openai-proxy --port 8080 --auth-path ~/.codex/auth.json
```

Run with three accounts:

```bash
./target/release/codex-openai-proxy \
  --port 8080 \
  --auth-path /path/to/account1-auth.json \
  --auth-path /path/to/account2-auth.json \
  --auth-path /path/to/account3-auth.json
```

Or with an environment variable:

```bash
AUTH_PATHS=/path/to/account1-auth.json,/path/to/account2-auth.json,/path/to/account3-auth.json \
./target/release/codex-openai-proxy --port 8080
```

Run with a proxy bearer token:

```bash
AUTH_PATHS=/path/to/account1-auth.json \
PROXY_BEARER_TOKEN=change-me \
./target/release/codex-openai-proxy --port 8080
```

Run with a dedicated Responses API key:

```bash
AUTH_PATHS=/path/to/account1-auth.json \
RESPONSES_API_KEY=sk-proj-or-sk-live-key \
./target/release/codex-openai-proxy --port 8080
```

Temporarily enable detailed debug logs:

```bash
AUTH_PATHS=/path/to/account1-auth.json \
PROXY_DEBUG_LOGS=true \
./target/release/codex-openai-proxy --port 8080
```

## Docker

Build:

```bash
docker build -t codex-openai-proxy:local .
```

Run:

```bash
docker run --rm -p 8080:8080 \
  -e AUTH_PATHS=/run/auth/account1.json,/run/auth/account2.json,/run/auth/account3.json \
  -e PROXY_BEARER_TOKEN=change-me \
  -e RESPONSES_API_KEY=sk-proj-or-sk-live-key \
  -v "$PWD/auth/account1.json:/run/auth/account1.json" \
  -v "$PWD/auth/account2.json:/run/auth/account2.json" \
  -v "$PWD/auth/account3.json:/run/auth/account3.json" \
  codex-openai-proxy:local
```

Compose example:

```bash
cp .env.example .env
# edit .env and set PROXY_BEARER_TOKEN and optionally RESPONSES_API_KEY
docker compose up --build
```

The included [docker-compose.yml](/docker-sites/openai-proxy/docker-compose.yml) expects:

- `./auth/account1.json`
- `./auth/account2.json`
- `./auth/account3.json`

Those mounts are intentionally writable so refreshed tokens can be persisted.
If `.env` defines `PROXY_BEARER_TOKEN`, every request to the proxy must include that bearer token.
If `.env` defines `RESPONSES_API_KEY`, `/responses` and `/v1/responses` will use that API key instead of the rotating account auth files.
`QUOTA_PROBE_INTERVAL_SECONDS` controls the background quota probe cadence (default `3600`).
`QUOTA_PROBE_MODEL` controls the probe model (default `gpt-5.4`).
`QUOTA_PROBE_INPUT` controls the probe input text (default `hi`).

## Endpoints

- `GET /health`
- `GET /status`
- `GET /v1/status`
- `GET /model-status`
- `GET /v1/model-status`
- `GET /models`
- `GET /v1/models`
- `POST /responses`
- `POST /v1/responses`
- `POST /embeddings`
- `POST /v1/embeddings`
- `POST /chat/completions`
- `POST /v1/chat/completions`

When `PROXY_BEARER_TOKEN` is set, call the proxy with:

```bash
curl -sS http://127.0.0.1:8080/health \
  -H 'Authorization: Bearer change-me'
```

Example health response:

```json
{
  "status": "ok",
  "service": "codex-openai-proxy",
  "accounts_loaded": 3,
  "accounts_healthy": 2,
  "next_account": "account-2",
  "accounts": [
    {
      "name": "account-1",
      "path": "/run/auth/account1.json",
      "healthy": true,
      "quota": {
        "observed_at": "2026-03-31T07:40:00Z",
        "source": "x-codex-*",
        "plan_type": "plus",
        "active_limit": "codex",
        "primary_used_percent": 10,
        "primary_remaining_percent": 90,
        "primary_window_minutes": 300,
        "primary_reset_at": 1774958172,
        "primary_reset_after_seconds": 15689,
        "secondary_used_percent": 3,
        "secondary_remaining_percent": 97,
        "secondary_window_minutes": 10080,
        "secondary_reset_at": 1775544972,
        "secondary_reset_after_seconds": 602489,
        "credits_has_credits": false,
        "credits_unlimited": false,
        "credits_balance": null,
        "raw_limit_name": null
      },
      "fail_count": 0,
      "disabled_until": null,
      "last_error": null
    }
  ]
}
```

The quota fields are inferred from observed `x-codex-*` response headers returned by the ChatGPT Codex backend. They appear to expose used percentages and reset windows, not an absolute token count.

For a more detailed per-account view, use `/status` or `/v1/status`. That payload includes:

- last used time per account
- last refresh time
- access token expiry and seconds remaining
- ID token expiry and seconds remaining
- whether refresh is currently recommended
- temporary disable/backoff timers
- latest observed quota/reset data

For model discovery:

- `/models` and `/v1/models` proxy the upstream `https://api.openai.com/v1/models` response using one of the configured accounts
- `/model-status` and `/v1/model-status` fetch the upstream model list for every configured account and also probe a small built-in candidate set for chat and embeddings support

The current probe candidates are:

- chat: `gpt-4`, `gpt-5`, `gpt-5.4`
- embeddings: `text-embedding-3-small`, `text-embedding-3-large`

`/model-status` is intentionally more expensive than `/models` because it performs live test requests per account. Use it for diagnostics, not as a high-frequency polling endpoint.

For direct OpenAI Responses API compatibility:

- `/responses` and `/v1/responses` proxy requests to the upstream `https://api.openai.com/v1/responses` endpoint
- if `RESPONSES_API_KEY` is configured, that route uses the dedicated API key instead of the rotating auth-file accounts
- non-streaming JSON responses are supported, including `input_file` content items such as `data:application/pdf;base64,...`
- streaming Responses API requests currently return `501 Not Implemented`

## Auth File Formats

The proxy accepts either nested token data:

```json
{
  "tokens": {
    "access_token": "eyJ...",
    "account_id": "uuid",
    "refresh_token": "optional"
  }
}
```

Or flat token data:

```json
{
  "access_token": "eyJ...",
  "account_id": "uuid",
  "refresh_token": "optional"
}
```

Or API key data:

```json
{
  "OPENAI_API_KEY": "sk-..."
}
```

## Token Refresh

For ChatGPT-style auth files, the proxy checks token expiry continuously:

- before each request for the selected account
- in a background refresh loop every 60 seconds
- immediately after upstream `401` or `403`

Refresh is inferred from the observed Codex auth file format and the OpenAI OAuth client used by Codex login. The proxy refreshes through `https://auth.openai.com/oauth/token` and updates `access_token`, `id_token`, `refresh_token`, and `last_refresh` in the same file when possible.

## Development

```bash
cargo fmt
cargo test
```

## Home Assistant

This repository includes a ready-to-use Home Assistant example under [examples/homeassistant/package.yaml](/docker-sites/openai-proxy/examples/homeassistant/package.yaml) and [examples/homeassistant/dashboard.yaml](/docker-sites/openai-proxy/examples/homeassistant/dashboard.yaml).

Based on the official Home Assistant REST integration docs:
https://www.home-assistant.io/integrations/rest/

Suggested setup:

1. Copy `examples/homeassistant/package.yaml` into your Home Assistant packages directory.
2. Replace `OPENAI_PROXY_HOST` with the hostname or IP of this proxy.
3. Add a secret named `openai_codex_proxy_bearer` in Home Assistant `secrets.yaml`.
4. Restart Home Assistant.
5. Import or copy the Lovelace example from `examples/homeassistant/dashboard.yaml`.

The example now reads `/status` and creates sensors for:

- proxy status
- loaded and healthy account counts
- next selected account
- observed plan types across accounts
- lowest primary and secondary remaining quota percentages across accounts

The dashboard example also renders a list of all accounts with:

- current health
- last used time
- last refresh time
- token expiry countdowns
- refresh recommendation state
- backoff timer
- primary and secondary remaining quota percentages

It also shows the raw `accounts` payload from the proxy status response for debugging.
