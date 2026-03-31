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
  -v "$PWD/auth/account1.json:/run/auth/account1.json" \
  -v "$PWD/auth/account2.json:/run/auth/account2.json" \
  -v "$PWD/auth/account3.json:/run/auth/account3.json" \
  codex-openai-proxy:local
```

Compose example:

```bash
cp .env.example .env
# edit .env and set PROXY_BEARER_TOKEN
docker compose up --build
```

The included [docker-compose.yml](/docker-sites/openai-proxy/docker-compose.yml) expects:

- `./auth/account1.json`
- `./auth/account2.json`
- `./auth/account3.json`

Those mounts are intentionally writable so refreshed tokens can be persisted.
If `.env` defines `PROXY_BEARER_TOKEN`, every request to the proxy must include that bearer token.

## Endpoints

- `GET /health`
- `GET /models`
- `GET /v1/models`
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

The example creates sensors for:

- proxy status
- loaded and healthy account counts
- next selected account
- observed plan type
- primary and secondary remaining quota percentages

It also shows the raw `accounts` payload from the proxy health response, which includes per-account quota and reset-window information.
