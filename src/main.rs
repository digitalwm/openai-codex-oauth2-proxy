use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, Utc};
use clap::Parser;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::Mutex;
use uuid::Uuid;
use warp::{Filter, Reply};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Port to listen on
    #[arg(short, long, default_value = "8080")]
    port: u16,

    /// Path(s) to auth.json files. Repeat the flag or use AUTH_PATHS with commas.
    #[arg(
        long = "auth-path",
        env = "AUTH_PATHS",
        value_delimiter = ',',
        num_args = 1..,
        default_value = "~/.codex/auth.json"
    )]
    auth_paths: Vec<String>,

    /// Optional bearer token required from clients calling this proxy.
    #[arg(long, env = "PROXY_BEARER_TOKEN")]
    proxy_bearer_token: Option<String>,

    /// Enable verbose request/debug logging.
    #[arg(long, env = "PROXY_DEBUG_LOGS", default_value_t = false)]
    debug_logs: bool,
}

#[allow(dead_code)]
#[derive(Deserialize, Debug, Clone)]
struct ChatCompletionsRequest {
    model: String,
    messages: Vec<ChatMessage>,
    temperature: Option<f32>,
    max_tokens: Option<i32>,
    stream: Option<bool>,
    tools: Option<Vec<Value>>,
    tool_choice: Option<Value>,
}

#[derive(Deserialize, Debug, Clone)]
struct ChatMessage {
    role: String,
    content: Value,
}

#[derive(Serialize, Debug)]
struct ChatCompletionsResponse {
    id: String,
    object: String,
    created: i64,
    model: String,
    choices: Vec<Choice>,
    usage: Option<Usage>,
}

#[derive(Serialize, Debug)]
struct Choice {
    index: i32,
    message: ChatResponseMessage,
    finish_reason: Option<String>,
}

#[derive(Serialize, Debug)]
struct ChatResponseMessage {
    role: String,
    content: String,
}

#[derive(Serialize, Debug)]
struct Usage {
    prompt_tokens: i32,
    completion_tokens: i32,
    total_tokens: i32,
}

#[derive(Serialize, Debug)]
struct ResponsesApiRequest {
    model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    instructions: Option<String>,
    input: Vec<ResponseItem>,
    tools: Vec<Value>,
    tool_choice: Value,
    parallel_tool_calls: bool,
    reasoning: Option<Value>,
    store: bool,
    stream: bool,
    include: Vec<String>,
}

#[derive(Serialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponseItem {
    Message {
        #[serde(skip_serializing_if = "Option::is_none")]
        id: Option<String>,
        role: String,
        content: Vec<ContentItem>,
    },
}

#[derive(Serialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ContentItem {
    InputText { text: String },
    OutputText { text: String },
}

#[derive(Deserialize, Debug, Clone)]
struct RawAuthData {
    auth_mode: Option<String>,
    #[serde(rename = "OPENAI_API_KEY")]
    openai_api_key: Option<String>,
    api_key: Option<String>,
    tokens: Option<TokenData>,
    access_token: Option<String>,
    account_id: Option<String>,
    refresh_token: Option<String>,
    last_refresh: Option<String>,
}

#[allow(dead_code)]
#[derive(Deserialize, Serialize, Debug, Clone)]
struct TokenData {
    #[serde(skip_serializing_if = "Option::is_none")]
    id_token: Option<String>,
    access_token: String,
    account_id: String,
    refresh_token: Option<String>,
}

#[derive(Debug, Clone)]
struct AuthData {
    auth_mode: Option<String>,
    api_key: Option<String>,
    tokens: Option<TokenData>,
    last_refresh: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
struct Account {
    name: String,
    auth_path: String,
    auth_data: AuthData,
    last_used: Option<DateTime<Utc>>,
    quota: Option<QuotaSnapshot>,
    fail_count: u32,
    disabled_until: Option<DateTime<Utc>>,
    last_error: Option<String>,
}

#[derive(Debug)]
struct AccountPool {
    accounts: Vec<Account>,
    next_index: usize,
}

#[derive(Debug, Clone)]
struct AccountLease {
    index: usize,
    name: String,
    auth_data: AuthData,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    service: &'static str,
    accounts_loaded: usize,
    accounts_healthy: usize,
    next_account: Option<String>,
    accounts: Vec<AccountHealth>,
}

#[derive(Serialize)]
struct AccountHealth {
    name: String,
    path: String,
    healthy: bool,
    quota: Option<QuotaSnapshot>,
    fail_count: u32,
    disabled_until: Option<String>,
    last_error: Option<String>,
}

struct ProxyServer {
    client: Client,
    account_pool: Arc<Mutex<AccountPool>>,
    proxy_bearer_token: Option<String>,
    debug_logs: bool,
}

const TOKEN_REFRESH_THRESHOLD: Duration = Duration::from_secs(300);
const TOKEN_REFRESH_INTERVAL: Duration = Duration::from_secs(60);
const CHATGPT_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";

#[derive(Debug)]
struct BackendEventSummary {
    response_id: Option<String>,
    text: String,
    deltas: Vec<String>,
}

#[derive(Debug)]
struct BackendResponse {
    model: String,
    summary: BackendEventSummary,
}

#[derive(Debug)]
struct AttemptError {
    retryable: bool,
    auth_failed: bool,
    message: String,
}

#[derive(Deserialize, Debug)]
struct RefreshTokenResponse {
    access_token: String,
    #[serde(default)]
    id_token: Option<String>,
    #[serde(default)]
    refresh_token: Option<String>,
}

#[derive(Serialize, Debug, Clone)]
struct QuotaSnapshot {
    observed_at: String,
    source: String,
    plan_type: Option<String>,
    active_limit: Option<String>,
    primary_used_percent: Option<u8>,
    primary_remaining_percent: Option<u8>,
    primary_window_minutes: Option<u32>,
    primary_reset_at: Option<i64>,
    primary_reset_after_seconds: Option<u32>,
    secondary_used_percent: Option<u8>,
    secondary_remaining_percent: Option<u8>,
    secondary_window_minutes: Option<u32>,
    secondary_reset_at: Option<i64>,
    secondary_reset_after_seconds: Option<u32>,
    credits_has_credits: Option<bool>,
    credits_unlimited: Option<bool>,
    credits_balance: Option<String>,
    raw_limit_name: Option<String>,
}

impl AttemptError {
    fn retryable(message: impl Into<String>) -> Self {
        Self {
            retryable: true,
            auth_failed: false,
            message: message.into(),
        }
    }

    fn auth_retryable(message: impl Into<String>) -> Self {
        Self {
            retryable: true,
            auth_failed: true,
            message: message.into(),
        }
    }

    fn fatal(message: impl Into<String>) -> Self {
        Self {
            retryable: false,
            auth_failed: false,
            message: message.into(),
        }
    }
}

impl RawAuthData {
    fn normalize(self) -> Result<AuthData> {
        if let Some(tokens) = self.tokens {
            return Ok(AuthData {
                auth_mode: self.auth_mode,
                api_key: self.openai_api_key.or(self.api_key),
                tokens: Some(tokens),
                last_refresh: parse_optional_timestamp(self.last_refresh.as_deref())?,
            });
        }

        if let (Some(access_token), Some(account_id)) = (self.access_token, self.account_id) {
            return Ok(AuthData {
                auth_mode: self.auth_mode,
                api_key: self.openai_api_key.or(self.api_key),
                tokens: Some(TokenData {
                    id_token: None,
                    access_token,
                    account_id,
                    refresh_token: self.refresh_token,
                }),
                last_refresh: parse_optional_timestamp(self.last_refresh.as_deref())?,
            });
        }

        let api_key = self.openai_api_key.or(self.api_key);
        if api_key.is_some() {
            return Ok(AuthData {
                auth_mode: self.auth_mode,
                api_key,
                tokens: None,
                last_refresh: parse_optional_timestamp(self.last_refresh.as_deref())?,
            });
        }

        bail!("auth.json did not contain tokens or an API key");
    }
}

impl AccountPool {
    async fn load(auth_paths: &[String]) -> Result<Self> {
        let mut accounts = Vec::with_capacity(auth_paths.len());
        for (index, path) in auth_paths.iter().enumerate() {
            let resolved_path = expand_home(path)?;
            let auth_content = tokio::fs::read_to_string(&resolved_path)
                .await
                .with_context(|| format!("Failed to read auth file {}", resolved_path))?;
            let raw_auth: RawAuthData = serde_json::from_str(&auth_content)
                .with_context(|| format!("Failed to parse auth file {}", resolved_path))?;
            let auth_data = raw_auth
                .normalize()
                .with_context(|| format!("Invalid auth data in {}", resolved_path))?;

            accounts.push(Account {
                name: format!("account-{}", index + 1),
                auth_path: resolved_path,
                auth_data,
                last_used: None,
                quota: None,
                fail_count: 0,
                disabled_until: None,
                last_error: None,
            });
        }

        if accounts.is_empty() {
            bail!("At least one auth path is required");
        }

        Ok(Self {
            accounts,
            next_index: 0,
        })
    }

    fn lease_next_account(&mut self) -> Result<AccountLease> {
        let total = self.accounts.len();
        if total == 0 {
            bail!("No accounts loaded");
        }

        let now = chrono::Utc::now();
        let start = self.next_index;

        for offset in 0..total {
            let index = (start + offset) % total;
            let account = &self.accounts[index];
            let available = account
                .disabled_until
                .map(|until| until <= now)
                .unwrap_or(true);
            if available {
                self.next_index = (index + 1) % total;
                let account = &mut self.accounts[index];
                account.last_used = Some(now);
                return Ok(AccountLease {
                    index,
                    name: account.name.clone(),
                    auth_data: account.auth_data.clone(),
                });
            }
        }

        let fallback_index = start % total;
        self.next_index = (fallback_index + 1) % total;
        let account = &mut self.accounts[fallback_index];
        account.last_used = Some(now);
        Ok(AccountLease {
            index: fallback_index,
            name: account.name.clone(),
            auth_data: account.auth_data.clone(),
        })
    }

    fn mark_success(&mut self, index: usize) {
        if let Some(account) = self.accounts.get_mut(index) {
            account.fail_count = 0;
            account.disabled_until = None;
            account.last_error = None;
        }
    }

    fn mark_failure(&mut self, index: usize, message: String) {
        if let Some(account) = self.accounts.get_mut(index) {
            account.fail_count = account.fail_count.saturating_add(1);
            let backoff = failure_backoff(account.fail_count);
            account.disabled_until = Some(
                chrono::Utc::now()
                    + chrono::Duration::from_std(backoff)
                        .unwrap_or_else(|_| chrono::Duration::seconds(60)),
            );
            account.last_error = Some(message);
        }
    }

    fn replace_auth_data(&mut self, index: usize, auth_data: AuthData) {
        if let Some(account) = self.accounts.get_mut(index) {
            account.auth_data = auth_data;
            account.fail_count = 0;
            account.disabled_until = None;
            account.last_error = None;
        }
    }

    fn update_quota(&mut self, index: usize, quota: QuotaSnapshot) {
        if let Some(account) = self.accounts.get_mut(index) {
            account.quota = Some(quota);
        }
    }

    fn auth_snapshot(&self, index: usize) -> Option<(String, String, AuthData)> {
        self.accounts.get(index).map(|account| {
            (
                account.name.clone(),
                account.auth_path.clone(),
                account.auth_data.clone(),
            )
        })
    }

    fn accounts_needing_refresh(&self, threshold: Duration) -> Vec<usize> {
        let now = Utc::now();
        self.accounts
            .iter()
            .enumerate()
            .filter_map(|(index, account)| {
                if account.auth_data.needs_refresh(now, threshold) {
                    Some(index)
                } else {
                    None
                }
            })
            .collect()
    }

    fn health(&self) -> HealthResponse {
        let now = chrono::Utc::now();
        let accounts = self
            .accounts
            .iter()
            .map(|account| {
                let healthy = account
                    .disabled_until
                    .map(|until| until <= now)
                    .unwrap_or(true);
                AccountHealth {
                    name: account.name.clone(),
                    path: account.auth_path.clone(),
                    healthy,
                    quota: account.quota.clone(),
                    fail_count: account.fail_count,
                    disabled_until: account.disabled_until.map(|v| v.to_rfc3339()),
                    last_error: account.last_error.clone(),
                }
            })
            .collect::<Vec<_>>();

        let accounts_healthy = accounts.iter().filter(|account| account.healthy).count();
        let next_account = self
            .accounts
            .get(self.next_index)
            .map(|account| account.name.clone());

        HealthResponse {
            status: "ok",
            service: "codex-openai-proxy",
            accounts_loaded: self.accounts.len(),
            accounts_healthy,
            next_account,
            accounts,
        }
    }
}

impl AuthData {
    fn needs_refresh(&self, now: DateTime<Utc>, threshold: Duration) -> bool {
        let Some(tokens) = &self.tokens else {
            return false;
        };

        let threshold =
            chrono::Duration::from_std(threshold).unwrap_or_else(|_| chrono::Duration::minutes(5));
        let refresh_by = now + threshold;

        let access_expiring = jwt_expiry(&tokens.access_token)
            .map(|expiry| expiry <= refresh_by)
            .unwrap_or(false);
        let id_expiring = tokens
            .id_token
            .as_deref()
            .and_then(jwt_expiry)
            .map(|expiry| expiry <= refresh_by)
            .unwrap_or(false);

        access_expiring || id_expiring
    }

    fn can_refresh(&self) -> bool {
        self.tokens
            .as_ref()
            .and_then(|tokens| tokens.refresh_token.as_ref())
            .map(|token| !token.is_empty())
            .unwrap_or(false)
    }

    fn to_persisted_json(&self) -> Value {
        match (&self.tokens, &self.api_key) {
            (Some(tokens), _) => json!({
                "auth_mode": self.auth_mode.clone().unwrap_or_else(|| "chatgpt".to_string()),
                "OPENAI_API_KEY": self.api_key,
                "tokens": tokens,
                "last_refresh": self.last_refresh.map(|v| v.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)),
            }),
            (None, Some(api_key)) => json!({
                "auth_mode": self.auth_mode,
                "OPENAI_API_KEY": api_key,
                "last_refresh": self.last_refresh.map(|v| v.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)),
            }),
            (None, None) => json!({}),
        }
    }
}

impl ProxyServer {
    async fn new(
        auth_paths: &[String],
        proxy_bearer_token: Option<String>,
        debug_logs: bool,
    ) -> Result<Self> {
        let client = Client::builder()
            .user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
            .build()
            .context("Failed to create HTTP client")?;

        let account_pool = AccountPool::load(auth_paths).await?;
        let server = Self {
            client,
            account_pool: Arc::new(Mutex::new(account_pool)),
            proxy_bearer_token,
            debug_logs,
        };
        server.spawn_refresh_loop();
        Ok(server)
    }

    async fn health(&self) -> HealthResponse {
        self.account_pool.lock().await.health()
    }

    async fn proxy_request(
        &self,
        chat_req: ChatCompletionsRequest,
    ) -> Result<ChatCompletionsResponse> {
        let backend = self.execute_backend_request(chat_req.clone()).await?;
        Ok(self.translate_to_chat_completion(chat_req.model, backend))
    }

    async fn proxy_stream_request(&self, chat_req: ChatCompletionsRequest) -> Result<String> {
        let requested_model = chat_req.model.clone();
        let backend = self.execute_backend_request(chat_req).await?;
        Ok(translate_to_chat_sse(&requested_model, backend.summary))
    }

    async fn execute_backend_request(
        &self,
        chat_req: ChatCompletionsRequest,
    ) -> Result<BackendResponse> {
        let total_accounts = self.account_pool.lock().await.accounts.len();
        let mut last_retryable_error = None;

        for _ in 0..total_accounts {
            let lease = {
                let mut pool = self.account_pool.lock().await;
                pool.lease_next_account()?
            };

            if let Err(error) = self.ensure_account_fresh(&lease).await {
                eprintln!("Refresh check failed for {}: {}", lease.name, error);
            }
            let effective_lease = {
                let pool = self.account_pool.lock().await;
                let Some((name, _, auth_data)) = pool.auth_snapshot(lease.index) else {
                    return Err(anyhow!("Account disappeared before request dispatch"));
                };
                AccountLease {
                    index: lease.index,
                    name,
                    auth_data,
                }
            };
            log_request(
                "POST",
                "/backend-api/codex/responses",
                Some(&effective_lease.name),
            );
            self.debug_log_chat_request(&chat_req);

            match self.send_backend_request(&effective_lease, &chat_req).await {
                Ok(response) => {
                    self.account_pool
                        .lock()
                        .await
                        .mark_success(effective_lease.index);
                    return Ok(response);
                }
                Err(error) if error.auth_failed => {
                    match self.refresh_account(effective_lease.index, true).await {
                        Ok(_) => {
                            let refreshed_lease = {
                                let pool = self.account_pool.lock().await;
                                let Some((name, _, auth_data)) =
                                    pool.auth_snapshot(effective_lease.index)
                                else {
                                    return Err(anyhow!("Account disappeared during refresh"));
                                };
                                AccountLease {
                                    index: effective_lease.index,
                                    name,
                                    auth_data,
                                }
                            };
                            match self.send_backend_request(&refreshed_lease, &chat_req).await {
                                Ok(response) => {
                                    self.account_pool
                                        .lock()
                                        .await
                                        .mark_success(refreshed_lease.index);
                                    return Ok(response);
                                }
                                Err(retry_error) if retry_error.retryable => {
                                    eprintln!(
                                        "Retryable upstream failure on {} after refresh: {}",
                                        refreshed_lease.name, retry_error.message
                                    );
                                    self.account_pool.lock().await.mark_failure(
                                        refreshed_lease.index,
                                        retry_error.message.clone(),
                                    );
                                    last_retryable_error = Some(retry_error.message);
                                }
                                Err(retry_error) => return Err(anyhow!(retry_error.message)),
                            }
                        }
                        Err(refresh_error) => {
                            eprintln!(
                                "Forced refresh failed for {}: {}",
                                effective_lease.name, refresh_error
                            );
                            self.account_pool
                                .lock()
                                .await
                                .mark_failure(effective_lease.index, error.message.clone());
                            last_retryable_error = Some(error.message);
                        }
                    }
                }
                Err(error) if error.retryable => {
                    eprintln!(
                        "Retryable upstream failure on {}: {}",
                        effective_lease.name, error.message
                    );
                    self.account_pool
                        .lock()
                        .await
                        .mark_failure(effective_lease.index, error.message.clone());
                    last_retryable_error = Some(error.message);
                }
                Err(error) => {
                    return Err(anyhow!(error.message));
                }
            }
        }

        Err(anyhow!(
            "All configured accounts failed. Last error: {}",
            last_retryable_error.unwrap_or_else(|| "unknown upstream failure".to_string())
        ))
    }

    async fn send_backend_request(
        &self,
        lease: &AccountLease,
        chat_req: &ChatCompletionsRequest,
    ) -> std::result::Result<BackendResponse, AttemptError> {
        let responses_req = self.convert_chat_to_responses(chat_req);
        if self.debug_logs {
            println!(
                "DEBUG upstream request: account={}, model={}, input_messages={}",
                lease.name,
                responses_req.model,
                responses_req.input.len()
            );
        }
        let mut request_builder = self
            .client
            .post("https://chatgpt.com/backend-api/codex/responses")
            .header("Content-Type", "application/json")
            .header("Accept", "text/event-stream")
            .header("Accept-Language", "en-US,en;q=0.9")
            .header("Accept-Encoding", "gzip, deflate, br")
            .header("Referer", "https://chatgpt.com/")
            .header("Origin", "https://chatgpt.com")
            .header("Sec-Fetch-Dest", "empty")
            .header("Sec-Fetch-Mode", "cors")
            .header("Sec-Fetch-Site", "same-origin")
            .header("Cache-Control", "no-cache")
            .header("Pragma", "no-cache")
            .header("DNT", "1")
            .header("OpenAI-Beta", "responses=experimental")
            .header("originator", "codex_cli_rs")
            .header("session_id", Uuid::new_v4().to_string());

        if let Some(tokens) = &lease.auth_data.tokens {
            request_builder = request_builder
                .header("Authorization", format!("Bearer {}", tokens.access_token))
                .header("chatgpt-account-id", &tokens.account_id);
        } else if let Some(api_key) = &lease.auth_data.api_key {
            request_builder =
                request_builder.header("Authorization", format!("Bearer {}", api_key));
        } else {
            return Err(AttemptError::fatal(format!(
                "{} does not have usable authentication data",
                lease.name
            )));
        }

        let response = request_builder
            .json(&responses_req)
            .send()
            .await
            .map_err(|error| AttemptError::retryable(format!("Network error: {}", error)))?;

        let status = response.status();
        let quota = quota_from_headers(response.headers());
        let body = response.text().await.map_err(|error| {
            AttemptError::retryable(format!("Failed to read upstream response: {}", error))
        })?;

        if let Some(quota) = quota {
            self.account_pool
                .lock()
                .await
                .update_quota(lease.index, quota);
        }

        if !status.is_success() {
            let summary = summarize_body(&body);
            if self.debug_logs {
                println!(
                    "DEBUG upstream error: status={}, body={}",
                    status,
                    preview_text(&body, 800)
                );
            }
            if status.as_u16() == 401 || status.as_u16() == 403 {
                return Err(AttemptError::auth_retryable(format!(
                    "Upstream status {} for {}: {}",
                    status, lease.name, summary
                )));
            }
            if status.is_server_error() || status.as_u16() == 429 {
                return Err(AttemptError::retryable(format!(
                    "Upstream status {} for {}: {}",
                    status, lease.name, summary
                )));
            }

            return Err(AttemptError::fatal(format!(
                "Upstream status {}: {}",
                status, summary
            )));
        }

        let summary = parse_responses_event_stream(&body).map_err(|error| {
            AttemptError::fatal(format!("Failed to parse upstream SSE: {}", error))
        })?;
        if self.debug_logs {
            println!(
                "DEBUG upstream success: response_id={:?}, text_preview={}",
                summary.response_id,
                preview_text(&summary.text, 180)
            );
        }

        Ok(BackendResponse {
            model: chat_req.model.clone(),
            summary,
        })
    }

    fn spawn_refresh_loop(&self) {
        let proxy = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(TOKEN_REFRESH_INTERVAL);
            interval.tick().await;
            loop {
                interval.tick().await;
                if let Err(error) = proxy.refresh_expiring_accounts().await {
                    eprintln!("Background refresh loop error: {}", error);
                }
            }
        });
    }

    async fn refresh_expiring_accounts(&self) -> Result<()> {
        let indices = {
            let pool = self.account_pool.lock().await;
            pool.accounts_needing_refresh(TOKEN_REFRESH_THRESHOLD)
        };

        for index in indices {
            if let Err(error) = self.refresh_account(index, false).await {
                eprintln!(
                    "Scheduled refresh failed for account {}: {}",
                    index + 1,
                    error
                );
            }
        }

        Ok(())
    }

    async fn ensure_account_fresh(&self, lease: &AccountLease) -> Result<()> {
        if lease
            .auth_data
            .needs_refresh(Utc::now(), TOKEN_REFRESH_THRESHOLD)
        {
            self.refresh_account(lease.index, false).await?;
        }
        Ok(())
    }

    async fn refresh_account(&self, index: usize, force: bool) -> Result<()> {
        let (name, path, auth_data) = {
            let pool = self.account_pool.lock().await;
            pool.auth_snapshot(index)
                .ok_or_else(|| anyhow!("Unknown account index {}", index))?
        };

        if !auth_data.can_refresh() {
            return Ok(());
        }
        if !force && !auth_data.needs_refresh(Utc::now(), TOKEN_REFRESH_THRESHOLD) {
            return Ok(());
        }

        let refreshed_auth = self.perform_refresh(&name, auth_data).await?;
        persist_auth_file(&path, &refreshed_auth)
            .await
            .with_context(|| format!("Failed to persist refreshed auth for {}", name))?;

        self.account_pool
            .lock()
            .await
            .replace_auth_data(index, refreshed_auth);
        println!("Refreshed credentials for {}", name);
        Ok(())
    }

    async fn perform_refresh(&self, account_name: &str, auth_data: AuthData) -> Result<AuthData> {
        let tokens = auth_data
            .tokens
            .clone()
            .ok_or_else(|| anyhow!("{} has no token-based auth to refresh", account_name))?;
        let refresh_token = tokens
            .refresh_token
            .clone()
            .filter(|token| !token.is_empty())
            .ok_or_else(|| anyhow!("{} has no refresh token", account_name))?;

        let response = self
            .client
            .post("https://auth.openai.com/oauth/token")
            .form(&[
                ("grant_type", "refresh_token"),
                ("client_id", CHATGPT_CLIENT_ID),
                ("refresh_token", refresh_token.as_str()),
                ("scope", "openid profile email offline_access"),
            ])
            .send()
            .await
            .with_context(|| format!("Refresh request failed for {}", account_name))?;

        let status = response.status();
        let body = response
            .text()
            .await
            .context("Failed to read refresh response")?;
        if !status.is_success() {
            bail!(
                "Refresh failed for {} with status {}: {}",
                account_name,
                status,
                summarize_body(&body)
            );
        }

        let refresh: RefreshTokenResponse = serde_json::from_str(&body)
            .with_context(|| format!("Invalid refresh response for {}", account_name))?;

        Ok(AuthData {
            auth_mode: auth_data.auth_mode,
            api_key: auth_data.api_key,
            tokens: Some(TokenData {
                id_token: refresh.id_token.or(tokens.id_token),
                access_token: refresh.access_token,
                account_id: tokens.account_id,
                refresh_token: refresh.refresh_token.or(tokens.refresh_token),
            }),
            last_refresh: Some(Utc::now()),
        })
    }

    fn convert_chat_to_responses(&self, chat_req: &ChatCompletionsRequest) -> ResponsesApiRequest {
        let mut input = Vec::new();
        let mut system_instructions = Vec::new();

        for msg in &chat_req.messages {
            if msg.role == "system" {
                let text = message_content_to_text(&msg.content);
                if !text.trim().is_empty() {
                    system_instructions.push(text);
                }
                continue;
            }
            input.push(ResponseItem::Message {
                id: None,
                role: msg.role.clone(),
                content: vec![content_item_for_role(
                    &msg.role,
                    message_content_to_text(&msg.content),
                )],
            });
        }

        let instructions =
            (!system_instructions.is_empty()).then(|| system_instructions.join("\n\n"));
        let tools = normalize_tools(chat_req.tools.as_deref().unwrap_or(&[]));
        let tool_choice = normalize_tool_choice(chat_req.tool_choice.as_ref());

        ResponsesApiRequest {
            model: chat_req.model.clone(),
            instructions,
            input,
            tools,
            tool_choice,
            parallel_tool_calls: false,
            reasoning: None,
            store: false,
            stream: true,
            include: vec![],
        }
    }

    fn debug_log_chat_request(&self, chat_req: &ChatCompletionsRequest) {
        if !self.debug_logs {
            return;
        }

        let roles = chat_req
            .messages
            .iter()
            .map(|message| message.role.as_str())
            .collect::<Vec<_>>();
        let system_count = chat_req
            .messages
            .iter()
            .filter(|message| message.role == "system")
            .count();

        println!(
            "DEBUG inbound chat request: model={}, stream={:?}, messages={}, roles={:?}, system_messages={}, tools={}",
            chat_req.model,
            chat_req.stream,
            chat_req.messages.len(),
            roles,
            system_count,
            chat_req.tools.as_ref().map(|tools| tools.len()).unwrap_or(0)
        );

        if let Some(tools) = &chat_req.tools {
            for (index, tool) in tools.iter().enumerate() {
                let tool_name = tool
                    .get("function")
                    .and_then(|function| function.get("name"))
                    .and_then(Value::as_str)
                    .or_else(|| tool.get("name").and_then(Value::as_str))
                    .unwrap_or("unknown");
                println!(
                    "DEBUG tool[{}]: type={}, name={}",
                    index,
                    tool.get("type")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown"),
                    tool_name
                );
            }
        }

        for (index, message) in chat_req.messages.iter().enumerate() {
            println!(
                "DEBUG message[{}]: role={}, preview={}",
                index,
                message.role,
                preview_text(&message_content_to_text(&message.content), 180)
            );
        }
    }

    fn translate_to_chat_completion(
        &self,
        requested_model: String,
        backend: BackendResponse,
    ) -> ChatCompletionsResponse {
        let content = if backend.summary.text.is_empty() {
            "Upstream returned no text content".to_string()
        } else {
            backend.summary.text
        };

        ChatCompletionsResponse {
            id: backend
                .summary
                .response_id
                .unwrap_or_else(|| format!("chatcmpl-{}", Uuid::new_v4())),
            object: "chat.completion".to_string(),
            created: chrono::Utc::now().timestamp(),
            model: backend.model.if_empty_then(requested_model),
            choices: vec![Choice {
                index: 0,
                message: ChatResponseMessage {
                    role: "assistant".to_string(),
                    content,
                },
                finish_reason: Some("stop".to_string()),
            }],
            usage: None,
        }
    }
}

trait StringFallback {
    fn if_empty_then(self, fallback: String) -> String;
}

impl StringFallback for String {
    fn if_empty_then(self, fallback: String) -> String {
        if self.is_empty() {
            fallback
        } else {
            self
        }
    }
}

fn failure_backoff(fail_count: u32) -> Duration {
    let seconds = match fail_count {
        0 | 1 => 15,
        2 => 30,
        3 => 60,
        _ => 120,
    };
    Duration::from_secs(seconds)
}

fn parse_optional_timestamp(value: Option<&str>) -> Result<Option<DateTime<Utc>>> {
    value
        .map(|timestamp| {
            DateTime::parse_from_rfc3339(timestamp)
                .map(|v| v.with_timezone(&Utc))
                .with_context(|| format!("invalid timestamp {}", timestamp))
        })
        .transpose()
}

fn expand_home(path: &str) -> Result<String> {
    if path.starts_with("~/") {
        let home = std::env::var("HOME").context("HOME environment variable not set")?;
        Ok(path.replacen('~', &home, 1))
    } else {
        Ok(path.to_string())
    }
}

fn jwt_expiry(token: &str) -> Option<DateTime<Utc>> {
    let payload = token.split('.').nth(1)?;
    let decoded = URL_SAFE_NO_PAD.decode(payload).ok()?;
    let claims: Value = serde_json::from_slice(&decoded).ok()?;
    let exp = claims.get("exp")?.as_i64()?;
    DateTime::<Utc>::from_timestamp(exp, 0)
}

async fn persist_auth_file(path: &str, auth_data: &AuthData) -> Result<()> {
    let json = serde_json::to_string_pretty(&auth_data.to_persisted_json())
        .context("Failed to serialize refreshed auth")?;
    tokio::fs::write(path, format!("{}\n", json))
        .await
        .with_context(|| format!("Failed to write {}", path))?;
    Ok(())
}

fn normalize_tools(tools: &[Value]) -> Vec<Value> {
    tools
        .iter()
        .map(|tool| {
            let Some(tool_type) = tool.get("type").and_then(Value::as_str) else {
                return tool.clone();
            };

            match tool_type {
                "function" => {
                    if let Some(function) = tool.get("function").and_then(Value::as_object) {
                        json!({
                            "type": "function",
                            "name": function.get("name").cloned().unwrap_or(Value::Null),
                            "description": function.get("description").cloned().unwrap_or(Value::Null),
                            "parameters": function.get("parameters").cloned().unwrap_or_else(|| json!({}))
                        })
                    } else {
                        tool.clone()
                    }
                }
                _ => tool.clone(),
            }
        })
        .collect()
}

fn normalize_tool_choice(tool_choice: Option<&Value>) -> Value {
    let Some(choice) = tool_choice else {
        return Value::String("auto".to_string());
    };

    if let Some(choice_str) = choice.as_str() {
        return Value::String(choice_str.to_string());
    }

    if let Some(choice_type) = choice.get("type").and_then(Value::as_str) {
        match choice_type {
            "auto" | "none" | "required" => return Value::String(choice_type.to_string()),
            "function" => {
                if let Some(function_name) = choice
                    .get("function")
                    .and_then(|function| function.get("name"))
                    .and_then(Value::as_str)
                {
                    return json!({
                        "type": "function",
                        "name": function_name
                    });
                }
            }
            _ => {}
        }
    }

    choice.clone()
}

fn quota_from_headers(headers: &reqwest::header::HeaderMap) -> Option<QuotaSnapshot> {
    let has_codex_headers = headers
        .keys()
        .any(|name| name.as_str().starts_with("x-codex-"));
    if !has_codex_headers {
        return None;
    }

    let source = if header_str(headers, "x-codex-bengalfox-limit-name").is_some() {
        "x-codex-bengalfox-*"
    } else {
        "x-codex-*"
    };

    let primary_used = header_u8(headers, "x-codex-primary-used-percent");
    let secondary_used = header_u8(headers, "x-codex-secondary-used-percent");

    Some(QuotaSnapshot {
        observed_at: Utc::now().to_rfc3339(),
        source: source.to_string(),
        plan_type: header_string(headers, "x-codex-plan-type"),
        active_limit: header_string(headers, "x-codex-active-limit"),
        primary_used_percent: primary_used,
        primary_remaining_percent: primary_used.map(|v| 100u8.saturating_sub(v)),
        primary_window_minutes: header_u32(headers, "x-codex-primary-window-minutes"),
        primary_reset_at: header_i64(headers, "x-codex-primary-reset-at"),
        primary_reset_after_seconds: header_u32(headers, "x-codex-primary-reset-after-seconds"),
        secondary_used_percent: secondary_used,
        secondary_remaining_percent: secondary_used.map(|v| 100u8.saturating_sub(v)),
        secondary_window_minutes: header_u32(headers, "x-codex-secondary-window-minutes"),
        secondary_reset_at: header_i64(headers, "x-codex-secondary-reset-at"),
        secondary_reset_after_seconds: header_u32(headers, "x-codex-secondary-reset-after-seconds"),
        credits_has_credits: header_bool(headers, "x-codex-credits-has-credits"),
        credits_unlimited: header_bool(headers, "x-codex-credits-unlimited"),
        credits_balance: header_string(headers, "x-codex-credits-balance"),
        raw_limit_name: header_string(headers, "x-codex-bengalfox-limit-name"),
    })
}

fn header_str<'a>(headers: &'a reqwest::header::HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

fn header_string(headers: &reqwest::header::HeaderMap, name: &str) -> Option<String> {
    header_str(headers, name)
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToString::to_string)
}

fn header_u8(headers: &reqwest::header::HeaderMap, name: &str) -> Option<u8> {
    header_str(headers, name)?.trim().parse().ok()
}

fn header_u32(headers: &reqwest::header::HeaderMap, name: &str) -> Option<u32> {
    header_str(headers, name)?.trim().parse().ok()
}

fn header_i64(headers: &reqwest::header::HeaderMap, name: &str) -> Option<i64> {
    header_str(headers, name)?.trim().parse().ok()
}

fn header_bool(headers: &reqwest::header::HeaderMap, name: &str) -> Option<bool> {
    match header_str(headers, name)?
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

fn message_content_to_text(content: &Value) -> String {
    match content {
        Value::String(s) => s.clone(),
        Value::Array(arr) => arr
            .iter()
            .filter_map(|value| {
                value
                    .get("text")
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
                    .or_else(|| value.as_str().map(ToString::to_string))
            })
            .collect::<Vec<_>>()
            .join(" "),
        _ => content.to_string(),
    }
}

fn content_item_for_role(role: &str, text: String) -> ContentItem {
    match role {
        "assistant" => ContentItem::OutputText { text },
        _ => ContentItem::InputText { text },
    }
}

fn parse_responses_event_stream(stream: &str) -> Result<BackendEventSummary> {
    let mut response_id = None;
    let mut deltas = Vec::new();
    let mut final_text_parts = Vec::new();

    for line in stream.lines() {
        if !line.starts_with("data: ") {
            continue;
        }

        let json_payload = &line[6..];
        if json_payload == "[DONE]" {
            break;
        }

        let event: Value = serde_json::from_str(json_payload).with_context(|| {
            format!(
                "invalid SSE event payload: {}",
                summarize_body(json_payload)
            )
        })?;

        if response_id.is_none() {
            response_id = event
                .get("response")
                .and_then(|response| response.get("id"))
                .and_then(Value::as_str)
                .map(ToString::to_string)
                .or_else(|| {
                    event
                        .get("id")
                        .and_then(Value::as_str)
                        .map(ToString::to_string)
                });
        }

        match event.get("type").and_then(Value::as_str) {
            Some("response.output_text.delta") => {
                if let Some(delta) = event.get("delta").and_then(Value::as_str) {
                    deltas.push(delta.to_string());
                }
            }
            Some("response.output_item.done") => {
                if deltas.is_empty() {
                    if let Some(content) = event
                        .get("item")
                        .and_then(|item| item.get("content"))
                        .and_then(Value::as_array)
                    {
                        for part in content {
                            if let Some(text) = part.get("text").and_then(Value::as_str) {
                                final_text_parts.push(text.to_string());
                            }
                        }
                    }
                }
            }
            Some("response.completed") => {
                if deltas.is_empty() {
                    if let Some(output) = event
                        .get("response")
                        .and_then(|response| response.get("output"))
                        .and_then(Value::as_array)
                    {
                        for item in output {
                            if let Some(content) = item.get("content").and_then(Value::as_array) {
                                for part in content {
                                    if let Some(text) = part.get("text").and_then(Value::as_str) {
                                        final_text_parts.push(text.to_string());
                                    }
                                }
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }

    let text = if deltas.is_empty() {
        final_text_parts.join("")
    } else {
        deltas.join("")
    };

    Ok(BackendEventSummary {
        response_id,
        text,
        deltas,
    })
}

fn translate_to_chat_sse(requested_model: &str, summary: BackendEventSummary) -> String {
    let chunk_id = summary
        .response_id
        .unwrap_or_else(|| format!("chatcmpl-{}", Uuid::new_v4()));
    let created = chrono::Utc::now().timestamp();
    let mut chunks = Vec::new();

    chunks.push(format!(
        "data: {{\"id\":\"{}\",\"object\":\"chat.completion.chunk\",\"created\":{},\"model\":\"{}\",\"choices\":[{{\"index\":0,\"delta\":{{\"role\":\"assistant\"}},\"finish_reason\":null}}]}}\n\n",
        chunk_id, created, requested_model
    ));

    if summary.deltas.is_empty() {
        if !summary.text.is_empty() {
            chunks.push(format!(
                "data: {{\"id\":\"{}\",\"object\":\"chat.completion.chunk\",\"created\":{},\"model\":\"{}\",\"choices\":[{{\"index\":0,\"delta\":{{\"content\":{}}},\"finish_reason\":null}}]}}\n\n",
                chunk_id,
                created,
                requested_model,
                serde_json::to_string(&summary.text).unwrap_or_else(|_| "\"\"".to_string())
            ));
        }
    } else {
        for delta in summary.deltas {
            chunks.push(format!(
                "data: {{\"id\":\"{}\",\"object\":\"chat.completion.chunk\",\"created\":{},\"model\":\"{}\",\"choices\":[{{\"index\":0,\"delta\":{{\"content\":{}}},\"finish_reason\":null}}]}}\n\n",
                chunk_id,
                created,
                requested_model,
                serde_json::to_string(&delta).unwrap_or_else(|_| "\"\"".to_string())
            ));
        }
    }

    chunks.push(format!(
        "data: {{\"id\":\"{}\",\"object\":\"chat.completion.chunk\",\"created\":{},\"model\":\"{}\",\"choices\":[{{\"index\":0,\"delta\":{{}},\"finish_reason\":\"stop\"}}]}}\n\n",
        chunk_id, created, requested_model
    ));
    chunks.push("data: [DONE]\n\n".to_string());

    chunks.join("")
}

fn summarize_body(body: &str) -> String {
    body.chars()
        .take(240)
        .collect::<String>()
        .replace('\n', " ")
}

fn preview_text(text: &str, max_chars: usize) -> String {
    let mut preview = text.chars().take(max_chars).collect::<String>();
    if text.chars().count() > max_chars {
        preview.push_str("...");
    }
    preview.replace('\n', "\\n")
}

fn log_request(method: &str, path: &str, account: Option<&str>) {
    let timestamp = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S%.3f UTC");
    match account {
        Some(account_name) => println!("[{}] {} {} via {}", timestamp, method, path, account_name),
        None => println!("[{}] {} {}", timestamp, method, path),
    }
}

fn extract_bearer_token(headers: &warp::http::HeaderMap) -> Option<&str> {
    let value = headers.get("authorization")?.to_str().ok()?;
    value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))
}

fn proxy_auth_reply() -> warp::reply::Response {
    warp::reply::with_status("Unauthorized", warp::http::StatusCode::UNAUTHORIZED).into_response()
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();

    let proxy = ProxyServer::new(
        &args.auth_paths,
        args.proxy_bearer_token.clone(),
        args.debug_logs,
    )
    .await?;
    let health = proxy.health().await;

    println!("Initializing Codex OpenAI Proxy...");
    println!("Loaded {} auth file(s)", health.accounts_loaded);
    println!("Listening on http://0.0.0.0:{}", args.port);
    println!(
        "Proxy bearer auth: {}",
        if args.proxy_bearer_token.is_some() {
            "enabled"
        } else {
            "disabled"
        }
    );
    println!(
        "Debug logs: {}",
        if args.debug_logs {
            "enabled"
        } else {
            "disabled"
        }
    );

    let proxy_filter = warp::any().map(move || proxy.clone());

    let cors = warp::cors()
        .allow_any_origin()
        .allow_headers(vec![
            "authorization",
            "content-type",
            "accept",
            "accept-encoding",
            "x-stainless-arch",
            "x-stainless-lang",
            "x-stainless-os",
            "x-stainless-package-version",
            "x-stainless-retry-count",
            "x-stainless-runtime",
            "x-stainless-runtime-version",
            "x-stainless-timeout",
        ])
        .allow_methods(vec!["GET", "POST", "PUT", "DELETE", "OPTIONS"]);

    let routes = warp::any()
        .and(warp::method())
        .and(warp::path::full())
        .and(warp::header::headers_cloned())
        .and(warp::body::bytes())
        .and(proxy_filter)
        .and_then(universal_request_handler)
        .with(cors)
        .with(warp::log("codex_proxy"));

    warp::serve(routes).run(([0, 0, 0, 0], args.port)).await;
    Ok(())
}

async fn universal_request_handler(
    method: warp::http::Method,
    path: warp::path::FullPath,
    headers: warp::http::HeaderMap,
    body: bytes::Bytes,
    proxy: ProxyServer,
) -> Result<impl warp::Reply, warp::Rejection> {
    let path_str = path.as_str();
    log_request(method.as_str(), path_str, None);

    if let Some(expected_token) = &proxy.proxy_bearer_token {
        let provided_token = extract_bearer_token(&headers);
        if provided_token != Some(expected_token.as_str()) {
            return Ok(proxy_auth_reply());
        }
    }

    match (method.as_str(), path_str) {
        ("GET", "/health") => Ok(warp::reply::json(&proxy.health().await).into_response()),
        ("GET", "/models") | ("GET", "/v1/models") => Ok(warp::reply::json(&json!({
            "object": "list",
            "data": [
                {
                    "id": "gpt-4",
                    "object": "model",
                    "created": 1687882411,
                    "owned_by": "openai"
                },
                {
                    "id": "gpt-5",
                    "object": "model",
                    "created": 1687882411,
                    "owned_by": "openai"
                }
            ]
        }))
        .into_response()),
        ("POST", "/chat/completions") | ("POST", "/v1/chat/completions") => {
            let chat_req: ChatCompletionsRequest = match serde_json::from_slice(&body) {
                Ok(req) => req,
                Err(error) => {
                    eprintln!("Invalid JSON body: {}", error);
                    return Ok(warp::reply::with_status(
                        "Invalid JSON",
                        warp::http::StatusCode::BAD_REQUEST,
                    )
                    .into_response());
                }
            };

            if chat_req.stream.unwrap_or(false) {
                match proxy.proxy_stream_request(chat_req).await {
                    Ok(sse_response) => {
                        let reply = warp::reply::with_header(
                            sse_response,
                            "content-type",
                            "text/event-stream",
                        );
                        let reply = warp::reply::with_header(reply, "cache-control", "no-cache");
                        let reply = warp::reply::with_header(reply, "connection", "keep-alive");
                        let reply =
                            warp::reply::with_header(reply, "access-control-allow-origin", "*");
                        Ok(reply.into_response())
                    }
                    Err(error) => {
                        eprintln!("Proxy error: {:#}", error);
                        Ok(warp::reply::json(&json!({
                            "error": {
                                "message": format!("Proxy error: {}", error),
                                "type": "proxy_error",
                                "code": "internal_error"
                            }
                        }))
                        .into_response())
                    }
                }
            } else {
                match proxy.proxy_request(chat_req).await {
                    Ok(response) => Ok(warp::reply::json(&response).into_response()),
                    Err(error) => {
                        eprintln!("Proxy error: {:#}", error);
                        Ok(warp::reply::json(&json!({
                            "error": {
                                "message": format!("Proxy error: {}", error),
                                "type": "proxy_error",
                                "code": "internal_error"
                            }
                        }))
                        .into_response())
                    }
                }
            }
        }
        _ => Ok(
            warp::reply::with_status("Not found", warp::http::StatusCode::NOT_FOUND)
                .into_response(),
        ),
    }
}

impl Clone for ProxyServer {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            account_pool: Arc::clone(&self.account_pool),
            proxy_bearer_token: self.proxy_bearer_token.clone(),
            debug_logs: self.debug_logs,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_nested_token_auth() {
        let raw: RawAuthData = serde_json::from_value(json!({
            "tokens": {
                "access_token": "token-a",
                "account_id": "account-a",
                "refresh_token": "refresh-a"
            }
        }))
        .unwrap();

        let auth = raw.normalize().unwrap();
        assert_eq!(auth.tokens.unwrap().account_id, "account-a");
    }

    #[test]
    fn normalizes_flat_token_auth() {
        let raw: RawAuthData = serde_json::from_value(json!({
            "access_token": "token-b",
            "account_id": "account-b"
        }))
        .unwrap();

        let auth = raw.normalize().unwrap();
        assert_eq!(auth.tokens.unwrap().access_token, "token-b");
    }

    #[test]
    fn supports_api_key_only_auth() {
        let raw: RawAuthData = serde_json::from_value(json!({
            "OPENAI_API_KEY": "sk-test"
        }))
        .unwrap();

        let auth = raw.normalize().unwrap();
        assert_eq!(auth.api_key.unwrap(), "sk-test");
    }

    #[test]
    fn account_pool_rotates_and_skips_backed_off_accounts() {
        let auth = AuthData {
            auth_mode: Some("chatgpt".to_string()),
            api_key: Some("sk-test".to_string()),
            tokens: None,
            last_refresh: None,
        };

        let mut pool = AccountPool {
            accounts: vec![
                Account {
                    name: "account-1".to_string(),
                    auth_path: "/tmp/a1".to_string(),
                    auth_data: auth.clone(),
                    last_used: None,
                    quota: None,
                    fail_count: 0,
                    disabled_until: Some(chrono::Utc::now() + chrono::Duration::seconds(30)),
                    last_error: Some("failed".to_string()),
                },
                Account {
                    name: "account-2".to_string(),
                    auth_path: "/tmp/a2".to_string(),
                    auth_data: auth.clone(),
                    last_used: None,
                    quota: None,
                    fail_count: 0,
                    disabled_until: None,
                    last_error: None,
                },
                Account {
                    name: "account-3".to_string(),
                    auth_path: "/tmp/a3".to_string(),
                    auth_data: auth,
                    last_used: None,
                    quota: None,
                    fail_count: 0,
                    disabled_until: None,
                    last_error: None,
                },
            ],
            next_index: 0,
        };

        let first = pool.lease_next_account().unwrap();
        let second = pool.lease_next_account().unwrap();

        assert_eq!(first.name, "account-2");
        assert_eq!(second.name, "account-3");
    }

    #[test]
    fn parses_delta_sse() {
        let summary = parse_responses_event_stream(
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"Hello\"}\n\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\" world\"}\n\ndata: [DONE]\n",
        )
        .unwrap();

        assert_eq!(summary.text, "Hello world");
        assert_eq!(summary.deltas.len(), 2);
    }

    #[test]
    fn detects_refresh_need_from_token_expiry() {
        let soon = Utc::now() + chrono::Duration::seconds(120);
        let token = make_jwt_with_exp(soon.timestamp());
        let auth = AuthData {
            auth_mode: Some("chatgpt".to_string()),
            api_key: None,
            tokens: Some(TokenData {
                id_token: None,
                access_token: token,
                account_id: "acct".to_string(),
                refresh_token: Some("rt".to_string()),
            }),
            last_refresh: None,
        };

        assert!(auth.needs_refresh(Utc::now(), Duration::from_secs(300)));
    }

    #[test]
    fn parses_codex_quota_headers() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("x-codex-plan-type", "plus".parse().unwrap());
        headers.insert("x-codex-active-limit", "codex".parse().unwrap());
        headers.insert("x-codex-primary-used-percent", "10".parse().unwrap());
        headers.insert("x-codex-primary-window-minutes", "300".parse().unwrap());
        headers.insert("x-codex-primary-reset-at", "1774958172".parse().unwrap());
        headers.insert("x-codex-secondary-used-percent", "3".parse().unwrap());
        headers.insert("x-codex-secondary-window-minutes", "10080".parse().unwrap());
        headers.insert("x-codex-credits-has-credits", "False".parse().unwrap());
        headers.insert("x-codex-credits-unlimited", "False".parse().unwrap());

        let quota = quota_from_headers(&headers).unwrap();
        assert_eq!(quota.plan_type.as_deref(), Some("plus"));
        assert_eq!(quota.primary_used_percent, Some(10));
        assert_eq!(quota.primary_remaining_percent, Some(90));
        assert_eq!(quota.secondary_used_percent, Some(3));
        assert_eq!(quota.secondary_remaining_percent, Some(97));
        assert_eq!(quota.credits_has_credits, Some(false));
    }

    #[test]
    fn extracts_proxy_bearer_token() {
        let mut headers = warp::http::HeaderMap::new();
        headers.insert("authorization", "Bearer secret-token".parse().unwrap());
        assert_eq!(extract_bearer_token(&headers), Some("secret-token"));
    }

    #[test]
    fn moves_system_messages_into_instructions() {
        let proxy = ProxyServer {
            client: Client::builder().build().unwrap(),
            account_pool: Arc::new(Mutex::new(AccountPool {
                accounts: vec![],
                next_index: 0,
            })),
            proxy_bearer_token: None,
            debug_logs: false,
        };

        let request = ChatCompletionsRequest {
            model: "gpt-5".to_string(),
            messages: vec![
                ChatMessage {
                    role: "system".to_string(),
                    content: Value::String("System instruction".to_string()),
                },
                ChatMessage {
                    role: "user".to_string(),
                    content: Value::String("Hello".to_string()),
                },
            ],
            temperature: None,
            max_tokens: None,
            stream: None,
            tools: None,
            tool_choice: None,
        };

        let translated = proxy.convert_chat_to_responses(&request);
        assert_eq!(
            translated.instructions.as_deref(),
            Some("System instruction")
        );
        assert_eq!(translated.input.len(), 1);
        match &translated.input[0] {
            ResponseItem::Message { role, .. } => assert_eq!(role, "user"),
        }
    }

    #[test]
    fn normalizes_function_tools_for_upstream() {
        let tools = vec![json!({
            "type": "function",
            "function": {
                "name": "lookup_state",
                "description": "Look up a state",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "entity_id": {"type": "string"}
                    }
                }
            }
        })];

        let normalized = normalize_tools(&tools);
        assert_eq!(normalized[0]["type"], "function");
        assert_eq!(normalized[0]["name"], "lookup_state");
        assert_eq!(normalized[0]["description"], "Look up a state");
        assert!(normalized[0].get("function").is_none());
    }

    #[test]
    fn encodes_assistant_history_as_output_text() {
        let proxy = ProxyServer {
            client: Client::builder().build().unwrap(),
            account_pool: Arc::new(Mutex::new(AccountPool {
                accounts: vec![],
                next_index: 0,
            })),
            proxy_bearer_token: None,
            debug_logs: false,
        };

        let request = ChatCompletionsRequest {
            model: "gpt-5".to_string(),
            messages: vec![
                ChatMessage {
                    role: "user".to_string(),
                    content: Value::String("Hello".to_string()),
                },
                ChatMessage {
                    role: "assistant".to_string(),
                    content: Value::String("Hi there".to_string()),
                },
            ],
            temperature: None,
            max_tokens: None,
            stream: None,
            tools: None,
            tool_choice: None,
        };

        let translated = proxy.convert_chat_to_responses(&request);
        match &translated.input[0] {
            ResponseItem::Message { content, .. } => match &content[0] {
                ContentItem::InputText { .. } => {}
                _ => panic!("user history should be input_text"),
            },
        }
        match &translated.input[1] {
            ResponseItem::Message { content, .. } => match &content[0] {
                ContentItem::OutputText { .. } => {}
                _ => panic!("assistant history should be output_text"),
            },
        }
    }

    fn make_jwt_with_exp(exp: i64) -> String {
        let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"none","typ":"JWT"}"#);
        let payload = URL_SAFE_NO_PAD.encode(format!(r#"{{"exp":{}}}"#, exp));
        format!("{}.{}.", header, payload)
    }
}
