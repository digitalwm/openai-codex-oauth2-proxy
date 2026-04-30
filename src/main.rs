use std::collections::BTreeMap;
use std::convert::Infallible;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, Utc};
use clap::Parser;
use futures_util::StreamExt;
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

    /// Optional dedicated API key for the OpenAI Responses API.
    #[arg(long, env = "RESPONSES_API_KEY")]
    responses_api_key: Option<String>,

    /// Enable verbose request/debug logging.
    #[arg(long, env = "PROXY_DEBUG_LOGS", default_value_t = false)]
    debug_logs: bool,

    /// Seconds between background quota probes.
    #[arg(long, env = "QUOTA_PROBE_INTERVAL_SECONDS", default_value_t = 3600)]
    quota_probe_interval_seconds: u64,

    /// Model used for background quota probes.
    #[arg(long, env = "QUOTA_PROBE_MODEL", default_value = "gpt-5.4")]
    quota_probe_model: String,

    /// Input text used for background quota probes.
    #[arg(long, env = "QUOTA_PROBE_INPUT", default_value = "hi")]
    quota_probe_input: String,
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
    #[serde(default)]
    content: Value,
    #[serde(default)]
    tool_calls: Option<Vec<ChatToolCall>>,
    #[serde(default)]
    tool_call_id: Option<String>,
    #[serde(default)]
    name: Option<String>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct ChatToolCall {
    id: String,
    #[serde(rename = "type")]
    kind: String,
    function: ChatToolCallFunction,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct ChatToolCallFunction {
    name: String,
    arguments: String,
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
    content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_calls: Option<Vec<ChatToolCall>>,
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
    FunctionCall {
        #[serde(skip_serializing_if = "Option::is_none")]
        id: Option<String>,
        call_id: String,
        name: String,
        arguments: String,
    },
    FunctionCallOutput {
        call_id: String,
        output: String,
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

#[derive(Serialize)]
struct StatusResponse {
    status: &'static str,
    service: &'static str,
    generated_at: String,
    accounts_loaded: usize,
    accounts_healthy: usize,
    next_account: Option<String>,
    refresh_threshold_seconds: u64,
    refresh_interval_seconds: u64,
    accounts: Vec<AccountStatus>,
}

#[derive(Serialize)]
struct AccountStatus {
    name: String,
    path: String,
    healthy: bool,
    last_used: Option<String>,
    last_refresh: Option<String>,
    access_token_expires_at: Option<String>,
    access_token_expires_in_seconds: Option<i64>,
    id_token_expires_at: Option<String>,
    id_token_expires_in_seconds: Option<i64>,
    refresh_recommended: bool,
    fail_count: u32,
    disabled_until: Option<String>,
    disabled_for_seconds: Option<i64>,
    last_error: Option<String>,
    quota: Option<QuotaSnapshot>,
}

struct ProxyServer {
    client: Client,
    account_pool: Arc<Mutex<AccountPool>>,
    proxy_bearer_token: Option<String>,
    responses_api_key: Option<String>,
    debug_logs: bool,
    quota_probe_interval: Duration,
    quota_probe_model: String,
    quota_probe_input: String,
}

const TOKEN_REFRESH_THRESHOLD: Duration = Duration::from_secs(300);
const TOKEN_REFRESH_INTERVAL: Duration = Duration::from_secs(60);
const CHATGPT_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const DEFAULT_CODEX_INSTRUCTIONS: &str = "You are a helpful assistant.";
const CHAT_MODEL_CANDIDATES: &[&str] = &["gpt-4", "gpt-5", "gpt-5.4"];
const EMBEDDING_MODEL_CANDIDATES: &[&str] = &["text-embedding-3-small", "text-embedding-3-large"];

#[allow(dead_code)]
#[derive(Debug)]
struct BackendEventSummary {
    response_id: Option<String>,
    text: String,
    deltas: Vec<String>,
    tool_calls: Vec<ChatToolCall>,
}

#[derive(Debug)]
struct BackendResponse {
    model: String,
    summary: BackendEventSummary,
}

#[derive(Debug)]
struct EmbeddingsResponse {
    body: Value,
}

#[derive(Debug)]
struct ResponsesApiResponse {
    body: Value,
}

#[derive(Serialize)]
struct ModelStatusResponse {
    status: &'static str,
    service: &'static str,
    generated_at: String,
    accounts: Vec<AccountModelStatus>,
}

#[derive(Serialize)]
struct AccountModelStatus {
    name: String,
    healthy: bool,
    api_models_count: usize,
    api_models: Vec<String>,
    chat_probes: Vec<ModelProbeResult>,
    embedding_probes: Vec<ModelProbeResult>,
}

#[derive(Serialize)]
struct ModelProbeResult {
    model: String,
    supported: bool,
    detail: String,
}

#[derive(Clone)]
struct AccountProbeTarget {
    index: usize,
    name: String,
    auth_data: AuthData,
    healthy: bool,
}

#[derive(Debug)]
struct AttemptError {
    retryable: bool,
    auth_failed: bool,
    message: String,
    status_code: Option<u16>,
    response_body: Option<String>,
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
            status_code: None,
            response_body: None,
        }
    }

    fn auth_retryable(message: impl Into<String>) -> Self {
        Self {
            retryable: true,
            auth_failed: true,
            message: message.into(),
            status_code: None,
            response_body: None,
        }
    }

    fn fatal(message: impl Into<String>) -> Self {
        Self {
            retryable: false,
            auth_failed: false,
            message: message.into(),
            status_code: None,
            response_body: None,
        }
    }

    fn retryable_upstream(
        status_code: u16,
        body: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            retryable: true,
            auth_failed: false,
            message: message.into(),
            status_code: Some(status_code),
            response_body: Some(body.into()),
        }
    }

    fn auth_retryable_upstream(
        status_code: u16,
        body: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            retryable: true,
            auth_failed: true,
            message: message.into(),
            status_code: Some(status_code),
            response_body: Some(body.into()),
        }
    }

    fn fatal_upstream(
        status_code: u16,
        body: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            retryable: false,
            auth_failed: false,
            message: message.into(),
            status_code: Some(status_code),
            response_body: Some(body.into()),
        }
    }
}

impl fmt::Display for AttemptError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.message.fmt(f)
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

    fn probe_targets(&self) -> Vec<AccountProbeTarget> {
        let now = Utc::now();
        self.accounts
            .iter()
            .enumerate()
            .map(|(index, account)| AccountProbeTarget {
                index,
                name: account.name.clone(),
                auth_data: account.auth_data.clone(),
                healthy: account
                    .disabled_until
                    .map(|until| until <= now)
                    .unwrap_or(true),
            })
            .collect()
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

    fn status(&self) -> StatusResponse {
        let now = Utc::now();
        let accounts = self
            .accounts
            .iter()
            .map(|account| {
                let healthy = account
                    .disabled_until
                    .map(|until| until <= now)
                    .unwrap_or(true);

                let access_expiry = account
                    .auth_data
                    .tokens
                    .as_ref()
                    .and_then(|tokens| jwt_expiry(&tokens.access_token));
                let id_expiry = account
                    .auth_data
                    .tokens
                    .as_ref()
                    .and_then(|tokens| tokens.id_token.as_deref())
                    .and_then(jwt_expiry);

                AccountStatus {
                    name: account.name.clone(),
                    path: account.auth_path.clone(),
                    healthy,
                    last_used: account.last_used.map(|value| value.to_rfc3339()),
                    last_refresh: account
                        .auth_data
                        .last_refresh
                        .map(|value| value.to_rfc3339()),
                    access_token_expires_at: access_expiry.map(|value| value.to_rfc3339()),
                    access_token_expires_in_seconds: access_expiry
                        .map(|value| value.signed_duration_since(now).num_seconds()),
                    id_token_expires_at: id_expiry.map(|value| value.to_rfc3339()),
                    id_token_expires_in_seconds: id_expiry
                        .map(|value| value.signed_duration_since(now).num_seconds()),
                    refresh_recommended: account
                        .auth_data
                        .needs_refresh(now, TOKEN_REFRESH_THRESHOLD),
                    fail_count: account.fail_count,
                    disabled_until: account.disabled_until.map(|value| value.to_rfc3339()),
                    disabled_for_seconds: account
                        .disabled_until
                        .map(|value| value.signed_duration_since(now).num_seconds())
                        .filter(|seconds| *seconds > 0),
                    last_error: account.last_error.clone(),
                    quota: account.quota.clone(),
                }
            })
            .collect::<Vec<_>>();

        let accounts_healthy = accounts.iter().filter(|account| account.healthy).count();
        let next_account = self
            .accounts
            .get(self.next_index)
            .map(|account| account.name.clone());

        StatusResponse {
            status: "ok",
            service: "codex-openai-proxy",
            generated_at: now.to_rfc3339(),
            accounts_loaded: self.accounts.len(),
            accounts_healthy,
            next_account,
            refresh_threshold_seconds: TOKEN_REFRESH_THRESHOLD.as_secs(),
            refresh_interval_seconds: TOKEN_REFRESH_INTERVAL.as_secs(),
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
        responses_api_key: Option<String>,
        debug_logs: bool,
        quota_probe_interval: Duration,
        quota_probe_model: String,
        quota_probe_input: String,
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
            responses_api_key,
            debug_logs,
            quota_probe_interval,
            quota_probe_model,
            quota_probe_input,
        };
        server.spawn_refresh_loop();
        Ok(server)
    }

    async fn health(&self) -> HealthResponse {
        self.account_pool.lock().await.health()
    }

    async fn status(&self) -> StatusResponse {
        self.account_pool.lock().await.status()
    }

    async fn proxy_request(
        &self,
        chat_req: ChatCompletionsRequest,
    ) -> Result<ChatCompletionsResponse> {
        let backend = self.execute_backend_request(chat_req.clone()).await?;
        Ok(self.translate_to_chat_completion(chat_req.model, backend))
    }

    async fn proxy_stream_request(
        &self,
        chat_req: ChatCompletionsRequest,
    ) -> Result<warp::reply::Response> {
        let requested_model = chat_req.model.clone();
        let response = self.execute_backend_stream_request(chat_req).await?;
        Ok(chat_stream_response(requested_model, response))
    }

    async fn proxy_embeddings_request(&self, embedding_req: Value) -> Result<Value> {
        let backend = self.execute_embeddings_request(embedding_req).await?;
        Ok(backend.body)
    }

    async fn proxy_responses_request(
        &self,
        responses_req: Value,
    ) -> std::result::Result<Value, AttemptError> {
        let backend = self.execute_responses_request(responses_req).await?;
        Ok(backend.body)
    }

    async fn proxy_models_request(&self) -> Result<Value> {
        let backend = self.execute_models_request().await?;
        Ok(backend)
    }

    async fn model_status(&self) -> Result<ModelStatusResponse> {
        let accounts = {
            let pool = self.account_pool.lock().await;
            pool.probe_targets()
        };
        let mut results = Vec::with_capacity(accounts.len());

        for account in accounts {
            let api_models = match self.fetch_models_for_account(&account).await {
                Ok(models) => models,
                Err(error) => {
                    results.push(AccountModelStatus {
                        name: account.name,
                        healthy: account.healthy,
                        api_models_count: 0,
                        api_models: vec![],
                        chat_probes: vec![ModelProbeResult {
                            model: "n/a".to_string(),
                            supported: false,
                            detail: format!("models fetch failed: {}", error),
                        }],
                        embedding_probes: vec![],
                    });
                    continue;
                }
            };

            let chat_probes = self.probe_chat_models(&account).await;
            let embedding_probes = self.probe_embedding_models(&account).await;

            results.push(AccountModelStatus {
                name: account.name,
                healthy: account.healthy,
                api_models_count: api_models.len(),
                api_models,
                chat_probes,
                embedding_probes,
            });
        }

        Ok(ModelStatusResponse {
            status: "ok",
            service: "codex-openai-proxy",
            generated_at: Utc::now().to_rfc3339(),
            accounts: results,
        })
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

    async fn execute_backend_stream_request(
        &self,
        chat_req: ChatCompletionsRequest,
    ) -> Result<reqwest::Response> {
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
                    return Err(anyhow!("Account disappeared before stream dispatch"));
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

            match self
                .send_backend_stream_request(&effective_lease, &chat_req)
                .await
            {
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
                                    return Err(anyhow!(
                                        "Account disappeared during stream refresh"
                                    ));
                                };
                                AccountLease {
                                    index: effective_lease.index,
                                    name,
                                    auth_data,
                                }
                            };
                            match self
                                .send_backend_stream_request(&refreshed_lease, &chat_req)
                                .await
                            {
                                Ok(response) => {
                                    self.account_pool
                                        .lock()
                                        .await
                                        .mark_success(refreshed_lease.index);
                                    return Ok(response);
                                }
                                Err(retry_error) if retry_error.retryable => {
                                    eprintln!(
                                        "Retryable upstream stream failure on {} after refresh: {}",
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
                        "Retryable upstream stream failure on {}: {}",
                        effective_lease.name, error.message
                    );
                    self.account_pool
                        .lock()
                        .await
                        .mark_failure(effective_lease.index, error.message.clone());
                    last_retryable_error = Some(error.message);
                }
                Err(error) => return Err(anyhow!(error.message)),
            }
        }

        Err(anyhow!(
            "All configured accounts failed. Last stream error: {}",
            last_retryable_error.unwrap_or_else(|| "unknown upstream failure".to_string())
        ))
    }

    async fn execute_embeddings_request(&self, embedding_req: Value) -> Result<EmbeddingsResponse> {
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
                    return Err(anyhow!("Account disappeared before embeddings dispatch"));
                };
                AccountLease {
                    index: lease.index,
                    name,
                    auth_data,
                }
            };

            log_request("POST", "/v1/embeddings", Some(&effective_lease.name));
            self.debug_log_embeddings_request(&embedding_req);

            match self
                .send_embeddings_request(&effective_lease, &embedding_req)
                .await
            {
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
                                    return Err(anyhow!(
                                        "Account disappeared during embeddings refresh"
                                    ));
                                };
                                AccountLease {
                                    index: effective_lease.index,
                                    name,
                                    auth_data,
                                }
                            };

                            match self
                                .send_embeddings_request(&refreshed_lease, &embedding_req)
                                .await
                            {
                                Ok(response) => {
                                    self.account_pool
                                        .lock()
                                        .await
                                        .mark_success(refreshed_lease.index);
                                    return Ok(response);
                                }
                                Err(retry_error) if retry_error.retryable => {
                                    eprintln!(
                                        "Retryable embeddings failure on {} after refresh: {}",
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
                        "Retryable embeddings failure on {}: {}",
                        effective_lease.name, error.message
                    );
                    self.account_pool
                        .lock()
                        .await
                        .mark_failure(effective_lease.index, error.message.clone());
                    last_retryable_error = Some(error.message);
                }
                Err(error) => return Err(anyhow!(error.message)),
            }
        }

        Err(anyhow!(
            "All configured accounts failed for embeddings. Last error: {}",
            last_retryable_error.unwrap_or_else(|| "unknown upstream failure".to_string())
        ))
    }

    async fn execute_responses_request(
        &self,
        responses_req: Value,
    ) -> std::result::Result<ResponsesApiResponse, AttemptError> {
        if self.responses_api_key.is_some() {
            let lease = {
                let mut pool = self.account_pool.lock().await;
                pool.lease_next_account()
                    .map_err(|error| AttemptError::fatal(error.to_string()))?
            };

            let effective_lease = {
                let pool = self.account_pool.lock().await;
                let Some((name, _, auth_data)) = pool.auth_snapshot(lease.index) else {
                    return Err(AttemptError::fatal(
                        "Account disappeared before responses dispatch",
                    ));
                };
                AccountLease {
                    index: lease.index,
                    name,
                    auth_data,
                }
            };

            log_request("POST", "/v1/responses", Some(&effective_lease.name));
            self.debug_log_responses_request(&responses_req);

            match self
                .send_responses_request(&effective_lease, &responses_req)
                .await
            {
                Ok(response) => {
                    self.account_pool
                        .lock()
                        .await
                        .mark_success(effective_lease.index);
                    return Ok(response);
                }
                Err(error) if error.retryable => {
                    self.account_pool
                        .lock()
                        .await
                        .mark_failure(effective_lease.index, error.message.clone());
                    return Err(error);
                }
                Err(error) => return Err(error),
            }
        }

        let total_accounts = self.account_pool.lock().await.accounts.len();
        let mut last_retryable_error = None;

        for _ in 0..total_accounts {
            let lease = {
                let mut pool = self.account_pool.lock().await;
                pool.lease_next_account()
                    .map_err(|error| AttemptError::fatal(error.to_string()))?
            };

            if let Err(error) = self.ensure_account_fresh(&lease).await {
                eprintln!("Refresh check failed for {}: {}", lease.name, error);
            }

            let effective_lease = {
                let pool = self.account_pool.lock().await;
                let Some((name, _, auth_data)) = pool.auth_snapshot(lease.index) else {
                    return Err(AttemptError::fatal(
                        "Account disappeared before responses dispatch",
                    ));
                };
                AccountLease {
                    index: lease.index,
                    name,
                    auth_data,
                }
            };

            log_request("POST", "/v1/responses", Some(&effective_lease.name));
            self.debug_log_responses_request(&responses_req);

            match self
                .send_responses_request(&effective_lease, &responses_req)
                .await
            {
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
                                    return Err(AttemptError::fatal(
                                        "Account disappeared during responses refresh",
                                    ));
                                };
                                AccountLease {
                                    index: effective_lease.index,
                                    name,
                                    auth_data,
                                }
                            };

                            match self
                                .send_responses_request(&refreshed_lease, &responses_req)
                                .await
                            {
                                Ok(response) => {
                                    self.account_pool
                                        .lock()
                                        .await
                                        .mark_success(refreshed_lease.index);
                                    return Ok(response);
                                }
                                Err(retry_error) if retry_error.retryable => {
                                    eprintln!(
                                        "Retryable responses failure on {} after refresh: {}",
                                        refreshed_lease.name, retry_error.message
                                    );
                                    self.account_pool.lock().await.mark_failure(
                                        refreshed_lease.index,
                                        retry_error.message.clone(),
                                    );
                                    last_retryable_error = Some(retry_error.message);
                                }
                                Err(retry_error) => return Err(retry_error),
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
                        "Retryable responses failure on {}: {}",
                        effective_lease.name, error.message
                    );
                    self.account_pool
                        .lock()
                        .await
                        .mark_failure(effective_lease.index, error.message.clone());
                    last_retryable_error = Some(error.message);
                }
                Err(error) => return Err(error),
            }
        }

        Err(AttemptError::fatal(format!(
            "All configured accounts failed for responses. Last error: {}",
            last_retryable_error.unwrap_or_else(|| "unknown upstream failure".to_string())
        )))
    }

    async fn execute_models_request(&self) -> Result<Value> {
        let total_accounts = self.account_pool.lock().await.accounts.len();
        let mut last_error = None;

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
                    return Err(anyhow!("Account disappeared before models dispatch"));
                };
                AccountLease {
                    index: lease.index,
                    name,
                    auth_data,
                }
            };

            match self.send_models_request(&effective_lease).await {
                Ok(response) => {
                    self.account_pool
                        .lock()
                        .await
                        .mark_success(effective_lease.index);
                    return Ok(response);
                }
                Err(error) if error.auth_failed => {
                    if let Err(refresh_error) =
                        self.refresh_account(effective_lease.index, true).await
                    {
                        eprintln!(
                            "Forced refresh failed for {}: {}",
                            effective_lease.name, refresh_error
                        );
                    }
                    last_error = Some(error.message);
                }
                Err(error) if error.retryable => {
                    self.account_pool
                        .lock()
                        .await
                        .mark_failure(effective_lease.index, error.message.clone());
                    last_error = Some(error.message);
                }
                Err(error) => return Err(anyhow!(error.message)),
            }
        }

        Err(anyhow!(
            "All configured accounts failed for models. Last error: {}",
            last_error.unwrap_or_else(|| "unknown upstream failure".to_string())
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

    async fn send_backend_stream_request(
        &self,
        lease: &AccountLease,
        chat_req: &ChatCompletionsRequest,
    ) -> std::result::Result<reqwest::Response, AttemptError> {
        let responses_req = self.convert_chat_to_responses(chat_req);
        if self.debug_logs {
            println!(
                "DEBUG upstream stream request: account={}, model={}, input_messages={}",
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
        if let Some(quota) = quota {
            self.account_pool
                .lock()
                .await
                .update_quota(lease.index, quota);
        }

        if !status.is_success() {
            let body = response.text().await.map_err(|error| {
                AttemptError::retryable(format!("Failed to read upstream response: {}", error))
            })?;
            let summary = summarize_body(&body);
            if self.debug_logs {
                println!(
                    "DEBUG upstream stream error: status={}, body={}",
                    status,
                    preview_text(&body, 800)
                );
            }
            if status.as_u16() == 401 || status.as_u16() == 403 {
                return Err(AttemptError::auth_retryable(format!(
                    "Upstream stream status {} for {}: {}",
                    status, lease.name, summary
                )));
            }
            if status.is_server_error() || status.as_u16() == 429 {
                return Err(AttemptError::retryable(format!(
                    "Upstream stream status {} for {}: {}",
                    status, lease.name, summary
                )));
            }

            return Err(AttemptError::fatal(format!(
                "Upstream stream status {}: {}",
                status, summary
            )));
        }

        Ok(response)
    }

    async fn send_embeddings_request(
        &self,
        lease: &AccountLease,
        embedding_req: &Value,
    ) -> std::result::Result<EmbeddingsResponse, AttemptError> {
        let mut request_builder = self
            .client
            .post("https://api.openai.com/v1/embeddings")
            .header("Content-Type", "application/json");

        if let Some(tokens) = &lease.auth_data.tokens {
            request_builder =
                request_builder.header("Authorization", format!("Bearer {}", tokens.access_token));
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
            .json(embedding_req)
            .send()
            .await
            .map_err(|error| AttemptError::retryable(format!("Network error: {}", error)))?;

        let status = response.status();
        let quota = quota_from_headers(response.headers());
        let body_text = response.text().await.map_err(|error| {
            AttemptError::retryable(format!("Failed to read upstream response: {}", error))
        })?;

        if let Some(quota) = quota {
            self.account_pool
                .lock()
                .await
                .update_quota(lease.index, quota);
        }

        if !status.is_success() {
            let summary = summarize_body(&body_text);
            if self.debug_logs {
                println!(
                    "DEBUG embeddings upstream error: status={}, body={}",
                    status,
                    preview_text(&body_text, 800)
                );
            }
            if status.as_u16() == 401 || status.as_u16() == 403 {
                return Err(AttemptError::auth_retryable(format!(
                    "Embeddings upstream status {} for {}: {}",
                    status, lease.name, summary
                )));
            }
            if status.is_server_error() || status.as_u16() == 429 {
                return Err(AttemptError::retryable(format!(
                    "Embeddings upstream status {} for {}: {}",
                    status, lease.name, summary
                )));
            }
            return Err(AttemptError::fatal(format!(
                "Embeddings upstream status {}: {}",
                status, summary
            )));
        }

        let body = serde_json::from_str::<Value>(&body_text)
            .map_err(|error| AttemptError::fatal(format!("Invalid embeddings JSON: {}", error)))?;

        if self.debug_logs {
            let model = body
                .get("model")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let count = body
                .get("data")
                .and_then(Value::as_array)
                .map(|v| v.len())
                .unwrap_or(0);
            println!(
                "DEBUG embeddings upstream success: model={}, vectors={}",
                model, count
            );
        }

        Ok(EmbeddingsResponse { body })
    }

    async fn send_models_request(
        &self,
        lease: &AccountLease,
    ) -> std::result::Result<Value, AttemptError> {
        let mut request_builder = self.client.get("https://api.openai.com/v1/models");

        if let Some(tokens) = &lease.auth_data.tokens {
            request_builder =
                request_builder.header("Authorization", format!("Bearer {}", tokens.access_token));
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
            .send()
            .await
            .map_err(|error| AttemptError::retryable(format!("Network error: {}", error)))?;

        let status = response.status();
        let body_text = response.text().await.map_err(|error| {
            AttemptError::retryable(format!("Failed to read upstream response: {}", error))
        })?;

        if !status.is_success() {
            let summary = summarize_body(&body_text);
            if status.as_u16() == 401 || status.as_u16() == 403 {
                return Err(AttemptError::auth_retryable(format!(
                    "Models upstream status {} for {}: {}",
                    status, lease.name, summary
                )));
            }
            if status.is_server_error() || status.as_u16() == 429 {
                return Err(AttemptError::retryable(format!(
                    "Models upstream status {} for {}: {}",
                    status, lease.name, summary
                )));
            }
            return Err(AttemptError::fatal(format!(
                "Models upstream status {}: {}",
                status, summary
            )));
        }

        serde_json::from_str::<Value>(&body_text)
            .map_err(|error| AttemptError::fatal(format!("Invalid models JSON: {}", error)))
    }

    async fn send_responses_request(
        &self,
        lease: &AccountLease,
        responses_req: &Value,
    ) -> std::result::Result<ResponsesApiResponse, AttemptError> {
        let mut request_builder = self
            .client
            .post("https://api.openai.com/v1/responses")
            .header("Content-Type", "application/json")
            .header("Accept", "application/json");

        if let Some(api_key) = &self.responses_api_key {
            request_builder =
                request_builder.header("Authorization", format!("Bearer {}", api_key));
        } else if let Some(tokens) = &lease.auth_data.tokens {
            request_builder =
                request_builder.header("Authorization", format!("Bearer {}", tokens.access_token));
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
            .json(responses_req)
            .send()
            .await
            .map_err(|error| AttemptError::retryable(format!("Network error: {}", error)))?;

        let status = response.status();
        let quota = quota_from_headers(response.headers());
        let body_text = response.text().await.map_err(|error| {
            AttemptError::retryable(format!("Failed to read upstream response: {}", error))
        })?;

        if let Some(quota) = quota {
            self.account_pool
                .lock()
                .await
                .update_quota(lease.index, quota);
        }

        if !status.is_success() {
            let summary = summarize_body(&body_text);
            if self.debug_logs {
                println!(
                    "DEBUG responses upstream error: status={}, body={}",
                    status,
                    preview_text(&body_text, 800)
                );
            }
            if status.as_u16() == 401 || status.as_u16() == 403 {
                return Err(AttemptError::auth_retryable_upstream(
                    status.as_u16(),
                    body_text,
                    format!(
                        "Responses upstream status {} for {}: {}",
                        status, lease.name, summary
                    ),
                ));
            }
            if status.is_server_error() || status.as_u16() == 429 {
                return Err(AttemptError::retryable_upstream(
                    status.as_u16(),
                    body_text,
                    format!(
                        "Responses upstream status {} for {}: {}",
                        status, lease.name, summary
                    ),
                ));
            }
            return Err(AttemptError::fatal_upstream(
                status.as_u16(),
                body_text,
                format!("Responses upstream status {}: {}", status, summary),
            ));
        }

        let body = serde_json::from_str::<Value>(&body_text)
            .map_err(|error| AttemptError::fatal(format!("Invalid responses JSON: {}", error)))?;

        if self.debug_logs {
            let response_id = body.get("id").and_then(Value::as_str).unwrap_or("unknown");
            let output_text = body
                .get("output_text")
                .and_then(Value::as_str)
                .map(|text| preview_text(text, 180))
                .or_else(|| {
                    extract_response_output_text(&body).map(|text| preview_text(&text, 180))
                })
                .unwrap_or_else(|| "no text".to_string());
            println!(
                "DEBUG responses upstream success: response_id={}, text_preview={}",
                response_id, output_text
            );
        }

        Ok(ResponsesApiResponse { body })
    }

    async fn fetch_models_for_account(&self, account: &AccountProbeTarget) -> Result<Vec<String>> {
        let lease = AccountLease {
            index: account.index,
            name: account.name.clone(),
            auth_data: account.auth_data.clone(),
        };
        let body = self
            .send_models_request(&lease)
            .await
            .map_err(|error| anyhow!(error.message))?;
        let mut ids = body
            .get("data")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| item.get("id").and_then(Value::as_str))
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        ids.sort();
        Ok(ids)
    }

    async fn probe_chat_models(&self, account: &AccountProbeTarget) -> Vec<ModelProbeResult> {
        let mut results = Vec::new();
        let lease = AccountLease {
            index: account.index,
            name: account.name.clone(),
            auth_data: account.auth_data.clone(),
        };

        for model in CHAT_MODEL_CANDIDATES {
            let request = ChatCompletionsRequest {
                model: (*model).to_string(),
                messages: vec![ChatMessage {
                    role: "user".to_string(),
                    content: Value::String("ping".to_string()),
                    tool_calls: None,
                    tool_call_id: None,
                    name: None,
                }],
                temperature: None,
                max_tokens: None,
                stream: Some(false),
                tools: None,
                tool_choice: None,
            };

            match self.send_backend_request(&lease, &request).await {
                Ok(_) => results.push(ModelProbeResult {
                    model: (*model).to_string(),
                    supported: true,
                    detail: "ok".to_string(),
                }),
                Err(error) => results.push(ModelProbeResult {
                    model: (*model).to_string(),
                    supported: false,
                    detail: error.message,
                }),
            }
        }

        results
    }

    async fn probe_embedding_models(&self, account: &AccountProbeTarget) -> Vec<ModelProbeResult> {
        let mut results = Vec::new();
        let lease = AccountLease {
            index: account.index,
            name: account.name.clone(),
            auth_data: account.auth_data.clone(),
        };

        for model in EMBEDDING_MODEL_CANDIDATES {
            let request = json!({
                "model": model,
                "input": "ping"
            });

            match self.send_embeddings_request(&lease, &request).await {
                Ok(_) => results.push(ModelProbeResult {
                    model: (*model).to_string(),
                    supported: true,
                    detail: "ok".to_string(),
                }),
                Err(error) => results.push(ModelProbeResult {
                    model: (*model).to_string(),
                    supported: false,
                    detail: error.message,
                }),
            }
        }

        results
    }

    fn spawn_refresh_loop(&self) {
        let proxy = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(TOKEN_REFRESH_INTERVAL);
            let mut quota_interval = tokio::time::interval(proxy.quota_probe_interval);
            interval.tick().await;
            quota_interval.tick().await;

            if let Err(error) = proxy.probe_account_quotas().await {
                eprintln!("Startup quota probe failed: {}", error);
            }

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(error) = proxy.refresh_expiring_accounts().await {
                            eprintln!("Background refresh loop error: {}", error);
                        }
                    }
                    _ = quota_interval.tick() => {
                        if let Err(error) = proxy.probe_account_quotas().await {
                            eprintln!("Background quota probe error: {}", error);
                        }
                    }
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

    async fn probe_account_quotas(&self) -> Result<()> {
        let accounts = {
            let pool = self.account_pool.lock().await;
            pool.probe_targets()
        };

        for account in accounts {
            if let Err(error) = self.probe_account_quota(&account).await {
                eprintln!("Quota probe failed for {}: {}", account.name, error);
            }
        }

        Ok(())
    }

    async fn probe_account_quota(&self, account: &AccountProbeTarget) -> Result<()> {
        let stale_lease = AccountLease {
            index: account.index,
            name: account.name.clone(),
            auth_data: account.auth_data.clone(),
        };
        if let Err(error) = self.ensure_account_fresh(&stale_lease).await {
            eprintln!("Refresh check failed for {}: {}", stale_lease.name, error);
        }

        let lease = {
            let pool = self.account_pool.lock().await;
            let Some((name, _, auth_data)) = pool.auth_snapshot(account.index) else {
                bail!("Account {} disappeared before quota probe", account.name);
            };
            AccountLease {
                index: account.index,
                name,
                auth_data,
            }
        };

        let request = ChatCompletionsRequest {
            model: self.quota_probe_model.clone(),
            messages: vec![ChatMessage {
                role: "user".to_string(),
                content: Value::String(self.quota_probe_input.clone()),
                tool_calls: None,
                tool_call_id: None,
                name: None,
            }],
            temperature: None,
            max_tokens: Some(8),
            stream: Some(false),
            tools: None,
            tool_choice: None,
        };

        if let Err(error) = self.send_backend_request(&lease, &request).await {
            bail!("quota probe request failed: {}", error.message);
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
            if msg.role == "tool" {
                input.push(ResponseItem::FunctionCallOutput {
                    call_id: msg
                        .tool_call_id
                        .clone()
                        .or_else(|| msg.name.clone())
                        .unwrap_or_else(|| "call_unknown".to_string()),
                    output: message_content_to_text(&msg.content),
                });
                continue;
            }

            let text = message_content_to_text(&msg.content);
            if msg.role != "assistant" || !text.trim().is_empty() || msg.tool_calls.is_none() {
                input.push(ResponseItem::Message {
                    id: None,
                    role: msg.role.clone(),
                    content: vec![content_item_for_role(&msg.role, text)],
                });
            }

            if msg.role == "assistant" {
                for tool_call in msg.tool_calls.as_deref().unwrap_or(&[]) {
                    input.push(ResponseItem::FunctionCall {
                        id: None,
                        call_id: tool_call.id.clone(),
                        name: tool_call.function.name.clone(),
                        arguments: tool_call.function.arguments.clone(),
                    });
                }
            }
        }

        let instructions = if system_instructions.is_empty() {
            Some(DEFAULT_CODEX_INSTRUCTIONS.to_string())
        } else {
            Some(system_instructions.join("\n\n"))
        };
        let tools = normalize_tools(chat_req.tools.as_deref().unwrap_or(&[]));
        let tool_choice = normalize_tool_choice(chat_req.tool_choice.as_ref());

        ResponsesApiRequest {
            model: normalize_codex_model_name(&chat_req.model),
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

    fn debug_log_embeddings_request(&self, embedding_req: &Value) {
        if !self.debug_logs {
            return;
        }

        let model = embedding_req
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let input_shape = match embedding_req.get("input") {
            Some(Value::String(_)) => "string".to_string(),
            Some(Value::Array(values)) => format!("array({})", values.len()),
            Some(Value::Object(_)) => "object".to_string(),
            Some(_) => "other".to_string(),
            None => "missing".to_string(),
        };
        println!(
            "DEBUG embeddings request: model={}, input_shape={}",
            model, input_shape
        );
    }

    fn debug_log_responses_request(&self, responses_req: &Value) {
        if !self.debug_logs {
            return;
        }

        let model = responses_req
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let input_len = responses_req
            .get("input")
            .and_then(Value::as_array)
            .map(|items| items.len())
            .unwrap_or(0);
        let input_file_count = count_input_files(responses_req);
        println!(
            "DEBUG responses request: model={}, input_items={}, input_file_present={}, input_file_count={}",
            model,
            input_len,
            input_file_count > 0,
            input_file_count
        );
    }

    fn translate_to_chat_completion(
        &self,
        requested_model: String,
        backend: BackendResponse,
    ) -> ChatCompletionsResponse {
        let tool_calls = if backend.summary.tool_calls.is_empty() {
            None
        } else {
            Some(backend.summary.tool_calls)
        };
        let has_tool_calls = tool_calls.is_some();
        let content = if backend.summary.text.is_empty() && !has_tool_calls {
            Some("Upstream returned no text content".to_string())
        } else if backend.summary.text.is_empty() {
            None
        } else {
            Some(backend.summary.text)
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
                    tool_calls,
                },
                finish_reason: Some(if has_tool_calls {
                    "tool_calls".to_string()
                } else {
                    "stop".to_string()
                }),
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

fn normalize_codex_model_name(model: &str) -> String {
    match model {
        "gpt-5.4-medium" => "gpt-5.4".to_string(),
        _ => model.to_string(),
    }
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
        Value::Null => String::new(),
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
    let mut tool_call_builders = BTreeMap::<usize, ToolCallBuilder>::new();
    let mut tool_call_ids = BTreeMap::<String, usize>::new();
    let mut next_tool_index = 0usize;

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
            Some("response.output_item.added") | Some("response.output_item.done") => {
                if let Some(item) = event.get("item") {
                    if item.get("type").and_then(Value::as_str) == Some("function_call") {
                        let index = tool_call_index_for_item(
                            &event,
                            item,
                            &mut tool_call_ids,
                            &mut next_tool_index,
                        );
                        let builder = tool_call_builders.entry(index).or_default();
                        apply_function_call_item(builder, item);
                    } else if event.get("type").and_then(Value::as_str)
                        == Some("response.output_item.done")
                        && deltas.is_empty()
                    {
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
            Some("response.output_text.delta") => {
                if let Some(delta) = event.get("delta").and_then(Value::as_str) {
                    deltas.push(delta.to_string());
                }
            }
            Some("response.function_call_arguments.delta") => {
                if let Some(delta) = event.get("delta").and_then(Value::as_str) {
                    let index =
                        tool_call_index_for_event(&event, &mut tool_call_ids, &mut next_tool_index);
                    tool_call_builders
                        .entry(index)
                        .or_default()
                        .arguments
                        .push_str(delta);
                }
            }
            Some("response.function_call_arguments.done") => {
                let index =
                    tool_call_index_for_event(&event, &mut tool_call_ids, &mut next_tool_index);
                let builder = tool_call_builders.entry(index).or_default();
                if let Some(arguments) = event.get("arguments").and_then(Value::as_str) {
                    builder.arguments = arguments.to_string();
                }
            }
            Some("response.completed") => {
                if deltas.is_empty() {
                    if let Some(output) = event
                        .get("response")
                        .and_then(|response| response.get("output"))
                        .and_then(Value::as_array)
                    {
                        for (output_index, item) in output.iter().enumerate() {
                            if item.get("type").and_then(Value::as_str) == Some("function_call") {
                                let index = tool_call_index_for_item_with_fallback(
                                    item,
                                    output_index,
                                    &mut tool_call_ids,
                                );
                                let builder = tool_call_builders.entry(index).or_default();
                                apply_function_call_item(builder, item);
                            } else if let Some(content) =
                                item.get("content").and_then(Value::as_array)
                            {
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
        tool_calls: tool_call_builders
            .into_values()
            .filter_map(ToolCallBuilder::into_chat_tool_call)
            .collect(),
    })
}

#[derive(Default)]
struct ToolCallBuilder {
    id: Option<String>,
    name: Option<String>,
    arguments: String,
}

impl ToolCallBuilder {
    fn into_chat_tool_call(self) -> Option<ChatToolCall> {
        let name = self.name?;
        Some(ChatToolCall {
            id: self
                .id
                .unwrap_or_else(|| format!("call_{}", Uuid::new_v4())),
            kind: "function".to_string(),
            function: ChatToolCallFunction {
                name,
                arguments: self.arguments,
            },
        })
    }
}

fn apply_function_call_item(builder: &mut ToolCallBuilder, item: &Value) {
    if builder.id.is_none() {
        builder.id = item
            .get("call_id")
            .and_then(Value::as_str)
            .or_else(|| item.get("id").and_then(Value::as_str))
            .map(ToString::to_string);
    }
    if builder.name.is_none() {
        builder.name = item
            .get("name")
            .and_then(Value::as_str)
            .map(ToString::to_string);
    }
    if builder.arguments.is_empty() {
        if let Some(arguments) = item.get("arguments").and_then(Value::as_str) {
            builder.arguments = arguments.to_string();
        }
    }
}

fn tool_call_index_for_event(
    event: &Value,
    ids: &mut BTreeMap<String, usize>,
    next_index: &mut usize,
) -> usize {
    let fallback = event
        .get("output_index")
        .and_then(Value::as_u64)
        .or_else(|| event.get("item_index").and_then(Value::as_u64))
        .unwrap_or(*next_index as u64) as usize;

    let Some(id) = event
        .get("call_id")
        .and_then(Value::as_str)
        .or_else(|| event.get("item_id").and_then(Value::as_str))
    else {
        *next_index = (*next_index).max(fallback + 1);
        return fallback;
    };

    *ids.entry(id.to_string()).or_insert_with(|| {
        *next_index = (*next_index).max(fallback + 1);
        fallback
    })
}

fn tool_call_index_for_item(
    event: &Value,
    item: &Value,
    ids: &mut BTreeMap<String, usize>,
    next_index: &mut usize,
) -> usize {
    let fallback = event
        .get("output_index")
        .and_then(Value::as_u64)
        .or_else(|| event.get("item_index").and_then(Value::as_u64))
        .unwrap_or(*next_index as u64) as usize;
    let index = tool_call_index_for_item_with_fallback(item, fallback, ids);
    *next_index = (*next_index).max(index + 1);
    index
}

fn tool_call_index_for_item_with_fallback(
    item: &Value,
    fallback: usize,
    ids: &mut BTreeMap<String, usize>,
) -> usize {
    let Some(id) = item
        .get("call_id")
        .and_then(Value::as_str)
        .or_else(|| item.get("id").and_then(Value::as_str))
    else {
        return fallback;
    };

    *ids.entry(id.to_string()).or_insert(fallback)
}

#[allow(dead_code)]
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

    let has_tool_calls = !summary.tool_calls.is_empty();
    for (index, tool_call) in summary.tool_calls.iter().enumerate() {
        chunks.push(format!(
            "data: {{\"id\":\"{}\",\"object\":\"chat.completion.chunk\",\"created\":{},\"model\":\"{}\",\"choices\":[{{\"index\":0,\"delta\":{{\"tool_calls\":[{{\"index\":{},\"id\":{},\"type\":\"function\",\"function\":{{\"name\":{},\"arguments\":{}}}}}]}},\"finish_reason\":null}}]}}\n\n",
            chunk_id,
            created,
            requested_model,
            index,
            serde_json::to_string(&tool_call.id).unwrap_or_else(|_| "\"\"".to_string()),
            serde_json::to_string(&tool_call.function.name).unwrap_or_else(|_| "\"\"".to_string()),
            serde_json::to_string(&tool_call.function.arguments).unwrap_or_else(|_| "\"\"".to_string())
        ));
    }

    chunks.push(format!(
        "data: {{\"id\":\"{}\",\"object\":\"chat.completion.chunk\",\"created\":{},\"model\":\"{}\",\"choices\":[{{\"index\":0,\"delta\":{{}},\"finish_reason\":\"{}\"}}]}}\n\n",
        chunk_id,
        created,
        requested_model,
        if has_tool_calls { "tool_calls" } else { "stop" }
    ));
    chunks.push("data: [DONE]\n\n".to_string());

    chunks.join("")
}

fn chat_stream_response(
    requested_model: String,
    upstream: reqwest::Response,
) -> warp::reply::Response {
    let created = Utc::now().timestamp();
    let mut buffer = String::new();
    let mut translator = SseChatTranslator::new(requested_model, created);
    let stream = upstream.bytes_stream().map(move |chunk| {
        let mut output = String::new();
        match chunk {
            Ok(bytes) => {
                buffer.push_str(&String::from_utf8_lossy(&bytes));
                while let Some(frame_end) = buffer.find("\n\n") {
                    let frame = buffer[..frame_end].to_string();
                    buffer = buffer[frame_end + 2..].to_string();
                    output.push_str(&translator.translate_frame(&frame));
                }
            }
            Err(error) => {
                output.push_str(&translator.translate_error(&error.to_string()));
            }
        }

        Ok::<_, Infallible>(bytes::Bytes::from(output))
    });

    warp::http::Response::builder()
        .header("content-type", "text/event-stream")
        .header("cache-control", "no-cache")
        .header("connection", "keep-alive")
        .header("access-control-allow-origin", "*")
        .body(warp::hyper::Body::wrap_stream(stream))
        .unwrap_or_else(|_| {
            warp::reply::with_status(
                "Failed to create stream response",
                warp::http::StatusCode::INTERNAL_SERVER_ERROR,
            )
            .into_response()
        })
}

struct SseChatTranslator {
    chunk_id: Option<String>,
    requested_model: String,
    created: i64,
    role_sent: bool,
    finished: bool,
    saw_text_delta: bool,
    emitted_tool_call: bool,
    tool_call_builders: BTreeMap<usize, ToolCallBuilder>,
    tool_call_ids: BTreeMap<String, usize>,
    announced_tool_calls: BTreeMap<usize, bool>,
    next_tool_index: usize,
}

impl SseChatTranslator {
    fn new(requested_model: String, created: i64) -> Self {
        Self {
            chunk_id: None,
            requested_model,
            created,
            role_sent: false,
            finished: false,
            saw_text_delta: false,
            emitted_tool_call: false,
            tool_call_builders: BTreeMap::new(),
            tool_call_ids: BTreeMap::new(),
            announced_tool_calls: BTreeMap::new(),
            next_tool_index: 0,
        }
    }

    fn translate_frame(&mut self, frame: &str) -> String {
        if self.finished {
            return String::new();
        }

        let payload = frame
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .collect::<Vec<_>>()
            .join("\n");

        if payload.is_empty() {
            return String::new();
        }
        if payload == "[DONE]" {
            return self.finish();
        }

        let Ok(event) = serde_json::from_str::<Value>(&payload) else {
            return String::new();
        };
        self.capture_response_id(&event);

        let mut output = String::new();
        match event.get("type").and_then(Value::as_str) {
            Some("response.output_text.delta") => {
                if let Some(delta) = event.get("delta").and_then(Value::as_str) {
                    self.saw_text_delta = true;
                    output.push_str(&self.ensure_role_chunk());
                    output.push_str(&self.content_chunk(delta));
                }
            }
            Some("response.output_item.added") | Some("response.output_item.done") => {
                if let Some(item) = event.get("item") {
                    if item.get("type").and_then(Value::as_str) == Some("function_call") {
                        let index = tool_call_index_for_item(
                            &event,
                            item,
                            &mut self.tool_call_ids,
                            &mut self.next_tool_index,
                        );
                        let builder = self.tool_call_builders.entry(index).or_default();
                        apply_function_call_item(builder, item);
                        output.push_str(&self.announce_tool_call(index));
                    } else if !self.saw_text_delta
                        && event.get("type").and_then(Value::as_str)
                            == Some("response.output_item.done")
                    {
                        output.push_str(&self.emit_item_text(item));
                    }
                }
            }
            Some("response.function_call_arguments.delta") => {
                let index = tool_call_index_for_event(
                    &event,
                    &mut self.tool_call_ids,
                    &mut self.next_tool_index,
                );
                let delta = event
                    .get("delta")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                self.tool_call_builders
                    .entry(index)
                    .or_default()
                    .arguments
                    .push_str(&delta);
                output.push_str(&self.ensure_role_chunk());
                if !self.announced_tool_calls.contains_key(&index) {
                    output.push_str(&self.announce_tool_call(index));
                } else if !delta.is_empty() {
                    self.emitted_tool_call = true;
                    output.push_str(&self.tool_arguments_chunk(index, &delta));
                }
            }
            Some("response.function_call_arguments.done") => {
                let index = tool_call_index_for_event(
                    &event,
                    &mut self.tool_call_ids,
                    &mut self.next_tool_index,
                );
                if let Some(arguments) = event.get("arguments").and_then(Value::as_str) {
                    self.tool_call_builders.entry(index).or_default().arguments =
                        arguments.to_string();
                }
                output.push_str(&self.ensure_role_chunk());
                output.push_str(&self.announce_tool_call(index));
            }
            Some("response.completed") => {
                output.push_str(&self.emit_completed_fallbacks(&event));
                output.push_str(&self.finish());
            }
            _ => {}
        }

        output
    }

    fn translate_error(&mut self, message: &str) -> String {
        if self.finished {
            return String::new();
        }
        let mut output = self.ensure_role_chunk();
        output.push_str(&self.content_chunk(&format!("Proxy stream error: {}", message)));
        output.push_str(&self.finish());
        output
    }

    fn capture_response_id(&mut self, event: &Value) {
        if self.chunk_id.is_some() {
            return;
        }
        self.chunk_id = event
            .get("response")
            .and_then(|response| response.get("id"))
            .and_then(Value::as_str)
            .or_else(|| event.get("id").and_then(Value::as_str))
            .map(ToString::to_string);
    }

    fn chunk_id(&self) -> String {
        self.chunk_id
            .clone()
            .unwrap_or_else(|| format!("chatcmpl-{}", Uuid::new_v4()))
    }

    fn ensure_role_chunk(&mut self) -> String {
        if self.role_sent {
            return String::new();
        }
        self.role_sent = true;
        self.chat_chunk(json!({"role": "assistant"}), None)
    }

    fn content_chunk(&self, content: &str) -> String {
        self.chat_chunk(json!({"content": content}), None)
    }

    fn announce_tool_call(&mut self, index: usize) -> String {
        if self.announced_tool_calls.contains_key(&index) {
            return String::new();
        }

        let Some(builder) = self.tool_call_builders.get(&index) else {
            return String::new();
        };
        let Some(name) = &builder.name else {
            return String::new();
        };
        let id = builder
            .id
            .clone()
            .unwrap_or_else(|| format!("call_{}", Uuid::new_v4()));
        let name = name.clone();

        self.announced_tool_calls.insert(index, true);
        self.emitted_tool_call = true;
        self.ensure_role_chunk()
            + &self.chat_chunk(
                json!({
                    "tool_calls": [{
                        "index": index,
                        "id": id,
                        "type": "function",
                        "function": {
                            "name": name,
                            "arguments": ""
                        }
                    }]
                }),
                None,
            )
    }

    fn tool_arguments_chunk(&self, index: usize, arguments: &str) -> String {
        self.chat_chunk(
            json!({
                "tool_calls": [{
                    "index": index,
                    "function": {
                        "arguments": arguments
                    }
                }]
            }),
            None,
        )
    }

    fn emit_item_text(&mut self, item: &Value) -> String {
        let mut output = String::new();
        if let Some(content) = item.get("content").and_then(Value::as_array) {
            for part in content {
                if let Some(text) = part.get("text").and_then(Value::as_str) {
                    output.push_str(&self.ensure_role_chunk());
                    output.push_str(&self.content_chunk(text));
                }
            }
        }
        output
    }

    fn emit_completed_fallbacks(&mut self, event: &Value) -> String {
        let mut output = String::new();
        let Some(items) = event
            .get("response")
            .and_then(|response| response.get("output"))
            .and_then(Value::as_array)
        else {
            return output;
        };

        for (output_index, item) in items.iter().enumerate() {
            if item.get("type").and_then(Value::as_str) == Some("function_call") {
                let index = tool_call_index_for_item_with_fallback(
                    item,
                    output_index,
                    &mut self.tool_call_ids,
                );
                let builder = self.tool_call_builders.entry(index).or_default();
                apply_function_call_item(builder, item);
                output.push_str(&self.announce_tool_call(index));
            } else if !self.saw_text_delta {
                output.push_str(&self.emit_item_text(item));
            }
        }

        output
    }

    fn finish(&mut self) -> String {
        if self.finished {
            return String::new();
        }
        self.finished = true;
        let mut output = self.ensure_role_chunk();
        output.push_str(&self.chat_chunk(
            json!({}),
            Some(if self.emitted_tool_call {
                "tool_calls"
            } else {
                "stop"
            }),
        ));
        output.push_str("data: [DONE]\n\n");
        output
    }

    fn chat_chunk(&self, delta: Value, finish_reason: Option<&str>) -> String {
        format!(
            "data: {}\n\n",
            json!({
                "id": self.chunk_id(),
                "object": "chat.completion.chunk",
                "created": self.created,
                "model": self.requested_model,
                "choices": [{
                    "index": 0,
                    "delta": delta,
                    "finish_reason": finish_reason
                }]
            })
        )
    }
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

fn count_input_files(responses_req: &Value) -> usize {
    responses_req
        .get("input")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.get("content").and_then(Value::as_array))
                .flatten()
                .filter(|content| content.get("type").and_then(Value::as_str) == Some("input_file"))
                .count()
        })
        .unwrap_or(0)
}

fn extract_response_output_text(body: &Value) -> Option<String> {
    if let Some(text) = body.get("output_text").and_then(Value::as_str) {
        return Some(text.to_string());
    }

    let mut parts = Vec::new();
    for output in body.get("output").and_then(Value::as_array)? {
        for content in output
            .get("content")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if let Some(text) = content.get("text").and_then(Value::as_str) {
                parts.push(text.to_string());
            }
        }
    }

    (!parts.is_empty()).then(|| parts.join("\n"))
}

fn json_error_reply(
    status: warp::http::StatusCode,
    message: impl Into<String>,
    code: &'static str,
) -> warp::reply::Response {
    warp::reply::with_status(
        warp::reply::json(&json!({
            "error": {
                "message": message.into(),
                "type": "proxy_error",
                "code": code
            }
        })),
        status,
    )
    .into_response()
}

fn attempt_error_reply(error: AttemptError) -> warp::reply::Response {
    if let Some(status_code) = error.status_code {
        let status = warp::http::StatusCode::from_u16(status_code)
            .unwrap_or(warp::http::StatusCode::BAD_GATEWAY);
        if let Some(body) = error.response_body {
            if let Ok(json_body) = serde_json::from_str::<Value>(&body) {
                return warp::reply::with_status(warp::reply::json(&json_body), status)
                    .into_response();
            }
            return warp::reply::with_status(body, status).into_response();
        }
        return json_error_reply(status, error.message, "upstream_error");
    }

    json_error_reply(
        warp::http::StatusCode::BAD_GATEWAY,
        error.message,
        "internal_error",
    )
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

fn is_public_proxy_route(method: &warp::http::Method, path: &str) -> bool {
    if method != warp::http::Method::GET {
        return false;
    }

    matches!(
        path,
        "/" | "/dashboard" | "/healthz" | "/status" | "/v1/status"
    )
}

fn render_dashboard(status: &StatusResponse) -> String {
    let health_percent = percent(status.accounts_healthy, status.accounts_loaded);
    let generated = format_relative_timestamp(&status.generated_at);
    let next_account = status.next_account.as_deref().unwrap_or("n/a");
    let account_cards = status
        .accounts
        .iter()
        .map(render_account_card)
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="60">
  <title>Codex Proxy Dashboard</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f5f7fb;
      --panel: #ffffff;
      --panel-2: #f9fbfe;
      --ink: #162033;
      --muted: #667085;
      --line: #d9e1ef;
      --ok: #0f9f6e;
      --warn: #c58105;
      --bad: #d43c3c;
      --accent: #246bfe;
      --accent-2: #26a7b8;
      --shadow: 0 18px 40px rgba(22, 32, 51, .08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: var(--bg);
      color: var(--ink);
    }}
    .topbar {{
      border-bottom: 1px solid var(--line);
      background: rgba(255,255,255,.86);
      backdrop-filter: blur(14px);
      position: sticky;
      top: 0;
      z-index: 2;
    }}
    .wrap {{
      width: min(1220px, calc(100% - 32px));
      margin: 0 auto;
    }}
    .nav {{
      min-height: 64px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
    }}
    .brand {{
      display: flex;
      align-items: center;
      gap: 12px;
      min-width: 0;
    }}
    .mark {{
      width: 36px;
      height: 36px;
      border-radius: 8px;
      background: linear-gradient(135deg, var(--accent), var(--accent-2));
      display: grid;
      place-items: center;
      color: #fff;
      font-weight: 800;
      letter-spacing: 0;
      box-shadow: 0 8px 20px rgba(36, 107, 254, .24);
    }}
    h1 {{
      font-size: 18px;
      line-height: 1.2;
      margin: 0;
      letter-spacing: 0;
    }}
    .subtle {{
      color: var(--muted);
      font-size: 13px;
      line-height: 1.35;
    }}
    .nav-actions {{
      display: flex;
      align-items: center;
      gap: 10px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }}
    .button {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 36px;
      padding: 0 12px;
      border: 1px solid var(--line);
      border-radius: 8px;
      color: var(--ink);
      background: #fff;
      text-decoration: none;
      font-size: 13px;
      font-weight: 650;
    }}
    .button.primary {{
      color: #fff;
      border-color: var(--accent);
      background: var(--accent);
    }}
    main {{
      padding: 28px 0 44px;
    }}
    .hero {{
      display: grid;
      grid-template-columns: minmax(0, 1.35fr) minmax(280px, .65fr);
      gap: 18px;
      align-items: stretch;
      margin-bottom: 18px;
    }}
    .summary {{
      border: 1px solid var(--line);
      background: var(--panel);
      border-radius: 8px;
      box-shadow: var(--shadow);
      padding: 22px;
      min-height: 206px;
      display: grid;
      grid-template-columns: minmax(0, 1fr) 160px;
      gap: 20px;
      align-items: center;
    }}
    .summary h2 {{
      margin: 0 0 8px;
      font-size: clamp(24px, 4vw, 42px);
      line-height: 1.05;
      letter-spacing: 0;
    }}
    .summary p {{
      margin: 0;
      color: var(--muted);
      font-size: 15px;
      line-height: 1.5;
      max-width: 680px;
    }}
    .donut {{
      width: 150px;
      aspect-ratio: 1;
      border-radius: 999px;
      background: conic-gradient(var(--ok) {health_percent}%, #e8edf7 0);
      display: grid;
      place-items: center;
      justify-self: end;
    }}
    .donut::after {{
      content: "{health_percent}%";
      width: 104px;
      aspect-ratio: 1;
      border-radius: 999px;
      background: #fff;
      display: grid;
      place-items: center;
      color: var(--ink);
      font-size: 24px;
      font-weight: 800;
      box-shadow: inset 0 0 0 1px var(--line);
    }}
    .facts {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 18px;
    }}
    .fact {{
      border: 1px solid var(--line);
      background: var(--panel);
      border-radius: 8px;
      box-shadow: var(--shadow);
      padding: 18px;
      min-height: 94px;
    }}
    .fact .label {{
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: .04em;
      margin-bottom: 10px;
    }}
    .fact .value {{
      font-size: 24px;
      font-weight: 800;
      line-height: 1.1;
      overflow-wrap: anywhere;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 18px;
    }}
    .card {{
      border: 1px solid var(--line);
      background: var(--panel);
      border-radius: 8px;
      box-shadow: var(--shadow);
      min-width: 0;
      overflow: hidden;
    }}
    .card-head {{
      padding: 18px 18px 14px;
      border-bottom: 1px solid var(--line);
      background: var(--panel-2);
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 12px;
    }}
    .account-title {{
      min-width: 0;
    }}
    .account-title h3 {{
      margin: 0 0 5px;
      font-size: 17px;
      line-height: 1.2;
      letter-spacing: 0;
      overflow-wrap: anywhere;
    }}
    .path {{
      color: var(--muted);
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 11px;
      line-height: 1.35;
      overflow-wrap: anywhere;
    }}
    .pill {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      border-radius: 999px;
      min-height: 28px;
      padding: 0 10px;
      font-size: 12px;
      font-weight: 800;
      white-space: nowrap;
      border: 1px solid transparent;
    }}
    .pill.ok {{ color: #07694a; background: #e7f7ef; border-color: #bde9d1; }}
    .pill.warn {{ color: #7a4b00; background: #fff4d9; border-color: #f4d084; }}
    .pill.bad {{ color: #9d2424; background: #ffe8e8; border-color: #f3b5b5; }}
    .card-body {{
      padding: 16px 18px 18px;
    }}
    .metric-row {{
      display: grid;
      gap: 12px;
      margin-bottom: 16px;
    }}
    .metric {{
      min-width: 0;
    }}
    .metric-top {{
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 7px;
      font-size: 12px;
      color: var(--muted);
      font-weight: 700;
    }}
    .metric-top strong {{
      color: var(--ink);
      font-size: 13px;
      overflow-wrap: anywhere;
    }}
    .bar {{
      height: 10px;
      border-radius: 999px;
      background: #e7edf6;
      overflow: hidden;
    }}
    .bar span {{
      display: block;
      height: 100%;
      width: var(--w);
      background: linear-gradient(90deg, var(--accent), var(--accent-2));
      border-radius: inherit;
    }}
    .bar.warn span {{ background: linear-gradient(90deg, #f2a43a, #db7d00); }}
    .bar.bad span {{ background: linear-gradient(90deg, #ef6a6a, #d43c3c); }}
    .details {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
      margin-top: 14px;
    }}
    .detail {{
      border: 1px solid var(--line);
      background: var(--panel-2);
      border-radius: 8px;
      padding: 10px;
      min-width: 0;
    }}
    .detail .k {{
      color: var(--muted);
      font-size: 11px;
      font-weight: 800;
      text-transform: uppercase;
      letter-spacing: .04em;
      margin-bottom: 5px;
    }}
    .detail .v {{
      font-size: 13px;
      font-weight: 700;
      line-height: 1.35;
      overflow-wrap: anywhere;
    }}
    .error {{
      margin-top: 14px;
      border: 1px solid #f3b5b5;
      background: #fff2f2;
      color: #8f1f1f;
      border-radius: 8px;
      padding: 10px 12px;
      font-size: 12px;
      line-height: 1.4;
      overflow-wrap: anywhere;
    }}
    .footer {{
      margin-top: 20px;
      color: var(--muted);
      font-size: 12px;
      display: flex;
      justify-content: space-between;
      gap: 12px;
      flex-wrap: wrap;
    }}
    @media (max-width: 980px) {{
      .hero {{ grid-template-columns: 1fr; }}
      .summary {{ grid-template-columns: minmax(0, 1fr); }}
      .donut {{ justify-self: start; }}
      .grid {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
    }}
    @media (max-width: 680px) {{
      .wrap {{ width: min(100% - 20px, 1220px); }}
      .nav {{ align-items: flex-start; flex-direction: column; padding: 12px 0; }}
      .nav-actions {{ justify-content: flex-start; }}
      main {{ padding-top: 16px; }}
      .facts, .grid, .details {{ grid-template-columns: 1fr; }}
      .summary {{ padding: 18px; }}
      .donut {{ width: 132px; }}
      .donut::after {{ width: 90px; font-size: 21px; }}
    }}
  </style>
</head>
<body>
  <header class="topbar">
    <div class="wrap nav">
      <div class="brand">
        <div class="mark">CP</div>
        <div>
          <h1>Codex OpenAI Proxy</h1>
          <div class="subtle">Account health and quota dashboard</div>
        </div>
      </div>
      <div class="nav-actions">
        <a class="button" href="/status">JSON status</a>
        <a class="button primary" href="/dashboard">Refresh</a>
      </div>
    </div>
  </header>
  <main class="wrap">
    <section class="hero">
      <div class="summary">
        <div>
          <h2>{healthy} of {loaded} accounts healthy</h2>
          <p>Generated {generated}. Requests currently rotate to <strong>{next_account}</strong> next. Token refresh is checked every {refresh_interval}s and refresh is recommended inside the final {refresh_threshold}s.</p>
        </div>
        <div class="donut" aria-label="{health_percent}% healthy"></div>
      </div>
      <div class="facts">
        <div class="fact">
          <div class="label">Loaded</div>
          <div class="value">{loaded}</div>
        </div>
        <div class="fact">
          <div class="label">Healthy</div>
          <div class="value">{healthy}</div>
        </div>
        <div class="fact">
          <div class="label">Next Account</div>
          <div class="value">{next_account}</div>
        </div>
        <div class="fact">
          <div class="label">Auto Refresh</div>
          <div class="value">60s</div>
        </div>
      </div>
    </section>
    <section class="grid" aria-label="Accounts">
      {account_cards}
    </section>
    <div class="footer">
      <span>Service: {service}</span>
      <span>Generated at {generated_raw}</span>
    </div>
  </main>
</body>
</html>"#,
        health_percent = health_percent,
        healthy = status.accounts_healthy,
        loaded = status.accounts_loaded,
        generated = html_escape(&generated),
        generated_raw = html_escape(&status.generated_at),
        next_account = html_escape(next_account),
        refresh_interval = status.refresh_interval_seconds,
        refresh_threshold = status.refresh_threshold_seconds,
        account_cards = account_cards,
        service = html_escape(status.service),
    )
}

fn render_account_card(account: &AccountStatus) -> String {
    let health_class = if account.healthy { "ok" } else { "bad" };
    let health_text = if account.healthy {
        "Healthy"
    } else {
        "Backoff"
    };
    let refresh_class = if account.refresh_recommended {
        "warn"
    } else {
        "ok"
    };
    let refresh_text = if account.refresh_recommended {
        "Refresh soon"
    } else {
        "Fresh"
    };
    let access_remaining = format_duration_seconds(account.access_token_expires_in_seconds);
    let id_remaining = format_duration_seconds(account.id_token_expires_in_seconds);
    let disabled_for = format_duration_seconds(account.disabled_for_seconds);
    let primary_used = account
        .quota
        .as_ref()
        .and_then(|quota| quota.primary_used_percent)
        .unwrap_or(0);
    let secondary_used = account
        .quota
        .as_ref()
        .and_then(|quota| quota.secondary_used_percent)
        .unwrap_or(0);
    let primary_bar = bar_class(primary_used);
    let secondary_bar = bar_class(secondary_used);
    let plan = account
        .quota
        .as_ref()
        .and_then(|quota| quota.plan_type.as_deref())
        .unwrap_or("unknown");
    let active_limit = account
        .quota
        .as_ref()
        .and_then(|quota| quota.active_limit.as_deref())
        .unwrap_or("unknown");
    let raw_limit = account
        .quota
        .as_ref()
        .and_then(|quota| quota.raw_limit_name.as_deref())
        .unwrap_or("n/a");
    let quota_seen = account
        .quota
        .as_ref()
        .map(|quota| format_relative_timestamp(&quota.observed_at))
        .unwrap_or_else(|| "never".to_string());
    let primary_reset = account
        .quota
        .as_ref()
        .and_then(|quota| quota.primary_reset_after_seconds)
        .map(|seconds| format_duration_seconds(Some(seconds as i64)))
        .unwrap_or_else(|| "n/a".to_string());
    let secondary_reset = account
        .quota
        .as_ref()
        .and_then(|quota| quota.secondary_reset_after_seconds)
        .map(|seconds| format_duration_seconds(Some(seconds as i64)))
        .unwrap_or_else(|| "n/a".to_string());
    let error = account
        .last_error
        .as_ref()
        .map(|message| format!(r#"<div class="error">{}</div>"#, html_escape(message)))
        .unwrap_or_default();

    format!(
        r#"<article class="card">
  <div class="card-head">
    <div class="account-title">
      <h3>{name}</h3>
      <div class="path">{path}</div>
    </div>
    <span class="pill {health_class}">{health_text}</span>
  </div>
  <div class="card-body">
    <div class="metric-row">
      <div class="metric">
        <div class="metric-top"><span>Primary quota used</span><strong>{primary_used}%</strong></div>
        <div class="bar {primary_bar}"><span style="--w:{primary_used}%"></span></div>
      </div>
      <div class="metric">
        <div class="metric-top"><span>Secondary quota used</span><strong>{secondary_used}%</strong></div>
        <div class="bar {secondary_bar}"><span style="--w:{secondary_used}%"></span></div>
      </div>
    </div>
    <div class="details">
      <div class="detail"><div class="k">Plan</div><div class="v">{plan}</div></div>
      <div class="detail"><div class="k">Limit</div><div class="v">{active_limit}</div></div>
      <div class="detail"><div class="k">Raw Limit</div><div class="v">{raw_limit}</div></div>
      <div class="detail"><div class="k">Quota Seen</div><div class="v">{quota_seen}</div></div>
      <div class="detail"><div class="k">Primary Reset</div><div class="v">{primary_reset}</div></div>
      <div class="detail"><div class="k">Secondary Reset</div><div class="v">{secondary_reset}</div></div>
      <div class="detail"><div class="k">Access Token</div><div class="v">{access_remaining}</div></div>
      <div class="detail"><div class="k">ID Token</div><div class="v">{id_remaining}</div></div>
      <div class="detail"><div class="k">Refresh</div><div class="v"><span class="pill {refresh_class}">{refresh_text}</span></div></div>
      <div class="detail"><div class="k">Failures</div><div class="v">{fail_count}</div></div>
      <div class="detail"><div class="k">Backoff</div><div class="v">{disabled_for}</div></div>
      <div class="detail"><div class="k">Last Used</div><div class="v">{last_used}</div></div>
      <div class="detail"><div class="k">Last Refresh</div><div class="v">{last_refresh}</div></div>
    </div>
    {error}
  </div>
</article>"#,
        name = html_escape(&account.name),
        path = html_escape(&account.path),
        health_class = health_class,
        health_text = health_text,
        primary_used = primary_used,
        secondary_used = secondary_used,
        primary_bar = primary_bar,
        secondary_bar = secondary_bar,
        plan = html_escape(plan),
        active_limit = html_escape(active_limit),
        raw_limit = html_escape(raw_limit),
        quota_seen = html_escape(&quota_seen),
        primary_reset = html_escape(&primary_reset),
        secondary_reset = html_escape(&secondary_reset),
        access_remaining = html_escape(&access_remaining),
        id_remaining = html_escape(&id_remaining),
        refresh_class = refresh_class,
        refresh_text = refresh_text,
        fail_count = account.fail_count,
        disabled_for = html_escape(&disabled_for),
        last_used = html_escape(&format_optional_timestamp(account.last_used.as_deref())),
        last_refresh = html_escape(&format_optional_timestamp(account.last_refresh.as_deref())),
        error = error,
    )
}

fn percent(part: usize, total: usize) -> usize {
    if total == 0 {
        0
    } else {
        ((part as f64 / total as f64) * 100.0).round() as usize
    }
}

fn bar_class(used: u8) -> &'static str {
    match used {
        85..=100 => "bad",
        65..=84 => "warn",
        _ => "",
    }
}

fn format_optional_timestamp(value: Option<&str>) -> String {
    value
        .map(format_relative_timestamp)
        .unwrap_or_else(|| "never".to_string())
}

fn format_relative_timestamp(value: &str) -> String {
    DateTime::parse_from_rfc3339(value)
        .map(|timestamp| {
            let timestamp = timestamp.with_timezone(&Utc);
            let seconds = Utc::now()
                .signed_duration_since(timestamp)
                .num_seconds()
                .max(0);
            if seconds < 60 {
                "just now".to_string()
            } else if seconds < 3600 {
                format!("{}m ago", seconds / 60)
            } else if seconds < 86_400 {
                format!("{}h ago", seconds / 3600)
            } else {
                format!("{}d ago", seconds / 86_400)
            }
        })
        .unwrap_or_else(|_| value.to_string())
}

fn format_duration_seconds(seconds: Option<i64>) -> String {
    let Some(seconds) = seconds else {
        return "n/a".to_string();
    };
    if seconds <= 0 {
        return "expired".to_string();
    }
    if seconds < 60 {
        return format!("{}s", seconds);
    }
    if seconds < 3600 {
        return format!("{}m", seconds / 60);
    }
    if seconds < 86_400 {
        let hours = seconds / 3600;
        let minutes = (seconds % 3600) / 60;
        return if minutes == 0 {
            format!("{}h", hours)
        } else {
            format!("{}h {}m", hours, minutes)
        };
    }
    let days = seconds / 86_400;
    let hours = (seconds % 86_400) / 3600;
    if hours == 0 {
        format!("{}d", days)
    } else {
        format!("{}d {}h", days, hours)
    }
}

fn html_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    let quota_probe_interval = Duration::from_secs(args.quota_probe_interval_seconds.max(1));

    let proxy = ProxyServer::new(
        &args.auth_paths,
        args.proxy_bearer_token.clone(),
        args.responses_api_key.clone(),
        args.debug_logs,
        quota_probe_interval,
        args.quota_probe_model.clone(),
        args.quota_probe_input.clone(),
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
        "Responses API key: {}",
        if args.responses_api_key.is_some() {
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
    println!(
        "Quota probe: every {}s using model={} input={:?}",
        args.quota_probe_interval_seconds.max(1),
        args.quota_probe_model,
        args.quota_probe_input
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

    if !is_public_proxy_route(&method, path_str) {
        if let Some(expected_token) = &proxy.proxy_bearer_token {
            let provided_token = extract_bearer_token(&headers);
            if provided_token != Some(expected_token.as_str()) {
                return Ok(proxy_auth_reply());
            }
        }
    }

    match (method.as_str(), path_str) {
        ("GET", "/") | ("GET", "/dashboard") => {
            Ok(warp::reply::html(render_dashboard(&proxy.status().await)).into_response())
        }
        ("GET", "/health") | ("GET", "/healthz") => {
            Ok(warp::reply::json(&proxy.health().await).into_response())
        }
        ("GET", "/status") | ("GET", "/v1/status") => {
            Ok(warp::reply::json(&proxy.status().await).into_response())
        }
        ("GET", "/model-status") | ("GET", "/v1/model-status") => {
            match proxy.model_status().await {
                Ok(response) => Ok(warp::reply::json(&response).into_response()),
                Err(error) => {
                    eprintln!("Proxy error: {:#}", error);
                    Ok(json_error_reply(
                        warp::http::StatusCode::BAD_GATEWAY,
                        format!("Proxy error: {}", error),
                        "internal_error",
                    ))
                }
            }
        }
        ("GET", "/models") | ("GET", "/v1/models") => match proxy.proxy_models_request().await {
            Ok(response) => Ok(warp::reply::json(&response).into_response()),
            Err(error) => {
                eprintln!("Proxy error: {:#}", error);
                Ok(json_error_reply(
                    warp::http::StatusCode::BAD_GATEWAY,
                    format!("Proxy error: {}", error),
                    "internal_error",
                ))
            }
        },
        ("POST", "/responses") | ("POST", "/v1/responses") => {
            let responses_req: Value = match serde_json::from_slice(&body) {
                Ok(req) => req,
                Err(error) => {
                    eprintln!("Invalid JSON body: {}", error);
                    return Ok(json_error_reply(
                        warp::http::StatusCode::BAD_REQUEST,
                        format!("Invalid JSON: {}", error),
                        "invalid_json",
                    ));
                }
            };

            if responses_req
                .get("stream")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                return Ok(json_error_reply(
                    warp::http::StatusCode::NOT_IMPLEMENTED,
                    "Streaming /v1/responses is not implemented yet",
                    "not_implemented",
                ));
            }

            match proxy.proxy_responses_request(responses_req).await {
                Ok(response) => Ok(warp::reply::json(&response).into_response()),
                Err(error) => {
                    eprintln!("Proxy error: {:#}", error);
                    Ok(attempt_error_reply(error))
                }
            }
        }
        ("POST", "/embeddings") | ("POST", "/v1/embeddings") => {
            let embedding_req: Value = match serde_json::from_slice(&body) {
                Ok(req) => req,
                Err(error) => {
                    eprintln!("Invalid JSON body: {}", error);
                    return Ok(json_error_reply(
                        warp::http::StatusCode::BAD_REQUEST,
                        format!("Invalid JSON: {}", error),
                        "invalid_json",
                    ));
                }
            };

            match proxy.proxy_embeddings_request(embedding_req).await {
                Ok(response) => Ok(warp::reply::json(&response).into_response()),
                Err(error) => {
                    eprintln!("Proxy error: {:#}", error);
                    Ok(json_error_reply(
                        warp::http::StatusCode::BAD_GATEWAY,
                        format!("Proxy error: {}", error),
                        "internal_error",
                    ))
                }
            }
        }
        ("POST", "/chat/completions") | ("POST", "/v1/chat/completions") => {
            let chat_req: ChatCompletionsRequest = match serde_json::from_slice(&body) {
                Ok(req) => req,
                Err(error) => {
                    eprintln!("Invalid JSON body: {}", error);
                    return Ok(json_error_reply(
                        warp::http::StatusCode::BAD_REQUEST,
                        format!("Invalid JSON: {}", error),
                        "invalid_json",
                    ));
                }
            };

            if chat_req.stream.unwrap_or(false) {
                match proxy.proxy_stream_request(chat_req).await {
                    Ok(response) => Ok(response),
                    Err(error) => {
                        eprintln!("Proxy error: {:#}", error);
                        Ok(json_error_reply(
                            warp::http::StatusCode::BAD_GATEWAY,
                            format!("Proxy error: {}", error),
                            "internal_error",
                        ))
                    }
                }
            } else {
                match proxy.proxy_request(chat_req).await {
                    Ok(response) => Ok(warp::reply::json(&response).into_response()),
                    Err(error) => {
                        eprintln!("Proxy error: {:#}", error);
                        Ok(json_error_reply(
                            warp::http::StatusCode::BAD_GATEWAY,
                            format!("Proxy error: {}", error),
                            "internal_error",
                        ))
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
            responses_api_key: self.responses_api_key.clone(),
            debug_logs: self.debug_logs,
            quota_probe_interval: self.quota_probe_interval,
            quota_probe_model: self.quota_probe_model.clone(),
            quota_probe_input: self.quota_probe_input.clone(),
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
        assert!(summary.tool_calls.is_empty());
    }

    #[test]
    fn parses_function_call_sse() {
        let summary = parse_responses_event_stream(
            "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"function_call\",\"id\":\"fc_1\",\"call_id\":\"call_1\",\"name\":\"bash\",\"arguments\":\"\"}}\n\n\
             data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"item_id\":\"fc_1\",\"delta\":\"{\\\"cmd\\\":\"}\n\n\
             data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"item_id\":\"fc_1\",\"delta\":\"\\\"ls\\\"}\"}\n\n\
             data: {\"type\":\"response.function_call_arguments.done\",\"output_index\":0,\"item_id\":\"fc_1\",\"arguments\":\"{\\\"cmd\\\":\\\"ls\\\"}\"}\n\n\
             data: [DONE]\n",
        )
        .unwrap();

        assert_eq!(summary.text, "");
        assert_eq!(summary.tool_calls.len(), 1);
        assert_eq!(summary.tool_calls[0].id, "call_1");
        assert_eq!(summary.tool_calls[0].function.name, "bash");
        assert_eq!(summary.tool_calls[0].function.arguments, "{\"cmd\":\"ls\"}");
    }

    #[test]
    fn translates_tool_calls_to_chat_sse() {
        let sse = translate_to_chat_sse(
            "gpt-5.5",
            BackendEventSummary {
                response_id: Some("resp_1".to_string()),
                text: String::new(),
                deltas: vec![],
                tool_calls: vec![ChatToolCall {
                    id: "call_1".to_string(),
                    kind: "function".to_string(),
                    function: ChatToolCallFunction {
                        name: "read".to_string(),
                        arguments: "{\"filePath\":\"Cargo.toml\"}".to_string(),
                    },
                }],
            },
        );

        assert!(sse.contains("\"tool_calls\""));
        assert!(sse.contains("\"name\":\"read\""));
        assert!(sse.contains("\"finish_reason\":\"tool_calls\""));
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
    fn marks_healthz_as_public_proxy_route() {
        assert!(is_public_proxy_route(&warp::http::Method::GET, "/healthz"));
        assert!(is_public_proxy_route(&warp::http::Method::GET, "/status"));
        assert!(is_public_proxy_route(
            &warp::http::Method::GET,
            "/v1/status"
        ));
        assert!(!is_public_proxy_route(&warp::http::Method::GET, "/health"));
        assert!(!is_public_proxy_route(
            &warp::http::Method::POST,
            "/healthz"
        ));
        assert!(!is_public_proxy_route(&warp::http::Method::POST, "/status"));
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
            responses_api_key: None,
            debug_logs: false,
            quota_probe_interval: Duration::from_secs(3600),
            quota_probe_model: "text-embedding-3-small".to_string(),
            quota_probe_input: "hi".to_string(),
        };

        let request = ChatCompletionsRequest {
            model: "gpt-5".to_string(),
            messages: vec![
                ChatMessage {
                    role: "system".to_string(),
                    content: Value::String("System instruction".to_string()),
                    tool_calls: None,
                    tool_call_id: None,
                    name: None,
                },
                ChatMessage {
                    role: "user".to_string(),
                    content: Value::String("Hello".to_string()),
                    tool_calls: None,
                    tool_call_id: None,
                    name: None,
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
            _ => panic!("expected user message"),
        }
    }

    #[test]
    fn sets_default_instructions_when_no_system_message_is_present() {
        let proxy = ProxyServer {
            client: Client::builder().build().unwrap(),
            account_pool: Arc::new(Mutex::new(AccountPool {
                accounts: vec![],
                next_index: 0,
            })),
            proxy_bearer_token: None,
            responses_api_key: None,
            debug_logs: false,
            quota_probe_interval: Duration::from_secs(3600),
            quota_probe_model: "text-embedding-3-small".to_string(),
            quota_probe_input: "hi".to_string(),
        };

        let request = ChatCompletionsRequest {
            model: "gpt-5".to_string(),
            messages: vec![ChatMessage {
                role: "user".to_string(),
                content: Value::String("Hello".to_string()),
                tool_calls: None,
                tool_call_id: None,
                name: None,
            }],
            temperature: None,
            max_tokens: None,
            stream: None,
            tools: None,
            tool_choice: None,
        };

        let translated = proxy.convert_chat_to_responses(&request);
        assert_eq!(
            translated.instructions.as_deref(),
            Some(DEFAULT_CODEX_INSTRUCTIONS)
        );
        assert_eq!(translated.input.len(), 1);
    }

    #[test]
    fn normalizes_gpt_5_4_medium_for_codex_chatgpt_accounts() {
        assert_eq!(normalize_codex_model_name("gpt-5.4-medium"), "gpt-5.4");
        assert_eq!(normalize_codex_model_name("gpt-5.4"), "gpt-5.4");
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
            responses_api_key: None,
            debug_logs: false,
            quota_probe_interval: Duration::from_secs(3600),
            quota_probe_model: "text-embedding-3-small".to_string(),
            quota_probe_input: "hi".to_string(),
        };

        let request = ChatCompletionsRequest {
            model: "gpt-5".to_string(),
            messages: vec![
                ChatMessage {
                    role: "user".to_string(),
                    content: Value::String("Hello".to_string()),
                    tool_calls: None,
                    tool_call_id: None,
                    name: None,
                },
                ChatMessage {
                    role: "assistant".to_string(),
                    content: Value::String("Hi there".to_string()),
                    tool_calls: None,
                    tool_call_id: None,
                    name: None,
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
            _ => panic!("expected user message"),
        }
        match &translated.input[1] {
            ResponseItem::Message { content, .. } => match &content[0] {
                ContentItem::OutputText { .. } => {}
                _ => panic!("assistant history should be output_text"),
            },
            _ => panic!("expected assistant message"),
        }
    }

    #[test]
    fn encodes_tool_call_history_for_responses_backend() {
        let proxy = ProxyServer {
            client: Client::builder().build().unwrap(),
            account_pool: Arc::new(Mutex::new(AccountPool {
                accounts: vec![],
                next_index: 0,
            })),
            proxy_bearer_token: None,
            responses_api_key: None,
            debug_logs: false,
            quota_probe_interval: Duration::from_secs(3600),
            quota_probe_model: "text-embedding-3-small".to_string(),
            quota_probe_input: "hi".to_string(),
        };

        let request = ChatCompletionsRequest {
            model: "gpt-5".to_string(),
            messages: vec![
                ChatMessage {
                    role: "assistant".to_string(),
                    content: Value::Null,
                    tool_calls: Some(vec![ChatToolCall {
                        id: "call_1".to_string(),
                        kind: "function".to_string(),
                        function: ChatToolCallFunction {
                            name: "grep".to_string(),
                            arguments: "{\"pattern\":\"obi\"}".to_string(),
                        },
                    }]),
                    tool_call_id: None,
                    name: None,
                },
                ChatMessage {
                    role: "tool".to_string(),
                    content: Value::String("banking/rules.ts".to_string()),
                    tool_calls: None,
                    tool_call_id: Some("call_1".to_string()),
                    name: Some("grep".to_string()),
                },
            ],
            temperature: None,
            max_tokens: None,
            stream: None,
            tools: None,
            tool_choice: None,
        };

        let translated = proxy.convert_chat_to_responses(&request);
        assert_eq!(translated.input.len(), 2);
        match &translated.input[0] {
            ResponseItem::FunctionCall {
                id,
                call_id,
                name,
                arguments,
            } => {
                assert!(id.is_none());
                assert_eq!(call_id, "call_1");
                assert_eq!(name, "grep");
                assert_eq!(arguments, "{\"pattern\":\"obi\"}");
            }
            _ => panic!("expected function_call item"),
        }
        match &translated.input[1] {
            ResponseItem::FunctionCallOutput { call_id, output } => {
                assert_eq!(call_id, "call_1");
                assert_eq!(output, "banking/rules.ts");
            }
            _ => panic!("expected function_call_output item"),
        }
    }

    #[test]
    fn leaves_string_tool_choice_as_auto() {
        assert_eq!(
            normalize_tool_choice(Some(&Value::String("auto".to_string()))),
            Value::String("auto".to_string())
        );
    }

    #[test]
    fn normalizes_named_function_tool_choice() {
        let value = json!({
            "type": "function",
            "function": {
                "name": "ha_query"
            }
        });
        let normalized = normalize_tool_choice(Some(&value));
        assert_eq!(normalized["type"], "function");
        assert_eq!(normalized["name"], "ha_query");
    }

    #[test]
    fn counts_input_files_in_responses_request() {
        let request = json!({
            "model": "gpt-5.3",
            "input": [{
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": "Read this PDF"
                    },
                    {
                        "type": "input_file",
                        "filename": "document.pdf",
                        "file_data": "data:application/pdf;base64,ZmFrZQ=="
                    }
                ]
            }]
        });

        assert_eq!(count_input_files(&request), 1);
    }

    #[test]
    fn extracts_output_text_from_responses_payload() {
        let payload = json!({
            "id": "resp_123",
            "output": [{
                "content": [{
                    "type": "output_text",
                    "text": "## Page 1\nhello"
                }]
            }]
        });

        assert_eq!(
            extract_response_output_text(&payload).as_deref(),
            Some("## Page 1\nhello")
        );
    }

    fn make_jwt_with_exp(exp: i64) -> String {
        let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"none","typ":"JWT"}"#);
        let payload = URL_SAFE_NO_PAD.encode(format!(r#"{{"exp":{}}}"#, exp));
        format!("{}.{}.", header, payload)
    }
}
