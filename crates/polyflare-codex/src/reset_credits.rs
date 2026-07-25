use polyflare_core::Account;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ResetCreditItem {
    pub id: String,
    pub reset_type: Option<String>,
    pub status: Option<String>,
    pub granted_at: Option<String>,
    pub expires_at: Option<String>,
    pub title: Option<String>,
    pub description: Option<String>,
    pub redeem_started_at: Option<String>,
    pub redeemed_at: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ResetCreditsResponse {
    pub credits: Vec<ResetCreditItem>,
    pub available_count: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConsumedCredit {
    pub id: Option<String>,
    pub reset_type: Option<String>,
    pub status: Option<String>,
    pub redeemed_at: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConsumeResetCreditResponse {
    pub code: String,
    pub credit: Option<ConsumedCredit>,
    pub windows_reset: i64,
}

#[derive(Debug, thiserror::Error)]
pub enum ResetCreditsError {
    #[error("invalid account identity")]
    InvalidIdentity,
    #[error("reset-credit upstream transport failed")]
    Transport(#[source] reqwest::Error),
    #[error("reset-credit upstream returned HTTP {status}")]
    Upstream { status: u16, code: Option<String> },
    #[error("reset-credit upstream returned an invalid response")]
    InvalidResponse(#[source] reqwest::Error),
}

#[derive(Serialize)]
struct ConsumeBody<'a> {
    credit_id: &'a str,
    redeem_request_id: &'a str,
}

pub async fn fetch_reset_credits(
    client: &reqwest::Client,
    account: &Account,
) -> Result<ResetCreditsResponse, ResetCreditsError> {
    let response = client
        .get(reset_credits_url(&account.base_url))
        .headers(identity_headers(account)?)
        .send()
        .await
        .map_err(ResetCreditsError::Transport)?;
    decode_response(response).await
}

pub async fn consume_reset_credit(
    client: &reqwest::Client,
    account: &Account,
    credit_id: &str,
    redeem_request_id: &str,
) -> Result<ConsumeResetCreditResponse, ResetCreditsError> {
    let mut headers = identity_headers(account)?;
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    let response = client
        .post(format!("{}/consume", reset_credits_url(&account.base_url)))
        .headers(headers)
        .json(&ConsumeBody {
            credit_id,
            redeem_request_id,
        })
        .send()
        .await
        .map_err(ResetCreditsError::Transport)?;
    decode_response(response).await
}

fn identity_headers(account: &Account) -> Result<HeaderMap, ResetCreditsError> {
    let account_id = account
        .chatgpt_account_id
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or(ResetCreditsError::InvalidIdentity)?;
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}", account.bearer_token))
            .map_err(|_| ResetCreditsError::InvalidIdentity)?,
    );
    headers.insert(
        HeaderName::from_static("chatgpt-account-id"),
        HeaderValue::from_str(account_id).map_err(|_| ResetCreditsError::InvalidIdentity)?,
    );
    if account.is_fedramp {
        headers.insert(
            HeaderName::from_static("x-openai-fedramp"),
            HeaderValue::from_static("true"),
        );
    }
    Ok(headers)
}

async fn decode_response<T: for<'de> Deserialize<'de>>(
    response: reqwest::Response,
) -> Result<T, ResetCreditsError> {
    let status = response.status();
    if !status.is_success() {
        let code = response
            .json::<serde_json::Value>()
            .await
            .ok()
            .and_then(|value| {
                value
                    .pointer("/error/code")
                    .or_else(|| value.get("code"))
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            });
        return Err(ResetCreditsError::Upstream {
            status: status.as_u16(),
            code,
        });
    }
    response
        .json::<T>()
        .await
        .map_err(ResetCreditsError::InvalidResponse)
}

fn reset_credits_url(base_url: &str) -> String {
    let normalized = base_url.trim_end_matches('/');
    let backend_root = normalized
        .find("/backend-api")
        .map(|index| &normalized[..index + "/backend-api".len()])
        .unwrap_or(normalized);
    let backend_root = if backend_root.ends_with("/backend-api") {
        backend_root.to_string()
    } else {
        format!("{backend_root}/backend-api")
    };
    format!("{backend_root}/wham/rate-limit-reset-credits")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_wham_reset_credit_url_from_codex_or_site_base() {
        assert_eq!(
            reset_credits_url("https://chatgpt.com/backend-api/codex"),
            "https://chatgpt.com/backend-api/wham/rate-limit-reset-credits"
        );
        assert_eq!(
            reset_credits_url("https://chatgpt.com"),
            "https://chatgpt.com/backend-api/wham/rate-limit-reset-credits"
        );
    }
}
