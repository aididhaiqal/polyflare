//! Probe whether a specific account can serve a specific model.
//!
//! Used to seed `account_model_support` for hidden models the `/models` endpoint does not list.
//! The signal is deliberately narrow: does upstream ACCEPT a minimal request for the model on this
//! account, or reject it as unsupported. Acceptance is NOT proof the account truly runs the model
//! — upstream may accept and silently serve a fallback — so a probe result is always overridable by
//! an operator (see `account_model_support`).

use std::time::Duration;

use polyflare_store::{Store, TokenCipher};

/// What a probe observed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProbeOutcome {
    /// Upstream began serving the model (stream opened / 2xx).
    Supported,
    /// Upstream explicitly rejected the model for this account.
    Unsupported,
    /// The probe could not decide — expired credentials, a transport error, or an unrecognised
    /// response. No support row should be written; leaving it unknown is more honest than a guess.
    Inconclusive(&'static str),
}

/// The model surface these hidden preview models require. Discovered empirically for
/// `gpt-daybreak-blue-latest`: it rejects unless `store:false` and `stream:true`.
fn probe_body(model: &str) -> serde_json::Value {
    serde_json::json!({
        "model": model,
        "store": false,
        "stream": true,
        "input": [{"role": "user", "content": [{"type": "input_text", "text": "hi"}]}],
    })
}

/// The `{upstream}/responses` URL — the base already ends at the Codex responses root.
fn responses_url(upstream_base: &str) -> String {
    format!("{}/responses", upstream_base.trim_end_matches('/'))
}

/// Probe one account for one model. Fetches the account's stored token and sends a minimal request;
/// interprets the outcome without consuming meaningful quota (one tiny turn, aborted at the header).
pub async fn probe_account_model(
    store: &Store,
    cipher: &TokenCipher,
    upstream_base: &str,
    account_id: &str,
    model: &str,
) -> ProbeOutcome {
    let (account, tokens) = match store.accounts().get_with_tokens(account_id, cipher).await {
        Ok(Some(pair)) => pair,
        Ok(None) => return ProbeOutcome::Inconclusive("account not found"),
        Err(_) => return ProbeOutcome::Inconclusive("token decrypt/read failed"),
    };

    let client = match polyflare_codex::build_client() {
        Ok(client) => client,
        Err(_) => return ProbeOutcome::Inconclusive("http client build failed"),
    };

    let mut request = client
        .post(responses_url(upstream_base))
        .timeout(Duration::from_secs(30))
        .header("Authorization", format!("Bearer {}", tokens.access_token))
        .header("Content-Type", "application/json")
        .header("Accept", "text/event-stream");
    if let Some(chatgpt_account_id) = &account.chatgpt_account_id {
        request = request.header("chatgpt-account-id", chatgpt_account_id);
    }
    if polyflare_codex::oauth::is_fedramp_account(&tokens.id_token) {
        request = request.header("x-openai-fedramp", "true");
    }

    let response = match request.json(&probe_body(model)).send().await {
        Ok(response) => response,
        Err(_) => return ProbeOutcome::Inconclusive("transport error"),
    };

    let status = response.status();
    if status.is_success() {
        // A 2xx means upstream accepted the model and began the stream. We do not need the body.
        return ProbeOutcome::Supported;
    }
    if status == reqwest::StatusCode::UNAUTHORIZED {
        // A stale token — the CLI does not run the refresh loop. Can't decide; leave unknown.
        return ProbeOutcome::Inconclusive("token expired (401)");
    }

    // A 4xx: read the error text and classify. An unsupported-model error is a definite "no"; a
    // parameter error means the MODEL was accepted (upstream got past model validation), so treat
    // it as supported for our purposes; anything else is inconclusive.
    let body = response.text().await.unwrap_or_default();
    let lower = body.to_lowercase();
    if lower.contains("not supported")
        || lower.contains("model_not_found")
        || lower.contains("does not exist")
    {
        ProbeOutcome::Unsupported
    } else if lower.contains("must be set") || lower.contains("must be") {
        // e.g. "Store must be set to false" / "Stream must be set to true" — the model was
        // recognised; only the request shape was wrong. That is acceptance of the model.
        ProbeOutcome::Supported
    } else {
        ProbeOutcome::Inconclusive("unrecognised rejection")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_probe_body_carries_the_required_hidden_model_params() {
        let body = probe_body("gpt-daybreak-blue-latest");
        assert_eq!(body["model"], "gpt-daybreak-blue-latest");
        assert_eq!(body["store"], false, "hidden preview requires store:false");
        assert_eq!(body["stream"], true, "hidden preview requires stream:true");
    }

    #[test]
    fn responses_url_joins_without_a_double_slash() {
        assert_eq!(responses_url("https://x.test"), "https://x.test/responses");
        assert_eq!(responses_url("https://x.test/"), "https://x.test/responses");
    }
}
