//! Anthropic subscription usage and limits, read from the free `GET /api/oauth/usage` endpoint the
//! Claude Code CLI itself polls. Authenticated with the account's own OAuth bearer plus the
//! `anthropic-beta: oauth-2025-04-20` header — the same grant that serves inference, so no wider
//! scope is required to READ usage (whether it is required is exactly what the probe confirms).
//!
//! This first cut returns the raw JSON body. Typed parsing, a poll loop, and dashboard surfacing
//! build on top of it once the live payload's real shape (which windows, whether a plan name is
//! present) is known rather than assumed from better-ccflare's types.

use std::time::Duration;

use polyflare_store::{Store, TokenCipher};

/// The `anthropic-beta` token the OAuth usage endpoint expects. Mirrors
/// `claude_wire::OAUTH_BETA`; kept as a local constant so this module has no cross-crate coupling.
const OAUTH_BETA: &str = "oauth-2025-04-20";

/// A recent Claude Code CLI version. The usage endpoint is CLI-facing and may gate on a
/// `claude-code/<semver>` User-Agent, so a real one is sent rather than a generic agent.
const CLAUDE_CODE_UA: &str = "claude-code/2.1.209";

/// The OAuth endpoints sit at the API root (`/api/oauth/...`), not under any versioned path.
fn oauth_url(upstream_base: &str, path: &str) -> String {
    format!("{}/api/oauth/{}", upstream_base.trim_end_matches('/'), path)
}

/// Fetch one account's usage payload. Returns `(http_status, body_text)`; the caller decides how to
/// present or parse it. The access token is decrypted only for the duration of the call.
pub async fn fetch_usage_raw(
    store: &Store,
    cipher: &TokenCipher,
    upstream_base: &str,
    account_id: &str,
) -> Result<(u16, String), Box<dyn std::error::Error + Send + Sync>> {
    fetch_oauth_raw(store, cipher, upstream_base, account_id, "usage").await
}

/// Fetch a raw `GET /api/oauth/{path}` body for an account, authenticated with its OAuth bearer and
/// the oauth beta header. Used both for `usage` and for probing whether a profile endpoint exists.
pub async fn fetch_oauth_raw(
    store: &Store,
    cipher: &TokenCipher,
    upstream_base: &str,
    account_id: &str,
    path: &str,
) -> Result<(u16, String), Box<dyn std::error::Error + Send + Sync>> {
    let tokens = store
        .accounts()
        .decrypt_tokens(account_id, cipher)
        .await?
        .ok_or("account not found, or it has no stored tokens")?;
    let http = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .build()?;
    let resp = http
        .get(oauth_url(upstream_base, path))
        .header("Authorization", format!("Bearer {}", tokens.access_token))
        .header("anthropic-beta", OAUTH_BETA)
        .header("User-Agent", CLAUDE_CODE_UA)
        .header("Accept", "application/json")
        .header("Content-Type", "application/json")
        .send()
        .await?;
    let status = resp.status().as_u16();
    let body = resp.text().await?;
    Ok((status, body))
}

// ---------------------------------------------------------------------------------------------
// Typed views of the two payloads. Every field is optional with `#[serde(default)]` so a shape the
// upstream extends (it is actively migrating windows into `limits[]`) parses instead of erroring.
// ---------------------------------------------------------------------------------------------

/// One usage window: `utilization` is a 0..=100 percentage, `resets_at` an RFC3339 timestamp.
#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct UsageWindow {
    #[serde(default)]
    pub utilization: Option<f64>,
    #[serde(default)]
    pub resets_at: Option<String>,
}

/// The model a per-model (`weekly_scoped`) limit applies to.
#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct LimitModel {
    #[serde(default)]
    pub display_name: Option<String>,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct LimitScope {
    #[serde(default)]
    pub model: Option<LimitModel>,
}

/// One entry of the generic `limits[]` array. `kind` is `session` | `weekly_all` | `weekly_scoped`;
/// per-model caps (e.g. Fable) appear ONLY as `weekly_scoped` with `scope.model.display_name`.
#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct UsageLimit {
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub percent: Option<f64>,
    #[serde(default)]
    pub severity: Option<String>,
    #[serde(default)]
    pub resets_at: Option<String>,
    #[serde(default)]
    pub scope: Option<LimitScope>,
}

/// Pay-as-you-go overage block. `disabled_reason == "out_of_credits"` is the billing signal that a
/// request failed for credit reasons, not account health — it must fail over WITHOUT benching.
#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct ExtraUsage {
    #[serde(default)]
    pub is_enabled: Option<bool>,
    #[serde(default)]
    pub disabled_reason: Option<String>,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct UsageResponse {
    #[serde(default)]
    pub five_hour: Option<UsageWindow>,
    #[serde(default)]
    pub seven_day: Option<UsageWindow>,
    #[serde(default)]
    pub limits: Vec<UsageLimit>,
    #[serde(default)]
    pub extra_usage: Option<ExtraUsage>,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct ProfileAccount {
    #[serde(default)]
    pub has_claude_max: Option<bool>,
    #[serde(default)]
    pub has_claude_pro: Option<bool>,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct ProfileOrganization {
    #[serde(default)]
    pub organization_type: Option<String>,
    /// e.g. `default_claude_max_20x`, `default_claude_max_5x` — the authoritative plan/tier signal.
    #[serde(default)]
    pub rate_limit_tier: Option<String>,
    #[serde(default)]
    pub subscription_status: Option<String>,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct ProfileResponse {
    #[serde(default)]
    pub account: ProfileAccount,
    #[serde(default)]
    pub organization: ProfileOrganization,
}

/// Parse an RFC3339 timestamp (e.g. `2026-08-18T14:29:59.650142+00:00`) to unix seconds.
pub fn parse_iso_to_unix(s: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(s.trim())
        .ok()
        .map(|dt| dt.timestamp())
}

/// A short, stable plan slug from `organization.rate_limit_tier`. Mirrors the codex `plan_type`
/// convention (lowercase, no spaces). Falls back to broader signals, then `unknown`.
pub fn plan_slug_from_tier(tier: Option<&str>) -> &'static str {
    match tier.map(str::to_ascii_lowercase) {
        Some(t) if t.contains("20x") => "max_20x",
        Some(t) if t.contains("5x") => "max_5x",
        Some(t) if t.contains("max") => "max",
        Some(t) if t.contains("pro") => "pro",
        Some(t) if t.contains("team") => "team",
        Some(t) if t.contains("free") => "free",
        _ => "unknown",
    }
}

/// The plan slug for a full profile, preferring the tier string and falling back to the account's
/// `has_claude_max`/`has_claude_pro` booleans when the tier is absent.
pub fn plan_slug_from_profile(profile: &ProfileResponse) -> &'static str {
    let from_tier = plan_slug_from_tier(profile.organization.rate_limit_tier.as_deref());
    if from_tier != "unknown" {
        return from_tier;
    }
    match (
        profile.account.has_claude_max,
        profile.account.has_claude_pro,
    ) {
        (Some(true), _) => "max",
        (_, Some(true)) => "pro",
        _ => "unknown",
    }
}

/// Display names of models whose per-model (`weekly_scoped`) weekly cap is exhausted (>= 100%).
/// Routing should avoid these models on this account until the window resets.
pub fn models_at_cap(usage: &UsageResponse) -> Vec<String> {
    model_cap_windows(usage)
        .into_iter()
        .filter(|w| w.percent >= 100.0)
        .map(|w| w.display_name)
        .collect()
}

/// One model's own weekly window, as the upstream reports it.
#[derive(Debug, Clone, PartialEq)]
pub struct ModelCapWindow {
    /// The upstream's display name for the model, e.g. `Fable`.
    pub display_name: String,
    /// 0..=100 utilization of THIS model's weekly allowance.
    pub percent: f64,
    /// Unix seconds when this model's window resets, when the upstream said.
    pub resets_at: Option<i64>,
}

/// EVERY per-model weekly window the payload carries — not only the exhausted ones — so the
/// dashboard can show how close each model is to its own cap, while routing filters on `>= 100`.
/// Per-model caps arrive ONLY as `weekly_scoped` entries with a `scope.model.display_name`.
pub fn model_cap_windows(usage: &UsageResponse) -> Vec<ModelCapWindow> {
    usage
        .limits
        .iter()
        .filter(|l| l.kind.as_deref() == Some("weekly_scoped"))
        .filter_map(|l| {
            Some(ModelCapWindow {
                display_name: l.scope.as_ref()?.model.as_ref()?.display_name.clone()?,
                percent: l.percent?,
                resets_at: l.resets_at.as_deref().and_then(parse_iso_to_unix),
            })
        })
        .collect()
}

/// Fetch and parse the usage payload for one account.
pub async fn fetch_usage(
    store: &Store,
    cipher: &TokenCipher,
    upstream_base: &str,
    account_id: &str,
) -> Result<UsageResponse, Box<dyn std::error::Error + Send + Sync>> {
    let (status, body) = fetch_oauth_raw(store, cipher, upstream_base, account_id, "usage").await?;
    if status != 200 {
        return Err(format!("usage endpoint returned HTTP {status}").into());
    }
    Ok(serde_json::from_str(&body)?)
}

/// Fetch and parse the profile payload for one account.
pub async fn fetch_profile(
    store: &Store,
    cipher: &TokenCipher,
    upstream_base: &str,
    account_id: &str,
) -> Result<ProfileResponse, Box<dyn std::error::Error + Send + Sync>> {
    let (status, body) =
        fetch_oauth_raw(store, cipher, upstream_base, account_id, "profile").await?;
    if status != 200 {
        return Err(format!("profile endpoint returned HTTP {status}").into());
    }
    Ok(serde_json::from_str(&body)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    // The verbatim payloads captured live on 2026-08-18 (account 25fb, aididhaiqal02@gmail.com).
    const USAGE_JSON: &str = r#"{"five_hour":{"utilization":7.0,"resets_at":"2026-08-18T14:29:59.650142+00:00"},"seven_day":{"utilization":76.0,"resets_at":"2026-08-24T03:59:59.650161+00:00"},"seven_day_opus":null,"extra_usage":{"is_enabled":false,"monthly_limit":8000,"used_credits":0.0,"disabled_reason":"out_of_credits"},"limits":[{"kind":"session","group":"session","percent":7,"severity":"normal","resets_at":"2026-08-18T14:29:59.650142+00:00","scope":null,"is_active":false},{"kind":"weekly_all","group":"weekly","percent":76,"severity":"warning","resets_at":"2026-08-24T03:59:59.650161+00:00","scope":null,"is_active":false},{"kind":"weekly_scoped","group":"weekly","percent":100,"severity":"critical","resets_at":"2026-08-24T03:59:59.650366+00:00","scope":{"model":{"id":null,"display_name":"Fable"},"surface":null},"is_active":true}]}"#;
    const PROFILE_JSON: &str = r#"{"account":{"uuid":"25fb809a","full_name":"Aidid","email":"a@b.com","has_claude_max":true,"has_claude_pro":false},"organization":{"uuid":"f3c55afc","organization_type":"claude_max","billing_type":"stripe_subscription","rate_limit_tier":"default_claude_max_20x","subscription_status":"active"},"application":{"name":"Claude Code","slug":"claude-code"}}"#;

    #[test]
    fn parses_real_usage_windows_and_limits() {
        let usage: UsageResponse = serde_json::from_str(USAGE_JSON).unwrap();
        assert_eq!(usage.five_hour.as_ref().unwrap().utilization, Some(7.0));
        assert_eq!(usage.seven_day.as_ref().unwrap().utilization, Some(76.0));
        assert_eq!(
            parse_iso_to_unix(usage.five_hour.as_ref().unwrap().resets_at.as_deref().unwrap()),
            Some(1_787_063_399) // 2026-08-18T14:29:59Z
        );
        assert_eq!(usage.limits.len(), 3);
    }

    #[test]
    fn detects_the_fable_per_model_cap_at_100() {
        let usage: UsageResponse = serde_json::from_str(USAGE_JSON).unwrap();
        assert_eq!(models_at_cap(&usage), vec!["Fable".to_string()]);
    }

    #[test]
    fn detects_out_of_credits() {
        let usage: UsageResponse = serde_json::from_str(USAGE_JSON).unwrap();
        assert_eq!(
            usage.extra_usage.as_ref().unwrap().disabled_reason.as_deref(),
            Some("out_of_credits")
        );
    }

    #[test]
    fn parses_real_profile_plan_as_max_20x() {
        let profile: ProfileResponse = serde_json::from_str(PROFILE_JSON).unwrap();
        assert_eq!(
            profile.organization.rate_limit_tier.as_deref(),
            Some("default_claude_max_20x")
        );
        assert_eq!(plan_slug_from_profile(&profile), "max_20x");
    }

    #[test]
    fn plan_slug_maps_known_tiers() {
        assert_eq!(plan_slug_from_tier(Some("default_claude_max_20x")), "max_20x");
        assert_eq!(plan_slug_from_tier(Some("default_claude_max_5x")), "max_5x");
        assert_eq!(plan_slug_from_tier(Some("default_claude_pro")), "pro");
        assert_eq!(plan_slug_from_tier(None), "unknown");
    }

    #[test]
    fn oauth_url_is_rooted_and_trims_a_trailing_slash() {
        assert_eq!(
            oauth_url("https://api.anthropic.com", "usage"),
            "https://api.anthropic.com/api/oauth/usage"
        );
        assert_eq!(
            oauth_url("https://api.anthropic.com/", "profile"),
            "https://api.anthropic.com/api/oauth/profile"
        );
    }
}
