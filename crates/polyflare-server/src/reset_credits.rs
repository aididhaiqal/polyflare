use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use base64::Engine;
use chrono::{DateTime, SecondsFormat, Utc};
use futures_util::stream::{self, StreamExt};
use polyflare_codex::{
    consume_reset_credit, fetch_reset_credits, ConsumeResetCreditResponse, ResetCreditsError,
    ResetCreditsResponse,
};
use polyflare_core::reset_credit::{
    optimize_reset_credits, ResetCreditCandidateInput, ResetCreditRecommendation,
};
use polyflare_core::{select::plan_capacity_secondary, Account as CoreAccount, AccountId};
use polyflare_store::{Account, ResetCredit, ResetCreditSnapshot};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::app::AppState;

const REFRESH_INTERVAL: Duration = Duration::from_secs(60);
const REFRESH_JITTER_SECONDS: u32 = 15;
const REFRESH_CONCURRENCY: usize = 16;
const SNAPSHOT_STALE_MIN_SECONDS: i64 = 150;
const CLAIM_LEASE_SECONDS: i64 = 30;
const CLAIM_RENEW_SECONDS: u64 = 10;
const CLAIM_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(15);
const FETCH_TIMEOUT: Duration = Duration::from_secs(15);
const CONSUME_TIMEOUT: Duration = Duration::from_secs(30);
const REQUEST_TTL_SECONDS: i64 = 24 * 3_600;

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0)
}

fn parse_time(value: Option<&str>) -> Option<i64> {
    value
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.timestamp())
}

fn format_time(value: Option<i64>) -> Option<String> {
    value
        .and_then(|seconds| DateTime::<Utc>::from_timestamp(seconds, 0))
        .map(|value| value.to_rfc3339_opts(SecondsFormat::Secs, true))
}

fn random_id() -> String {
    let mut bytes = [0u8; 16];
    rand::rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

fn snapshot_stale_after_seconds(account_count: usize) -> i64 {
    let batches = account_count.max(1).div_ceil(REFRESH_CONCURRENCY) as u64;
    let worst_case_round = FETCH_TIMEOUT.as_secs().saturating_mul(batches);
    let longest_scheduled_cycle =
        worst_case_round.max(REFRESH_INTERVAL.as_secs() + u64::from(REFRESH_JITTER_SECONDS));
    i64::try_from(longest_scheduled_cycle.saturating_mul(2))
        .unwrap_or(i64::MAX)
        .max(SNAPSHOT_STALE_MIN_SECONDS)
}

fn core_account(
    state: &AppState,
    account: &Account,
    access_token: String,
    id_token: &str,
) -> CoreAccount {
    CoreAccount {
        id: account.id.clone(),
        base_url: state.upstream_base_url.clone(),
        bearer_token: access_token,
        chatgpt_account_id: account.chatgpt_account_id.clone(),
        is_fedramp: polyflare_codex::oauth::is_fedramp_account(id_token),
    }
}

fn stored_credit(account_id: &str, credit: &polyflare_codex::ResetCreditItem) -> ResetCredit {
    ResetCredit {
        account_id: account_id.to_string(),
        credit_id: credit.id.clone(),
        reset_type: credit.reset_type.clone(),
        status: credit.status.clone(),
        granted_at: parse_time(credit.granted_at.as_deref()),
        expires_at: parse_time(credit.expires_at.as_deref()),
        title: credit.title.clone(),
        description: credit.description.clone(),
        redeem_started_at: parse_time(credit.redeem_started_at.as_deref()),
        redeemed_at: parse_time(credit.redeemed_at.as_deref()),
    }
}

async fn store_snapshot(
    state: &AppState,
    account_id: &str,
    response: &ResetCreditsResponse,
) -> Result<(), polyflare_store::StoreError> {
    let credits: Vec<_> = response
        .credits
        .iter()
        .map(|credit| stored_credit(account_id, credit))
        .collect();
    state
        .store
        .reset_credits()
        .replace_snapshot(account_id, response.available_count, unix_now(), &credits)
        .await
}

async fn refresh_account(state: &AppState, account: &Account) -> Result<(), ResetRefreshError> {
    if account.provider != "codex"
        || account.chatgpt_account_id.is_none()
        || matches!(
            account.status.as_str(),
            "paused" | "reauth_required" | "deactivated"
        )
    {
        state.store.reset_credits().invalidate(&account.id).await?;
        return Ok(());
    }
    let Some(tokens) = state
        .store
        .accounts()
        .decrypt_tokens(&account.id, &state.cipher)
        .await?
    else {
        return Ok(());
    };
    let upstream = core_account(
        state,
        account,
        tokens.access_token.clone(),
        &tokens.id_token,
    );
    let response = tokio::time::timeout(
        FETCH_TIMEOUT,
        fetch_reset_credits(&state.control_client, &upstream),
    )
    .await
    .map_err(|_| ResetRefreshError::Timeout)??;
    store_snapshot(state, &account.id, &response).await?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
enum ResetRefreshError {
    #[error(transparent)]
    Store(#[from] polyflare_store::StoreError),
    #[error(transparent)]
    Upstream(#[from] ResetCreditsError),
    #[error("reset-credit refresh timed out")]
    Timeout,
}

pub fn spawn_reset_credit_refresh(state: Arc<AppState>) {
    tokio::spawn(async move {
        loop {
            let round_started = tokio::time::Instant::now();
            let accounts = state.store.accounts().list().await.unwrap_or_default();
            stream::iter(accounts)
                .for_each_concurrent(REFRESH_CONCURRENCY, |account| {
                    let state = state.clone();
                    async move {
                        if let Err(error) = refresh_account(&state, &account).await {
                            tracing::warn!(
                                account_id = %account.id,
                                error = %error,
                                "reset-credit refresh failed"
                            );
                        }
                    }
                })
                .await;
            let jitter =
                Duration::from_secs((rand::rng().next_u32() % (REFRESH_JITTER_SECONDS + 1)) as u64);
            tokio::time::sleep((REFRESH_INTERVAL + jitter).saturating_sub(round_started.elapsed()))
                .await;
        }
    });
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecommendationView {
    RedeemNow,
    RedeemBeforeExpiry,
    Hold,
    WaitForNaturalReset,
    LowBenefit,
    NoCredit,
    Unavailable,
}

impl From<ResetCreditRecommendation> for RecommendationView {
    fn from(value: ResetCreditRecommendation) -> Self {
        match value {
            ResetCreditRecommendation::RedeemNow => Self::RedeemNow,
            ResetCreditRecommendation::RedeemBeforeExpiry => Self::RedeemBeforeExpiry,
            ResetCreditRecommendation::Hold => Self::Hold,
            ResetCreditRecommendation::WaitForNaturalReset => Self::WaitForNaturalReset,
            ResetCreditRecommendation::LowBenefit => Self::LowBenefit,
            ResetCreditRecommendation::NoCredit => Self::NoCredit,
            ResetCreditRecommendation::Unavailable => Self::Unavailable,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ResetPlanCandidateView {
    pub account_id: String,
    pub email: String,
    pub alias: Option<String>,
    pub plan_type: String,
    pub pools: Vec<String>,
    pub weekly_used_percent: f64,
    pub weekly_reset_at: Option<i64>,
    pub available_credits: i64,
    pub earliest_credit_expires_at: Option<i64>,
    pub snapshot_fetched_at: Option<i64>,
    pub recoverable_credits: f64,
    pub time_weighted_value: f64,
    pub recommendation: RecommendationView,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResetPlanView {
    pub generated_at: i64,
    pub total_credits: i64,
    pub accounts_with_credits: i64,
    pub recommended_now: i64,
    pub candidates: Vec<ResetPlanCandidateView>,
}

pub(crate) async fn build_plan(state: &AppState, pool: Option<&str>) -> Result<ResetPlanView, ()> {
    let now = unix_now();
    let accounts = state.store.accounts().list().await.map_err(|_| ())?;
    let snapshots = state
        .account_cache
        .snapshots(&state.store)
        .await
        .map_err(|_| ())?;
    let account_by_id: HashMap<_, _> = accounts
        .into_iter()
        .filter(|account| {
            account.provider == "codex"
                && pool.is_none_or(|pool| {
                    snapshots
                        .iter()
                        .find(|snapshot| snapshot.id.as_str() == account.id)
                        .is_some_and(|snapshot| snapshot.pools.iter().any(|value| value == pool))
                })
        })
        .map(|account| (account.id.clone(), account))
        .collect();
    let reset_snapshots = state
        .store
        .reset_credits()
        .list_snapshots()
        .await
        .map_err(|_| ())?;
    let reset_by_id: HashMap<_, _> = reset_snapshots
        .into_iter()
        .map(|snapshot| (snapshot.account_id.clone(), snapshot))
        .collect();

    let inputs: Vec<_> = snapshots
        .iter()
        .filter_map(|snapshot| {
            let account = account_by_id.get(snapshot.id.as_str())?;
            let credits = reset_by_id.get(snapshot.id.as_str());
            let weekly = snapshot.weekly_quota.as_ref();
            Some(ResetCreditCandidateInput {
                account_id: account.id.clone(),
                capacity_credits: snapshot
                    .capacity_credits
                    .unwrap_or_else(|| plan_capacity_secondary(&account.plan_type)),
                weekly_used_percent: weekly
                    .map(|window| window.used_percent)
                    .unwrap_or(snapshot.secondary_used_percent),
                weekly_reset_at: weekly.and_then(|window| window.reset_at),
                available_credits: credits.map(|value| value.available_count).unwrap_or(0),
                earliest_credit_expires_at: credits.and_then(earliest_expiry),
                snapshot_fetched_at: credits.map(|value| value.fetched_at).unwrap_or(0),
                eligible: !matches!(
                    account.status.as_str(),
                    "paused" | "reauth_required" | "deactivated"
                ) && weekly.is_some_and(|window| !window.stale),
            })
        })
        .collect();
    let optimized = optimize_reset_credits(
        &inputs,
        now,
        snapshot_stale_after_seconds(account_by_id.len()),
    );
    let mut rows = Vec::with_capacity(optimized.len());
    for candidate in optimized {
        let Some(account) = account_by_id.get(&candidate.account_id) else {
            continue;
        };
        let snapshot = snapshots
            .iter()
            .find(|snapshot| snapshot.id.as_str() == candidate.account_id)
            .ok_or(())?;
        let credits = reset_by_id.get(&candidate.account_id);
        rows.push(ResetPlanCandidateView {
            account_id: account.id.clone(),
            email: account.email.clone(),
            alias: account.alias.clone(),
            plan_type: account.plan_type.clone(),
            pools: snapshot.pools.clone(),
            weekly_used_percent: snapshot
                .weekly_quota
                .as_ref()
                .map(|window| window.used_percent)
                .unwrap_or(snapshot.secondary_used_percent),
            weekly_reset_at: snapshot
                .weekly_quota
                .as_ref()
                .and_then(|window| window.reset_at),
            available_credits: credits.map(|value| value.available_count).unwrap_or(0),
            earliest_credit_expires_at: credits.and_then(earliest_expiry),
            snapshot_fetched_at: credits.map(|value| value.fetched_at),
            recoverable_credits: candidate.recoverable_credits,
            time_weighted_value: candidate.time_weighted_value,
            recommendation: candidate.recommendation.into(),
            reason: candidate.reason.to_string(),
        });
    }
    let total_credits = rows.iter().map(|row| row.available_credits).sum();
    let accounts_with_credits = rows.iter().filter(|row| row.available_credits > 0).count() as i64;
    let recommended_now = rows
        .iter()
        .filter(|row| {
            matches!(
                row.recommendation,
                RecommendationView::RedeemNow | RecommendationView::RedeemBeforeExpiry
            )
        })
        .count() as i64;
    Ok(ResetPlanView {
        generated_at: now,
        total_credits,
        accounts_with_credits,
        recommended_now,
        candidates: rows,
    })
}

pub(crate) async fn available_count_for_scope(state: &AppState, pool: Option<&str>) -> i64 {
    build_plan(state, pool)
        .await
        .map(|plan| {
            plan.candidates
                .iter()
                .filter(|candidate| {
                    !matches!(
                        candidate.recommendation,
                        RecommendationView::Unavailable | RecommendationView::NoCredit
                    )
                })
                .map(|candidate| candidate.available_credits)
                .sum()
        })
        .unwrap_or(0)
}

fn earliest_expiry(snapshot: &ResetCreditSnapshot) -> Option<i64> {
    snapshot
        .credits
        .iter()
        .filter(|credit| credit.status.as_deref() == Some("available"))
        .filter_map(|credit| credit.expires_at)
        .min()
}

pub async fn plan_handler(State(state): State<Arc<AppState>>) -> Response {
    match build_plan(&state, None).await {
        Ok(plan) => Json(plan).into_response(),
        Err(()) => (
            StatusCode::SERVICE_UNAVAILABLE,
            "reset-credit plan unavailable",
        )
            .into_response(),
    }
}

#[derive(Debug, Deserialize)]
pub struct RedeemRequest {
    pub redeem_request_id: String,
    #[serde(default)]
    pub credit_id: Option<String>,
    #[serde(default)]
    pub require_recommended: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct RedeemResult {
    pub account_id: String,
    pub code: String,
    pub windows_reset: i64,
    pub redeemed_at: Option<i64>,
}

#[derive(Debug, thiserror::Error)]
enum RedeemError {
    #[error("account is not eligible for reset credits")]
    Ineligible,
    #[error("no available reset credit")]
    NoCredit,
    #[error("another redemption is in progress")]
    Busy,
    #[error("reset-credit recommendation is no longer actionable")]
    NotRecommended,
    #[error("reset-credit redemption lease was lost")]
    LeaseLost,
    #[error("reset-credit state unavailable")]
    Store,
    #[error("reset-credit upstream unavailable")]
    Upstream,
}

struct ClaimGuard {
    repo: polyflare_store::ResetCreditRepo,
    account_id: String,
    holder_id: String,
    heartbeat: tokio::task::JoinHandle<()>,
}

impl ClaimGuard {
    async fn release(self) {
        self.heartbeat.abort();
        let _ = self
            .repo
            .release_claim(&self.account_id, &self.holder_id)
            .await;
    }
}

impl Drop for ClaimGuard {
    fn drop(&mut self) {
        // Dropping/cancelling the request must never detach a task that renews the lease forever.
        // We intentionally leave the DB row to expire if the async normal-release path cannot run.
        self.heartbeat.abort();
    }
}

fn completed_result(
    account_id: &str,
    previous: polyflare_store::ResetCreditRedeemResult,
) -> RedeemResult {
    RedeemResult {
        account_id: account_id.to_string(),
        code: previous.result_code,
        windows_reset: previous.windows_reset,
        redeemed_at: previous.redeemed_at,
    }
}

async fn redeem_one(
    state: &AppState,
    account_id: &str,
    requested_credit_id: Option<&str>,
    redeem_request_id: &str,
    require_recommended: bool,
    native_request_id: Option<&str>,
) -> Result<RedeemResult, RedeemError> {
    let repo = state.store.reset_credits();
    let now = unix_now();
    if let Some(previous) = repo
        .get_result(account_id, redeem_request_id, now, REQUEST_TTL_SECONDS)
        .await
        .map_err(|_| RedeemError::Store)?
    {
        let result = completed_result(account_id, previous);
        repair_post_completion(state, account_id).await;
        return Ok(result);
    }

    let holder_id = random_id();
    let deadline = tokio::time::Instant::now() + CLAIM_ACQUIRE_TIMEOUT;
    loop {
        if repo
            .try_acquire_claim(
                account_id,
                &holder_id,
                unix_now(),
                unix_now() + CLAIM_LEASE_SECONDS,
            )
            .await
            .map_err(|_| RedeemError::Store)?
        {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(RedeemError::Busy);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let heartbeat_repo = repo.clone();
    let heartbeat_account = account_id.to_string();
    let heartbeat_holder = holder_id.clone();
    let (lease_lost_tx, mut lease_lost_rx) = tokio::sync::oneshot::channel();
    let heartbeat = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(CLAIM_RENEW_SECONDS)).await;
            if !heartbeat_repo
                .renew_claim(
                    &heartbeat_account,
                    &heartbeat_holder,
                    unix_now() + CLAIM_LEASE_SECONDS,
                )
                .await
                .unwrap_or(false)
            {
                let _ = lease_lost_tx.send(());
                return;
            }
        }
    });
    let guard = ClaimGuard {
        repo,
        account_id: account_id.to_string(),
        holder_id,
        heartbeat,
    };
    let outcome = tokio::select! {
        _ = &mut lease_lost_rx => Err(RedeemError::LeaseLost),
        result = redeem_one_claimed(
            state,
            account_id,
            requested_credit_id,
            redeem_request_id,
            require_recommended,
            native_request_id,
        ) => result,
    };
    guard.release().await;
    outcome
}

async fn redeem_one_claimed(
    state: &AppState,
    account_id: &str,
    requested_credit_id: Option<&str>,
    redeem_request_id: &str,
    require_recommended: bool,
    native_request_id: Option<&str>,
) -> Result<RedeemResult, RedeemError> {
    let account = state
        .store
        .accounts()
        .get(account_id)
        .await
        .map_err(|_| RedeemError::Store)?
        .filter(|account| {
            account.provider == "codex"
                && account.chatgpt_account_id.is_some()
                && !matches!(
                    account.status.as_str(),
                    "paused" | "reauth_required" | "deactivated"
                )
        })
        .ok_or(RedeemError::Ineligible)?;
    let tokens = state
        .store
        .accounts()
        .decrypt_tokens(account_id, &state.cipher)
        .await
        .map_err(|_| RedeemError::Store)?
        .ok_or(RedeemError::Ineligible)?;
    let upstream = core_account(
        state,
        &account,
        tokens.access_token.clone(),
        &tokens.id_token,
    );
    if let Some(previous) = state
        .store
        .reset_credits()
        .get_result(
            account_id,
            redeem_request_id,
            unix_now(),
            REQUEST_TTL_SECONDS,
        )
        .await
        .map_err(|_| RedeemError::Store)?
    {
        let result = completed_result(account_id, previous);
        repair_post_completion(state, account_id).await;
        return Ok(result);
    }
    let existing = state
        .store
        .reset_credits()
        .get_request(
            account_id,
            redeem_request_id,
            unix_now(),
            REQUEST_TTL_SECONDS,
        )
        .await
        .map_err(|_| RedeemError::Store)?;
    if let Some(native_request_id) = native_request_id {
        let native = state
            .store
            .reset_credits()
            .get_native_request(native_request_id, unix_now(), REQUEST_TTL_SECONDS)
            .await
            .map_err(|_| RedeemError::Store)?;
        if native.is_some_and(|request| request.result_code.as_deref() == Some("no_credit")) {
            return Err(RedeemError::NoCredit);
        }
    }
    if require_recommended && existing.is_none() {
        let refreshed = tokio::time::timeout(
            FETCH_TIMEOUT,
            crate::usage_refresh::refresh_account_now(state, account_id),
        )
        .await
        .map_err(|_| RedeemError::Upstream)?
        .map_err(|_| RedeemError::Upstream)?;
        if !refreshed {
            return Err(RedeemError::Upstream);
        }
    }
    let fresh = tokio::time::timeout(
        FETCH_TIMEOUT,
        fetch_reset_credits(&state.control_client, &upstream),
    )
    .await
    .map_err(|_| RedeemError::Upstream)?
    .map_err(|_| RedeemError::Upstream)?;
    store_snapshot(state, account_id, &fresh)
        .await
        .map_err(|_| RedeemError::Store)?;
    if require_recommended && existing.is_none() {
        let plan = build_plan(state, None)
            .await
            .map_err(|_| RedeemError::Store)?;
        if !plan.candidates.iter().any(|candidate| {
            candidate.account_id == account_id
                && matches!(
                    candidate.recommendation,
                    RecommendationView::RedeemNow | RecommendationView::RedeemBeforeExpiry
                )
        }) {
            return Err(RedeemError::NotRecommended);
        }
    }

    let selected = if let Some(existing) = &existing {
        existing.credit_id.clone()
    } else {
        let mut available: Vec<_> = fresh
            .credits
            .iter()
            .filter(|credit| credit.status.as_deref() == Some("available"))
            .filter(|credit| requested_credit_id.is_none_or(|id| credit.id == id))
            .collect();
        available.sort_by(|left, right| {
            left.expires_at
                .is_none()
                .cmp(&right.expires_at.is_none())
                .then_with(|| left.expires_at.cmp(&right.expires_at))
                .then_with(|| left.id.cmp(&right.id))
        });
        match available.first() {
            Some(credit) => credit.id.clone(),
            None => {
                if let Some(native_request_id) = native_request_id {
                    state
                        .store
                        .reset_credits()
                        .complete_native_account_no_credit(
                            native_request_id,
                            account_id,
                            unix_now(),
                        )
                        .await
                        .map_err(|_| RedeemError::Store)?;
                }
                return Err(RedeemError::NoCredit);
            }
        }
    };
    let pinned = state
        .store
        .reset_credits()
        .pin_request(
            account_id,
            redeem_request_id,
            &selected,
            unix_now(),
            REQUEST_TTL_SECONDS,
        )
        .await
        .map_err(|_| RedeemError::Store)?;
    let consumed = match tokio::time::timeout(
        CONSUME_TIMEOUT,
        consume_reset_credit(&state.control_client, &upstream, &pinned, redeem_request_id),
    )
    .await
    .map_err(|_| RedeemError::Upstream)?
    {
        Ok(consumed) => consumed,
        Err(ResetCreditsError::Upstream {
            code: Some(code), ..
        }) if matches!(
            code.as_str(),
            "no_credit" | "already_redeemed" | "nothing_to_reset"
        ) =>
        {
            ConsumeResetCreditResponse {
                code,
                credit: None,
                windows_reset: 0,
            }
        }
        Err(_) => return Err(RedeemError::Upstream),
    };
    complete_and_refresh(
        state,
        account_id,
        redeem_request_id,
        native_request_id,
        consumed,
    )
    .await
}

async fn complete_and_refresh(
    state: &AppState,
    account_id: &str,
    redeem_request_id: &str,
    native_request_id: Option<&str>,
    consumed: ConsumeResetCreditResponse,
) -> Result<RedeemResult, RedeemError> {
    let redeemed_at = consumed
        .credit
        .as_ref()
        .and_then(|credit| parse_time(credit.redeemed_at.as_deref()));
    let terminal = state
        .store
        .reset_credits()
        .complete_request_with_native(
            account_id,
            redeem_request_id,
            &consumed.code,
            consumed.windows_reset,
            redeemed_at,
            unix_now(),
            native_request_id,
        )
        .await
        .map_err(|_| RedeemError::Store)?;
    repair_post_completion(state, account_id).await;
    Ok(RedeemResult {
        account_id: account_id.to_string(),
        code: terminal.result_code,
        windows_reset: terminal.windows_reset,
        redeemed_at: terminal.redeemed_at,
    })
}

async fn repair_post_completion(state: &AppState, account_id: &str) {
    // Once the terminal ledger commit succeeds, never turn success into a 503 because cache
    // cleanup failed. Queue authoritative usage first, then make invalidation repairable on replay.
    state
        .runtime
        .request_usage_refresh(&AccountId::from(account_id));
    if let Err(error) = state.store.reset_credits().invalidate(account_id).await {
        tracing::warn!(
            account_id,
            error = %error,
            "could not invalidate reset-credit snapshot after durable completion"
        );
    }
}

fn redeem_error(error: RedeemError) -> Response {
    let (status, code) = match error {
        RedeemError::Ineligible => (StatusCode::CONFLICT, "reset_credit_account_ineligible"),
        RedeemError::NoCredit => (StatusCode::CONFLICT, "no_available_reset_credit"),
        RedeemError::Busy => (StatusCode::CONFLICT, "reset_credit_redeem_in_progress"),
        RedeemError::NotRecommended => {
            (StatusCode::CONFLICT, "reset_credit_recommendation_changed")
        }
        RedeemError::LeaseLost => (
            StatusCode::SERVICE_UNAVAILABLE,
            "reset_credit_redeem_lease_lost",
        ),
        RedeemError::Store => (
            StatusCode::SERVICE_UNAVAILABLE,
            "reset_credit_state_unavailable",
        ),
        RedeemError::Upstream => (
            StatusCode::SERVICE_UNAVAILABLE,
            "reset_credit_upstream_unavailable",
        ),
    };
    (
        status,
        Json(serde_json::json!({
            "error": { "type": "reset_credit_error", "code": code, "message": error.to_string() }
        })),
    )
        .into_response()
}

pub async fn account_redeem_handler(
    State(state): State<Arc<AppState>>,
    Path(account_id): Path<String>,
    Json(body): Json<RedeemRequest>,
) -> Response {
    if body.redeem_request_id.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            "redeem_request_id must not be empty",
        )
            .into_response();
    }
    match redeem_one(
        &state,
        &account_id,
        body.credit_id.as_deref(),
        &body.redeem_request_id,
        body.require_recommended,
        None,
    )
    .await
    {
        Ok(result) => Json(result).into_response(),
        Err(error) => redeem_error(error),
    }
}

#[derive(Debug, Deserialize)]
pub struct FleetRedeemRequest {
    pub redeem_request_id: String,
    pub account_ids: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct FleetRedeemResponse {
    pub results: Vec<RedeemResult>,
    pub errors: Vec<FleetRedeemError>,
}

#[derive(Debug, Serialize)]
pub struct FleetRedeemError {
    pub account_id: String,
    pub message: String,
}

pub async fn fleet_redeem_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<FleetRedeemRequest>,
) -> Response {
    let mut seen = HashSet::new();
    if body.redeem_request_id.trim().is_empty()
        || body.account_ids.is_empty()
        || body
            .account_ids
            .iter()
            .any(|account_id| account_id.is_empty() || !seen.insert(account_id))
    {
        return (
            StatusCode::BAD_REQUEST,
            "redeem_request_id and account_ids must be unique and non-empty",
        )
            .into_response();
    }
    let account_ids_json = match serde_json::to_string(&body.account_ids) {
        Ok(value) => value,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    let pinned_accounts = match state
        .store
        .reset_credits()
        .pin_fleet_request(
            &body.redeem_request_id,
            &account_ids_json,
            unix_now(),
            REQUEST_TTL_SECONDS,
        )
        .await
    {
        Ok(value) => value,
        Err(_) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
    };
    if pinned_accounts != account_ids_json {
        return (
            StatusCode::CONFLICT,
            "redeem_request_id is already pinned to a different account selection",
        )
            .into_response();
    }
    let mut results = Vec::new();
    let mut errors = Vec::new();
    for account_id in body.account_ids {
        let mut hash = Sha256::new();
        hash.update(body.redeem_request_id.as_bytes());
        hash.update([0]);
        hash.update(account_id.as_bytes());
        let account_request_id = format!("fleet-{}", hex::encode(hash.finalize()));
        match redeem_one(&state, &account_id, None, &account_request_id, false, None).await {
            Ok(result) => results.push(result),
            Err(error) => errors.push(FleetRedeemError {
                account_id,
                message: error.to_string(),
            }),
        }
    }
    Json(FleetRedeemResponse { results, errors }).into_response()
}

#[derive(Debug, Serialize)]
pub struct NativeResetCredit {
    pub id: String,
    pub reset_type: String,
    pub status: String,
    pub granted_at: String,
    pub expires_at: Option<String>,
    pub title: Option<String>,
    pub description: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct NativeResetCreditsResponse {
    pub credits: Vec<NativeResetCredit>,
    pub available_count: i64,
}

fn opaque_credit_id(account_id: &str, credit_id: &str) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(format!("{account_id}\0{credit_id}"))
}

fn decode_opaque_credit_id(value: &str) -> Option<(String, String)> {
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(value)
        .ok()?;
    let value = String::from_utf8(bytes).ok()?;
    let (account, credit) = value.split_once('\0')?;
    (!account.is_empty() && !credit.is_empty()).then(|| (account.to_string(), credit.to_string()))
}

fn client_authenticated(state: &AppState, headers: &HeaderMap) -> bool {
    if state.enforce_client_keys {
        return false;
    }
    headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("Bearer ") && value.len() > 7)
}

async fn native_list(state: Arc<AppState>, pool: Option<String>, headers: HeaderMap) -> Response {
    let authenticated = if state.enforce_client_keys {
        crate::auth::authenticate_client_key(&state, &headers).await
    } else {
        client_authenticated(&state, &headers)
    };
    if !authenticated {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Ok(plan) = build_plan(&state, pool.as_deref()).await else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let allowed: HashSet<_> = plan
        .candidates
        .iter()
        .filter(|candidate| {
            !matches!(
                candidate.recommendation,
                RecommendationView::Unavailable | RecommendationView::NoCredit
            )
        })
        .map(|candidate| candidate.account_id.as_str())
        .collect();
    let snapshots = state.store.reset_credits().list_snapshots().await;
    let Ok(snapshots) = snapshots else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let mut credits = Vec::new();
    for snapshot in snapshots {
        if !allowed.contains(snapshot.account_id.as_str()) {
            continue;
        }
        for credit in snapshot
            .credits
            .into_iter()
            .filter(|credit| credit.status.as_deref() == Some("available"))
        {
            credits.push(NativeResetCredit {
                id: opaque_credit_id(&snapshot.account_id, &credit.credit_id),
                reset_type: credit
                    .reset_type
                    .unwrap_or_else(|| "rate_limit_reset".to_string()),
                status: "available".to_string(),
                granted_at: format_time(credit.granted_at)
                    .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string()),
                expires_at: format_time(credit.expires_at),
                title: credit.title,
                description: credit.description,
            });
        }
    }
    credits.sort_by(|left, right| {
        left.expires_at
            .is_none()
            .cmp(&right.expires_at.is_none())
            .then_with(|| left.expires_at.cmp(&right.expires_at))
            .then_with(|| left.id.cmp(&right.id))
    });
    Json(NativeResetCreditsResponse {
        available_count: credits.len() as i64,
        credits,
    })
    .into_response()
}

pub async fn native_list_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    native_list(state, None, headers).await
}

pub async fn pooled_native_list_handler(
    State(state): State<Arc<AppState>>,
    Path(pool): Path<String>,
    headers: HeaderMap,
) -> Response {
    native_list(state, Some(pool), headers).await
}

#[derive(Debug, Deserialize)]
pub struct NativeConsumeRequest {
    pub redeem_request_id: String,
    #[serde(default)]
    pub credit_id: Option<String>,
}

async fn native_consume(
    state: Arc<AppState>,
    pool: Option<String>,
    headers: HeaderMap,
    body: NativeConsumeRequest,
) -> Response {
    let authenticated = if state.enforce_client_keys {
        crate::auth::authenticate_client_key(&state, &headers).await
    } else {
        client_authenticated(&state, &headers)
    };
    if !authenticated {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if body.redeem_request_id.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            "redeem_request_id must not be empty",
        )
            .into_response();
    }
    let explicitly_requested = match body.credit_id.as_deref() {
        Some(value) => match decode_opaque_credit_id(value) {
            Some(decoded) => Some(decoded),
            None => {
                return (StatusCode::CONFLICT, "requested credit is unavailable").into_response()
            }
        },
        None => None,
    };
    let repo = state.store.reset_credits();
    let now = unix_now();
    let requested_pool_scope = pool.as_deref().unwrap_or("").to_string();
    let existing_pin = match repo
        .get_native_request(&body.redeem_request_id, now, REQUEST_TTL_SECONDS)
        .await
    {
        Ok(value) => value,
        Err(_) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
    };
    if existing_pin.as_ref().is_some_and(|pin| {
        pin.pool_scope
            .as_deref()
            .is_some_and(|scope| scope != requested_pool_scope)
    }) {
        return (
            StatusCode::CONFLICT,
            "redeem_request_id is already pinned to a different pool scope",
        )
            .into_response();
    }
    if let Some(terminal) = existing_pin
        .as_ref()
        .filter(|pin| pin.result_code.is_some())
    {
        if let Some((requested_account, requested_credit)) = explicitly_requested.as_ref() {
            if terminal.account_id.as_deref() != Some(requested_account.as_str())
                || terminal.requested_credit_id.as_deref() != Some(requested_credit.as_str())
            {
                return (
                    StatusCode::CONFLICT,
                    "redeem_request_id is already pinned to a different operation",
                )
                    .into_response();
            }
        }
        if terminal.pool_scope.is_none() {
            if let (Some(pool), Some(account_id)) =
                (pool.as_deref(), terminal.account_id.as_deref())
            {
                let scoped = match build_plan(&state, Some(pool)).await {
                    Ok(plan) => plan
                        .candidates
                        .iter()
                        .any(|candidate| candidate.account_id == account_id),
                    Err(()) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
                };
                if !scoped {
                    return StatusCode::FORBIDDEN.into_response();
                }
            }
        }
        return Json(serde_json::json!({
            "code": terminal.result_code.as_deref().unwrap_or("no_credit"),
            "windows_reset": terminal.windows_reset.unwrap_or(0)
        }))
        .into_response();
    }
    let plan = match build_plan(&state, pool.as_deref()).await {
        Ok(plan) => plan,
        Err(()) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
    };
    let pinned = match existing_pin {
        Some(pinned) => pinned,
        None => {
            let selected = match &explicitly_requested {
                Some((account_id, credit_id)) => (account_id.clone(), Some(credit_id.clone())),
                None => match plan.candidates.iter().find(|candidate| {
                    candidate.available_credits > 0
                        && !matches!(
                            candidate.recommendation,
                            RecommendationView::Unavailable
                                | RecommendationView::WaitForNaturalReset
                                | RecommendationView::NoCredit
                        )
                }) {
                    Some(candidate) => (candidate.account_id.clone(), None),
                    None => {
                        match repo
                            .complete_native_no_credit(
                                &body.redeem_request_id,
                                &requested_pool_scope,
                                now,
                                REQUEST_TTL_SECONDS,
                            )
                            .await
                        {
                            Ok(terminal)
                                if terminal.pool_scope.as_deref()
                                    != Some(requested_pool_scope.as_str()) =>
                            {
                                return (
                                    StatusCode::CONFLICT,
                                    "redeem_request_id is already pinned to a different pool scope",
                                )
                                    .into_response()
                            }
                            Ok(terminal) if terminal.result_code.is_some() => {
                                return Json(serde_json::json!({
                                    "code": terminal.result_code.as_deref().unwrap_or("no_credit"),
                                    "windows_reset": terminal.windows_reset.unwrap_or(0)
                                }))
                                .into_response()
                            }
                            Ok(_) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
                            Err(_) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
                        }
                    }
                },
            };
            if !plan.candidates.iter().any(|candidate| {
                candidate.account_id == selected.0
                    && !matches!(
                        candidate.recommendation,
                        RecommendationView::Unavailable | RecommendationView::NoCredit
                    )
            }) {
                return StatusCode::FORBIDDEN.into_response();
            }
            match repo
                .pin_native_request(
                    &body.redeem_request_id,
                    &selected.0,
                    selected.1.as_deref(),
                    &requested_pool_scope,
                    now,
                    REQUEST_TTL_SECONDS,
                )
                .await
            {
                Ok(pinned) => pinned,
                Err(_) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
            }
        }
    };
    if pinned
        .pool_scope
        .as_deref()
        .is_some_and(|scope| scope != requested_pool_scope)
    {
        return (
            StatusCode::CONFLICT,
            "redeem_request_id is already pinned to a different pool scope",
        )
            .into_response();
    }
    if let Some((requested_account, requested_credit)) = explicitly_requested.as_ref() {
        if pinned.account_id.as_deref() != Some(requested_account.as_str())
            || pinned.requested_credit_id.as_deref() != Some(requested_credit.as_str())
        {
            return (
                StatusCode::CONFLICT,
                "redeem_request_id is already pinned to a different credit",
            )
                .into_response();
        }
    }
    let Some(account_id) = pinned.account_id.as_deref() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    // A durable pin is recovery state, not a fresh selection. Only enforce that its original
    // account still belongs to this root/pool scope; current credit availability or account health
    // must not prevent an exact terminal replay or an ambiguous same-operation recovery.
    if !plan
        .candidates
        .iter()
        .any(|candidate| candidate.account_id == account_id)
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    match redeem_one(
        &state,
        account_id,
        pinned.requested_credit_id.as_deref(),
        &body.redeem_request_id,
        false,
        Some(&body.redeem_request_id),
    )
    .await
    {
        Ok(result) => Json(serde_json::json!({
            "code": result.code,
            "windows_reset": result.windows_reset
        }))
        .into_response(),
        Err(RedeemError::NoCredit) => Json(serde_json::json!({
            "code": "no_credit", "windows_reset": 0
        }))
        .into_response(),
        Err(error) => redeem_error(error),
    }
}

pub async fn native_consume_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<NativeConsumeRequest>,
) -> Response {
    native_consume(state, None, headers, body).await
}

pub async fn pooled_native_consume_handler(
    State(state): State<Arc<AppState>>,
    Path(pool): Path<String>,
    headers: HeaderMap,
    Json(body): Json<NativeConsumeRequest>,
) -> Response {
    native_consume(state, Some(pool), headers, body).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_freshness_budget_scales_with_bounded_refresh_batches() {
        assert!(
            snapshot_stale_after_seconds(1) >= SNAPSHOT_STALE_MIN_SECONDS,
            "small fleets retain the conservative minimum freshness budget"
        );
        assert!(
            snapshot_stale_after_seconds(160) >= 300,
            "large fleets remain fresh for at least two worst-case refresh rounds"
        );
    }

    #[test]
    fn opaque_credit_ids_round_trip_without_delimiter_ambiguity() {
        let encoded = opaque_credit_id("account:a", "credit/b");
        assert_eq!(
            decode_opaque_credit_id(&encoded),
            Some(("account:a".to_string(), "credit/b".to_string()))
        );
    }
}
