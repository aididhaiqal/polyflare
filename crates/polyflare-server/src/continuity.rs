//! The Codex continuity state machine: a store-backed `Continuity` impl. Holds a `ContinuityRepo`;
//! persists NO conversation content — only session state + a response_id -> owner map.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use polyflare_core::{
    AccountId, Continuity, ContinuityDirective, ContinuityError, KeyStrength, Prepared,
    PreparedRequest, RecoveryPlan, RequestCtx, SessionKey, TurnOutcome, WatchdogArm,
};
use polyflare_store::{ContinuityRepo, StoreError};

/// TA6(b)'s sole capability tag today (mirrors `WatchdogError::CapabilityRejection`'s
/// `"security_work"` label and `AccountSnapshot::security_work_authorized`'s naming). Kept as a
/// constant so the sticky-stamp call site (`mark_required_capability`) and the sticky-read site
/// (`prepare`) can never drift apart on the literal.
const SECURITY_WORK_CAPABILITY: &str = "security_work";

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn box_store_err(e: StoreError) -> ContinuityError {
    ContinuityError::Store(Box::new(e))
}

fn strength_str(s: KeyStrength) -> &'static str {
    match s {
        KeyStrength::Hard => "hard",
        KeyStrength::Soft => "soft",
    }
}

/// Codex continuity backed by a `ContinuityRepo`. `watchdog_timeout` (N) is stamped into the
/// directive on every anchor-bearing request.
pub struct CodexContinuity {
    repo: ContinuityRepo,
    watchdog_timeout: Duration,
}

impl CodexContinuity {
    pub fn new(repo: ContinuityRepo, watchdog_timeout: Duration) -> Self {
        Self {
            repo,
            watchdog_timeout,
        }
    }
}

#[async_trait]
impl Continuity for CodexContinuity {
    async fn prepare(
        &self,
        req: PreparedRequest,
        ctx: &RequestCtx,
    ) -> Result<Prepared, ContinuityError> {
        let now = now_secs();
        let session_key = ctx.session_key.clone();
        let anchor = ctx.client_previous_response_id.clone();

        // Resolve the owner: the client-supplied anchor map is authoritative; else the session row.
        // The session row is ALSO where TA6(b) Task 3's sticky-cyber stamp lives, so fetch it
        // unconditionally (not just on an owner-resolution miss) — a turn whose owner resolved via
        // the anchor map must still pick up the sticky requirement for this turn's selection.
        //
        // S3(a) invariant (2026-07-22 incident): the session row is AFFINITY, not ownership — it
        // fills in only when no anchor resolves, and a disagreement is NEVER an error (the anchor
        // owner wins; the stale row is overwritten by the next record_completion/record_recovery).
        let mut anchor_owner: Option<AccountId> = None;
        if let Some(rid) = anchor.as_deref() {
            if let Some(acc) = self
                .repo
                .get_anchor_owner(rid)
                .await
                .map_err(box_store_err)?
            {
                anchor_owner = Some(AccountId::from(acc));
            }
        }
        let mut session_owner: Option<AccountId> = None;
        let mut require_security_work_authorized = false;
        if let Some(sk) = session_key.as_ref() {
            if let Some(row) = self
                .repo
                .get_session(&sk.value)
                .await
                .map_err(box_store_err)?
            {
                session_owner = row.owning_account_id.clone().map(AccountId::from);
                require_security_work_authorized = row.has_capability(SECURITY_WORK_CAPABILITY);
            }
        }
        let (owner, source) = match (&anchor_owner, &session_owner) {
            (Some(a), _) => (Some(a.clone()), "anchor_map"),
            (None, Some(s)) => (Some(s.clone()), "session_row"),
            (None, None) => (None, "none"),
        };
        // S3(a) directive 4: one content-free resolution line per selection — which sources spoke,
        // what each said, which won. `stale_affinity` flags the incident shape (both sources
        // present + disagreeing); `session` is already a sha256 key, ids are account ids — no
        // content. This line is what made the codex-lb incident diagnosable in minutes.
        tracing::info!(
            target: "continuity_owner_resolution",
            session = session_key.as_ref().map(|k| k.value.as_str()).unwrap_or("-"),
            anchor_present = anchor.is_some(),
            anchor_owner = anchor_owner.as_ref().map(|a| a.as_str()).unwrap_or("-"),
            session_owner = session_owner.as_ref().map(|a| a.as_str()).unwrap_or("-"),
            resolved = owner.as_ref().map(|a| a.as_str()).unwrap_or("-"),
            source,
            stale_affinity = matches!((&anchor_owner, &session_owner), (Some(a), Some(s)) if a != s),
            "owner resolution"
        );

        // Ensure a session row exists (Fresh on miss); mark reattaching when an anchor is in flight.
        // The anchored case does both in ONE UPSERT (`ensure_session_reattaching`) instead of
        // ensure-then-set_state — one fewer per-request write/fsync on the hot path.
        if let Some(sk) = session_key.as_ref() {
            if anchor.is_some() {
                self.repo
                    .ensure_session_reattaching(&sk.value, strength_str(sk.strength), now)
                    .await
                    .map_err(box_store_err)?;
            } else {
                self.repo
                    .ensure_session(&sk.value, strength_str(sk.strength), now)
                    .await
                    .map_err(box_store_err)?;
            }
        }

        // Arm the watchdog ONLY on anchor-bearing requests; pick the recovery strategy.
        let (watchdog, recovery) = if anchor.is_some() {
            let arm = WatchdogArm::Armed {
                timeout: self.watchdog_timeout,
            };
            if ctx.is_full_resend {
                // Build the anchor-stripped full-resend body. On the native pass-through the body is
                // NOT materialized (the wire bytes live in `raw_body`), so parse it from there — this
                // is the COLD recovery path, reached only when an armed owner turned out ineligible.
                // A translated request carries a materialized `body` instead. Either way we drop
                // `previous_response_id` and re-serialize (the executor serializes `body`).
                let base: Option<serde_json::Value> = match &req.raw_body {
                    Some(raw) => serde_json::from_slice(raw).ok(),
                    None => req.body.clone(),
                };
                let anchorless_req = match base {
                    Some(mut v) => {
                        if let Some(obj) = v.as_object_mut() {
                            obj.remove("previous_response_id");
                        }
                        PreparedRequest {
                            body: Some(v),
                            model: req.model.clone(),
                            forward_headers: req.forward_headers.clone(),
                            raw_body: None,
                        }
                    }
                    // Degenerate (a `raw_body` that somehow fails to re-parse): forward the original
                    // bytes rather than panicking on an empty body — the anchor stays, but the
                    // executor invariant (something to send) holds.
                    None => PreparedRequest {
                        body: None,
                        model: req.model.clone(),
                        forward_headers: req.forward_headers.clone(),
                        raw_body: req.raw_body.clone(),
                    },
                };
                (arm, RecoveryPlan::ResendFull { anchorless_req })
            } else {
                (arm, RecoveryPlan::SignalClient)
            }
        } else {
            (WatchdogArm::Disarmed, RecoveryPlan::None)
        };

        Ok(Prepared {
            req,
            directive: ContinuityDirective {
                pin_account: owner,
                watchdog,
                recovery,
                session_key,
                require_security_work_authorized,
            },
        })
    }

    async fn observe(
        &self,
        outcome: TurnOutcome,
        _ctx: &RequestCtx,
    ) -> Result<(), ContinuityError> {
        let now = now_secs();
        match outcome {
            TurnOutcome::Completed {
                session_key,
                account,
                response_id,
                input_fingerprint,
                input_count,
                ..
            } => {
                if let (Some(sk), Some(rid)) = (session_key, response_id) {
                    self.repo
                        .record_completion(
                            &sk.value,
                            strength_str(sk.strength),
                            account.as_str(),
                            &rid,
                            &input_fingerprint,
                            input_count as i64,
                            now,
                        )
                        .await
                        .map_err(box_store_err)?;
                }
                Ok(())
            }
            TurnOutcome::Recovered {
                session_key,
                account,
                new_response_id,
            } => {
                if let Some(sk) = session_key {
                    self.repo
                        .record_recovery(
                            &sk.value,
                            account.as_str(),
                            new_response_id.as_deref(),
                            now,
                        )
                        .await
                        .map_err(box_store_err)?;
                }
                Ok(())
            }
            TurnOutcome::Failed { .. } => Ok(()),
        }
    }

    async fn mark_required_capability(
        &self,
        session_key: &SessionKey,
        capability: &'static str,
    ) -> Result<(), ContinuityError> {
        let now = now_secs();
        self.repo
            .set_required_capability(&session_key.value, capability, now)
            .await
            .map_err(box_store_err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polyflare_store::Store;

    async fn make() -> (Store, CodexContinuity) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("s.db")).await.unwrap();
        std::mem::forget(dir);
        let cont = CodexContinuity::new(store.continuity(), Duration::from_millis(150));
        (store, cont)
    }

    /// A translated-style request: a materialized `body`, no raw pass-through.
    fn req(body: serde_json::Value) -> PreparedRequest {
        PreparedRequest {
            body: Some(body),
            model: "gpt-5.6-sol".to_string(),
            forward_headers: vec![],
            raw_body: None,
        }
    }

    /// A native-style request: the wire bytes in `raw_body`, no materialized `body` (mirrors the
    /// `/responses` pass-through). The recovery path must reparse the resend body from these bytes.
    fn native_req(body: serde_json::Value) -> PreparedRequest {
        PreparedRequest {
            body: None,
            model: "gpt-5.6-sol".to_string(),
            forward_headers: vec![],
            raw_body: Some(bytes::Bytes::from(serde_json::to_vec(&body).unwrap())),
        }
    }

    #[tokio::test]
    async fn no_anchor_disarms_and_does_not_pin() {
        let (_s, cont) = make().await;
        let ctx = RequestCtx {
            session_key: Some(polyflare_core::SessionKey {
                value: "sk".into(),
                strength: KeyStrength::Soft,
            }),
            ..Default::default()
        };
        let p = cont
            .prepare(req(serde_json::json!({"input": "hi"})), &ctx)
            .await
            .unwrap();
        assert!(p.directive.pin_account.is_none());
        assert!(matches!(p.directive.watchdog, WatchdogArm::Disarmed));
        assert!(matches!(p.directive.recovery, RecoveryPlan::None));
    }

    #[tokio::test]
    async fn anchor_full_resend_arms_with_resendfull_stripped() {
        let (_s, cont) = make().await;
        let ctx = RequestCtx {
            session_key: Some(polyflare_core::SessionKey {
                value: "sk".into(),
                strength: KeyStrength::Soft,
            }),
            client_previous_response_id: Some("resp_dead".into()),
            is_full_resend: true,
            ..Default::default()
        };
        let body =
            serde_json::json!({"previous_response_id": "resp_dead", "input": [{"a":1},{"b":2}]});
        let p = cont.prepare(req(body), &ctx).await.unwrap();
        assert!(matches!(p.directive.watchdog, WatchdogArm::Armed { .. }));
        match p.directive.recovery {
            RecoveryPlan::ResendFull { anchorless_req } => {
                let body = anchorless_req
                    .body
                    .as_ref()
                    .expect("resend body materialized");
                assert!(
                    body.get("previous_response_id").is_none(),
                    "anchor stripped"
                );
                assert!(body.get("input").is_some(), "full input preserved");
            }
            other => panic!("expected ResendFull, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn native_anchor_full_resend_reparses_stripped_body_from_raw() {
        // The native pass-through carries NO materialized `body` (only `raw_body`). The recovery
        // path must reparse the resend body FROM those wire bytes, strip the anchor, and preserve
        // the full input — proving the `Option<body>` split didn't drop the native recovery.
        let (_s, cont) = make().await;
        let ctx = RequestCtx {
            session_key: Some(polyflare_core::SessionKey {
                value: "sk".into(),
                strength: KeyStrength::Soft,
            }),
            client_previous_response_id: Some("resp_dead".into()),
            is_full_resend: true,
            ..Default::default()
        };
        let body =
            serde_json::json!({"previous_response_id": "resp_dead", "input": [{"a":1},{"b":2}]});
        let p = cont.prepare(native_req(body), &ctx).await.unwrap();
        match p.directive.recovery {
            RecoveryPlan::ResendFull { anchorless_req } => {
                let rebuilt = anchorless_req
                    .body
                    .as_ref()
                    .expect("resend body reparsed from raw_body");
                assert!(
                    rebuilt.get("previous_response_id").is_none(),
                    "anchor stripped from reparsed body"
                );
                assert_eq!(
                    rebuilt
                        .get("input")
                        .and_then(|i| i.as_array())
                        .map(|a| a.len()),
                    Some(2),
                    "full input preserved through the reparse"
                );
                assert!(
                    anchorless_req.raw_body.is_none(),
                    "resend forwards the stripped body, not the original bytes"
                );
            }
            other => panic!("expected ResendFull, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn anchor_bare_tail_arms_with_signalclient() {
        let (_s, cont) = make().await;
        let ctx = RequestCtx {
            session_key: Some(polyflare_core::SessionKey {
                value: "sk".into(),
                strength: KeyStrength::Soft,
            }),
            client_previous_response_id: Some("resp_x".into()),
            is_full_resend: false,
            ..Default::default()
        };
        let body = serde_json::json!({"previous_response_id": "resp_x", "input": "tail"});
        let p = cont.prepare(req(body), &ctx).await.unwrap();
        assert!(matches!(p.directive.watchdog, WatchdogArm::Armed { .. }));
        assert!(matches!(p.directive.recovery, RecoveryPlan::SignalClient));
    }

    #[tokio::test]
    async fn anchor_map_resolves_owner_for_pin() {
        let (store, cont) = make().await;
        // Seed an account + a completed turn so the anchor map knows resp_1 -> A.
        sqlx::query(
            "INSERT INTO accounts (id, email, access_token_enc, refresh_token_enc, id_token_enc, created_at) \
             VALUES ('A', 'e@x', X'00', X'00', X'00', 0)",
        )
        .execute(store.pool())
        .await
        .unwrap();
        store
            .continuity()
            .ensure_session("skA", "soft", 1)
            .await
            .unwrap();
        store
            .continuity()
            .record_completion("skA", "soft", "A", "resp_1", "fp", 2, 1)
            .await
            .unwrap();

        let ctx = RequestCtx {
            session_key: Some(polyflare_core::SessionKey {
                value: "skZ".into(),
                strength: KeyStrength::Soft,
            }),
            client_previous_response_id: Some("resp_1".into()),
            is_full_resend: true,
            ..Default::default()
        };
        let body =
            serde_json::json!({"previous_response_id": "resp_1", "input": [{"a":1},{"b":2}]});
        let p = cont.prepare(req(body), &ctx).await.unwrap();
        assert_eq!(
            p.directive.pin_account,
            Some(AccountId::from("A")),
            "anchor map pins to owner"
        );
    }

    #[tokio::test]
    async fn observe_completed_records_owner_and_anchor() {
        let (store, cont) = make().await;
        sqlx::query(
            "INSERT INTO accounts (id, email, access_token_enc, refresh_token_enc, id_token_enc, created_at) \
             VALUES ('A', 'e@x', X'00', X'00', X'00', 0)",
        )
        .execute(store.pool())
        .await
        .unwrap();
        let sk = polyflare_core::SessionKey {
            value: "skC".into(),
            strength: KeyStrength::Soft,
        };
        cont.repo.ensure_session("skC", "soft", 1).await.unwrap();
        cont.observe(
            TurnOutcome::Completed {
                session_key: Some(sk),
                account: AccountId::from("A"),
                response_id: Some("resp_7".into()),
                input_fingerprint: "fp".into(),
                input_count: 2,
                reasoning: None,
            },
            &RequestCtx::default(),
        )
        .await
        .unwrap();
        let owner = store.continuity().get_anchor_owner("resp_7").await.unwrap();
        assert_eq!(owner.as_deref(), Some("A"));
        let row = store
            .continuity()
            .get_session("skC")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.state, "anchored");
    }

    // ---- TA6(b) Task 3: sticky-cyber pre-filter ------------------------------------------------

    /// After `mark_required_capability` stamps a session (simulating a successful cyber move), a
    /// LATER `prepare` on that SAME session must set `require_security_work_authorized = true` for
    /// the turn — WITHOUT any second `cyber_policy` rejection ever occurring. This is the
    /// "cost-once" contract: the capability requirement is read off the session row, not
    /// re-discovered by hitting a rejection again.
    #[tokio::test]
    async fn sticky_session_prefilters_a_later_turn_without_a_second_rejection() {
        let (_store, cont) = make().await;
        let sk = polyflare_core::SessionKey {
            value: "sk-sticky".into(),
            strength: KeyStrength::Soft,
        };
        // Turn 1 equivalent: a session row exists (as `prepare` would have ensured).
        cont.repo
            .ensure_session("sk-sticky", "soft", 1)
            .await
            .unwrap();
        // The stamp `reroute_cyber_rejection` performs on a successful cyber move.
        cont.mark_required_capability(&sk, "security_work")
            .await
            .unwrap();

        // Turn 2: a fresh (unanchored) request on the SAME session.
        let ctx = RequestCtx {
            session_key: Some(sk),
            ..Default::default()
        };
        let p = cont
            .prepare(req(serde_json::json!({"input": "turn 2"})), &ctx)
            .await
            .unwrap();
        assert!(
            p.directive.require_security_work_authorized,
            "a sticky-cyber session pre-filters the very next turn, cost paid once"
        );
    }

    /// Regression: a session that never had a cyber move (never stamped) must NOT pre-filter —
    /// the capability requirement stays false, and non-cyber sessions are entirely unaffected.
    #[tokio::test]
    async fn non_cyber_session_is_never_capability_filtered() {
        let (_store, cont) = make().await;
        let sk = polyflare_core::SessionKey {
            value: "sk-plain".into(),
            strength: KeyStrength::Soft,
        };
        cont.repo
            .ensure_session("sk-plain", "soft", 1)
            .await
            .unwrap();

        let ctx = RequestCtx {
            session_key: Some(sk),
            ..Default::default()
        };
        let p = cont
            .prepare(req(serde_json::json!({"input": "turn 2"})), &ctx)
            .await
            .unwrap();
        assert!(
            !p.directive.require_security_work_authorized,
            "a session with no cyber history is never capability-filtered"
        );
    }

    /// A session that was never even `ensure_session`'d yet (a genuinely brand-new session key)
    /// also must not spuriously require the capability.
    #[tokio::test]
    async fn brand_new_session_is_never_capability_filtered() {
        let (_store, cont) = make().await;
        let ctx = RequestCtx {
            session_key: Some(polyflare_core::SessionKey {
                value: "sk-new".into(),
                strength: KeyStrength::Soft,
            }),
            ..Default::default()
        };
        let p = cont
            .prepare(req(serde_json::json!({"input": "turn 1"})), &ctx)
            .await
            .unwrap();
        assert!(!p.directive.require_security_work_authorized);
    }

    // ---- S3(a): affinity is never ownership (2026-07-22 codex-lb owner-conflict incident) ------

    /// The exact incident resolution state: the anchor map says the conversation is owned by A,
    /// but the session row's `owning_account_id` (the affinity hint) points at B — stale, because
    /// affinity rows can drift (in codex-lb: TTL-less sticky rows + capacity-routed first turns).
    /// The anchor map MUST win, silently: pin A, no error, the affinity hint structurally unable
    /// to veto. (codex-lb turned this same state into a terminal 503 loop.)
    #[tokio::test]
    async fn anchor_owner_beats_stale_session_owner_without_error() {
        let (store, cont) = make().await;
        for id in ["A", "B"] {
            sqlx::query(
                "INSERT INTO accounts (id, email, access_token_enc, refresh_token_enc, id_token_enc, created_at) \
                 VALUES (?, 'e@x', X'00', X'00', X'00', 0)",
            )
            .bind(id)
            .execute(store.pool())
            .await
            .unwrap();
        }
        // Turn 1 completed on A → anchor map resp_1→A, session row owner A.
        cont.repo.ensure_session("skS", "soft", 1).await.unwrap();
        cont.repo
            .record_completion("skS", "soft", "A", "resp_1", "fp", 2, 1)
            .await
            .unwrap();
        // Inject the stale-affinity drift: the session row now claims B.
        sqlx::query("UPDATE continuity_sessions SET owning_account_id = 'B' WHERE session_key = 'skS'")
            .execute(store.pool())
            .await
            .unwrap();

        let ctx = RequestCtx {
            session_key: Some(polyflare_core::SessionKey {
                value: "skS".into(),
                strength: KeyStrength::Soft,
            }),
            client_previous_response_id: Some("resp_1".into()),
            is_full_resend: true,
            ..Default::default()
        };
        let body =
            serde_json::json!({"previous_response_id": "resp_1", "input": [{"a":1},{"b":2}]});
        let p = cont.prepare(req(body), &ctx).await.unwrap();
        assert_eq!(
            p.directive.pin_account,
            Some(AccountId::from("A")),
            "the anchor map's owner wins over the stale session-row affinity — no conflict error"
        );
    }

    /// The stale affinity entry is CORRECTED, not honored: after the disagreeing turn completes on
    /// the true owner, the session row's `owning_account_id` reads the owner again.
    #[tokio::test]
    async fn completion_overwrites_stale_session_owner() {
        let (store, cont) = make().await;
        for id in ["A", "B"] {
            sqlx::query(
                "INSERT INTO accounts (id, email, access_token_enc, refresh_token_enc, id_token_enc, created_at) \
                 VALUES (?, 'e@x', X'00', X'00', X'00', 0)",
            )
            .bind(id)
            .execute(store.pool())
            .await
            .unwrap();
        }
        cont.repo.ensure_session("skT", "soft", 1).await.unwrap();
        cont.repo
            .record_completion("skT", "soft", "A", "resp_1", "fp", 2, 1)
            .await
            .unwrap();
        sqlx::query("UPDATE continuity_sessions SET owning_account_id = 'B' WHERE session_key = 'skT'")
            .execute(store.pool())
            .await
            .unwrap();

        // The next turn completes on the true owner A (as the anchor-map pin routed it).
        cont.observe(
            TurnOutcome::Completed {
                session_key: Some(polyflare_core::SessionKey {
                    value: "skT".into(),
                    strength: KeyStrength::Soft,
                }),
                account: AccountId::from("A"),
                response_id: Some("resp_2".into()),
                input_fingerprint: "fp".into(),
                input_count: 3,
                reasoning: None,
            },
            &RequestCtx::default(),
        )
        .await
        .unwrap();
        let row = store
            .continuity()
            .get_session("skT")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            row.owning_account_id.as_deref(),
            Some("A"),
            "the stale affinity entry is overwritten by the completion"
        );
    }

    #[tokio::test]
    async fn observe_recovered_rehomes_owner() {
        let (store, cont) = make().await;
        for id in ["A", "B"] {
            sqlx::query(
                "INSERT INTO accounts (id, email, access_token_enc, refresh_token_enc, id_token_enc, created_at) \
                 VALUES (?, 'e@x', X'00', X'00', X'00', 0)",
            )
            .bind(id)
            .execute(store.pool())
            .await
            .unwrap();
        }
        cont.repo.ensure_session("skR", "soft", 1).await.unwrap();
        cont.repo
            .record_completion("skR", "soft", "A", "resp_1", "fp", 2, 1)
            .await
            .unwrap();
        let sk = polyflare_core::SessionKey {
            value: "skR".into(),
            strength: KeyStrength::Soft,
        };
        cont.observe(
            TurnOutcome::Recovered {
                session_key: Some(sk),
                account: AccountId::from("B"),
                new_response_id: Some("resp_2".into()),
            },
            &RequestCtx::default(),
        )
        .await
        .unwrap();
        assert_eq!(
            store
                .continuity()
                .get_anchor_owner("resp_2")
                .await
                .unwrap()
                .as_deref(),
            Some("B")
        );
    }
}
