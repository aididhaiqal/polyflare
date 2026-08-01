//! Surfacing an upstream that accepts a priority request and serves it as standard.
//!
//! The distinction that matters: PolyFlare's OWN priority policy downgrading a turn is not the
//! upstream declining it, and must not be reported as one.

mod support;

use polyflare_store::RequestLogRecord;

fn record(request_id: &str, requested: Option<&str>, actual: Option<&str>) -> RequestLogRecord {
    RequestLogRecord {
        requested_at: 100,
        provider: "codex".into(),
        method: "POST".into(),
        path: "/responses".into(),
        aliased: false,
        status: 200,
        duration_ms: 10,
        account_id: Some("acct-1".into()),
        target_kind: Some("account".into()),
        provider_credential_id: None,
        model: Some("gpt-5.6-sol".into()),
        upstream_model: None,
        upstream_transport: None,
        profile_revision: None,
        reasoning_effort: None,
        service_tier: actual.map(str::to_string),
        requested_service_tier: requested.map(str::to_string),
        actual_service_tier: actual.map(str::to_string),
        transport: Some("sse".into()),
        ttft_ms: None,
        total_tokens: None,
        cached_tokens: None,
        subagent: None,
        request_id: Some(request_id.into()),
        session_key: None,
        input_tokens: None,
        output_tokens: None,
        cached_input_tokens: None,
        reasoning_tokens: None,
        orchestration_input_tokens: None,
        orchestration_output_tokens: None,
        orchestration_cached_input_tokens: None,
        cost_usd: None,
        latency_first_token_ms: None,
        protocol_outcome: None,
        error_code: None,
    }
}

#[tokio::test]
async fn a_declined_priority_request_is_flagged_and_nothing_else_is() {
    let (pf, app_state) = support::spawn("http://127.0.0.1:9".to_string()).await;
    let repo = app_state.store.request_log();

    for (id, requested, actual) in [
        // The case being surfaced: asked priority, served standard.
        ("declined", Some("priority"), Some("standard")),
        ("declined-fast", Some("fast"), Some("standard")),
        // Honoured — not a downgrade.
        ("honoured", Some("priority"), Some("priority")),
        ("honoured-fast", Some("priority"), Some("fast")),
        // Never asked for priority.
        ("plain", Some("standard"), Some("standard")),
        // PolyFlare's own policy downgraded it, so priority was never asked of the upstream.
        ("policy-downgraded", Some("standard"), Some("standard")),
        // Upstream reported no tier: unknown must not read as declined.
        ("unknown-tier", Some("priority"), None),
    ] {
        repo.insert(&record(id, requested, actual)).await.unwrap();
    }

    let body: serde_json::Value = reqwest::Client::new()
        .get(format!("{pf}/api/requests?limit=50"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let flagged: std::collections::HashMap<String, bool> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|row| {
            Some((
                row["request_id"].as_str()?.to_string(),
                row["priority_downgraded"].as_bool()?,
            ))
        })
        .collect();

    assert_eq!(flagged.get("declined"), Some(&true));
    assert_eq!(flagged.get("declined-fast"), Some(&true));
    assert_eq!(flagged.get("honoured"), Some(&false));
    assert_eq!(
        flagged.get("honoured-fast"),
        Some(&false),
        "fast is a priority tier, so priority->fast is honoured"
    );
    assert_eq!(flagged.get("plain"), Some(&false));
    assert_eq!(
        flagged.get("policy-downgraded"),
        Some(&false),
        "PolyFlare's own downgrade must not be reported as an upstream refusal"
    );
    assert_eq!(
        flagged.get("unknown-tier"),
        Some(&false),
        "an unreported tier is unknown, not a downgrade"
    );
}
