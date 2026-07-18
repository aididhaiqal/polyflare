use futures_util::StreamExt;
use polyflare_codex::executor::CodexExecutor;
use polyflare_core::{Account, Executor, PreparedRequest, RequestCtx};
use polyflare_testkit::MockUpstream;

#[tokio::test]
async fn executor_streams_upstream_events_and_forwards_body() {
    let mock = MockUpstream::new(vec![
        r#"{"type":"response.output_text.delta","delta":"hi"}"#.to_string(),
        r#"{"type":"response.completed"}"#.to_string(),
    ]);
    let handle = mock.clone();
    let base = mock.spawn().await;

    let executor = CodexExecutor::new().unwrap();
    let account = Account {
        id: "test".into(),
        base_url: base,
        bearer_token: "test-token".into(),
        chatgpt_account_id: None,
    };
    let req = PreparedRequest {
        body: Some(serde_json::json!({"model": "gpt-5.6-sol", "input": "hello"})),
        model: "gpt-5.6-sol".into(),
        forward_headers: vec![],
        raw_body: None,
    };

    let mut stream = executor
        .execute(req, &account, &RequestCtx::default())
        .await
        .unwrap();
    let mut collected = String::new();
    while let Some(chunk) = stream.next().await {
        collected.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
    }

    assert!(collected.contains("response.output_text.delta"));
    assert!(collected.contains("response.completed"));
    assert_eq!(handle.last_body().unwrap()["model"], "gpt-5.6-sol");
}

#[tokio::test]
async fn raw_body_is_forwarded_verbatim_with_exactly_one_content_type() {
    // Native pass-through: `raw_body` is sent as-is, and a native client's OWN forwarded
    // `content-type` must be preserved WITHOUT being duplicated (the executor must not append a
    // second one on top of the forwarded header).
    let mock = MockUpstream::new(vec![r#"{"type":"response.completed"}"#.to_string()]);
    let handle = mock.clone();
    let base = mock.spawn().await;

    let executor = CodexExecutor::new().unwrap();
    let account = Account {
        id: "test".into(),
        base_url: base,
        bearer_token: "test-token".into(),
        chatgpt_account_id: None,
    };
    // Bytes a real client sent — note the deliberate key order / spacing that a re-serialize would
    // NOT reproduce, proving verbatim forwarding.
    let raw = br#"{"model":"gpt-5.6-sol","input":"hi","extra_field":true}"#.to_vec();
    let req = PreparedRequest {
        // Native pass-through: no materialized body — the wire bytes in `raw_body` are forwarded.
        body: None,
        model: "gpt-5.6-sol".into(),
        // A native client always sends its own content-type; it is forwarded (not in the drop-list).
        forward_headers: vec![("content-type".to_string(), "application/json".to_string())],
        raw_body: Some(bytes::Bytes::from(raw.clone())),
    };

    let mut stream = executor
        .execute(req, &account, &RequestCtx::default())
        .await
        .unwrap();
    while stream.next().await.is_some() {}

    // Body reached upstream (content preserved).
    assert_eq!(handle.last_body().unwrap()["extra_field"], true);
    // EXACTLY ONE content-type on the wire — not the duplicate the append bug produced.
    let headers = handle.last_headers().unwrap();
    assert_eq!(
        headers.get_all("content-type").iter().count(),
        1,
        "native raw path must send exactly one content-type, got {:?}",
        headers.get_all("content-type").iter().collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn executor_sends_selected_account_chatgpt_account_id_overriding_forwarded() {
    // The real Codex CLI pairs `ChatGPT-Account-ID` with the Bearer. PolyFlare swaps the Bearer to
    // the SELECTED account, so it must also send THAT account's id — never a stale forwarded one.
    let mock = MockUpstream::new(vec![r#"{"type":"response.completed"}"#.to_string()]);
    let handle = mock.clone();
    let base = mock.spawn().await;

    let executor = CodexExecutor::new().unwrap();
    let account = Account {
        id: "test".into(),
        base_url: base,
        bearer_token: "test-token".into(),
        chatgpt_account_id: Some("acct-selected".into()),
    };
    let req = PreparedRequest {
        body: Some(serde_json::json!({"model": "gpt-5.6-sol", "input": "hi"})),
        model: "gpt-5.6-sol".into(),
        // A client forwarded a DIFFERENT account's id — it must be replaced, not shipped alongside
        // our overridden Bearer (a mismatched (token, account) pair is what the backend rejects).
        forward_headers: vec![(
            "chatgpt-account-id".to_string(),
            "client-stale-acct".to_string(),
        )],
        raw_body: None,
    };

    let mut stream = executor
        .execute(req, &account, &RequestCtx::default())
        .await
        .unwrap();
    while stream.next().await.is_some() {}

    let headers = handle.last_headers().unwrap();
    assert_eq!(
        headers
            .get("chatgpt-account-id")
            .and_then(|v| v.to_str().ok()),
        Some("acct-selected"),
        "must send the SELECTED account's id, overriding any forwarded client value"
    );
}

#[tokio::test]
async fn executor_surfaces_upstream_error_status() {
    // No route for this path on the mock → 404 → ExecError::Upstream.
    let base = MockUpstream::new(vec![]).spawn().await;
    let executor = CodexExecutor::new().unwrap();
    let account = Account {
        id: "test".into(),
        base_url: format!("{base}/nonexistent-base"),
        bearer_token: "t".into(),
        chatgpt_account_id: None,
    };
    let req = PreparedRequest {
        body: Some(serde_json::json!({"model": "m"})),
        model: "m".into(),
        forward_headers: vec![],
        raw_body: None,
    };
    let err = executor
        .execute(req, &account, &RequestCtx::default())
        .await
        .err()
        .unwrap();
    // A non-2xx (404 here) now surfaces the structured status for routing-health classification.
    assert!(
        matches!(&err, polyflare_core::ExecError::UpstreamStatus(s) if s.status == 404),
        "expected UpstreamStatus(404), got {err:?}"
    );
}

async fn run_error_status(status: u16, body: &str) -> polyflare_core::ExecError {
    let base = MockUpstream::error_status(status, body).spawn().await;
    let executor = CodexExecutor::new().unwrap();
    let account = Account {
        id: "test".into(),
        base_url: base,
        bearer_token: "t".into(),
        chatgpt_account_id: None,
    };
    let req = PreparedRequest {
        body: Some(serde_json::json!({"model": "m"})),
        model: "m".into(),
        forward_headers: vec![],
        raw_body: None,
    };
    executor
        .execute(req, &account, &RequestCtx::default())
        .await
        .err()
        .unwrap()
}

#[tokio::test]
async fn executor_extracts_error_code_from_openai_shape_without_leaking_message() {
    let err = run_error_status(
        403,
        r#"{"error":{"code":"account_deactivated","message":"secret detail"}}"#,
    )
    .await;

    match &err {
        polyflare_core::ExecError::UpstreamStatus(s) => {
            assert_eq!(s.status, 403);
            assert_eq!(s.error_code.as_deref(), Some("account_deactivated"));
        }
        other => panic!("expected UpstreamStatus, got {other:?}"),
    }

    // Content-safety: the message text must never surface via Display or Debug.
    let display = format!("{err}");
    let debug = format!("{err:?}");
    assert!(
        !display.contains("secret detail"),
        "Display leaked the error message: {display}"
    );
    assert!(
        !debug.contains("secret detail"),
        "Debug leaked the error message: {debug}"
    );
}

#[tokio::test]
async fn executor_does_not_scrape_a_code_out_of_prose_detail() {
    let err = run_error_status(429, r#"{"detail":"you have been rate limited, try later"}"#).await;
    match &err {
        polyflare_core::ExecError::UpstreamStatus(s) => {
            assert_eq!(s.status, 429);
            assert_eq!(s.error_code, None);
        }
        other => panic!("expected UpstreamStatus, got {other:?}"),
    }
}

#[tokio::test]
async fn executor_tolerates_malformed_json_error_body() {
    let err = run_error_status(500, "not json at all {{{").await;
    match &err {
        polyflare_core::ExecError::UpstreamStatus(s) => {
            assert_eq!(s.status, 500);
            assert_eq!(s.error_code, None);
        }
        other => panic!("expected UpstreamStatus, got {other:?}"),
    }
}

#[tokio::test]
async fn executor_bounds_the_error_body_read_on_an_oversized_body() {
    // 256 KiB of filler, far past the 64 KiB cap — must not hang or OOM; a best-effort result
    // (status still correct) is all that's required.
    let huge_body = format!(r#"{{"padding":"{}"}}"#, "x".repeat(256 * 1024));
    let err = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        run_error_status(502, &huge_body),
    )
    .await
    .expect("bounded read must not hang on an oversized error body");

    match &err {
        polyflare_core::ExecError::UpstreamStatus(s) => {
            assert_eq!(s.status, 502);
            // Truncated JSON has no parseable code — best-effort None, not a panic.
            assert_eq!(s.error_code, None);
        }
        other => panic!("expected UpstreamStatus, got {other:?}"),
    }
}
