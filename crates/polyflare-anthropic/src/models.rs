//! The Anthropic model catalog (`GET /v1/models`).
//!
//! Returns model IDs only. The dashboard uses them to suggest translation targets, so nothing here
//! needs the display name, creation date, or any other field the endpoint carries — and taking less
//! than is offered keeps this insensitive to shape changes in the parts we do not read.

use std::time::Duration;

use serde::Deserialize;

/// Pages to follow before giving up. The catalog is on the order of tens of models, so this is a
/// runaway guard rather than a real limit: a server that always reports `has_more` cannot spin us.
const MAX_PAGES: usize = 10;
/// Page size the endpoint accepts. The maximum, to make one page the normal case.
const PAGE_LIMIT: u32 = 1000;

#[derive(Debug, thiserror::Error)]
pub enum ModelsError {
    #[error("model catalog transport error: {0}")]
    Transport(String),
    #[error("model catalog endpoint returned status {0}")]
    Status(u16),
    #[error("model catalog response was malformed: {0}")]
    Malformed(String),
}

#[derive(Deserialize)]
struct ModelsPage {
    data: Vec<ModelEntry>,
    #[serde(default)]
    has_more: bool,
    #[serde(default)]
    last_id: Option<String>,
}

#[derive(Deserialize)]
struct ModelEntry {
    id: String,
}

/// Fetch every model ID the account can address, in the order the upstream lists them.
///
/// `bearer` is the selected account's access token; an API-key account passes its key the same way,
/// since this endpoint accepts either as a bearer. Ordering is preserved rather than sorted:
/// upstream lists newest first, which is the order an operator wants to see in a suggestion list.
pub async fn fetch_model_ids(
    client: &reqwest::Client,
    base_url: &str,
    bearer: &str,
) -> Result<Vec<String>, ModelsError> {
    let base = base_url.trim_end_matches('/');
    let mut ids = Vec::new();
    let mut after: Option<String> = None;

    for _ in 0..MAX_PAGES {
        let mut url = format!("{base}/v1/models?limit={PAGE_LIMIT}");
        if let Some(cursor) = after.as_deref() {
            url.push_str(&format!("&after_id={cursor}"));
        }
        let response = client
            .get(&url)
            .bearer_auth(bearer)
            .header("anthropic-version", crate::claude_wire::ANTHROPIC_VERSION)
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| ModelsError::Transport(e.to_string()))?;

        if !response.status().is_success() {
            return Err(ModelsError::Status(response.status().as_u16()));
        }
        let page: ModelsPage = response
            .json()
            .await
            .map_err(|e| ModelsError::Malformed(e.to_string()))?;

        ids.extend(page.data.into_iter().map(|entry| entry.id));

        // A `has_more` with no cursor cannot be followed; stop with what we have rather than
        // refetching page one forever.
        match (page.has_more, page.last_id) {
            (true, Some(cursor)) => after = Some(cursor),
            _ => break,
        }
    }

    Ok(ids)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn collects_ids_across_pages_and_sends_the_version_header() {
        let mock = polyflare_testkit::MockAnthropicModels::paged(vec![
            vec![
                "claude-opus-4-5".to_string(),
                "claude-sonnet-4-6".to_string(),
            ],
            vec!["claude-haiku-4-5".to_string()],
        ]);
        let handle = mock.clone();
        let base = mock.spawn().await;

        let ids = fetch_model_ids(&reqwest::Client::new(), &base, "token-abc")
            .await
            .unwrap();

        assert_eq!(
            ids,
            vec!["claude-opus-4-5", "claude-sonnet-4-6", "claude-haiku-4-5"],
            "upstream order is preserved across pages"
        );
        assert_eq!(handle.request_count(), 2, "the second page was followed");
        assert_eq!(
            handle.last_authorization().as_deref(),
            Some("Bearer token-abc")
        );
        assert_eq!(
            handle.last_version_header().as_deref(),
            Some(crate::claude_wire::ANTHROPIC_VERSION)
        );
    }

    #[tokio::test]
    async fn a_single_page_makes_exactly_one_request() {
        let mock = polyflare_testkit::MockAnthropicModels::paged(vec![vec![
            "claude-sonnet-4-6".to_string()
        ]]);
        let handle = mock.clone();
        let base = mock.spawn().await;

        let ids = fetch_model_ids(&reqwest::Client::new(), &base, "t")
            .await
            .unwrap();
        assert_eq!(ids, vec!["claude-sonnet-4-6"]);
        assert_eq!(handle.request_count(), 1);
    }

    #[tokio::test]
    async fn a_truncated_page_stops_instead_of_looping() {
        // `has_more` with no cursor: following it would refetch page one forever.
        let mock = polyflare_testkit::MockAnthropicModels::has_more_without_cursor(vec![
            "claude-sonnet-4-6".to_string(),
        ]);
        let handle = mock.clone();
        let base = mock.spawn().await;

        let ids = fetch_model_ids(&reqwest::Client::new(), &base, "t")
            .await
            .unwrap();
        assert_eq!(ids, vec!["claude-sonnet-4-6"]);
        assert_eq!(handle.request_count(), 1, "must not spin");
    }

    #[tokio::test]
    async fn an_error_status_is_reported_rather_than_returning_an_empty_catalog() {
        let mock = polyflare_testkit::MockAnthropicModels::error(401);
        let base = mock.spawn().await;

        // Silently returning `[]` would look like "this account has no models" and quietly empty
        // the operator's suggestion list.
        let error = fetch_model_ids(&reqwest::Client::new(), &base, "bad")
            .await
            .unwrap_err();
        assert!(matches!(error, ModelsError::Status(401)), "got {error:?}");
    }
}
