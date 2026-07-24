use std::sync::{Arc, LazyLock};

use reqwest::cookie::CookieStore;
use reqwest::cookie::Jar;
use reqwest::header::HeaderValue;

// WARNING: this store is process-global and shared by every Codex HTTP client built by this
// crate. It must only ever retain Cloudflare infrastructure cookies. ChatGPT account, session,
// authentication, and other user-specific cookies require owner-scoped storage and must never be
// added to this store.
static SHARED_CHATGPT_CLOUDFLARE_COOKIE_STORE: LazyLock<Arc<ChatGptCloudflareCookieStore>> =
    LazyLock::new(|| Arc::new(ChatGptCloudflareCookieStore::default()));

#[derive(Debug, Default)]
struct ChatGptCloudflareCookieStore {
    jar: Jar,
}

impl CookieStore for ChatGptCloudflareCookieStore {
    fn set_cookies(
        &self,
        cookie_headers: &mut dyn Iterator<Item = &HeaderValue>,
        url: &reqwest::Url,
    ) {
        if !is_chatgpt_cookie_url(url) {
            return;
        }

        let mut allowed =
            cookie_headers.filter(|header| is_allowed_cloudflare_set_cookie_header(header));
        self.jar.set_cookies(&mut allowed, url);
    }

    fn cookies(&self, url: &reqwest::Url) -> Option<HeaderValue> {
        if !is_chatgpt_cookie_url(url) {
            return None;
        }

        self.jar.cookies(url).and_then(only_cloudflare_cookies)
    }
}

/// Adds the process-local ChatGPT Cloudflare affinity-cookie store to a Codex HTTP client.
///
/// The store is intentionally global so clients reuse the same Cloudflare affinity state across
/// gateway routes. Its implementation rejects every cookie outside a fixed infrastructure
/// allowlist; it is not a ChatGPT session or authentication cookie jar.
pub(crate) fn with_chatgpt_cloudflare_cookie_store(
    builder: reqwest::ClientBuilder,
) -> reqwest::ClientBuilder {
    builder.cookie_provider(Arc::clone(&SHARED_CHATGPT_CLOUDFLARE_COOKIE_STORE))
}

fn is_chatgpt_cookie_url(url: &reqwest::Url) -> bool {
    if url.scheme() != "https" {
        return false;
    }

    url.host_str().is_some_and(is_allowed_chatgpt_host)
}

fn is_allowed_chatgpt_host(host: &str) -> bool {
    const EXACT_HOSTS: &[&str] = &["chatgpt.com", "chat.openai.com", "chatgpt-staging.com"];
    const SUBDOMAIN_SUFFIXES: &[&str] = &[".chatgpt.com", ".chatgpt-staging.com"];

    EXACT_HOSTS.contains(&host)
        || SUBDOMAIN_SUFFIXES
            .iter()
            .any(|suffix| host.ends_with(suffix))
}

fn is_allowed_cloudflare_set_cookie_header(header: &HeaderValue) -> bool {
    header
        .to_str()
        .ok()
        .and_then(set_cookie_name)
        .is_some_and(is_allowed_cloudflare_cookie_name)
}

fn set_cookie_name(header: &str) -> Option<&str> {
    let (name, _) = header.split_once('=')?;
    let name = name.trim();
    (!name.is_empty()).then_some(name)
}

fn only_cloudflare_cookies(header: HeaderValue) -> Option<HeaderValue> {
    let header = header.to_str().ok()?;
    let cookies = header
        .split(';')
        .filter_map(|cookie| {
            let cookie = cookie.trim();
            let name = cookie.split_once('=')?.0.trim();
            is_allowed_cloudflare_cookie_name(name).then_some(cookie)
        })
        .collect::<Vec<_>>()
        .join("; ");

    if cookies.is_empty() {
        None
    } else {
        HeaderValue::from_str(&cookies).ok()
    }
}

fn is_allowed_cloudflare_cookie_name(name: &str) -> bool {
    // Cloudflare's documented service-cookie names:
    // https://developers.cloudflare.com/fundamentals/reference/policies-compliances/cloudflare-cookies/
    matches!(
        name,
        "__cf_bm"
            | "__cflb"
            | "__cfruid"
            | "__cfseq"
            | "__cfwaitingroom"
            | "_cfuvid"
            | "cf_clearance"
            | "cf_ob_info"
            | "cf_use_ob"
    ) || name.starts_with("cf_chl_")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retains_only_cloudflare_affinity_cookies_for_https_chatgpt_hosts() {
        let store = ChatGptCloudflareCookieStore::default();
        let url = reqwest::Url::parse("https://chatgpt.com/backend-api/ps/mcp").expect("valid URL");
        let load_balancer = HeaderValue::from_static("__cflb=west; Path=/; Secure; HttpOnly");
        let visitor = HeaderValue::from_static("_cfuvid=visitor; Path=/; Secure; HttpOnly");
        let session =
            HeaderValue::from_static("__Secure-next-auth.session-token=secret; Path=/; Secure");

        store.set_cookies(&mut [&load_balancer, &visitor, &session].into_iter(), &url);

        let mut cookies = store
            .cookies(&url)
            .and_then(|value| value.to_str().ok().map(str::to_owned))
            .expect("allowed affinity cookies should be replayed")
            .split("; ")
            .map(str::to_owned)
            .collect::<Vec<_>>();
        cookies.sort();

        assert_eq!(
            cookies,
            vec!["__cflb=west".to_owned(), "_cfuvid=visitor".to_owned()]
        );
    }

    #[test]
    fn rejects_non_https_and_non_chatgpt_cookie_urls() {
        for url in [
            "http://chatgpt.com/backend-api/ps/mcp",
            "wss://chatgpt.com/backend-api/ps/mcp",
            "https://api.openai.com/v1/responses",
            "https://chatgpt.com.evil.example/backend-api/ps/mcp",
            "https://evilchatgpt.com/backend-api/ps/mcp",
        ] {
            assert!(!is_chatgpt_cookie_url(
                &reqwest::Url::parse(url).expect("valid URL")
            ));
        }
    }

    #[test]
    fn never_replays_chatgpt_cookies_to_an_untrusted_url() {
        let store = ChatGptCloudflareCookieStore::default();
        let chatgpt =
            reqwest::Url::parse("https://chatgpt.com/backend-api/ps/mcp").expect("valid URL");
        let visitor = HeaderValue::from_static("_cfuvid=visitor; Path=/; Secure; HttpOnly");
        store.set_cookies(&mut std::iter::once(&visitor), &chatgpt);

        for url in [
            "http://chatgpt.com/backend-api/ps/mcp",
            "https://api.openai.com/v1/responses",
            "https://chatgpt.com.evil.example/backend-api/ps/mcp",
        ] {
            assert_eq!(
                store.cookies(&reqwest::Url::parse(url).expect("valid URL")),
                None
            );
        }
    }

    #[test]
    fn recognizes_only_the_cloudflare_service_cookie_allowlist() {
        for name in [
            "__cf_bm",
            "__cflb",
            "__cfruid",
            "__cfseq",
            "__cfwaitingroom",
            "_cfuvid",
            "cf_clearance",
            "cf_ob_info",
            "cf_use_ob",
            "cf_chl_rc_i",
        ] {
            assert!(is_allowed_cloudflare_cookie_name(name), "{name}");
        }

        for name in [
            "__Secure-next-auth.session-token",
            "chatgpt_session",
            "oai-auth-token",
            "not_cf_clearance",
        ] {
            assert!(!is_allowed_cloudflare_cookie_name(name), "{name}");
        }
    }
}
