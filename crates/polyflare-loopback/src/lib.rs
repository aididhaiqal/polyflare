use axum::{
    body::Body,
    extract::{
        ws::CloseFrame as DownstreamCloseFrame, ws::Message as DownstreamMessage, ws::WebSocket,
        ws::WebSocketUpgrade, FromRequestParts, State,
    },
    http::{header, HeaderMap, HeaderName, Request, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use futures_util::{SinkExt, StreamExt};
use reqwest::{redirect::Policy, Url};
use std::{
    collections::HashSet,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio_tungstenite::{
    tungstenite::{
        client::IntoClientRequest, protocol::frame::coding::CloseCode,
        protocol::CloseFrame as UpstreamCloseFrame, Message as UpstreamMessage,
    },
    MaybeTlsStream, WebSocketStream,
};

pub const HEALTH_PATH: &str = "/_polyflare-loopback/health";
// PolyFlare may spend its 60s recovery budget and then up to 30s waiting for response headers.
// Keep this outer transport bound above that contract so the companion stays transparent.
const UPSTREAM_HEADER_TIMEOUT: Duration = Duration::from_secs(120);
const UPSTREAM_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
// Remote PolyFlare itself allows 30s for its upstream remote-control handshake. The companion's
// outer bound must be longer or it can manufacture a premature 502 while PolyFlare is still validly
// waiting. The extra margin covers tailnet transport latency.
const WS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(45);

type UpstreamWebSocket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

#[derive(Clone, Debug)]
pub struct Config {
    listen: SocketAddr,
    upstream: Url,
}

impl Config {
    pub fn try_new(listen: SocketAddr, upstream: Url) -> Result<Self, ConfigError> {
        if !listen.ip().is_loopback() {
            return Err(ConfigError::NonLoopbackListen);
        }
        if upstream.scheme() != "https" {
            return Err(ConfigError::UpstreamMustUseHttps);
        }
        validate_remote_origin(&upstream)?;
        Ok(Self { listen, upstream })
    }

    pub fn listen(&self) -> SocketAddr {
        self.listen
    }

    pub fn upstream_origin(&self) -> &Url {
        &self.upstream
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("listen address must be loopback")]
    NonLoopbackListen,
    #[error("remote upstream must use HTTPS")]
    UpstreamMustUseHttps,
    #[error("upstream must be a remote origin without a path, query, fragment, or credentials")]
    InvalidRemoteOrigin,
    #[error("upstream must not resolve to a loopback hostname or address")]
    LoopbackUpstream,
}

fn validate_remote_origin(upstream: &Url) -> Result<(), ConfigError> {
    if upstream.host().is_none()
        || !upstream.username().is_empty()
        || upstream.password().is_some()
        || !matches!(upstream.path(), "" | "/")
        || upstream.query().is_some()
        || upstream.fragment().is_some()
    {
        return Err(ConfigError::InvalidRemoteOrigin);
    }
    let host = upstream
        .host_str()
        .expect("host checked above")
        .trim_matches(['[', ']'])
        .trim_end_matches('.')
        .to_ascii_lowercase();
    let is_loopback_name = host == "localhost" || host.ends_with(".localhost");
    let is_forbidden_ip = host
        .parse::<std::net::IpAddr>()
        .is_ok_and(forbidden_upstream_ip);
    if is_loopback_name || is_forbidden_ip {
        return Err(ConfigError::LoopbackUpstream);
    }
    Ok(())
}

fn forbidden_upstream_ip(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => {
            address.is_loopback()
                || address.is_unspecified()
                || address.is_multicast()
                || address.is_link_local()
                || address.is_broadcast()
        }
        IpAddr::V6(address) => {
            address.is_loopback()
                || address.is_unspecified()
                || address.is_multicast()
                || address.is_unicast_link_local()
                || address
                    .to_ipv4_mapped()
                    .is_some_and(|mapped| forbidden_upstream_ip(IpAddr::V4(mapped)))
        }
    }
}

async fn resolve_upstream(upstream: &Url) -> Result<Vec<SocketAddr>, PrepareError> {
    let host = upstream.host_str().ok_or(PrepareError::NoAddresses)?;
    let port = upstream
        .port_or_known_default()
        .ok_or(PrepareError::NoAddresses)?;
    let mut addresses: Vec<SocketAddr> = tokio::net::lookup_host((host, port))
        .await
        .map_err(PrepareError::Resolve)?
        .collect();
    addresses.sort_unstable();
    addresses.dedup();
    if addresses.is_empty() {
        return Err(PrepareError::NoAddresses);
    }
    // Reject the complete DNS answer if any candidate is local. Pinning only the apparently safe
    // subset would make split-horizon mistakes hard to notice and could change behavior by host.
    if addresses
        .iter()
        .any(|address| forbidden_upstream_ip(address.ip()))
    {
        return Err(PrepareError::UnsafeAddress);
    }
    Ok(addresses)
}

#[derive(Clone)]
struct ProxyConfig {
    upstream: Url,
    upstream_addresses: Vec<SocketAddr>,
    http: reqwest::Client,
}

impl ProxyConfig {
    async fn from_config(config: &Config) -> Result<Self, PrepareError> {
        let upstream_addresses = resolve_upstream(&config.upstream).await?;
        let host = config
            .upstream
            .host_str()
            .ok_or(PrepareError::NoAddresses)?;
        Ok(Self {
            upstream: config.upstream.clone(),
            upstream_addresses: upstream_addresses.clone(),
            http: reqwest::Client::builder()
                .redirect(Policy::none())
                .no_proxy()
                .connect_timeout(UPSTREAM_CONNECT_TIMEOUT)
                .resolve_to_addrs(host, &upstream_addresses)
                .build()
                .map_err(PrepareError::Client)?,
        })
    }

    #[cfg(test)]
    fn target(&self, path_and_query: &str) -> Result<Url, ()> {
        let uri: Uri = path_and_query.parse().map_err(|_| ())?;
        self.target_uri(&uri, false)
    }

    fn target_uri(&self, uri: &Uri, websocket: bool) -> Result<Url, ()> {
        let mut target = self.upstream.clone();
        target.set_path(uri.path());
        target.set_query(uri.query());
        if websocket {
            let scheme = match target.scheme() {
                "https" => "wss",
                #[cfg(test)]
                "http" => "ws",
                _ => return Err(()),
            };
            target.set_scheme(scheme).map_err(|_| ())?;
        }
        Ok(target)
    }
}

#[cfg(test)]
struct TestConfig(ProxyConfig);

#[cfg(test)]
impl TestConfig {
    fn new(origin: &str) -> Self {
        Self(ProxyConfig {
            upstream: origin.parse().unwrap(),
            upstream_addresses: vec![
                origin
                    .parse::<Url>()
                    .unwrap()
                    .socket_addrs(|| None)
                    .unwrap()[0],
            ],
            http: reqwest::Client::builder()
                .redirect(Policy::none())
                .no_proxy()
                .build()
                .unwrap(),
        })
    }

    fn target(&self, path_and_query: &str) -> Result<Url, ()> {
        self.0.target(path_and_query)
    }
}

#[cfg(test)]
impl From<TestConfig> for ProxyConfig {
    fn from(value: TestConfig) -> Self {
        value.0
    }
}

const HOP_BY_HOP: &[&str] = &[
    "connection",
    "content-length",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

fn end_to_end_headers(headers: &HeaderMap) -> HeaderMap {
    let mut forwarded = headers.clone();
    let connection_named: HashSet<HeaderName> = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|name| name.trim().parse().ok())
        .collect();
    for name in HOP_BY_HOP {
        forwarded.remove(*name);
    }
    for name in connection_named {
        forwarded.remove(name);
    }
    forwarded.remove(header::HOST);
    forwarded
}

fn websocket_headers(headers: &HeaderMap) -> HeaderMap {
    let mut forwarded = end_to_end_headers(headers);
    for controlled in [
        "sec-websocket-key",
        "sec-websocket-version",
        "sec-websocket-extensions",
    ] {
        forwarded.remove(controlled);
    }
    forwarded
}

fn error_response(status: StatusCode, code: &'static str) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        format!("{{\"error\":\"{code}\"}}"),
    )
        .into_response()
}

async fn health() -> impl IntoResponse {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        "{\"status\":\"ok\",\"mode\":\"remote-polyflare-loopback\"}",
    )
}

async fn proxy_handler(State(state): State<Arc<ProxyConfig>>, request: Request<Body>) -> Response {
    let (mut parts, body) = request.into_parts();
    if let Ok(ws) = WebSocketUpgrade::from_request_parts(&mut parts, &state).await {
        return proxy_websocket(state, ws, Request::from_parts(parts, body)).await;
    }
    proxy_http(state, Request::from_parts(parts, body)).await
}

async fn proxy_http(state: Arc<ProxyConfig>, request: Request<Body>) -> Response {
    let target = match state.target_uri(request.uri(), false) {
        Ok(target) => target,
        Err(_) => return error_response(StatusCode::BAD_REQUEST, "invalid_target"),
    };
    let (parts, body) = request.into_parts();
    let method = parts.method.clone();
    let upstream = tokio::time::timeout(
        UPSTREAM_HEADER_TIMEOUT,
        state
            .http
            .request(parts.method, target)
            .headers(end_to_end_headers(&parts.headers))
            .body(reqwest::Body::wrap_stream(body.into_data_stream()))
            .send(),
    )
    .await;
    let upstream = match upstream {
        Ok(Ok(response)) => response,
        Ok(Err(_)) | Err(_) => {
            tracing::warn!(method = %method, "upstream HTTP request failed");
            return error_response(StatusCode::BAD_GATEWAY, "upstream_unavailable");
        }
    };
    let status = upstream.status();
    if status.is_redirection() {
        tracing::warn!(method = %method, status = %status, "upstream redirect rejected");
        return error_response(StatusCode::BAD_GATEWAY, "upstream_redirect_rejected");
    }
    let headers = end_to_end_headers(upstream.headers());
    let body = Body::from_stream(
        upstream
            .bytes_stream()
            .map(|chunk| chunk.map_err(std::io::Error::other)),
    );
    let mut response = Response::new(body);
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    response
}

async fn proxy_websocket(
    state: Arc<ProxyConfig>,
    ws: WebSocketUpgrade,
    request: Request<Body>,
) -> Response {
    let target = match state.target_uri(request.uri(), true) {
        Ok(target) => target,
        Err(_) => return error_response(StatusCode::BAD_REQUEST, "invalid_target"),
    };
    let mut upstream_request = match target.as_str().into_client_request() {
        Ok(request) => request,
        Err(_) => return error_response(StatusCode::BAD_REQUEST, "invalid_target"),
    };
    for (name, value) in websocket_headers(request.headers()) {
        if let Some(name) = name {
            upstream_request.headers_mut().append(name, value);
        }
    }
    let connected = tokio::time::timeout(
        WS_HANDSHAKE_TIMEOUT,
        connect_websocket(&state, upstream_request),
    )
    .await;
    let (upstream, handshake) = match connected {
        Ok(Ok(connected)) => connected,
        Ok(Err(_)) | Err(_) => {
            tracing::warn!("upstream WebSocket handshake failed");
            return error_response(StatusCode::BAD_GATEWAY, "upstream_unavailable");
        }
    };
    let accepted_protocol = handshake
        .headers()
        .get(header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let ws = match accepted_protocol {
        Some(protocol) => ws.protocols([protocol]),
        None => ws,
    };
    ws.on_upgrade(move |downstream| relay_websocket(downstream, upstream))
}

async fn connect_websocket(
    state: &ProxyConfig,
    request: axum::http::Request<()>,
) -> Result<
    (
        UpstreamWebSocket,
        tokio_tungstenite::tungstenite::handshake::client::Response,
    ),
    tokio_tungstenite::tungstenite::Error,
> {
    let mut last_error = None;
    for address in &state.upstream_addresses {
        match tokio::net::TcpStream::connect(address).await {
            Ok(stream) => {
                return tokio_tungstenite::client_async_tls_with_config(
                    request, stream, None, None,
                )
                .await;
            }
            Err(error) => last_error = Some(error),
        }
    }
    Err(tokio_tungstenite::tungstenite::Error::Io(
        last_error.unwrap_or_else(|| std::io::Error::other("no safe upstream address")),
    ))
}

fn to_upstream(message: DownstreamMessage) -> UpstreamMessage {
    match message {
        DownstreamMessage::Text(value) => UpstreamMessage::Text(value.to_string().into()),
        DownstreamMessage::Binary(value) => UpstreamMessage::Binary(value),
        DownstreamMessage::Ping(value) => UpstreamMessage::Ping(value),
        DownstreamMessage::Pong(value) => UpstreamMessage::Pong(value),
        DownstreamMessage::Close(frame) => {
            UpstreamMessage::Close(frame.map(|frame| UpstreamCloseFrame {
                code: CloseCode::from(frame.code),
                reason: frame.reason.to_string().into(),
            }))
        }
    }
}

fn to_downstream(message: UpstreamMessage) -> Option<DownstreamMessage> {
    match message {
        UpstreamMessage::Text(value) => Some(DownstreamMessage::Text(value.to_string().into())),
        UpstreamMessage::Binary(value) => Some(DownstreamMessage::Binary(value)),
        UpstreamMessage::Ping(value) => Some(DownstreamMessage::Ping(value)),
        UpstreamMessage::Pong(value) => Some(DownstreamMessage::Pong(value)),
        UpstreamMessage::Close(frame) => Some(DownstreamMessage::Close(frame.map(|frame| {
            DownstreamCloseFrame {
                code: u16::from(frame.code),
                reason: frame.reason.to_string().into(),
            }
        }))),
        UpstreamMessage::Frame(_) => None,
    }
}

async fn relay_websocket(downstream: WebSocket, upstream: UpstreamWebSocket) {
    let (mut downstream_tx, mut downstream_rx) = downstream.split();
    let (mut upstream_tx, mut upstream_rx) = upstream.split();
    loop {
        tokio::select! {
            message = downstream_rx.next() => {
                let Some(Ok(message)) = message else {
                    let _ = upstream_tx.send(UpstreamMessage::Close(None)).await;
                    break;
                };
                if let DownstreamMessage::Close(frame) = message {
                    let _ = upstream_tx
                        .send(to_upstream(DownstreamMessage::Close(frame)))
                        .await;
                    // Axum/tungstenite queues the protocol-required close acknowledgement while
                    // yielding the received frame. Flush that queued frame instead of attempting
                    // a second close send, which tungstenite correctly rejects as SendAfterClosing.
                    let _ = downstream_tx.flush().await;
                    break;
                }
                if upstream_tx.send(to_upstream(message)).await.is_err() {
                    break;
                }
            }
            message = upstream_rx.next() => {
                let Some(Ok(message)) = message else {
                    let _ = downstream_tx.send(DownstreamMessage::Close(None)).await;
                    break;
                };
                if let UpstreamMessage::Close(frame) = message {
                    let downstream_close = to_downstream(UpstreamMessage::Close(frame.clone()))
                        .expect("a close frame always maps downstream");
                    // The upstream tungstenite leg likewise queued its acknowledgement on read.
                    let _ = upstream_tx.flush().await;
                    let _ = downstream_tx.send(downstream_close).await;
                    break;
                }
                if let Some(message) = to_downstream(message) {
                    if downstream_tx.send(message).await.is_err() {
                        break;
                    }
                }
            }
        }
    }
}

fn router_with_state(state: ProxyConfig) -> Router {
    Router::new()
        .route(HEALTH_PATH, get(health))
        .fallback(proxy_handler)
        .with_state(Arc::new(state))
}

#[cfg(test)]
fn router(config: TestConfig) -> Router {
    router_with_state(config.into())
}

pub async fn run(config: Config) -> Result<(), RunError> {
    run_until(config, shutdown_signal()).await
}

pub async fn run_until<F>(config: Config, shutdown: F) -> Result<(), RunError>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind(config.listen)
        .await
        .map_err(RunError::Bind)?;
    let state = ProxyConfig::from_config(&config)
        .await
        .map_err(RunError::Prepare)?;
    tracing::info!(listen = %config.listen, upstream_host = config.upstream.host_str(), "PolyFlare loopback companion started");
    axum::serve(listener, router_with_state(state))
        .with_graceful_shutdown(shutdown)
        .await
        .map_err(RunError::Serve)
}

pub async fn check_upstream(config: &Config) -> Result<(), RunError> {
    ProxyConfig::from_config(config)
        .await
        .map(|_| ())
        .map_err(RunError::Prepare)
}

#[derive(Debug, Error)]
pub enum PrepareError {
    #[error("remote upstream DNS resolution failed")]
    Resolve(#[source] std::io::Error),
    #[error("remote upstream resolved to no addresses")]
    NoAddresses,
    #[error("remote upstream resolved to a forbidden local or non-routable address")]
    UnsafeAddress,
    #[error("could not construct the pinned upstream client")]
    Client(#[source] reqwest::Error),
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[derive(Debug, Error)]
pub enum RunError {
    #[error("could not bind the loopback listener")]
    Bind(#[source] std::io::Error),
    #[error("could not prepare the pinned remote upstream")]
    Prepare(#[source] PrepareError),
    #[error("loopback server stopped unexpectedly")]
    Serve(#[source] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        extract::{ws::WebSocketUpgrade, State},
        http::{header, HeaderMap, Request, StatusCode},
        response::{IntoResponse, Response},
        routing::{any, get},
        Router,
    };
    use futures_util::{SinkExt, StreamExt};
    use std::{net::SocketAddr, sync::Arc, time::Duration};
    use tokio::net::TcpListener;
    use tokio::sync::{oneshot, Mutex, Notify};
    use tokio_tungstenite::{connect_async, tungstenite::Message};

    #[test]
    fn accepts_only_explicit_remote_https_origin_and_loopback_listener() {
        let valid = Config::try_new(
            "127.0.0.1:8080".parse().unwrap(),
            "https://ultraflux.example.ts.net".parse().unwrap(),
        );
        assert!(valid.is_ok());

        for listen in ["0.0.0.0:8080", "192.0.2.10:8080", "[::]:8080"] {
            let result = Config::try_new(
                listen.parse().unwrap(),
                "https://ultraflux.example.ts.net".parse().unwrap(),
            );
            assert!(matches!(result, Err(ConfigError::NonLoopbackListen)));
        }

        for upstream in [
            "http://ultraflux.example.ts.net",
            "https://localhost",
            "https://127.0.0.1:8080",
            "https://[::ffff:127.0.0.1]",
            "https://[::]",
            "https://user:secret@ultraflux.example.ts.net",
            "https://ultraflux.example.ts.net/backend-api",
            "https://ultraflux.example.ts.net/?token=secret",
        ] {
            assert!(
                Config::try_new("127.0.0.1:8080".parse().unwrap(), upstream.parse().unwrap(),)
                    .is_err(),
                "accepted unsafe upstream {upstream}"
            );
        }
    }

    #[test]
    fn rejects_forbidden_addresses_from_dns_including_mapped_ipv4() {
        for address in [
            "127.0.0.1",
            "0.0.0.0",
            "::1",
            "::",
            "::ffff:127.0.0.1",
            "224.0.0.1",
            "169.254.169.254",
            "255.255.255.255",
            "ff02::1",
            "fe80::1",
        ] {
            assert!(
                forbidden_upstream_ip(address.parse().unwrap()),
                "accepted forbidden resolved address {address}"
            );
        }
        for address in ["100.64.0.1", "192.168.1.5", "fd7a:115c:a1e0::1"] {
            assert!(
                !forbidden_upstream_ip(address.parse().unwrap()),
                "rejected valid tailnet or private remote address {address}"
            );
        }
    }

    #[test]
    fn target_is_pinned_to_upstream_origin_and_preserves_path_and_query() {
        let config = TestConfig::new("http://192.0.2.44:9999");
        let target = config
            .target("/backend-api/wham/usage?account=abc%2Fdef")
            .unwrap();
        assert_eq!(
            target.as_str(),
            "http://192.0.2.44:9999/backend-api/wham/usage?account=abc%2Fdef"
        );
    }

    #[test]
    fn removes_static_and_connection_nominated_hop_by_hop_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(header::AUTHORIZATION, "Bearer secret".parse().unwrap());
        headers.insert(
            header::CONNECTION,
            "keep-alive, x-remove-me".parse().unwrap(),
        );
        headers.insert("x-remove-me", "private".parse().unwrap());
        headers.insert("x-keep-me", "public".parse().unwrap());
        let filtered = end_to_end_headers(&headers);
        assert!(filtered.get(header::CONNECTION).is_none());
        assert!(filtered.get("x-remove-me").is_none());
        assert_eq!(filtered.get("x-keep-me").unwrap(), "public");
        assert_eq!(
            filtered.get(header::AUTHORIZATION).unwrap(),
            "Bearer secret"
        );
    }

    async fn spawn(app: Router) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        address
    }

    async fn echo_request(request: Request<Body>) -> Response {
        let path = request
            .uri()
            .path_and_query()
            .map(ToString::to_string)
            .unwrap();
        let marker = request
            .headers()
            .get("x-forward-marker")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        let stream = futures_util::stream::unfold(0, |step| async move {
            match step {
                0 => Some((
                    Ok::<_, std::io::Error>(bytes::Bytes::from("data: one\n\n")),
                    1,
                )),
                1 => {
                    tokio::time::sleep(Duration::from_millis(75)).await;
                    Some((Ok(bytes::Bytes::from("data: two\n\n")), 2))
                }
                _ => None,
            }
        });
        let mut response = Response::new(Body::from_stream(stream));
        response
            .headers_mut()
            .insert(header::CONTENT_TYPE, "text/event-stream".parse().unwrap());
        response
            .headers_mut()
            .insert("x-seen-path", path.parse().unwrap());
        response
            .headers_mut()
            .insert("x-seen-marker", marker.parse().unwrap());
        response
    }

    async fn ws_echo(ws: WebSocketUpgrade) -> Response {
        ws.on_upgrade(|mut socket| async move {
            while let Some(Ok(message)) = socket.recv().await {
                if socket.send(message).await.is_err() {
                    break;
                }
            }
        })
    }

    async fn ws_echo_with_protocol(ws: WebSocketUpgrade) -> Response {
        ws.protocols(["codex-test"])
            .on_upgrade(|mut socket| async move {
                while let Some(Ok(message)) = socket.recv().await {
                    if socket.send(message).await.is_err() {
                        break;
                    }
                }
            })
    }

    async fn delayed_ws_echo(ws: WebSocketUpgrade) -> Response {
        tokio::time::sleep(Duration::from_millis(75)).await;
        ws_echo(ws).await
    }

    async fn request_stream_probe(
        State(first_chunk): State<Arc<Mutex<Option<oneshot::Sender<()>>>>>,
        request: Request<Body>,
    ) -> Response {
        let mut stream = request.into_body().into_data_stream();
        if stream.next().await.is_some() {
            if let Some(sender) = first_chunk.lock().await.take() {
                let _ = sender.send(());
            }
        }
        StatusCode::NO_CONTENT.into_response()
    }

    async fn redirect_elsewhere() -> Response {
        (
            StatusCode::FOUND,
            [(header::LOCATION, "https://example.com/not-polyflare")],
        )
            .into_response()
    }

    #[tokio::test]
    async fn streams_http_and_sse_without_waiting_for_completion() {
        let upstream = spawn(Router::new().fallback(any(echo_request))).await;
        let companion = spawn(router(TestConfig::new(&format!("http://{upstream}")))).await;
        let response = reqwest::Client::new()
            .get(format!(
                "http://{companion}/backend-api/wham/usage?account=abc"
            ))
            .header("x-forward-marker", "present")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["x-seen-path"],
            "/backend-api/wham/usage?account=abc"
        );
        assert_eq!(response.headers()["x-seen-marker"], "present");
        let started = tokio::time::Instant::now();
        let mut bytes = response.bytes_stream();
        assert_eq!(bytes.next().await.unwrap().unwrap(), "data: one\n\n");
        assert!(started.elapsed() < Duration::from_millis(60));
        assert_eq!(bytes.next().await.unwrap().unwrap(), "data: two\n\n");
    }

    #[tokio::test]
    async fn forwards_websocket_bidirectionally() {
        let upstream = spawn(Router::new().route("/backend-api/ws", get(ws_echo))).await;
        let companion = spawn(router(TestConfig::new(&format!("http://{upstream}")))).await;
        let (mut socket, _) = connect_async(format!("ws://{companion}/backend-api/ws?x=1"))
            .await
            .unwrap();
        socket.send(Message::Text("hello".into())).await.unwrap();
        assert_eq!(
            socket.next().await.unwrap().unwrap(),
            Message::Text("hello".into())
        );
    }

    #[tokio::test]
    async fn preserves_websocket_protocol_binary_and_close_frame() {
        let upstream =
            spawn(Router::new().route("/backend-api/ws", get(ws_echo_with_protocol))).await;
        let companion = spawn(router(TestConfig::new(&format!("http://{upstream}")))).await;
        let mut request = format!("ws://{companion}/backend-api/ws")
            .into_client_request()
            .unwrap();
        request.headers_mut().insert(
            header::SEC_WEBSOCKET_PROTOCOL,
            "codex-test".parse().unwrap(),
        );
        let (mut socket, response) = connect_async(request).await.unwrap();
        assert_eq!(
            response.headers()[header::SEC_WEBSOCKET_PROTOCOL],
            "codex-test"
        );
        socket
            .send(Message::Binary(bytes::Bytes::from_static(b"binary")))
            .await
            .unwrap();
        assert_eq!(
            socket.next().await.unwrap().unwrap(),
            Message::Binary(bytes::Bytes::from_static(b"binary"))
        );
        let close = tokio_tungstenite::tungstenite::protocol::CloseFrame {
            code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::from(4001),
            reason: "switch-host".into(),
        };
        socket
            .send(Message::Close(Some(close.clone())))
            .await
            .unwrap();
        assert_eq!(
            socket.next().await.unwrap().unwrap(),
            Message::Close(Some(close))
        );
    }

    #[tokio::test]
    async fn refuses_downstream_websocket_upgrade_when_upstream_is_unavailable() {
        let dead = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead_address = dead.local_addr().unwrap();
        drop(dead);
        let companion = spawn(router(TestConfig::new(&format!("http://{dead_address}")))).await;
        let error = connect_async(format!("ws://{companion}/backend-api/ws"))
            .await
            .unwrap_err();
        match error {
            tokio_tungstenite::tungstenite::Error::Http(response) => {
                assert_eq!(response.status(), StatusCode::BAD_GATEWAY)
            }
            other => panic!("expected an HTTP 502 handshake rejection, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn allows_delayed_upstream_websocket_handshake_with_outer_margin() {
        assert!(WS_HANDSHAKE_TIMEOUT > Duration::from_secs(30));
        let upstream = spawn(Router::new().route("/backend-api/ws", get(delayed_ws_echo))).await;
        let companion = spawn(router(TestConfig::new(&format!("http://{upstream}")))).await;
        let (mut socket, _) = connect_async(format!("ws://{companion}/backend-api/ws"))
            .await
            .unwrap();
        socket.send(Message::Text("ready".into())).await.unwrap();
        assert_eq!(
            socket.next().await.unwrap().unwrap(),
            Message::Text("ready".into())
        );
    }

    #[tokio::test]
    async fn streams_request_body_before_the_client_finishes_producing_it() {
        let (first_chunk_tx, first_chunk_rx) = oneshot::channel();
        let upstream_state = Arc::new(Mutex::new(Some(first_chunk_tx)));
        let upstream = spawn(
            Router::new()
                .route(
                    "/backend-api/upload",
                    axum::routing::post(request_stream_probe),
                )
                .with_state(upstream_state),
        )
        .await;
        let companion = spawn(router(TestConfig::new(&format!("http://{upstream}")))).await;
        let release = Arc::new(Notify::new());
        let release_stream = release.clone();
        let body = futures_util::stream::unfold(0, move |step| {
            let release = release_stream.clone();
            async move {
                match step {
                    0 => Some((Ok::<_, std::io::Error>(bytes::Bytes::from("first")), 1)),
                    1 => {
                        release.notified().await;
                        Some((Ok(bytes::Bytes::from("second")), 2))
                    }
                    _ => None,
                }
            }
        });
        let request = tokio::spawn(async move {
            reqwest::Client::new()
                .post(format!("http://{companion}/backend-api/upload"))
                .body(reqwest::Body::wrap_stream(body))
                .send()
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), first_chunk_rx)
            .await
            .expect("upstream saw the first chunk while the second was blocked")
            .unwrap();
        release.notify_one();
        assert_eq!(
            request.await.unwrap().unwrap().status(),
            StatusCode::NO_CONTENT
        );
    }

    #[tokio::test]
    async fn does_not_forward_or_follow_upstream_redirects() {
        let upstream = spawn(Router::new().fallback(any(redirect_elsewhere))).await;
        let companion = spawn(router(TestConfig::new(&format!("http://{upstream}")))).await;
        let response = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap()
            .get(format!("http://{companion}/backend-api/redirect"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        assert!(response.headers().get(header::LOCATION).is_none());
    }

    #[tokio::test]
    async fn health_is_local_and_upstream_failure_is_fail_closed() {
        let dead = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead_address = dead.local_addr().unwrap();
        drop(dead);
        let companion = spawn(router(TestConfig::new(&format!("http://{dead_address}")))).await;
        let client = reqwest::Client::new();
        let health = client
            .get(format!("http://{companion}{HEALTH_PATH}"))
            .send()
            .await
            .unwrap();
        assert_eq!(health.status(), StatusCode::OK);
        assert_eq!(
            health.json::<serde_json::Value>().await.unwrap()["status"],
            "ok"
        );
        let failed = client
            .get(format!("http://{companion}/backend-api/failure"))
            .send()
            .await
            .unwrap();
        assert_eq!(failed.status(), StatusCode::BAD_GATEWAY);
    }
}
