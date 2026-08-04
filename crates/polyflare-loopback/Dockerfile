# syntax=docker/dockerfile:1

ARG RUST_VERSION=1.97.0
ARG SOURCE_REVISION

FROM rust:${RUST_VERSION}-bookworm@sha256:8fa55b2f3ddf97471ab6a767bfa3f37e6bad0986ba823e75fea57e2a2a5c3073 AS builder
ARG SOURCE_REVISION
WORKDIR /src
COPY . .
RUN test -n "${SOURCE_REVISION}" \
    && cargo build --locked --release -p polyflare-loopback

FROM debian:bookworm-slim@sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818 AS runtime
ARG SOURCE_REVISION
RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates curl socat tini \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system polyflare \
    && useradd --system --gid polyflare --no-create-home polyflare

COPY --from=builder /src/target/release/polyflare-loopback /usr/local/bin/polyflare-loopback
LABEL org.opencontainers.image.revision="${SOURCE_REVISION}"

# The companion retains its loopback-only invariant on 8001. Socat exposes it only inside the
# container so Docker can publish the host side explicitly on 127.0.0.1:8000.
USER polyflare
EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=6 \
    CMD curl --fail --silent --show-error \
        http://127.0.0.1:8000/_polyflare-loopback/health >/dev/null || exit 1

ENTRYPOINT ["/usr/bin/tini", "-g", "--", "/bin/bash", "-c"]
CMD ["set -uo pipefail; socat TCP-LISTEN:8000,bind=0.0.0.0,reuseaddr,fork TCP:127.0.0.1:8001 & relay_pid=$!; /usr/local/bin/polyflare-loopback --listen 127.0.0.1:8001 --upstream-origin \"$POLYFLARE_LOOPBACK_UPSTREAM_ORIGIN\" & companion_pid=$!; wait -n \"$relay_pid\" \"$companion_pid\"; kill \"$relay_pid\" \"$companion_pid\" 2>/dev/null || true; wait || true; exit 1"]
