# syntax=docker/dockerfile:1.7
# marco image: a release build of the marco server with the console and the
# docs (ui/, built with node) embedded, and marco-load, on a slim non-root
# runtime.
#
#   docker build -t marco:local .
#   docker run -p 7300:7300 -v $PWD/snap:/var/lib/marco/snap marco:local \
#     serve --snapshot /var/lib/marco/snap --jetstream wss://jetstream2.us-east.bsky.network
#
# Configuration is the CLI flags and their MARCO_* env vars (`marco serve --help`,
# docs/operations.md). The admin listener (console, /metrics, /debug) is on
# 7301, bound to all interfaces here: publish it only where the token or your
# proxy guards it.

# --- console and docs ---------------------------------------------------------
FROM node:26-bookworm-slim AS ui
WORKDIR /src/ui
COPY ui/package.json ui/package-lock.json ./
RUN --mount=type=cache,target=/root/.npm npm ci --no-audit --no-fund
COPY ui/ ./
# the docs site (/docs) is rendered from ../docs at build time
COPY docs/ /src/docs/
RUN npm run build

# --- rust release build -------------------------------------------------------
FROM rust:1.100.0-bookworm AS build

# cmake/clang: aws-lc-sys (rustls) and the vendored libsecp256k1 / jemalloc / zstd C builds
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake clang \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY rust-toolchain.toml ./
# installs the pinned toolchain if the base image's differs
RUN rustup show active-toolchain
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
# the server embeds ui/dist (crates/marco-server/src/ui.rs)
COPY --from=ui /src/ui/dist ./ui/dist
# No debug info in the image (Cargo.toml keeps debug = 1 for local profiling).
# Symbols stay, so panics and backtraces still name functions.
ENV CARGO_PROFILE_RELEASE_DEBUG=0 CARGO_PROFILE_DIST_DEBUG=0
# release (thin LTO) by default; the image workflow builds tags with dist (fat
# LTO, one codegen unit: Cargo.toml), which takes several times as long.
ARG CARGO_PROFILE=release
ARG MARCO_COMMIT=""
# vlatproto is a git dependency on jazware/vlatproto (Cargo.toml), fetched here.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    MARCO_COMMIT=$MARCO_COMMIT cargo build --profile "$CARGO_PROFILE" --locked -p marco-server -p marco-load -p seek-server \
    && mkdir -p /out /out-seek \
    && cp target/$CARGO_PROFILE/marco target/$CARGO_PROFILE/marco-load /out/ \
    && cp target/$CARGO_PROFILE/seek /out-seek/ \
    && /out/marco --help >/dev/null && /out-seek/seek --help >/dev/null

# --- seek: docker build --target seek -t seek:local . --------------------------
FROM debian:bookworm-slim AS seek
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl tini \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 seek \
    && useradd --system --uid 10001 --gid seek --home-dir /var/lib/seek --create-home seek
USER seek:seek
WORKDIR /var/lib/seek
ENV SEEK_LISTEN=0.0.0.0:7310 SEEK_OPS_LISTEN=0.0.0.0:7311 SEEK_DATA=/var/lib/seek
# 7310: bsky.Service/SearchActorsTypeahead, /healthz, /readyz; 7311: /metrics, /debug
EXPOSE 7310 7311
ENTRYPOINT ["/usr/bin/tini", "--", "seek"]
CMD ["--help"]
COPY --from=build /out-seek/ /usr/local/bin/

# --- runtime ------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl tini \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 marco \
    && useradd --system --uid 10001 --gid marco --home-dir /var/lib/marco --create-home marco
USER marco:marco
WORKDIR /var/lib/marco
ENV MARCO_LISTEN=0.0.0.0:7300 \
    MARCO_ADMIN_LISTEN=0.0.0.0:7301
# 7300: marco.v1.Graph, /docs, /healthz, /readyz
# 7301: Graph, marco.v1.Admin, the console at /admin, /metrics, /debug
EXPOSE 7300 7301
HEALTHCHECK --interval=10s --timeout=3s --start-period=60s --retries=3 \
    CMD curl -sf http://127.0.0.1:7300/healthz || exit 1
# tini forwards SIGTERM to marco.
ENTRYPOINT ["/usr/bin/tini", "--", "marco"]
CMD ["--help"]
COPY --from=build /out/ /usr/local/bin/
