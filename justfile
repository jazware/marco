# marco: the engine (marco-core), the server (marco-server, binary `marco`),
# its API (marco-proto, from proto/) and the bulk loader (marco-load)

set shell := ["bash", "-euo", "pipefail", "-c"]

# Codegen tools, pinned and installed once per revision under one shared dir.
# Keep buffa and connect-rust in step with the runtimes in Cargo.toml.
buf_version := "v1.73.0"
buffa_version := "0.9.2"
connect_rust_version := "0.9.0"
tools_rev := "r1"
tools_dir := env("MARCO_TOOLS", home_directory() / ".cache/marco-tools" / tools_rev)

# protoc-gen-es comes from the console's node_modules (ui/package.json pins it).
export PATH := tools_dir / "bin" + ":" + justfile_directory() / "ui/node_modules/.bin" + ":" + home_directory() / ".cargo/bin" + ":" + env("PATH")

import? 'internal/justfile'

default:
    @just --list --unsorted

check: check-private check-gen
    cargo check --all-targets
    cargo clippy --all-targets -- -D warnings
    cargo fmt --check
    cd ui && npm run typecheck
    cd ui && npm run check-docs

# The console and the docs: build into ui/dist (a release build of the server embeds it), dev servers, screenshots
ui:
    cd ui && npm ci && npm run build

ui-dev url="http://127.0.0.1:7301":
    cd ui && MARCO_URL={{url}} npm run dev

ui-mock:
    cd ui && npm run dev:mock

# The docs site (/docs; docs/_style.md): validate it and print its nav
docs-check:
    cd ui && npm run check-docs

# Screenshots of every docs page: desktop and 390 px, light and dark; fails on console errors
docs-shots *args:
    cd ui && npm run docs-shots -- {{args}}

# Screenshots of the landing page and its explorer: desktop and 390 px, light and dark; fails on console
# errors. `--url` shoots a running marco, `--api <origin>` sends the explorer's calls to another one
home-shots *args:
    cd ui && npm run home-shots -- {{args}}

# Redraw the social card (ui/public/og/marco.png, committed)
og-card:
    cd ui && npm run og-card

# Screenshots of every section against the mock: desktop, 390 px, light and dark; fails on console errors
shots *args:
    cd ui && npm run shots -- {{args}}

test *args:
    cargo test {{args}}

fmt:
    cargo fmt

# No private host, domain, address or name outside internal/: this tree is published. The deny
# lists stay in the monorepo, so the public repo skips it.
check-private:
    #!/usr/bin/env bash
    set -euo pipefail
    lists=../../scripts/check-private
    if [ ! -f "$lists/check_private.py" ]; then echo "check-private: no deny lists here, skipped"; exit 0; fi
    python3 "$lists/check_private.py" --deny "$lists/delta.deny" --allow "$lists/delta.allow" --exclude internal .
    python3 "$lists/check_private.py" --deny "$lists/marco.deny" --allow "$lists/marco.allow" --exclude internal .

# Build a snapshot from export files (see marco-load --help)
load *args:
    cargo build --release -p marco-load
    ${CARGO_TARGET_DIR:-target}/release/marco-load {{args}}

# Install the pinned buf and protoc plugins (idempotent)
tools:
    #!/usr/bin/env bash
    set -euo pipefail
    bin="{{tools_dir}}/bin"
    mkdir -p "$bin"
    if [ ! -x "$bin/buf" ]; then
        GOBIN="$bin" go install github.com/bufbuild/buf/cmd/buf@{{buf_version}}
    fi
    if [ ! -x "$bin/protoc-gen-buffa" ] || [ ! -x "$bin/protoc-gen-connect-rust" ] || [ ! -x "$bin/protoc-gen-buffa-packaging" ]; then
        cargo install --locked --root "{{tools_dir}}" \
            protoc-gen-buffa@{{buffa_version}} \
            protoc-gen-buffa-packaging@{{buffa_version}} \
            connectrpc-codegen@{{connect_rust_version}}
    fi
    if [ ! -d ui/node_modules ]; then
        (cd ui && npm ci)
    fi

gen_dirs := "crates/marco-proto/src/gen ui/src/gen crates/seek-api/src/gen"

# Regenerate the API code from proto/
gen: tools
    cd proto && buf lint && buf generate
    cd crates/seek-api/proto && buf lint && buf generate

# Fail if the generated code is stale: regenerate it and compare
check-gen: tools
    #!/usr/bin/env bash
    set -euo pipefail
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' EXIT
    for d in {{gen_dirs}}; do mkdir -p "$tmp/$d" && cp -R "$d/." "$tmp/$d"; done
    (cd proto && buf lint && buf generate)
    (cd crates/seek-api/proto && buf lint && buf generate)
    for d in {{gen_dirs}}; do diff -r "$tmp/$d" "$d"; done

# The server on a snapshot (see marco serve --help)
serve *args:
    cargo build --release -p marco-server
    ${CARGO_TARGET_DIR:-target}/release/marco serve {{args}}

# seek's bucket test against a throwaway MinIO in Docker (the vlpds harness's image, build/Dockerfile.minio there)
seek-minio-test port="19400":
    #!/usr/bin/env bash
    set -euo pipefail
    name="seek-minio-test-{{port}}-$$"
    img=${SEEK_MINIO_IMAGE:-vlpds-minio:local}
    docker image inspect "$img" >/dev/null 2>&1 || docker build -t "$img" -f ../vlpds/build/Dockerfile.minio ../vlpds/build
    docker run -d --rm --name "$name" -p 127.0.0.1:{{port}}:9000 -e MINIO_ROOT_USER=seektest -e MINIO_ROOT_PASSWORD=seektestsecret "$img" server /data >/dev/null
    trap 'docker rm -f "$name" >/dev/null' EXIT
    until curl -sf http://127.0.0.1:{{port}}/minio/health/ready >/dev/null; do sleep 0.5; done
    docker exec "$name" mc alias set local http://127.0.0.1:9000 seektest seektestsecret >/dev/null
    docker exec "$name" mc mb local/seek-test >/dev/null
    creds=$(mktemp)
    echo '{"access_key_id": "seektest", "secret_access_key": "seektestsecret"}' > "$creds"
    SEEK_TEST_S3=http://127.0.0.1:{{port}} SEEK_TEST_S3_BUCKET=seek-test SEEK_TEST_S3_CREDENTIALS="$creds" \
      cargo test -p seek-server --test bucket -- --ignored
    rm -f "$creds"
