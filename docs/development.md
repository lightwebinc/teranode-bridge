# Development

Build, CI and container image details for teranode-bridge.

## Requirements

- Go 1.26 or later
- For `make ci` / `make ci-*`: Docker (Dagger provisions its engine through it)
- For `make proto`: [`buf`](https://buf.build) plus `protoc-gen-go` and
  `protoc-gen-go-grpc` on `PATH`

## Build

```bash
make build          # static binary at ./teranode-bridge
make test           # go test -race ./...
make lint           # golangci-lint
make ci             # full containerised pipeline (see below)
make help           # list targets
```

Or without the Makefile:

```bash
go build ./cmd/teranode-bridge
```

## Container image

The Dockerfile produces a `gcr.io/distroless/static:nonroot` image with a single
static binary at `/usr/local/bin/teranode-bridge`. No in-image `ENV` defaults are
set — the bridge is configured entirely by flags, so pass them as the container
command or Helm `args`.

```bash
docker build --build-arg VERSION=0.10.1 -t teranode-bridge:0.10.1 .
docker run --rm teranode-bridge:0.10.1 -mode sink
```

Published images are gated behind a manual `image-publish` workflow run
(`ghcr.io/lightwebinc/teranode-bridge:<tag>`); there is no automatic push.

## CI

`make ci` runs the whole pipeline in containers via
[Dagger](https://dagger.io) — `tidy` → `lint` → `vuln` → `unit` → `build` →
`image` — so a local run and a GitHub Actions run execute the same steps. Each
stage is also available on its own (`make ci-unit`, `make ci-lint`, …), and
`make ci-shell` drops you into the builder container.

CI resolves `shard-common` from a sibling checkout and applies a local replace,
picking the branch matching the current one when it exists and `main` otherwise —
so a shared-library change can be validated here before it is tagged. The image
build deliberately does *not* use that replace: a published image always resolves
`shard-common` from the module proxy at its committed version.

## Dependencies

- [`github.com/lightwebinc/shard-common`](https://github.com/lightwebinc/shard-common) — `objfmt`, the push object-frame codecs shared with the rest of the stack
- [`github.com/twmb/franz-go`](https://github.com/twmb/franz-go) — Kafka producer
- `google.golang.org/grpc`, `google.golang.org/protobuf` — blockchain notification stream

The bridge deliberately does **not** link Teranode's own module. The two contracts
it needs — a three-field announcement message and a one-method notification
stream — are reproduced from their wire definitions instead, so a small bridge does
not pull in a full node's dependency tree.
