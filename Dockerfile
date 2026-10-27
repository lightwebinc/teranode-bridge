# syntax=docker/dockerfile:1.28.0@sha256:bb22d9815c728170f72750f4e5b0d672e06176142e1d602c7e66c050100b7e5b
#
# Multi-stage Dockerfile for teranode-bridge. Produces a single static binary
# at /usr/local/bin/teranode-bridge on a distroless nonroot base.
#
# No ENV defaults are baked in: the bridge is configured entirely by flags, so
# pass them as the container command / Helm `args`. See docs/configuration.md.

FROM golang:1.27-alpine@sha256:d200d2dd76b03a99b52651fd1325aff7dcd35422c35c01bac56571b0507f3deb AS builder
RUN apk add --no-cache git ca-certificates
WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .
ARG VERSION=dev
ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    set -eux; \
    mkdir -p /out; \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
      go build -trimpath -buildvcs=false \
        -ldflags "-s -w -X main.Version=${VERSION}" \
        -o /out/teranode-bridge ./cmd/teranode-bridge

FROM gcr.io/distroless/static:nonroot@sha256:18f4cc26c9ffa7f20a359298be7deadac5c607c8e03a02b8119f1a5fc2f82c99
USER nonroot:nonroot
COPY --from=builder /out/ /usr/local/bin/

# The licences travel with the image, not only with the source tree. The binary
# above is statically linked, so it contains the code of every module in
# LICENSE-THIRD-PARTY and none of their licence files. Apache-2.0 section 4,
# MIT and BSD-3 all require their text to reach the recipient of a binary.
COPY LICENSE NOTICE LICENSE-THIRD-PARTY /usr/share/doc/teranode-bridge/
# tx / subtree / block delivery lanes, the retrieval plane, then the metrics
# and health listener (-metrics-addr, which also serves /health*).
EXPOSE 8725 9143 9144 9145 9146
ENTRYPOINT ["/usr/local/bin/teranode-bridge"]
