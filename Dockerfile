# syntax=docker/dockerfile:1.29.0@sha256:572cebbc2b15e6f84c077742952674889cb63cd5e9b80abdffcbe587b6f19183
#
# Multi-stage Dockerfile for teranode-bridge. Produces a single static binary
# at /usr/local/bin/teranode-bridge on a distroless nonroot base.
#
# No ENV defaults are baked in: the bridge is configured entirely by flags, so
# pass them as the container command / Helm `args`. See docs/configuration.md.

FROM golang:1.27-alpine@sha256:70bd7ddea8b6cbe5d6943cde279d6b67feb10cc924fb9100d1d8757c1575b280 AS builder
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

FROM gcr.io/distroless/static:nonroot@sha256:8ce6dd87e9823c5b48dcfb2c8604714ccb351ce59607327bcc4957448da279b5
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
