# syntax=docker/dockerfile:1.7
#
# Multi-stage Dockerfile for teranode-bridge. Produces a single static binary
# at /usr/local/bin/teranode-bridge on a distroless nonroot base.
#
# No ENV defaults are baked in: the bridge is configured entirely by flags, so
# pass them as the container command / Helm `args`. See docs/configuration.md.

FROM golang:1.26-alpine AS builder
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

FROM gcr.io/distroless/static:nonroot
USER nonroot:nonroot
COPY --from=builder /out/ /usr/local/bin/
# tx / subtree / block delivery lanes, then the retrieval plane.
EXPOSE 8725 9143 9144 9145
ENTRYPOINT ["/usr/local/bin/teranode-bridge"]
