# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.26-alpine3.23 AS builder
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod .
COPY go.sum .
RUN go mod download
COPY . .
RUN  \
    --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /out/ ./cmd/...

FROM alpine:3.24
LABEL org.opencontainers.image.source=https://github.com/soyelmismo/substream

# Substream is native and stateless: No ffmpeg, no mpv required.
RUN apk add -U --no-cache \
    ca-certificates \
    tzdata \
    tini \
    shared-mime-info

COPY --from=builder /out/* /usr/local/bin/

# Only one volume needed for DB and Ephemeral cache
VOLUME ["/data"]

EXPOSE 4533

ENV TZ=
ENV SUBSTREAM_DB_PATH=/data/substream.db
ENV SUBSTREAM_CACHE_PATH=/data/cache
ENV SUBSTREAM_LISTEN_ADDR=:4533

ENTRYPOINT ["/sbin/tini", "--"]
CMD ["substream"]
