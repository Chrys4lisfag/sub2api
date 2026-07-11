# syntax=docker/dockerfile:1.7
# =============================================================================
# Sub2API Multi-Stage Dockerfile
# =============================================================================
# Stage 1: Build frontend
# Stage 2: Build Go backend with embedded frontend
# Stage 3: Final minimal image
# =============================================================================

ARG NODE_IMAGE=node:24-alpine
ARG GOLANG_IMAGE=golang:1.26.5-alpine
ARG ALPINE_IMAGE=alpine:3.21
ARG POSTGRES_IMAGE=postgres:18-alpine
ARG GOPROXY=https://goproxy.cn,direct
ARG GOSUMDB=sum.golang.google.cn
ARG NPM_CONFIG_REGISTRY=

# -----------------------------------------------------------------------------
# Stage 1: Frontend Builder
# -----------------------------------------------------------------------------
FROM ${NODE_IMAGE} AS frontend-builder
ARG NPM_CONFIG_REGISTRY

WORKDIR /app/frontend

# Install pnpm (pinned to v9 to match CI and keep builds reproducible)
RUN corepack enable && corepack prepare pnpm@9 --activate

# Install dependencies first (better caching)
COPY frontend/package.json frontend/pnpm-lock.yaml ./
RUN --mount=type=cache,id=sub2api-pnpm-store,target=/root/.local/share/pnpm/store \
    if [ -n "${NPM_CONFIG_REGISTRY}" ]; then pnpm config set registry "${NPM_CONFIG_REGISTRY}"; fi && \
    pnpm install --frozen-lockfile --prefer-offline

# Copy frontend source and build.
# LegalDocumentView.vue (admin-compliance gate) build-time imports
# ../../../../docs/legal/*.md?raw, so docs/legal/ must sit beside frontend/
# in the image (WORKDIR /app/frontend -> resolves to /app/docs/legal/*.md).
# Copy only that subtree to keep the build dependency minimal.
COPY frontend/ ./
COPY docs/legal/ /app/docs/legal/
RUN pnpm run build

# -----------------------------------------------------------------------------
# Stage 2: Backend Builder
# -----------------------------------------------------------------------------
FROM ${GOLANG_IMAGE} AS backend-builder

# Build arguments for version info (set by CI)
ARG VERSION=
ARG COMMIT=docker
ARG DATE
ARG GOPROXY
ARG GOSUMDB

ENV GOPROXY=${GOPROXY}
ENV GOSUMDB=${GOSUMDB}

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /app/backend

# Copy go mod files first (better caching).
# `backend/third_party/` holds locally-vendored modules referenced by
# go.mod `replace` directives (e.g. agymimic). Without it `go mod download`
# fails because the replace targets don't exist in the build context.
COPY backend/go.mod backend/go.sum ./
COPY backend/third_party ./third_party
RUN go mod download

# Copy backend source first
COPY backend/ ./

# Copy frontend dist from previous stage (must be after backend copy to avoid being overwritten)
COPY --from=frontend-builder /app/backend/internal/web/dist ./internal/web/dist

# Probe the embed source so a broken multi-stage COPY surfaces in build logs
# rather than silently producing a frontend-less binary that 404s on every
# route. Fail loudly if vite output didn't land where go-embed expects.
RUN echo "=== embed-source content ===" && \
    ls -la ./internal/web/dist/ && \
    test -f ./internal/web/dist/index.html || (echo "FATAL: dist/index.html missing in backend-builder" && exit 1) && \
    test -d ./internal/web/dist/assets || (echo "FATAL: dist/assets missing in backend-builder" && exit 1) && \
    asset_count=$(ls ./internal/web/dist/assets/ | wc -l) && \
    echo "dist/assets entry count: $asset_count" && \
    test "$asset_count" -gt 10 || (echo "FATAL: dist/assets has fewer than 10 entries — vite likely failed silently" && exit 1)

# Build the binary (BuildType=release for CI builds, embed frontend)
# Version precedence: build arg VERSION > exact git tag > cmd/server/VERSION
RUN VERSION_VALUE="${VERSION}" && \
    if [ -z "${VERSION_VALUE}" ]; then VERSION_VALUE="$(./scripts/resolve-version.sh)"; fi && \
    DATE_VALUE="${DATE:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}" && \
    CGO_ENABLED=0 GOOS=linux go build \
    -tags embed \
    -ldflags="-s -w -X main.Version=${VERSION_VALUE} -X main.Commit=${COMMIT} -X main.Date=${DATE_VALUE} -X main.BuildType=release" \
    -trimpath \
    -o /app/sub2api \
    ./cmd/server

# Verify the embed actually picked up the dist/. If asset hashes from
# vite's chunk names don't appear in the binary, the build is broken
# and we fail fast rather than ship an image that 404s every route.
RUN echo "=== binary embed check ===" && \
    ls -la /app/sub2api && \
    bin_size=$(stat -c '%s' /app/sub2api) && \
    echo "DIAG: binary size = $bin_size" && \
    grep -c vendor-vue /app/sub2api > /tmp/vc.txt 2>/dev/null || echo "0" > /tmp/vc.txt; vc=$(cat /tmp/vc.txt) && \
    echo "DIAG: vendor-vue refs in binary = $vc" && \
    grep -c AccountsView /app/sub2api > /tmp/av.txt 2>/dev/null || echo "0" > /tmp/av.txt; av=$(cat /tmp/av.txt) && \
    echo "DIAG: AccountsView refs = $av" && \
    grep -ao 'assets/index-[A-Za-z0-9_]*' /app/sub2api | head -1 > /tmp/ai.txt 2>/dev/null || echo "NONE" > /tmp/ai.txt; ai=$(cat /tmp/ai.txt) && \
    echo "DIAG: first asset hash = $ai" && \
    echo "=== /diag ==="

# -----------------------------------------------------------------------------
# Stage 3: PostgreSQL Client (version-matched with docker-compose)
# -----------------------------------------------------------------------------
FROM ${POSTGRES_IMAGE} AS pg-client

# -----------------------------------------------------------------------------
# Stage 4: Final Runtime Image
# -----------------------------------------------------------------------------
FROM ${ALPINE_IMAGE}

# Labels
LABEL maintainer="Wei-Shaw <github.com/Wei-Shaw>"
LABEL description="Sub2API - AI API Gateway Platform"
LABEL org.opencontainers.image.source="https://github.com/Wei-Shaw/sub2api"

# Install runtime dependencies
RUN apk add --no-cache \
    ca-certificates \
    tzdata \
    su-exec \
    libpq \
    zstd-libs \
    lz4-libs \
    krb5-libs \
    libldap \
    libedit \
    && rm -rf /var/cache/apk/*

# Copy pg_dump and psql from the same postgres image used in docker-compose
# This ensures version consistency between backup tools and the database server
COPY --from=pg-client /usr/local/bin/pg_dump /usr/local/bin/pg_dump
COPY --from=pg-client /usr/local/bin/psql /usr/local/bin/psql
COPY --from=pg-client /usr/local/lib/libpq.so.5* /usr/local/lib/

# Create non-root user
RUN addgroup -g 1000 sub2api && \
    adduser -u 1000 -G sub2api -s /bin/sh -D sub2api

# Set working directory
WORKDIR /app

# Copy binary/resources with ownership to avoid extra full-layer chown copy
COPY --from=backend-builder --chown=sub2api:sub2api /app/sub2api /app/sub2api
COPY --from=backend-builder --chown=sub2api:sub2api /app/backend/resources /app/resources

# Create data directory
RUN mkdir -p /app/data && chown sub2api:sub2api /app/data

# Copy entrypoint script (fixes volume permissions then drops to sub2api)
COPY deploy/docker-entrypoint.sh /app/docker-entrypoint.sh
RUN chmod +x /app/docker-entrypoint.sh

# Expose port (can be overridden by SERVER_PORT env var)
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD wget -q -T 5 -O /dev/null http://localhost:${SERVER_PORT:-8080}/health || exit 1

# Run the application (entrypoint fixes /app/data ownership then execs as sub2api)
ENTRYPOINT ["/app/docker-entrypoint.sh"]
CMD ["/app/sub2api"]
