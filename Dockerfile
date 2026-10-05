# syntax=docker/dockerfile:1

# ---- build stage -----------------------------------------------------------
FROM golang:1.27-bookworm AS build
WORKDIR /src

# Cache dependencies first (the module has zero external deps, but keep the
# standard layout for future-proofing).
COPY go.mod ./
RUN go mod download

COPY . .

# World-writable data dir for the distroless non-root runtime user; copied
# into the final image below (distroless has no shell for RUN mkdir).
RUN mkdir -m 1777 /data

# Static binaries so they run on a rootless distroless base.
ARG TARGETOS=linux
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -buildvcs=false -trimpath -ldflags="-s -w" -o /out/server        ./cmd/server && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -buildvcs=false -trimpath -ldflags="-s -w" -o /out/smoke         ./cmd/smoke && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -buildvcs=false -trimpath -ldflags="-s -w" -o /out/restart-smoke ./cmd/restart-smoke && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -buildvcs=false -trimpath -ldflags="-s -w" -o /out/healthcheck   ./cmd/healthcheck

# ---- verify stage ----------------------------------------------------------
# One-shot gate image: full Go toolchain + bash so scripts/verify.sh can run
# `go test`, build everything, exercise a separately running app over the
# network and cycle its own local server for restart-durability checks.
FROM golang:1.27-bookworm AS verify
WORKDIR /src
COPY . .
RUN chmod +x scripts/verify.sh
# Default target is the one-shot verifier; BASE_URL is injected by Compose.
ENTRYPOINT ["scripts/verify.sh"]

# ---- runtime stage ---------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot

# 1777 so the non-root user can write whether /data is a fresh image dir or a
# named volume initialised from the image.
COPY --from=build --chown=nonroot:nonroot /out/server        /usr/local/bin/server
COPY --from=build --chown=nonroot:nonroot /out/smoke         /usr/local/bin/smoke
COPY --from=build --chown=nonroot:nonroot /out/restart-smoke /usr/local/bin/restart-smoke
COPY --from=build /out/healthcheck /usr/local/bin/healthcheck

# Writable data directory (named volume initialised from this image dir).
COPY --from=build --chown=nonroot:nonroot /data /data

USER nonroot:nonroot
VOLUME ["/data"]

ENV APP_PORT=8080 \
    DATA_DIR=/data \
    RETENTION_LIMIT=100

EXPOSE 8080

HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=12 \
    CMD ["/usr/local/bin/healthcheck"]

ENTRYPOINT ["/usr/local/bin/server"]
