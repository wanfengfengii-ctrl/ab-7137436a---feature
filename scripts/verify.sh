#!/usr/bin/env bash
#
# verify.sh — one-shot verifier for the interlock event service.
#
# It waits for the service to answer GET /health and then runs every check,
# folding results into a bitmask exit code:
#
#    1  publish: global ids, idempotent replay, 409 zero-write
#    2  SSE live delivery and idle heartbeat
#    4  SSE resume via Last-Event-ID (gapless, exactly-once)
#    8  expired cursor -> 410 with earliestAvailableId
#   16  `go test ./...`
#   32  build of all binaries
#   64  durability across a real process restart (ids/replay/conflict/410)
#
# Usage:
#   scripts/verify.sh                # builds binaries, manages a local server
#   BASE_URL=http://host:8080 scripts/verify.sh   # run smoke against an
#                                                  # already-running service
#
# Tunables: APP_PORT, RETENTION_LIMIT, BASE_URL, KEEP_DATA.
set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PORT="${APP_PORT:-18080}"
RETENTION="${RETENTION_LIMIT:-25}"
BASE="${BASE_URL:-}"
BIN="$ROOT/bin"
WORK="$(mktemp -d)"
DATA="${KEEP_DATA:+$ROOT/.verify-data}"; DATA="${DATA:-$WORK/data}"
LOG="$WORK/server.log"
STATE="$WORK/restart-state.json"

CODE=0
SERVER_PID=""

cleanup() {
  if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill -TERM "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  [ -n "${KEEP_DATA:-}" ] || rm -rf "$WORK"
}
trap cleanup EXIT

log() { printf '\033[1;34m[verify]\033[0m %s\n' "$*"; }

wait_healthy() {
  local url="$1" tries="${2:-120}"
  log "waiting for health at $url"
  for _ in $(seq 1 "$tries"); do
    if HEALTH_URL="$url" "$BIN/healthcheck" >/dev/null 2>&1; then
      log "service healthy"
      return 0
    fi
    sleep 0.5
  done
  echo "service did not become healthy: $url" >&2
  return 1
}

start_server() {
  mkdir -p "$DATA"
  log "starting server: port=$PORT data=$DATA retention=$RETENTION"
  APP_PORT="$PORT" DATA_DIR="$DATA" RETENTION_LIMIT="$RETENTION" \
    "$BIN/server" >"$LOG" 2>&1 &
  SERVER_PID=$!
  wait_healthy "http://localhost:$PORT/health" || {
    echo "--- server log ---" >&2; cat "$LOG" >&2; return 1;
  }
}

stop_server() {
  if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
    log "stopping server (pid $SERVER_PID)"
    kill -TERM "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    SERVER_PID=""
  fi
}

# ---- 1. build --------------------------------------------------------------
log "building all binaries"
mkdir -p "$BIN"
if ! ( CGO_ENABLED=0 go build -buildvcs=false -o "$BIN/server"         ./cmd/server && \
       CGO_ENABLED=0 go build -buildvcs=false -o "$BIN/smoke"          ./cmd/smoke && \
       CGO_ENABLED=0 go build -buildvcs=false -o "$BIN/restart-smoke"  ./cmd/restart-smoke && \
       CGO_ENABLED=0 go build -buildvcs=false -o "$BIN/healthcheck"    ./cmd/healthcheck ); then
  echo "BUILD FAILED" >&2
  exit 32
fi
log "build ok"

# ---- 2. tests --------------------------------------------------------------
log "running go test ./..."
if ! go test -count=1 ./...; then
  echo "UNIT/INTEGRATION TESTS FAILED (bit 16)" >&2
  CODE=$((CODE | 16))
fi

# ---- 3. bring up a server unless BASE_URL was provided ---------------------
if [ -n "$BASE" ]; then
  wait_healthy "$BASE/health" || exit $((CODE | 1 | 2 | 4 | 8))
else
  start_server || exit $((CODE | 1 | 2 | 4 | 8))
  BASE="http://localhost:$PORT"
fi

# ---- 4. end-to-end smoke: publish / live / resume / expired cursor ---------
log "running end-to-end smoke against $BASE"
if "$BIN/smoke" -base "$BASE"; then
  log "smoke passed (bits 1/2/4/8 clear)"
else
  rc=$?
  echo "SMOKE reported failing bits: $rc (1=publish 2=live 4=resume 8=gone)" >&2
  CODE=$((CODE | rc))
fi

# ---- 5. durability across a real restart -----------------------------------
# Restart persistence must be tested against storage WE control. If smoke ran
# against an external BASE_URL, spawn a private local server for this section.
RS_PORT="$PORT"
if [ -n "${BASE_URL:-}" ]; then
  RS_PORT=$(( PORT + 1 ))
  RS_DATA="$WORK/rs-data"
  log "spawning private server on $RS_PORT for restart-durability phase"
  APP_PORT="$RS_PORT" DATA_DIR="$RS_DATA" RETENTION_LIMIT="$RETENTION" \
    "$BIN/server" >"$WORK/server-rs.log" 2>&1 &
  SERVER_PID=$!
  wait_healthy "http://localhost:$RS_PORT/health" || exit $((CODE | 64))
  RS_BASE="http://localhost:$RS_PORT"
else
  RS_BASE="$BASE"
fi

log "restart phase: before"
if ! "$BIN/restart-smoke" -base "$RS_BASE" -state "$STATE" \
     -phase before -retention "$RETENTION"; then
  echo "RESTART before-phase FAILED (bit 64)" >&2
  CODE=$((CODE | 64))
else
  if [ -z "${BASE_URL:-}" ]; then
    stop_server
    log "restarting server from same data directory"
    APP_PORT="$PORT" DATA_DIR="$DATA" RETENTION_LIMIT="$RETENTION" \
      "$BIN/server" >"$LOG.2" 2>&1 &
    SERVER_PID=$!
    wait_healthy "http://localhost:$PORT/health" || { cat "$LOG.2" >&2; exit $((CODE | 64)); }
  else
    # Private server spawned above: restart it the same way.
    stop_server
    APP_PORT="$RS_PORT" DATA_DIR="$WORK/rs-data" RETENTION_LIMIT="$RETENTION" \
      "$BIN/server" >"$WORK/server-rs-2.log" 2>&1 &
    SERVER_PID=$!
    wait_healthy "http://localhost:$RS_PORT/health" || exit $((CODE | 64))
  fi

  log "restart phase: after"
  if ! "$BIN/restart-smoke" -base "$RS_BASE" -state "$STATE" -phase after; then
    echo "RESTART after-phase FAILED (bit 64)" >&2
    CODE=$((CODE | 64))
  fi
fi

# ---- summary ---------------------------------------------------------------
echo
if [ "$CODE" -eq 0 ]; then
  echo "ALL VERIFICATIONS PASSED (exit 0)"
else
  echo "VERIFICATION FAILED, aggregated exit code: $CODE"
  echo "  bits: 1=publish 2=live 4=resume 8=gone 16=tests 32=build 64=restart"
fi
exit "$CODE"
