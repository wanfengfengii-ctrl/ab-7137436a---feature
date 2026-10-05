# Interlock Events

Per-channel safety-event ingestion and resumable event stream for a particle
accelerator interlock console. After a network blip the console reconnects
with `Last-Event-ID` and picks up **exactly where it left off** — it never
misses a beam-stop signal, and it never re-displays history as new alarms.

The monitoring hall can also watch several accelerator sections (linac,
storage ring, …) over **one** connection: `GET /streams?channel=a&channel=b`
merges every selected channel into a single stream ordered by the global id,
so cross-region interlock causality is restored in true publish order without
juggling per-channel reconnects.

The service is a single static Go binary (standard library only) plus a
multi-stage Dockerfile. No external datastores: state lives in local
crash-safe WAL files rebuilt on startup.

## Guarantees

| Requirement | Guarantee |
|---|---|
| Global ordering | Every accepted event gets the next **global** `id` (1-based, monotonic across **all** channels). Ids are allocated in batch order under one lock, so concurrent publishes interleave in commit/ack order. |
| Idempotent ingestion | An event whose `eventKey` is already known **with identical content** replays the original result (same ids, `"replay": true`); no id is allocated and nothing is appended. |
| Atomic conflict | If any event in a batch carries a known key with **different** content (or the same key appears twice in the batch with different content), the whole batch gets **HTTP 409 with zero writes**, with per-event pinpoint details. |
| Gapless SSE resume | `GET /streams/{channel}` honors `Last-Event-ID`. History newer than the cursor is delivered first, then transitions seamlessly to live. Each event appears at most once, in id order. |
| Aggregated stream | `GET /streams?channel=a&channel=b` (1–8 distinct channels) merges the selected channels into one id-ordered stream; every frame keeps the global id and original fields plus its source `channel`. One global resume cursor covers the whole selection; unselected channels never appear. |
| Exactly-once under concurrency | The hub carries no payload — it only wakes connections; each stream re-reads the store from its own cursor. A wakeup coalescing or arriving out of order can neither lose nor duplicate an event. |
| Retention / expired cursor | Each channel keeps the newest `RETENTION_LIMIT` readable events. A resume cursor older than the oldest retained id is answered **HTTP 410** with `earliestAvailableId`. On the aggregate stream the 410 fires exactly when lossless resume is impossible — a selected channel trimmed events newer than the cursor — and pinpoints every channel's `earliestAvailableId`. |
| Durability & restart | Committed batches are fsynced WAL files (temp + fsync + rename). After restart: global numbering, replay verdicts, conflict verdicts and the 410 boundary are unchanged; numbering continues above every id ever assigned. The durable WAL retains full history even though reads are retention-bounded. |
| Heartbeat | An idle stream sends an SSE comment `: heartbeat …` every 4 s (< 5 s). |
| Health | `GET /health` → 200 `{"status":"ok"}`; used by the container `HEALTHCHECK`. |

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `APP_PORT` | `8080` | Listen port (also used by Compose port mapping). |
| `DATA_DIR` | `/data` | Directory for durable WAL files (mount a volume). |
| `RETENTION_LIMIT` | `100` | Readable events kept per channel (WAL on disk keeps everything). |
| `HEALTH_URL` | `http://localhost:8080/health` | Container healthcheck URL. |
| `SHUTDOWN_TIMEOUT_SECONDS` | `10` | Graceful shutdown budget. |

## API

### `GET /health`

```json
{"status":"ok"}
```

### `POST /events/{channel}`

Atomic batch of **1–50** events. Content-Type `application/json`, body a JSON
array:

```json
[
  {"eventKey":"trip-linac-07","time":"2026-10-05T12:00:00Z",
   "severity":"critical","message":"beam dump, sector 7"}
]
```

`time` must be RFC3339. Unknown JSON fields are rejected; empty `eventKey` /
`severity` and malformed batches → 400.

**200 — accepted (or fully replayed)**

```json
{
  "channel": "linac",
  "results": [
    {"id": 42, "replay": false},
    {"id": 43, "replay": false}
  ]
}
```

Retrying the exact same batch returns the same ids with `"replay": true`.

**409 — key/content mismatch, nothing written**

```json
{
  "error": "conflict: ... no events were written",
  "channel": "linac",
  "items": [
    {
      "index": 0,
      "eventKey": "trip-linac-07",
      "incoming": {"eventKey":"trip-linac-07","time":"…","severity":"critical","message":"edited"},
      "existing": {"id":42,"eventKey":"trip-linac-07","time":"…","severity":"critical","message":"beam dump, sector 7"}
    }
  ]
}
```

`existing` is `null` when the clash is between two entries of the same batch.
The console can surface the exact batch index/key and is guaranteed the
stored event was not altered.

### `GET /streams/{channel}` — SSE

Standard Server-Sent Events (`event` frames use the global id):

```
id: 42
event: event
data: {"id":42,"eventKey":"trip-linac-07","time":"…","severity":"critical","message":"…"}

id: 43
event: event
data: {"id":43,...}

: heartbeat 2026-10-05T12:00:05Z
```

Resume exactly like a browser EventSource:
`Last-Event-ID: 42` → delivers ids `>42` (history first, then live).
`Last-Event-ID` may also be passed as `?lastEventId=`. Missing → replay the
retained window from the beginning.

**410 — cursor expired by retention** (sent before any SSE bytes):

```json
{"error":"Last-Event-ID 5 is older than the earliest retained id 18; resync from earliestAvailableId",
 "earliestAvailableId":18}
```

If retention crosses an open stream's cursor mid-connection, the stream ends
with an `event: error` carrying the same `earliestAvailableId`, so the client
resyncs.

### `GET /streams?channel=a&channel=b` — aggregated SSE

One connection watching **1–8** channels: repeat the `channel` query
parameter for each section to observe. Names must be non-empty and mutually
distinct (violations → 400); the selection is fixed for the lifetime of the
connection.

```
GET /streams?channel=linac&channel=ring

id: 42
event: event
data: {"id":42,"eventKey":"trip-linac-07","time":"…","severity":"critical","message":"…","channel":"linac"}

id: 43
event: event
data: {"id":43,"eventKey":"orbit-warn","time":"…","severity":"warning","message":"…","channel":"ring"}
```

Every `event` frame keeps the **global** id and the original event fields and
adds the source `channel`. Frames are emitted strictly in ascending id order
(= global publish order) across the whole selection; events from unselected
channels never appear. The heartbeat cadence is the same 4 s.

Resume uses the same single global cursor: `Last-Event-ID: 42` (or
`?lastEventId=`) delivers every retained event of the selected channels with
id `> 42`; no cursor replays all retained events of the selection.

**410 — lossless resume impossible** (sent before any SSE bytes):

```json
{
  "error": "Last-Event-ID 5 fell behind the retention window of channel(s) \"linac\": events newer than the cursor were trimmed; resync without Last-Event-ID to receive every retained event of the selected channels",
  "channels": [
    {"channel": "linac", "earliestAvailableId": 18, "expired": true},
    {"channel": "ring",  "earliestAvailableId": 30, "expired": false}
  ]
}
```

The aggregate 410 fires exactly when a selected channel **trimmed events
newer than the cursor** — i.e. events the console never saw are gone and a
lossless continuation is impossible. Because ids interleave globally, a
merged cursor legitimately sits below another channel's first retained id
without any loss; that alone never triggers a 410. `channels[]` lists every
selected channel in request order with its live-window start
(`earliestAvailableId`, `0` while the channel is empty) and an `expired`
flag, so the console can locate exactly which sections overflowed. Dropping
the cursor and reconnecting yields every still-retained event of the
selection. If retention crosses the cursor mid-connection, the stream ends
with an `event: error` carrying the same per-channel body.

## Console recovery in publish order

Reconnecting with the last displayed global id restores, in id order
(= publish order), every still-available critical/warning event — on one
channel via `GET /streams/{channel}`, or across the whole monitored selection
via `GET /streams?channel=…` with the same single cursor. Nothing older than
`earliestAvailableId` can be sent (the 410 tells the console where each live
window starts, per channel); any conflicting batch returns a locatable 409
and changes nothing.

## Run with Docker

```bash
docker compose up -d --build app
curl -s localhost:8080/health

# Publish
curl -s -XPOST localhost:8080/events/linac -H 'content-type: application/json' \
  -d '[{"eventKey":"k1","time":"2026-10-05T12:00:00Z","severity":"critical","message":"dump"}]'

# Stream (Ctrl-C to stop)
curl -N localhost:8080/streams/linac

# Or watch several sections over one connection
curl -N "localhost:8080/streams?channel=linac&channel=ring"
```

## One-shot `verify`

Waits for `/health`, then aggregates **code tests, builds, publish/replay/
conflict, live SSE + heartbeat, resume, expired-cursor, the aggregated
multi-channel stream and real-restart durability** into one bitmask exit code
(`0` = all pass):

| bit | section |
|---|---|
| 1 | publish / global-ids / replay / 409 zero-write |
| 2 | SSE live delivery + 4 s heartbeat |
| 4 | SSE `Last-Event-ID` resume (gapless, exactly-once) |
| 8 | expired cursor → 410 + resync |
| 16 | `go test ./...` |
| 32 | build all binaries |
| 64 | durability across an actual process restart |
| 128 | aggregate stream: merged history, cross-channel live interleave, per-channel 410 + resync |

```bash
make verify                       # native one-shot gate
docker compose build verify && \
  docker compose run --rm verify  # containerized one-shot gate (build inside)
```

Also run the test suite with the race detector directly:

```bash
make test
```

## Project layout

```
cmd/server/         HTTP/SSE service entrypoint
cmd/smoke/          one-shot e2e verifier (publish/live/resume/410/aggregate)
cmd/restart-smoke/  two-phase durability verifier (before/after restart)
cmd/healthcheck/    tiny static /health probe for distroless HEALTHCHECK
internal/store/     append-only log: global ids, atomic append, WAL, retention
internal/api/       HTTP handlers + payloadless wake-up hub for SSE
scripts/verify.sh   aggregates build + tests + smoke + restart into exit code
Dockerfile          build → distroless non-root runtime (+ verify stage)
docker-compose.yml  app + one-shot verify service
```

## Design notes

- **One lock, one critical section per batch.** Conflict detection, id
  allocation and append are atomic by construction; there is no window in
  which a concurrent POST can see a half-applied batch.
- **Wake-up-only hub.** SSE connections never receive data through the
  fan-out layer. They register, read history, then re-read everything with
  `id > cursor` whenever the hub signals. That removes the entire class of
  registration-window races (lost/duplicated/reordered events). Aggregate
  connections register one signal under every selected channel and merge
  each re-read under the same store lock, so the cross-channel snapshot is
  always consistent.
- **Exact aggregate expiry boundary.** Trimming records the highest trimmed
  id per channel. The aggregate 410 compares the resume cursor against that
  boundary — lossless resume is impossible exactly when a selected channel
  dropped events newer than the cursor — instead of against each channel's
  earliest retained id, which would false-positive whenever global ids
  interleave across channels. The boundary is recomputed during WAL replay,
  so it is identical after a restart.
- **Full-history WAL, bounded live window.** Trimming changes only what
  reads expose; the idempotency index is rebuilt from the complete WAL on
  boot, so aged-out keys keep the same replay/conflict verdict forever and
  the 410 boundary is reconstructable after restart.
- **Fail-closed durability.** If the WAL write fails, the in-memory commit is
  rolled back and the POST 500s; the service never acknowledges an event it
  cannot survive a restart with.
