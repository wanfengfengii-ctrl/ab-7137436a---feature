package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"interlock-events/internal/store"
)

const (
	// MaxBatchSize is the maximum number of events accepted by one POST.
	MaxBatchSize = 50
	// HeartbeatInterval is the SSE idle comment interval (< 5s required).
	HeartbeatInterval = 4 * time.Second
)

// Config holds runtime knobs for the server.
type Config struct {
	Retention int
}

// Server wires the store, the fan-out hub and the HTTP mux.
type Server struct {
	store *store.Store
	hub   *hub
}

// New builds the HTTP handler backed by st.
func New(st *store.Store, _ Config) http.Handler {
	s := &Server{store: st, hub: newHub()}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("POST /events/{channel}", s.handlePost)
	mux.HandleFunc("GET /streams/{channel}", s.handleStream)
	return withCommon(mux)
}

func withCommon(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// postEvent is the wire shape of one inbound event.
type postEvent struct {
	EventKey string `json:"eventKey"`
	Time     string `json:"time"`
	Severity string `json:"severity"`
	Message  string `json:"message"`
}

// postResponse is returned for an accepted (or exactly replayed) batch.
type postResponse struct {
	Channel string               `json:"channel"`
	Results []store.AppendResult `json:"results"`
}

// errorBody is the standard error envelope. Conflict responses additionally
// embed the pinpoint list, so a rejected batch is directly locatable.
type errorBody struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorBody{Error: msg})
}

func (s *Server) handlePost(w http.ResponseWriter, r *http.Request) {
	channel := r.PathValue("channel")
	if strings.TrimSpace(channel) == "" {
		writeErr(w, http.StatusBadRequest, "channel must not be empty")
		return
	}

	var inbound []postEvent
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&inbound); err != nil {
		writeErr(w, http.StatusBadRequest, "request body must be a JSON array of events: "+err.Error())
		return
	}
	// Reject trailing data after the array (e.g. a second JSON document).
	var extra json.RawMessage
	if err := dec.Decode(&extra); err != io.EOF {
		writeErr(w, http.StatusBadRequest, "unexpected trailing data after the events array")
		return
	}
	if len(inbound) == 0 {
		writeErr(w, http.StatusBadRequest, "batch must contain at least 1 event")
		return
	}
	if len(inbound) > MaxBatchSize {
		writeErr(w, http.StatusBadRequest,
			fmt.Sprintf("batch must contain at most %d events, got %d", MaxBatchSize, len(inbound)))
		return
	}

	events := make([]store.Event, len(inbound))
	for i, e := range inbound {
		if strings.TrimSpace(e.EventKey) == "" {
			writeErr(w, http.StatusBadRequest, fmt.Sprintf("events[%d].eventKey must not be empty", i))
			return
		}
		if _, err := time.Parse(time.RFC3339, e.Time); err != nil {
			writeErr(w, http.StatusBadRequest,
				fmt.Sprintf("events[%d].time must be RFC3339 (e.g. 2026-10-05T12:00:00Z): %v", i, err))
			return
		}
		if strings.TrimSpace(e.Severity) == "" {
			writeErr(w, http.StatusBadRequest, fmt.Sprintf("events[%d].severity must not be empty", i))
			return
		}
		events[i] = store.Event{
			EventKey: e.EventKey,
			Time:     e.Time,
			Severity: e.Severity,
			Message:  e.Message,
		}
	}

	results, err := s.store.Append(channel, events)
	if err != nil {
		var ce *store.ConflictError
		if errors.As(err, &ce) {
			// 409: whole batch rejected, zero writes. Include per-event
			// detail so the operator can locate every offending key.
			writeJSON(w, http.StatusConflict, map[string]any{
				"error":   "conflict: one or more eventKeys already exist with different content; no events were written",
				"channel": channel,
				"items":   ce.Items,
			})
			return
		}
		writeErr(w, http.StatusInternalServerError, "persist failed: "+err.Error())
		return
	}

	// Wake live streams only after the commit is durable. Publishing once
	// per batch is enough: subscribers re-read from their cursor.
	s.hub.publish(channel)

	writeJSON(w, http.StatusOK, postResponse{Channel: channel, Results: results})
}

// goneBody answers an expired resume cursor.
type goneBody struct {
	Error             string `json:"error"`
	EarliestAvailable int64  `json:"earliestAvailableId"`
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	channel := r.PathValue("channel")
	if strings.TrimSpace(channel) == "" {
		writeErr(w, http.StatusBadRequest, "channel must not be empty")
		return
	}

	lastID, ok := parseLastEventID(r)
	if !ok {
		writeErr(w, http.StatusBadRequest, "Last-Event-ID must be a non-negative integer")
		return
	}

	// Register on the hub BEFORE reading history: a publish landing anywhere
	// around connection setup will set the signal and trigger another store
	// read, so nothing in the gap is lost or delivered twice.
	signal := s.hub.subscribe(channel)
	defer s.hub.unsubscribe(channel, signal)

	// Expired-cursor check must produce a real HTTP 410, which is only
	// possible before the SSE response starts.
	initial := s.store.ReadHistory(channel, lastID)
	if initial.Gone {
		writeJSON(w, http.StatusGone, goneBody{
			Error: fmt.Sprintf(
				"Last-Event-ID %d is older than the earliest retained id %d; resync from earliestAvailableId",
				lastID, initial.EarliestAvailable),
			EarliestAvailable: initial.EarliestAvailable,
		})
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache, must-revalidate")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "retry: 3000\n\n")
	flusher.Flush()

	ctx := r.Context()
	cursor := lastID

	heartbeat := time.NewTicker(HeartbeatInterval)
	defer heartbeat.Stop()

	// Drain anything already pending on the signal so the first wait is
	// driven only by commits that happen after the initial read.
	select {
	case <-signal:
	default:
	}

	for {
		// Re-read everything newer than the cursor. The store read is the
		// single source of truth; the hub only wakes us.
		res := s.store.ReadHistory(channel, cursor)
		if res.Gone {
			writeSSEEvent(w, "error", 0, goneBody{
				Error:             fmt.Sprintf("cursor %d expired due to retention; use earliestAvailableId", cursor),
				EarliestAvailable: res.EarliestAvailable,
			})
			flusher.Flush()
			return
		}
		for _, e := range res.Events {
			writeSSEEvent(w, "event", e.ID, e)
			cursor = e.ID
		}
		if len(res.Events) > 0 {
			flusher.Flush()
		}

		select {
		case <-ctx.Done():
			return
		case <-signal:
			// A commit may have happened; loop and re-read.
		case <-heartbeat.C:
			// SSE comment line; keeps proxies from closing an idle stream.
			_, _ = fmt.Fprintf(w, ": heartbeat %s\n\n", time.Now().UTC().Format(time.RFC3339))
			flusher.Flush()
		}
	}
}

// parseLastEventID reads the resume cursor from the Last-Event-ID header
// (also accepts ?lastEventId= as a convenience). Missing -> 0.
func parseLastEventID(r *http.Request) (int64, bool) {
	raw := r.Header.Get("Last-Event-ID")
	if raw == "" {
		raw = r.URL.Query().Get("lastEventId")
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, true
	}
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id < 0 {
		return 0, false
	}
	return id, true
}

// writeSSEEvent writes one event frame. id=0 means "do not emit an id line"
// (used for terminal error frames that must not advance a client cursor).
func writeSSEEvent(w http.ResponseWriter, event string, id int64, data any) {
	payload, err := json.Marshal(data)
	if err != nil {
		return
	}
	if id > 0 {
		_, _ = fmt.Fprintf(w, "id: %d\n", id)
	}
	_, _ = fmt.Fprintf(w, "event: %s\n", event)
	// JSON is encoded on a single line, but split defensively on any
	// embedded newline to keep SSE framing valid.
	for _, line := range splitForSSE(payload) {
		_, _ = fmt.Fprintf(w, "data: %s\n", line)
	}
	_, _ = w.Write([]byte("\n"))
}

func splitForSSE(b []byte) []string {
	return strings.Split(string(b), "\n")
}
