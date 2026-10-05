package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"interlock-events/internal/store"
)

const rfc = "2026-10-05T12:00:00Z"

func newTestServer(t *testing.T, retention int) (*httptest.Server, *store.Store) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "data")
	st, err := store.Open(dir, retention)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(New(st, Config{Retention: retention}))
	t.Cleanup(srv.Close)
	return srv, st
}

func postBatch(t *testing.T, base, channel string, evs []map[string]any) (int, []byte) {
	t.Helper()
	body, _ := json.Marshal(evs)
	resp, err := http.Post(base+"/events/"+channel, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

func oneEvent(key, sev, msg string) map[string]any {
	return map[string]any{"eventKey": key, "time": rfc, "severity": sev, "message": msg}
}

func TestHealth(t *testing.T) {
	srv, _ := newTestServer(t, 0)
	resp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("health status %d", resp.StatusCode)
	}
}

func TestPostValidationAndResults(t *testing.T) {
	srv, _ := newTestServer(t, 0)

	if code, _ := postBatch(t, srv.URL, "ch", nil); code != http.StatusBadRequest {
		t.Fatalf("empty batch = %d", code)
	}
	big := make([]map[string]any, 51)
	for i := range big {
		big[i] = oneEvent(fmt.Sprintf("b%d", i), "info", "x")
	}
	if code, _ := postBatch(t, srv.URL, "ch", big); code != http.StatusBadRequest {
		t.Fatalf("51 batch = %d", code)
	}
	badTime := oneEvent("k", "info", "x")
	badTime["time"] = "not-a-time"
	if code, raw := postBatch(t, srv.URL, "ch", []map[string]any{badTime}); code != http.StatusBadRequest ||
		!strings.Contains(string(raw), "RFC3339") {
		t.Fatalf("bad time = %d %s", code, raw)
	}

	code, raw := postBatch(t, srv.URL, "ch", []map[string]any{
		oneEvent("trip", "critical", "dump"),
		oneEvent("warn", "warning", "hot"),
	})
	if code != 200 {
		t.Fatalf("fresh = %d %s", code, raw)
	}
	var first struct {
		Results []store.AppendResult `json:"results"`
	}
	if err := json.Unmarshal(raw, &first); err != nil {
		t.Fatal(err)
	}
	if len(first.Results) != 2 || first.Results[0].ID != 1 || first.Results[1].ID != 2 {
		t.Fatalf("results = %s", raw)
	}

	// Exact retry: same ids, replay true.
	code, raw = postBatch(t, srv.URL, "ch", []map[string]any{
		oneEvent("trip", "critical", "dump"),
		oneEvent("warn", "warning", "hot"),
	})
	if code != 200 {
		t.Fatalf("retry = %d", code)
	}
	json.Unmarshal(raw, &first)
	if first.Results[0].ID != 1 || !first.Results[0].Replay || first.Results[1].ID != 2 {
		t.Fatalf("replay results = %s", raw)
	}

	// Different content -> 409 with locatable items, zero writes.
	code, raw = postBatch(t, srv.URL, "ch", []map[string]any{
		oneEvent("trip", "critical", "edited message"),
	})
	if code != http.StatusConflict {
		t.Fatalf("conflict = %d %s", code, raw)
	}
	var cb struct {
		Items []struct {
			Index    int    `json:"index"`
			EventKey string `json:"eventKey"`
			Existing struct {
				ID int64 `json:"id"`
			} `json:"existing"`
		} `json:"items"`
	}
	if err := json.Unmarshal(raw, &cb); err != nil || len(cb.Items) != 1 ||
		cb.Items[0].Index != 0 || cb.Items[0].EventKey != "trip" || cb.Items[0].Existing.ID != 1 {
		t.Fatalf("conflict body not locatable: %s", raw)
	}
}

// frame is a parsed SSE dispatch.
type frame struct {
	id      int64
	event   string
	data    string
	comment bool
}

func openSSE(ctx context.Context, t *testing.T, base, channel string, lastID int64) (int, <-chan frame) {
	t.Helper()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/streams/"+channel, nil)
	if lastID > 0 {
		req.Header.Set("Last-Event-ID", strconv.FormatInt(lastID, 10))
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		_, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
		return resp.StatusCode, nil
	}
	out := make(chan frame, 64)
	go func() {
		defer close(out)
		defer resp.Body.Close()
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
		var f frame
		var data []string
		flush := func() {
			if f.id == 0 && f.event == "" && len(data) == 0 {
				return
			}
			f.data = strings.Join(data, "\n")
			select {
			case out <- f:
			case <-ctx.Done():
			}
			f, data = frame{}, nil
		}
		for sc.Scan() {
			line := sc.Text()
			switch {
			case line == "":
				flush()
			case strings.HasPrefix(line, ":"):
				select {
				case out <- frame{comment: true}:
				case <-ctx.Done():
					return
				}
			case strings.HasPrefix(line, "id:"):
				f.id, _ = strconv.ParseInt(strings.TrimSpace(line[3:]), 10, 64)
			case strings.HasPrefix(line, "event:"):
				f.event = strings.TrimSpace(line[6:])
			case strings.HasPrefix(line, "data:"):
				d := line[5:]
				d = strings.TrimPrefix(d, " ")
				data = append(data, d)
			}
		}
	}()
	return http.StatusOK, out
}

func waitEvent(t *testing.T, frames <-chan frame, timeout time.Duration) frame {
	t.Helper()
	for {
		select {
		case f := <-frames:
			if f.comment {
				continue
			}
			return f
		case <-time.After(timeout):
			t.Fatal("timed out waiting for event frame")
		}
	}
}

func TestSSEHistoryLiveAndHeartbeat(t *testing.T) {
	srv, _ := newTestServer(t, 0)
	postBatch(t, srv.URL, "ch", []map[string]any{oneEvent("h1", "warning", "history one")})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	code, frames := openSSE(ctx, t, srv.URL, "ch", 0)
	if code != 200 {
		t.Fatalf("stream code %d", code)
	}

	f := waitEvent(t, frames, 3*time.Second)
	if f.id != 1 || f.event != "event" || !strings.Contains(f.data, "history one") {
		t.Fatalf("history frame = %+v", f)
	}

	// Idle heartbeat within 5s.
	select {
	case f := <-frames:
		if !f.comment {
			t.Fatalf("expected heartbeat comment, got %+v", f)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no heartbeat within 5s")
	}

	// Live publish.
	postBatch(t, srv.URL, "ch", []map[string]any{oneEvent("live1", "critical", "beam off")})
	f = waitEvent(t, frames, 3*time.Second)
	if f.id != 2 || !strings.Contains(f.data, "beam off") {
		t.Fatalf("live frame = %+v", f)
	}
}

func TestSSEResumeExactlyOnceGapless(t *testing.T) {
	srv, _ := newTestServer(t, 0)
	ch := "resume"
	postBatch(t, srv.URL, ch, []map[string]any{oneEvent("a", "info", "1")})
	postBatch(t, srv.URL, ch, []map[string]any{oneEvent("b", "info", "2")})

	ctx1, cancel1 := context.WithTimeout(context.Background(), 8*time.Second)
	code, frames1 := openSSE(ctx1, t, srv.URL, ch, 0)
	if code != 200 {
		t.Fatal(code)
	}
	f1 := waitEvent(t, frames1, 3*time.Second)
	f2 := waitEvent(t, frames1, 3*time.Second)
	if f1.id != 1 || f2.id != 2 {
		t.Fatalf("history %d,%d", f1.id, f2.id)
	}
	cancel1() // network drop

	// Published while disconnected.
	postBatch(t, srv.URL, ch, []map[string]any{oneEvent("c", "info", "3")})
	postBatch(t, srv.URL, ch, []map[string]any{oneEvent("d", "critical", "4")})

	ctx2, cancel2 := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel2()
	_, frames2 := openSSE(ctx2, t, srv.URL, ch, 2)
	g1 := waitEvent(t, frames2, 3*time.Second)
	g2 := waitEvent(t, frames2, 3*time.Second)
	if g1.id != 3 || g2.id != 4 {
		t.Fatalf("gap resume = %d,%d", g1.id, g2.id)
	}

	// Seamless live event after resume, delivered exactly once.
	postBatch(t, srv.URL, ch, []map[string]any{oneEvent("e", "info", "5")})
	live := waitEvent(t, frames2, 3*time.Second)
	if live.id != 5 {
		t.Fatalf("post-resume live = %d", live.id)
	}
	select {
	case extra := <-frames2:
		if !extra.comment {
			t.Fatalf("duplicate/extra frame id=%d", extra.id)
		}
	case <-time.After(1500 * time.Millisecond):
	}
}

func TestSSEExpiredCursor410(t *testing.T) {
	srv, _ := newTestServer(t, 2)
	ch := "trim"
	postBatch(t, srv.URL, ch, []map[string]any{oneEvent("a", "info", "1")}) // id 1
	postBatch(t, srv.URL, ch, []map[string]any{oneEvent("b", "info", "2")}) // id 2
	postBatch(t, srv.URL, ch, []map[string]any{oneEvent("c", "info", "3")}) // id 3 -> window 2,3

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/streams/"+ch, nil)
	req.Header.Set("Last-Event-ID", "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusGone {
		t.Fatalf("want 410, got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Fatalf("410 content-type = %q", ct)
	}
	var body struct {
		EarliestAvailable int64 `json:"earliestAvailableId"`
	}
	raw, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatal(err)
	}
	if body.EarliestAvailable != 2 {
		t.Fatalf("earliestAvailableId = %d, want 2, body=%s", body.EarliestAvailable, raw)
	}
}

func TestConcurrentPublishesExactlyOnce(t *testing.T) {
	srv, _ := newTestServer(t, 0)
	ch := "concurrent"

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, frames := openSSE(ctx, t, srv.URL, ch, 0)

	const publishers = 8
	const perPub = 15
	var wg sync.WaitGroup
	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perPub; i++ {
				key := fmt.Sprintf("p%d-e%d", p, i)
				code, raw := postBatch(t, srv.URL, ch, []map[string]any{oneEvent(key, "info", key)})
				if code != 200 {
					t.Errorf("publish %s: %d %s", key, code, raw)
					return
				}
			}
		}(p)
	}
	wg.Wait()

	want := publishers * perPub
	got := make(map[int64]string, want)
	var prev int64
	deadline := time.After(10 * time.Second)
	for len(got) < want {
		select {
		case f := <-frames:
			if f.comment {
				continue
			}
			if f.id <= prev {
				t.Fatalf("non-monotonic/duplicate delivery: id %d after %d", f.id, prev)
			}
			if _, dup := got[f.id]; dup {
				t.Fatalf("id %d delivered twice", f.id)
			}
			prev = f.id
			var data struct {
				EventKey string `json:"eventKey"`
			}
			json.Unmarshal([]byte(f.data), &data)
			got[f.id] = data.EventKey
		case <-deadline:
			t.Fatalf("got %d/%d events", len(got), want)
		}
	}
	if len(got) != want {
		t.Fatalf("unique ids = %d, want %d", len(got), want)
	}
}

// ---- aggregated multi-channel stream (GET /streams?channel=...) -----------

// aggURL builds the aggregate stream URL for the given channel set.
func aggURL(base string, channels []string) string {
	q := make(url.Values)
	for _, c := range channels {
		q.Add("channel", c)
	}
	return base + "/streams?" + q.Encode()
}

// openSSEMulti is openSSE for the aggregate endpoint.
func openSSEMulti(ctx context.Context, t *testing.T, base string, channels []string, lastID int64) (int, <-chan frame) {
	t.Helper()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, aggURL(base, channels), nil)
	if lastID > 0 {
		req.Header.Set("Last-Event-ID", strconv.FormatInt(lastID, 10))
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		_, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
		return resp.StatusCode, nil
	}
	out := make(chan frame, 256)
	go func() {
		defer close(out)
		defer resp.Body.Close()
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
		var f frame
		var data []string
		flush := func() {
			if f.id == 0 && f.event == "" && len(data) == 0 {
				return
			}
			f.data = strings.Join(data, "\n")
			select {
			case out <- f:
			case <-ctx.Done():
			}
			f, data = frame{}, nil
		}
		for sc.Scan() {
			line := sc.Text()
			switch {
			case line == "":
				flush()
			case strings.HasPrefix(line, ":"):
				select {
				case out <- frame{comment: true}:
				case <-ctx.Done():
					return
				}
			case strings.HasPrefix(line, "id:"):
				f.id, _ = strconv.ParseInt(strings.TrimSpace(line[3:]), 10, 64)
			case strings.HasPrefix(line, "event:"):
				f.event = strings.TrimSpace(line[6:])
			case strings.HasPrefix(line, "data:"):
				d := line[5:]
				d = strings.TrimPrefix(d, " ")
				data = append(data, d)
			}
		}
	}()
	return http.StatusOK, out
}

// aggStatus performs a GET against the aggregate endpoint and returns the
// status plus raw body (200 responses are closed immediately).
func aggStatus(t *testing.T, base string, channels []string, lastID int64) (int, []byte) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, aggURL(base, channels), nil)
	if lastID > 0 {
		req.Header.Set("Last-Event-ID", strconv.FormatInt(lastID, 10))
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

// channelOf extracts the source channel from an aggregate frame payload.
func channelOf(t *testing.T, f frame) string {
	t.Helper()
	var data struct {
		ID      int64  `json:"id"`
		Channel string `json:"channel"`
	}
	if err := json.Unmarshal([]byte(f.data), &data); err != nil {
		t.Fatalf("frame data not JSON: %q", f.data)
	}
	if data.ID != f.id {
		t.Fatalf("frame id line %d disagrees with payload id %d", f.id, data.ID)
	}
	return data.Channel
}

func TestAggregateStreamValidation(t *testing.T) {
	srv, _ := newTestServer(t, 0)

	cases := []struct {
		name     string
		channels []string
	}{
		{"none", nil},
		{"empty name", []string{"linac", ""}},
		{"blank name", []string{"  "}},
		{"duplicate", []string{"linac", "ring", "linac"}},
		{"nine", []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"}},
	}
	for _, tc := range cases {
		code, raw := aggStatus(t, srv.URL, tc.channels, 0)
		if code != http.StatusBadRequest {
			t.Fatalf("%s: want 400, got %d: %s", tc.name, code, raw)
		}
	}

	// One and eight channels are valid selections.
	for _, n := range []int{1, 8} {
		channels := make([]string, n)
		for i := range channels {
			channels[i] = fmt.Sprintf("valid-%d", i)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		code, frames := openSSEMulti(ctx, t, srv.URL, channels, 0)
		if code != 200 {
			cancel()
			t.Fatalf("%d channels: want 200, got %d", n, code)
		}
		cancel()
		for range frames { // drain
		}
	}

	// A malformed cursor is still a 400 on the aggregate endpoint.
	code, _ := func() (int, []byte) {
		req, _ := http.NewRequest(http.MethodGet, aggURL(srv.URL, []string{"x"}), nil)
		req.Header.Set("Last-Event-ID", "abc")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, raw
	}()
	if code != http.StatusBadRequest {
		t.Fatalf("bad cursor: want 400, got %d", code)
	}
}

func TestAggregateHistoryMergesChannelsInIDOrder(t *testing.T) {
	srv, _ := newTestServer(t, 0)

	// Interleave publishes across two selected channels and one unselected.
	postBatch(t, srv.URL, "alpha", []map[string]any{oneEvent("a1", "critical", "alpha one")}) // id 1
	postBatch(t, srv.URL, "beta", []map[string]any{oneEvent("b1", "warning", "beta one")})    // id 2
	postBatch(t, srv.URL, "alpha", []map[string]any{oneEvent("a2", "info", "alpha two")})     // id 3
	postBatch(t, srv.URL, "gamma", []map[string]any{oneEvent("g1", "critical", "gamma one")}) // id 4 (unselected)
	postBatch(t, srv.URL, "beta", []map[string]any{oneEvent("b2", "critical", "beta two")})   // id 5

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	code, frames := openSSEMulti(ctx, t, srv.URL, []string{"alpha", "beta"}, 0)
	if code != 200 {
		t.Fatalf("stream code %d", code)
	}

	want := []struct {
		id      int64
		channel string
		msg     string
	}{
		{1, "alpha", "alpha one"},
		{2, "beta", "beta one"},
		{3, "alpha", "alpha two"},
		{5, "beta", "beta two"},
	}
	for _, w := range want {
		f := waitEvent(t, frames, 3*time.Second)
		if f.id != w.id || f.event != "event" {
			t.Fatalf("frame = id %d event %q, want id %d", f.id, f.event, w.id)
		}
		if ch := channelOf(t, f); ch != w.channel {
			t.Fatalf("frame %d channel = %q, want %q", f.id, ch, w.channel)
		}
		if !strings.Contains(f.data, w.msg) {
			t.Fatalf("frame %d missing original fields: %s", f.id, f.data)
		}
	}

	// The unselected channel must never appear; nothing else may arrive.
	select {
	case f := <-frames:
		if !f.comment {
			t.Fatalf("unexpected extra frame id=%d (gamma id 4 must be filtered)", f.id)
		}
	case <-time.After(1500 * time.Millisecond):
	}
}

func TestAggregateLiveInterleaveHeartbeatAndResume(t *testing.T) {
	srv, _ := newTestServer(t, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	code, frames := openSSEMulti(ctx, t, srv.URL, []string{"west", "east"}, 0)
	if code != 200 {
		t.Fatalf("stream code %d", code)
	}

	// Idle heartbeat within 5s, same cadence as the single-channel stream.
	select {
	case f := <-frames:
		if !f.comment {
			t.Fatalf("expected heartbeat comment, got %+v", f)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no aggregate heartbeat within 5s")
	}

	// Live publishes alternating across channels arrive in global id order,
	// each tagged with its source channel, exactly once.
	postBatch(t, srv.URL, "east", []map[string]any{oneEvent("e1", "warning", "east one")})  // id 1
	postBatch(t, srv.URL, "west", []map[string]any{oneEvent("w1", "critical", "west one")}) // id 2
	postBatch(t, srv.URL, "east", []map[string]any{oneEvent("e2", "critical", "east two")}) // id 3
	postBatch(t, srv.URL, "elsewhere", []map[string]any{oneEvent("x1", "info", "noise")})   // id 4 (unselected)
	postBatch(t, srv.URL, "west", []map[string]any{oneEvent("w2", "info", "west two")})     // id 5

	wantIDs := []int64{1, 2, 3, 5}
	wantCh := []string{"east", "west", "east", "west"}
	for i := range wantIDs {
		f := waitEvent(t, frames, 3*time.Second)
		if f.id != wantIDs[i] {
			t.Fatalf("live frame %d: want id %d, got %d", i, wantIDs[i], f.id)
		}
		if ch := channelOf(t, f); ch != wantCh[i] {
			t.Fatalf("live frame id %d: want channel %q, got %q", f.id, wantCh[i], ch)
		}
	}
	select {
	case f := <-frames:
		if !f.comment {
			t.Fatalf("duplicate/unselected frame id=%d", f.id)
		}
	case <-time.After(1500 * time.Millisecond):
	}
	cancel()

	// Resume with a cursor: only strictly newer events, still merged in order.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel2()
	code, frames2 := openSSEMulti(ctx2, t, srv.URL, []string{"west", "east"}, 2)
	if code != 200 {
		t.Fatalf("resume code %d", code)
	}
	for _, want := range []int64{3, 5} {
		f := waitEvent(t, frames2, 3*time.Second)
		if f.id != want {
			t.Fatalf("resume frame: want id %d, got %d", want, f.id)
		}
	}
}

func TestAggregateExpiredCursor410PerChannel(t *testing.T) {
	srv, _ := newTestServer(t, 2) // retention 2 per channel

	postBatch(t, srv.URL, "ring", []map[string]any{oneEvent("r1", "info", "1")})  // id 1
	postBatch(t, srv.URL, "ring", []map[string]any{oneEvent("r2", "info", "2")})  // id 2
	postBatch(t, srv.URL, "ring", []map[string]any{oneEvent("r3", "info", "3")})  // id 3
	postBatch(t, srv.URL, "linac", []map[string]any{oneEvent("l1", "info", "4")}) // id 4
	postBatch(t, srv.URL, "ring", []map[string]any{oneEvent("r4", "info", "5")})  // id 5
	// ring retained window: ids 3,5 (ids 1,2 trimmed). linac window: id 4.

	// Cursor 1: ring trimmed ids 1,2 — id 2 is newer than the cursor and
	// unrecoverable, so lossless resume is impossible -> 410. linac trimmed
	// nothing and must be reported as not expired.
	code, raw := aggStatus(t, srv.URL, []string{"ring", "linac"}, 1)
	if code != http.StatusGone {
		t.Fatalf("want 410, got %d: %s", code, raw)
	}
	req, _ := http.NewRequest(http.MethodGet, aggURL(srv.URL, []string{"ring", "linac"}), nil)
	req.Header.Set("Last-Event-ID", "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	ct := resp.Header.Get("Content-Type")
	resp.Body.Close()
	if !strings.Contains(ct, "application/json") {
		t.Fatalf("410 content-type = %q", ct)
	}
	var body struct {
		Error    string `json:"error"`
		Channels []struct {
			Channel           string `json:"channel"`
			EarliestAvailable int64  `json:"earliestAvailableId"`
			Expired           bool   `json:"expired"`
		} `json:"channels"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("410 body not JSON: %s", raw)
	}
	if len(body.Channels) != 2 {
		t.Fatalf("want per-channel boundaries for both channels: %s", raw)
	}
	if body.Channels[0].Channel != "ring" || body.Channels[0].EarliestAvailable != 3 || !body.Channels[0].Expired {
		t.Fatalf("ring boundary wrong: %+v", body.Channels[0])
	}
	if body.Channels[1].Channel != "linac" || body.Channels[1].EarliestAvailable != 4 || body.Channels[1].Expired {
		t.Fatalf("linac boundary wrong (linac trimmed nothing): %+v", body.Channels[1])
	}
	if !strings.Contains(body.Error, "ring") {
		t.Fatalf("error must name the expired channel: %q", body.Error)
	}

	// Cursor 2 is exactly at the trim boundary: everything newer (3,4,5) is
	// still retained, so the resume is lossless and must be accepted even
	// though the cursor is below linac's earliest retained id.
	ctx0, cancel0 := context.WithTimeout(context.Background(), 5*time.Second)
	code, frames0 := openSSEMulti(ctx0, t, srv.URL, []string{"ring", "linac"}, 2)
	if code != 200 {
		cancel0()
		t.Fatalf("cursor 2: want 200, got %d", code)
	}
	for _, want := range []int64{3, 4, 5} {
		f := waitEvent(t, frames0, 3*time.Second)
		if f.id != want {
			cancel0()
			t.Fatalf("cursor 2 frame: want id %d, got %d", want, f.id)
		}
	}
	cancel0()

	// Dropping the cursor resyncs everything still retained, in id order.
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	code, frames2 := openSSEMulti(ctx, t, srv.URL, []string{"ring", "linac"}, 0)
	if code != 200 {
		t.Fatalf("resync code %d", code)
	}
	wantIDs := []int64{3, 4, 5}
	wantCh := []string{"ring", "linac", "ring"}
	for i := range wantIDs {
		f := waitEvent(t, frames2, 3*time.Second)
		if f.id != wantIDs[i] || channelOf(t, f) != wantCh[i] {
			t.Fatalf("resync frame %d: want %d/%s, got %d/%s", i, wantIDs[i], wantCh[i], f.id, channelOf(t, f))
		}
	}
}

func TestAggregateConcurrentPublishesExactlyOnce(t *testing.T) {
	srv, _ := newTestServer(t, 0)
	channels := []string{"agg-a", "agg-b"}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	code, frames := openSSEMulti(ctx, t, srv.URL, channels, 0)
	if code != 200 {
		t.Fatalf("stream code %d", code)
	}

	const publishers = 4 // per channel
	const perPub = 10
	var wg sync.WaitGroup
	for _, ch := range channels {
		for p := 0; p < publishers; p++ {
			wg.Add(1)
			go func(ch string, p int) {
				defer wg.Done()
				for i := 0; i < perPub; i++ {
					key := fmt.Sprintf("%s-p%d-e%d", ch, p, i)
					if code, raw := postBatch(t, srv.URL, ch, []map[string]any{oneEvent(key, "info", key)}); code != 200 {
						t.Errorf("publish %s: %d %s", key, code, raw)
						return
					}
				}
			}(ch, p)
		}
	}
	wg.Wait()

	want := len(channels) * publishers * perPub
	got := make(map[int64]string, want)
	var prev int64
	deadline := time.After(10 * time.Second)
	for len(got) < want {
		select {
		case f := <-frames:
			if f.comment {
				continue
			}
			if f.id <= prev {
				t.Fatalf("non-monotonic/duplicate delivery: id %d after %d", f.id, prev)
			}
			if _, dup := got[f.id]; dup {
				t.Fatalf("id %d delivered twice", f.id)
			}
			prev = f.id
			got[f.id] = channelOf(t, f)
		case <-deadline:
			t.Fatalf("got %d/%d events", len(got), want)
		}
	}
	for id, ch := range got {
		if ch != "agg-a" && ch != "agg-b" {
			t.Fatalf("id %d tagged with unselected channel %q", id, ch)
		}
	}
}
