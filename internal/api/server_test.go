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
	return openSSEPath(ctx, t, base, "/streams/"+channel, lastID)
}

// openAggregateSSE opens GET /streams with repeated channel query parameters.
func openAggregateSSE(ctx context.Context, t *testing.T, base string, channels []string, lastID int64) (int, <-chan frame) {
	t.Helper()
	q := url.Values{}
	for _, c := range channels {
		q.Add("channel", c)
	}
	return openSSEPath(ctx, t, base, "/streams?"+q.Encode(), lastID)
}

func openSSEPath(ctx context.Context, t *testing.T, base, path string, lastID int64) (int, <-chan frame) {
	t.Helper()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+path, nil)
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
		case f, ok := <-frames:
			if !ok {
				t.Fatal("SSE stream closed before expected event frame arrived")
			}
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

// ---- aggregate GET /streams?channel=... ------------------------------------

func aggEventData(t *testing.T, f frame) struct {
	ID       int64  `json:"id"`
	Channel  string `json:"channel"`
	EventKey string `json:"eventKey"`
} {
	t.Helper()
	var d struct {
		ID       int64  `json:"id"`
		Channel  string `json:"channel"`
		EventKey string `json:"eventKey"`
	}
	if err := json.Unmarshal([]byte(f.data), &d); err != nil {
		t.Fatalf("frame data not json: %v (%s)", err, f.data)
	}
	return d
}

func TestAggregateStreamValidation(t *testing.T) {
	srv, _ := newTestServer(t, 0)
	get := func(raw string) int {
		resp, err := http.Get(srv.URL + "/streams" + raw)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp.StatusCode
	}

	if code := get(""); code != http.StatusBadRequest {
		t.Fatalf("no channel param = %d, want 400", code)
	}
	if code := get("?channel=&channel=b"); code != http.StatusBadRequest {
		t.Fatalf("empty channel = %d, want 400", code)
	}
	if code := get("?channel=a&channel=a"); code != http.StatusBadRequest {
		t.Fatalf("duplicate channel = %d, want 400", code)
	}
	if code := get("?channel=" + strings.Repeat("c&channel=", 8) + "c"); code != http.StatusBadRequest {
		t.Fatalf("nine channels = %d, want 400", code)
	}
	// Bad cursor is rejected even with a valid channel set.
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/streams?channel=a", nil)
	req.Header.Set("Last-Event-ID", "nope")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad cursor = %d, want 400", resp.StatusCode)
	}

	// Exactly eight distinct channels is accepted and streams.
	q := url.Values{}
	for i := 0; i < 8; i++ {
		q.Add("channel", fmt.Sprintf("c%d", i))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	code, frames := openSSEPath(ctx, t, srv.URL, "/streams?"+q.Encode(), 0)
	if code != http.StatusOK {
		t.Fatalf("eight channels = %d, want 200", code)
	}
	select {
	case f := <-frames:
		if !f.comment {
			t.Fatalf("idle aggregate stream should heartbeat first, got %+v", f)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no heartbeat on idle aggregate stream")
	}
}

func TestAggregateHistoryMergedWithChannelField(t *testing.T) {
	srv, _ := newTestServer(t, 0)
	postBatch(t, srv.URL, "linac", []map[string]any{oneEvent("l1", "critical", "linac one")})
	postBatch(t, srv.URL, "ring", []map[string]any{oneEvent("r1", "warning", "ring one")})
	postBatch(t, srv.URL, "ring", []map[string]any{oneEvent("r2", "warning", "ring two")})
	postBatch(t, srv.URL, "linac", []map[string]any{oneEvent("l2", "critical", "linac two")})

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	code, frames := openAggregateSSE(ctx, t, srv.URL, []string{"linac", "ring"}, 0)
	if code != http.StatusOK {
		t.Fatalf("aggregate stream code %d", code)
	}

	want := []struct {
		id      int64
		channel string
		key     string
	}{
		{1, "linac", "l1"},
		{2, "ring", "r1"},
		{3, "ring", "r2"},
		{4, "linac", "l2"},
	}
	for _, w := range want {
		f := waitEvent(t, frames, 3*time.Second)
		if f.id != w.id {
			t.Fatalf("frame id = %d, want %d (global order broken)", f.id, w.id)
		}
		d := aggEventData(t, f)
		if d.Channel != w.channel || d.EventKey != w.key || d.ID != w.id {
			t.Fatalf("frame = %+v, want channel %q key %q", d, w.channel, w.key)
		}
	}
}

func TestAggregateLiveInterleavingFiltersUnselected(t *testing.T) {
	srv, _ := newTestServer(t, 0)
	channels := []string{"linac", "ring"}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	code, frames := openAggregateSSE(ctx, t, srv.URL, channels, 0)
	if code != http.StatusOK {
		t.Fatalf("aggregate stream code %d", code)
	}

	// Alternate publishes across the two watched channels; also publish on a
	// channel that is NOT part of the fixed subscription set.
	const perChannel = 6
	wantByID := make(map[int64]string)
	var prev int64
	for i := 0; i < perChannel; i++ {
		for _, ch := range channels {
			key := fmt.Sprintf("%s-live-%d", ch, i)
			code, raw := postBatch(t, srv.URL, ch, []map[string]any{oneEvent(key, "critical", key)})
			if code != 200 {
				t.Fatalf("publish %s: %d %s", key, code, raw)
			}
			var pr struct {
				Results []store.AppendResult `json:"results"`
			}
			json.Unmarshal(raw, &pr)
			id := pr.Results[0].ID
			if id <= prev {
				t.Fatalf("publish ids not increasing: %d after %d", id, prev)
			}
			prev = id
			wantByID[id] = ch
		}
		postBatch(t, srv.URL, "ignored", []map[string]any{oneEvent(
			fmt.Sprintf("ignore-%d", i), "info", "must not appear")})
	}

	got := make(map[int64]string, len(wantByID))
	var deliveredPrev int64
	for len(got) < len(wantByID) {
		f := waitEvent(t, frames, 10*time.Second)
		d := aggEventData(t, f)
		if d.Channel == "ignored" {
			t.Fatalf("event from unselected channel delivered: id=%d", f.id)
		}
		want, ok := wantByID[f.id]
		if !ok {
			t.Fatalf("unexpected id %d (unselected channel or duplicate)", f.id)
		}
		if d.Channel != want {
			t.Fatalf("id %d attributed to %q, want %q", f.id, d.Channel, want)
		}
		if _, dup := got[f.id]; dup {
			t.Fatalf("id %d delivered twice", f.id)
		}
		if f.id <= deliveredPrev {
			t.Fatalf("non-monotonic delivery: %d arrived after %d", f.id, deliveredPrev)
		}
		deliveredPrev = f.id
		got[f.id] = d.Channel
	}
}

func TestAggregateResumeMergedAcrossGap(t *testing.T) {
	srv, _ := newTestServer(t, 0)
	channels := []string{"linac", "ring"}

	publishID := func(ch, key string) int64 {
		code, raw := postBatch(t, srv.URL, ch, []map[string]any{oneEvent(key, "warning", key)})
		if code != 200 {
			t.Fatalf("publish %s: %d %s", key, code, raw)
		}
		var pr struct {
			Results []store.AppendResult `json:"results"`
		}
		json.Unmarshal(raw, &pr)
		return pr.Results[0].ID
	}
	publishID("linac", "pre-l")
	publishID("ring", "pre-r")

	ctx1, cancel1 := context.WithTimeout(context.Background(), 8*time.Second)
	code, frames1 := openAggregateSSE(ctx1, t, srv.URL, channels, 0)
	if code != 200 {
		t.Fatal(code)
	}
	f1 := waitEvent(t, frames1, 3*time.Second)
	f2 := waitEvent(t, frames1, 3*time.Second)
	if f1.id != 1 || f2.id != 2 {
		t.Fatalf("history ids = %d,%d want 1,2", f1.id, f2.id)
	}
	cancel1()

	// Gap while disconnected, spanning both channels in global order.
	var gap []struct {
		id      int64
		channel string
	}
	gap = append(gap, struct {
		id      int64
		channel string
	}{publishID("ring", "gap-r1"), "ring"})
	gap = append(gap, struct {
		id      int64
		channel string
	}{publishID("linac", "gap-l1"), "linac"})
	gap = append(gap, struct {
		id      int64
		channel string
	}{publishID("ring", "gap-r2"), "ring"})

	ctx2, cancel2 := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel2()
	_, frames2 := openAggregateSSE(ctx2, t, srv.URL, channels, 2)
	for _, w := range gap {
		f := waitEvent(t, frames2, 3*time.Second)
		if f.id != w.id {
			t.Fatalf("gap resume out of order: id %d, want %d", f.id, w.id)
		}
		d := aggEventData(t, f)
		if d.Channel != w.channel {
			t.Fatalf("id %d channel = %q, want %q", f.id, d.Channel, w.channel)
		}
	}

	// Live seam after the resumed gap.
	liveID := publishID("linac", "post-resume")
	f := waitEvent(t, frames2, 3*time.Second)
	if f.id != liveID {
		t.Fatalf("post-resume live = %d want %d", f.id, liveID)
	}
	select {
	case extra := <-frames2:
		if !extra.comment {
			t.Fatalf("duplicate/extra frame id=%d", extra.id)
		}
	case <-time.After(1200 * time.Millisecond):
	}
}

func TestAggregateExpiredCursor410PerChannel(t *testing.T) {
	srv, _ := newTestServer(t, 3)
	// linac gets id 1; ring id 2; three more linac ids 3,4,5 trim linac's
	// window to [3,4,5] while ring keeps [2]; ring then gets id 6.
	postBatch(t, srv.URL, "linac", []map[string]any{oneEvent("old", "info", "old")})
	postBatch(t, srv.URL, "ring", []map[string]any{oneEvent("b0", "info", "b0")})
	postBatch(t, srv.URL, "linac", []map[string]any{
		oneEvent("a1", "info", "1"),
		oneEvent("a2", "info", "2"),
		oneEvent("a3", "info", "3"),
	})
	postBatch(t, srv.URL, "ring", []map[string]any{oneEvent("b1", "info", "b1")})

	// Cursor 2 is inside ring's window (earliest 2) but aged out of linac's
	// window (earliest 3): the 410 must pinpoint linac and NOT ring, and must
	// arrive as JSON before any SSE bytes.
	req, _ := http.NewRequest(http.MethodGet,
		srv.URL+"/streams?channel=linac&channel=ring", nil)
	req.Header.Set("Last-Event-ID", "2")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusGone {
		t.Fatalf("want 410, got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Fatalf("410 must be JSON before SSE bytes, content-type %q", ct)
	}
	raw, _ := io.ReadAll(resp.Body)
	var body struct {
		EarliestAvailable map[string]int64 `json:"earliestAvailableId"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatal(err)
	}
	if len(body.EarliestAvailable) != 1 || body.EarliestAvailable["linac"] != 3 {
		t.Fatalf("410 boundaries = %s, want only linac=3", raw)
	}
	if _, flagged := body.EarliestAvailable["ring"]; flagged {
		t.Fatalf("ring (cursor still in window) must not be flagged: %s", raw)
	}

	// Reconnect WITHOUT a cursor: every still-retained event on BOTH channels
	// must arrive in global id order, and nothing below the per-channel
	// earliestAvailableId may appear.
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	code, frames := openAggregateSSE(ctx, t, srv.URL, []string{"linac", "ring"}, 0)
	if code != http.StatusOK {
		t.Fatalf("cursorless resync code %d", code)
	}
	want := []struct {
		id      int64
		channel string
	}{
		{2, "ring"},
		{3, "linac"},
		{4, "linac"},
		{5, "linac"},
		{6, "ring"},
	}
	var prev int64
	for _, w := range want {
		f := waitEvent(t, frames, 3*time.Second)
		if f.id <= prev || f.id != w.id {
			t.Fatalf("resync order: got %d, want %d (prev %d)", f.id, w.id, prev)
		}
		d := aggEventData(t, f)
		if d.Channel != w.channel {
			t.Fatalf("resync id %d channel %q want %q", f.id, d.Channel, w.channel)
		}
		prev = f.id
	}
}

func TestAggregateConcurrentPublishersExactlyOnce(t *testing.T) {
	srv, _ := newTestServer(t, 0)
	watched := []string{"linac", "ring", "booster"}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, frames := openAggregateSSE(ctx, t, srv.URL, watched, 0)

	const publishers = 6
	const perPub = 12
	var wg sync.WaitGroup
	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perPub; i++ {
				ch := watched[p%len(watched)]
				key := fmt.Sprintf("p%d-e%d", p, i)
				code, raw := postBatch(t, srv.URL, ch, []map[string]any{oneEvent(key, "info", key)})
				if code != 200 {
					t.Errorf("publish %s: %d %s", key, code, raw)
				}
			}
		}(p)
	}
	wg.Wait()

	want := publishers * perPub
	allowed := map[string]bool{"linac": true, "ring": true, "booster": true}
	got := make(map[int64]string, want)
	var prev int64
	for len(got) < want {
		f := waitEvent(t, frames, 10*time.Second)
		if f.id <= prev {
			t.Fatalf("non-monotonic/duplicate aggregate delivery: %d after %d", f.id, prev)
		}
		if _, dup := got[f.id]; dup {
			t.Fatalf("aggregate id %d delivered twice", f.id)
		}
		d := aggEventData(t, f)
		if !allowed[d.Channel] {
			t.Fatalf("unselected channel %q delivered at id %d", d.Channel, f.id)
		}
		prev = f.id
		got[f.id] = d.Channel
	}
}

// TestAggregateExpiresMidConnection sends an `event: error` frame with the
// per-channel boundary when retention crosses the live cursor while the
// aggregate connection is open (mirrors the single-stream mid-connection 410).
func TestAggregateExpiresMidConnection(t *testing.T) {
	srv, _ := newTestServer(t, 3)
	chans := []string{"linac", "ring"}
	// Anchor establishes cursor 1 on the open stream.
	postBatch(t, srv.URL, "linac", []map[string]any{oneEvent("anchor", "critical", "anchor")})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	code, frames := openAggregateSSE(ctx, t, srv.URL, chans, 0)
	if code != http.StatusOK {
		t.Fatalf("stream code %d", code)
	}
	if f := waitEvent(t, frames, 3*time.Second); f.id != 1 {
		t.Fatalf("anchor frame = %d", f.id)
	}

	// One batch (one wakeup, so the cursor stays at 1 across the trim) pushes
	// four linac events in; retention 3 leaves [3,4,5], stranding cursor 1.
	postBatch(t, srv.URL, "linac", []map[string]any{
		oneEvent("n1", "info", "1"),
		oneEvent("n2", "info", "2"),
		oneEvent("n3", "info", "3"),
		oneEvent("n4", "info", "4"),
	})

	f := waitEvent(t, frames, 5*time.Second)
	if f.event != "error" || f.id != 0 {
		t.Fatalf("want terminal error frame without id, got %+v", f)
	}
	var body struct {
		EarliestAvailable map[string]int64 `json:"earliestAvailableId"`
	}
	if err := json.Unmarshal([]byte(f.data), &body); err != nil {
		t.Fatal(err)
	}
	if body.EarliestAvailable["linac"] != 3 {
		t.Fatalf("mid-connection boundary = %s, want linac=3", f.data)
	}
	if _, flagged := body.EarliestAvailable["ring"]; flagged {
		t.Fatalf("empty ring must not impose a boundary: %s", f.data)
	}
	// The terminal error closes the stream.
	if _, ok := <-frames; ok {
		t.Fatal("aggregate stream should close after the mid-connection error frame")
	}
}

// TestSingleChannelWireShapeUnchanged guards compatibility: the original
// per-channel stream must keep emitting frames WITHOUT a channel JSON field.
func TestSingleChannelWireShapeUnchanged(t *testing.T) {
	srv, _ := newTestServer(t, 0)
	postBatch(t, srv.URL, "solo", []map[string]any{oneEvent("k", "info", "m")})

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	_, frames := openSSE(ctx, t, srv.URL, "solo", 0)
	f := waitEvent(t, frames, 3*time.Second)
	if strings.Contains(f.data, `"channel"`) {
		t.Fatalf("single-channel frame must not gain a channel field: %s", f.data)
	}
}
