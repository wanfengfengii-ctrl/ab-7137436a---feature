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
