// Command smoke is the one-shot end-to-end verifier. It waits for the
// service to become healthy, then exercises five sections and folds their
// results into a bitmask exit code:
//
//	1    publishing / idempotent replay / 409 zero-write
//	2    SSE live delivery and idle heartbeat
//	4    SSE resume with Last-Event-ID (exactly-once, gapless)
//	8    expired cursor -> HTTP 410 with earliestAvailableId
//	128  aggregate GET /streams: merged history, cross-channel live
//	     interleaving, one-cursor resume and per-channel expired cursor
//
// Build failures and `go test` are aggregated by the verify entrypoint
// (bits 16 and 32 respectively).
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	bitPublish   = 1
	bitLive      = 2
	bitResume    = 4
	bitGone      = 8
	bitTests     = 16
	bitBuild     = 32
	bitAggregate = 128
)

type event struct {
	EventKey string `json:"eventKey"`
	Time     string `json:"time"`
	Severity string `json:"severity"`
	Message  string `json:"message"`
}

type appendResult struct {
	ID     int64 `json:"id"`
	Replay bool  `json:"replay"`
}

type postResp struct {
	Results []appendResult `json:"results"`
}

type conflictBody struct {
	Error             string `json:"error"`
	EarliestAvailable int64  `json:"earliestAvailableId"`
	Items             []struct {
		Index    int    `json:"index"`
		EventKey string `json:"eventKey"`
	} `json:"items"`
}

// sseEvent is one dispatched SSE frame.
type sseEvent struct {
	ID      int64
	Event   string
	Data    string
	Comment bool
}

func main() {
	base := flag.String("base", envOr("BASE_URL", "http://localhost:8080"), "service base URL")
	retention := flag.Int("retention", envInt("RETENTION_LIMIT", 25), "server RETENTION_LIMIT (events kept per channel)")
	flag.Parse()

	failed := 0
	run := func(name string, bit int, fn func() error) {
		fmt.Printf("\n=== smoke: %s ===\n", name)
		if err := fn(); err != nil {
			fmt.Printf("[FAIL] %s: %v\n", name, err)
			failed |= bit
			return
		}
		fmt.Printf("[ OK ] %s\n", name)
	}

	if err := waitHealthy(*base, 60*time.Second); err != nil {
		fmt.Printf("[FATAL] service never became healthy: %v\n", err)
		os.Exit(bitBuild | bitPublish | bitLive | bitResume | bitGone | bitAggregate)
	}
	fmt.Println("service is healthy")

	run("publish + replay + 409 zero-write", bitPublish, func() error { return checkPublish(*base) })
	run("SSE live delivery + heartbeat", bitLive, func() error { return checkLive(*base) })
	run("SSE resume Last-Event-ID exactly-once", bitResume, func() error { return checkResume(*base) })
	run("expired cursor -> 410", bitGone, func() error { return checkGone(*base) })
	run("aggregate stream: history + interleaving + resume + 410", bitAggregate, func() error {
		return checkAggregate(*base, *retention)
	})

	fmt.Println()
	if failed != 0 {
		fmt.Printf("SMOKE FAILED, aggregated exit code %d\n", failed)
	} else {
		fmt.Println("SMOKE PASSED")
	}
	os.Exit(failed)
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func envInt(k string, d int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return d
}

func waitHealthy(base string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 2 * time.Second}
	for time.Now().Before(deadline) {
		resp, err := client.Get(base + "/health")
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == 200 && strings.Contains(string(body), "ok") {
				return nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timeout after %s", timeout)
}

func postEvents(base, channel string, evs []event) (int, postResp, conflictBody, []byte) {
	body, _ := json.Marshal(evs)
	resp, err := http.Post(base+"/events/"+channel, "application/json", bytes.NewReader(body))
	if err != nil {
		return -1, postResp{}, conflictBody{}, []byte(err.Error())
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var pr postResp
	_ = json.Unmarshal(raw, &pr)
	var cb conflictBody
	_ = json.Unmarshal(raw, &cb)
	return resp.StatusCode, pr, cb, raw
}

func mkEvent(key, sev, msg string) event {
	return event{
		EventKey: key,
		Time:     time.Now().UTC().Format(time.RFC3339),
		Severity: sev,
		Message:  msg,
	}
}

func checkPublish(base string) error {
	ch := fmt.Sprintf("smoke-pub-%d", time.Now().UnixNano())

	// Empty and oversized batches must be rejected.
	if code, _, _, _ := postEvents(base, ch, nil); code != http.StatusBadRequest {
		return fmt.Errorf("empty batch: want 400, got %d", code)
	}
	big := make([]event, 51)
	for i := range big {
		big[i] = mkEvent(fmt.Sprintf("big-%d", i), "info", "x")
	}
	if code, _, _, _ := postEvents(base, ch, big); code != http.StatusBadRequest {
		return fmt.Errorf("51-event batch: want 400, got %d", code)
	}

	// First batch: two fresh keys.
	e1 := mkEvent("trip-1", "critical", "beam dump A")
	e2 := mkEvent("warn-1", "warning", "magnet temp high")
	code, first, _, raw := postEvents(base, ch, []event{e1, e2})
	if code != http.StatusOK {
		return fmt.Errorf("first batch: want 200, got %d: %s", code, raw)
	}
	if len(first.Results) != 2 {
		return fmt.Errorf("want 2 results, got %d", len(first.Results))
	}
	if first.Results[0].Replay || first.Results[1].Replay {
		return fmt.Errorf("fresh keys must not be marked replay: %+v", first.Results)
	}
	if first.Results[1].ID != first.Results[0].ID+1 {
		return fmt.Errorf("ids in a batch must be consecutive and ordered: %+v", first.Results)
	}
	id1, id2 := first.Results[0].ID, first.Results[1].ID

	// Identical retry replays the original ids, allocates nothing new.
	code, retry, _, raw := postEvents(base, ch, []event{e1, e2})
	if code != http.StatusOK {
		return fmt.Errorf("retry batch: want 200, got %d: %s", code, raw)
	}
	if retry.Results[0].ID != id1 || retry.Results[1].ID != id2 {
		return fmt.Errorf("retry must replay original ids: first=%+v retry=%+v", first.Results, retry.Results)
	}
	if !retry.Results[0].Replay || !retry.Results[1].Replay {
		return fmt.Errorf("replayed results must carry replay=true: %+v", retry.Results)
	}

	// Changed content -> 409, locatable items, zero writes: a subsequent
	// identical retry must still replay the original ids.
	conflicting := e1
	conflicting.Message = "beam dump A (edited)"
	code, _, cb, raw := postEvents(base, ch, []event{conflicting, e2})
	if code != http.StatusConflict {
		return fmt.Errorf("conflict batch: want 409, got %d: %s", code, raw)
	}
	if len(cb.Items) != 1 || cb.Items[0].EventKey != "trip-1" || cb.Items[0].Index != 0 {
		return fmt.Errorf("409 must pinpoint index/key, got: %s", raw)
	}
	code, after, _, raw := postEvents(base, ch, []event{e1, e2})
	if code != http.StatusOK || after.Results[0].ID != id1 || after.Results[1].ID != id2 {
		return fmt.Errorf("after 409 nothing may have changed: code=%d ids=%+v raw=%s", code, after.Results, raw)
	}

	// A 50-event batch is accepted.
	fifty := make([]event, 50)
	for i := range fifty {
		fifty[i] = mkEvent(fmt.Sprintf("fifty-%d", i), "info", "bulk")
	}
	if code, fiftyResp, _, raw := postEvents(base, ch, fifty); code != http.StatusOK ||
		len(fiftyResp.Results) != 50 || fiftyResp.Results[0].Replay {
		return fmt.Errorf("50-event batch: want 200/50 fresh, got %d %+v %s", code, fiftyResp.Results, raw)
	}
	return nil
}

func checkLive(base string) error {
	ch := fmt.Sprintf("smoke-live-%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	frames, err := openStream(ctx, base, ch, 0) // 0 = no Last-Event-ID
	if err != nil {
		return err
	}

	// Heartbeat must arrive while idle within ~5s.
	select {
	case f := <-frames:
		if !f.Comment {
			return fmt.Errorf("expected idle heartbeat comment first on empty channel, got %+v", f)
		}
	case <-time.After(6 * time.Second):
		return fmt.Errorf("no heartbeat within 6s of idle time")
	}

	// Live event after subscribe.
	ev := mkEvent("live-1", "critical", "stopping signal")
	code, pr, _, raw := postEvents(base, ch, []event{ev})
	if code != 200 {
		return fmt.Errorf("publish: %d %s", code, raw)
	}
	select {
	case f := <-frames:
		if f.Event != "event" || f.ID != pr.Results[0].ID || !strings.Contains(f.Data, "stopping signal") {
			return fmt.Errorf("unexpected live frame: %+v", f)
		}
	case <-time.After(5 * time.Second):
		return fmt.Errorf("live event not delivered within 5s")
	}
	return nil
}

func checkResume(base string) error {
	ch := fmt.Sprintf("smoke-resume-%d", time.Now().UnixNano())

	// History before connecting.
	var historyIDs []int64
	for i := 0; i < 3; i++ {
		_, pr, _, _ := postEvents(base, ch, []event{mkEvent(fmt.Sprintf("h-%d", i), "warning", fmt.Sprintf("history %d", i))})
		historyIDs = append(historyIDs, pr.Results[0].ID)
	}

	ctx1, cancel1 := context.WithTimeout(context.Background(), 10*time.Second)
	frames1, err := openStream(ctx1, base, ch, 0)
	if err != nil {
		cancel1()
		return err
	}
	seen := map[int64]bool{}
	var lastID int64
	for len(seen) < 3 {
		select {
		case f := <-frames1:
			if f.Comment {
				continue
			}
			if seen[f.ID] {
				cancel1()
				return fmt.Errorf("duplicate id %d in history", f.ID)
			}
			seen[f.ID] = true
			lastID = f.ID
		case <-time.After(5 * time.Second):
			cancel1()
			return fmt.Errorf("timeout reading history, got %v", seen)
		}
	}
	for i, id := range historyIDs {
		if !seen[id] {
			cancel1()
			return fmt.Errorf("missing history id %d", id)
		}
		_ = i
	}
	cancel1() // "network drop"

	// Events published while disconnected must not be lost.
	var gapIDs []int64
	for i := 0; i < 2; i++ {
		_, pr, _, _ := postEvents(base, ch, []event{mkEvent(fmt.Sprintf("gap-%d", i), "critical", fmt.Sprintf("gap %d", i))})
		gapIDs = append(gapIDs, pr.Results[0].ID)
	}

	// Reconnect exactly like an EventSource: Last-Event-ID = last seen.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()
	frames2, err := openStream(ctx2, base, ch, lastID)
	if err != nil {
		return err
	}
	for _, want := range gapIDs {
		select {
		case f := <-frames2:
			if f.Comment {
				continue
			}
			if f.ID != want {
				return fmt.Errorf("resume out of order: want %d, got %d", want, f.ID)
			}
			if f.ID <= lastID {
				return fmt.Errorf("resume redelivered history id %d (<= cursor %d)", f.ID, lastID)
			}
			lastID = f.ID
		case <-time.After(5 * time.Second):
			return fmt.Errorf("missing gap event id %d after resume", want)
		}
	}

	// Seamless: a live event published after resume is delivered once.
	_, pr, _, _ := postEvents(base, ch, []event{mkEvent("after-resume", "warning", "post resume")})
	liveID := pr.Results[0].ID
	select {
	case f := <-frames2:
		if f.ID != liveID {
			return fmt.Errorf("post-resume live event: want %d got %d", liveID, f.ID)
		}
	case <-time.After(5 * time.Second):
		return fmt.Errorf("post-resume live event not delivered")
	}
	return nil
}

func checkGone(base string) error {
	ch := fmt.Sprintf("smoke-gone-%d", time.Now().UnixNano())

	_, first, _, _ := postEvents(base, ch, []event{mkEvent("anchor", "info", "anchor")})
	oldCursor := first.Results[0].ID

	// Publish on this channel until retention pushes the anchor out.
	var earliest int64
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		_, _, _, _ = postEvents(base, ch, []event{mkEvent(
			fmt.Sprintf("filler-%d", time.Now().UnixNano()), "info", "filler")})
		code, cb, raw := getStreamStatus(base, ch, oldCursor)
		switch code {
		case http.StatusGone:
			if cb.EarliestAvailable <= oldCursor {
				return fmt.Errorf("earliestAvailableId %d must exceed expired cursor %d", cb.EarliestAvailable, oldCursor)
			}
			earliest = cb.EarliestAvailable
		case http.StatusOK:
			continue
		default:
			return fmt.Errorf("unexpected status %d probing cursor: %s", code, raw)
		}

		// Resync from earliestAvailableId must succeed and only expose the
		// retained window.
		ctx2, cancel2 := context.WithTimeout(context.Background(), 6*time.Second)
		frames2, err := openStream(ctx2, base, ch, earliest)
		if err != nil {
			cancel2()
			return fmt.Errorf("resync from earliestAvailableId failed: %w", err)
		}
		select {
		case f := <-frames2:
			if f.Comment {
				cancel2()
				return fmt.Errorf("expected retained event on resync, got heartbeat")
			}
			if f.ID < earliest {
				cancel2()
				return fmt.Errorf("resync exposed id %d below earliestAvailableId %d", f.ID, earliest)
			}
			cancel2()
			return nil
		case <-time.After(5 * time.Second):
			cancel2()
			return fmt.Errorf("no event delivered after resync")
		}
	}
	return fmt.Errorf("retention window never crossed (publishing could not age out id %d)", oldCursor)
}

// getStreamStatus opens the SSE endpoint with a cursor and returns just the
// HTTP status (draining/closing immediately). A 200 response is cancelled at
// once; body is read minimally to obtain error JSON for 410.
func getStreamStatus(base, channel string, after int64) (int, conflictBody, []byte) {
	req, _ := http.NewRequest(http.MethodGet, base+"/streams/"+channel, nil)
	if after > 0 {
		req.Header.Set("Last-Event-ID", strconv.FormatInt(after, 10))
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return -1, conflictBody{}, []byte(err.Error())
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		// Live SSE stream: the status alone is what the probe needs; do not
		// block reading an endless body.
		return http.StatusOK, conflictBody{}, nil
	}
	raw, _ := io.ReadAll(resp.Body)
	var cb conflictBody
	_ = json.Unmarshal(raw, &cb)
	return resp.StatusCode, cb, raw
}

// aggGoneBody is the aggregate 410 envelope with per-channel boundaries.
type aggGoneBody struct {
	Error             string           `json:"error"`
	EarliestAvailable map[string]int64 `json:"earliestAvailableId"`
}

// aggFrameData is the JSON payload carried by an aggregate event frame.
type aggFrameData struct {
	ID       int64  `json:"id"`
	Channel  string `json:"channel"`
	EventKey string `json:"eventKey"`
}

// getAggregateStatus probes GET /streams?channel=... with a cursor and returns
// the status plus any per-channel 410 boundaries, without consuming the live
// body on 200.
func getAggregateStatus(base string, channels []string, after int64) (int, aggGoneBody, []byte) {
	q := url.Values{}
	for _, c := range channels {
		q.Add("channel", c)
	}
	req, _ := http.NewRequest(http.MethodGet, base+"/streams?"+q.Encode(), nil)
	if after > 0 {
		req.Header.Set("Last-Event-ID", strconv.FormatInt(after, 10))
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return -1, aggGoneBody{}, []byte(err.Error())
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return http.StatusOK, aggGoneBody{}, nil
	}
	raw, _ := io.ReadAll(resp.Body)
	var b aggGoneBody
	_ = json.Unmarshal(raw, &b)
	return resp.StatusCode, b, raw
}

// checkAggregate exercises the aggregate event stream end to end:
//   - validation (no / empty / duplicate / 9 channels -> 400);
//   - retained history from multiple channels delivered in global id order
//     with a channel field and no event from an unselected channel;
//   - cross-channel live interleaving, then resume across a disconnect with a
//     single global Last-Event-ID (gapless, exactly-once, channel-tagged);
//   - an expired cursor answered 410 with locatable per-channel boundaries
//     (only the genuinely aged-out channel), after which a cursorless
//     reconnect yields exactly every still-retained selected-channel event.
func checkAggregate(base string, retention int) error {
	suffix := time.Now().UnixNano()
	linac := fmt.Sprintf("agg-linac-%d", suffix)
	ring := fmt.Sprintf("agg-ring-%d", suffix)
	ignored := fmt.Sprintf("agg-ignored-%d", suffix)
	watched := []string{linac, ring}

	// --- validation -----------------------------------------------------------
	if code, _, raw := getAggregateStatus(base, nil, 0); code != http.StatusBadRequest {
		return fmt.Errorf("aggregate without channels: want 400, got %d %s", code, raw)
	}
	if code, _, raw := getAggregateStatus(base, []string{"", "b"}, 0); code != http.StatusBadRequest {
		return fmt.Errorf("aggregate empty channel: want 400, got %d %s", code, raw)
	}
	if code, _, raw := getAggregateStatus(base, []string{"x", "x"}, 0); code != http.StatusBadRequest {
		return fmt.Errorf("aggregate duplicate channel: want 400, got %d %s", code, raw)
	}
	nine := make([]string, 9)
	for i := range nine {
		nine[i] = fmt.Sprintf("c%d", i)
	}
	if code, _, raw := getAggregateStatus(base, nine, 0); code != http.StatusBadRequest {
		return fmt.Errorf("aggregate 9 channels: want 400, got %d %s", code, raw)
	}

	// --- setup: interleaved history on watched + unselected channels ----------
	published := map[string][]int64{}
	pubID := func(ch, key string) (int64, error) {
		code, pr, _, raw := postEvents(base, ch, []event{mkEvent(key, "critical", key)})
		if code != http.StatusOK {
			return 0, fmt.Errorf("publish %s/%s: %d %s", ch, key, code, raw)
		}
		id := pr.Results[0].ID
		published[ch] = append(published[ch], id)
		return id, nil
	}
	// setupErr short-circuits the one-shot setup: a publish failure aborts the
	// check rather than panicking the whole verifier.
	var setupErr error
	mustPub := func(ch, key string) int64 {
		if setupErr != nil {
			return 0
		}
		id, err := pubID(ch, key)
		if err != nil {
			setupErr = err
			return 0
		}
		return id
	}
	type idch struct {
		id      int64
		channel string
	}
	history := []idch{
		{mustPub(linac, "h-l-1"), linac},
		{mustPub(ring, "h-r-1"), ring},
		{mustPub(ring, "h-r-2"), ring},
		{mustPub(linac, "h-l-2"), linac},
	}
	if _, err := pubID(ignored, "h-x-1"); err != nil {
		return err
	}
	if setupErr != nil {
		return setupErr
	}

	// Read frames in the exact expected id order; heartbeat comments are
	// skipped. A closed channel or a wrong id/channel/order fails the check.
	expectOrder := func(frames <-chan sseEvent, want []idch, label string) (int64, error) {
		var cursor int64
		for _, w := range want {
			for {
				f, ok := <-frames
				if !ok {
					return cursor, fmt.Errorf("%s: stream closed waiting for id %d", label, w.id)
				}
				if f.Comment {
					continue
				}
				var d aggFrameData
				if err := json.Unmarshal([]byte(f.Data), &d); err != nil {
					return cursor, fmt.Errorf("%s: bad frame json: %w", label, err)
				}
				if f.ID != w.id || d.ID != w.id {
					return cursor, fmt.Errorf("%s: id mismatch frame=%d data=%d want=%d", label, f.ID, d.ID, w.id)
				}
				if d.Channel != w.channel {
					return cursor, fmt.Errorf("%s: id %d channel=%q want %q", label, w.id, d.Channel, w.channel)
				}
				if f.ID <= cursor {
					return cursor, fmt.Errorf("%s: non-monotonic id %d after %d", label, f.ID, cursor)
				}
				cursor = f.ID
				break
			}
		}
		return cursor, nil
	}

	// --- history merged in global id order ------------------------------------
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	frames, err := openAggregateStream(ctx, base, watched, 0)
	if err != nil {
		cancel()
		return err
	}
	if _, err := expectOrder(frames, history, "history"); err != nil {
		cancel()
		return err
	}

	// --- cross-channel live interleaving (with unselected noise) --------------
	var liveSeq []idch
	for i := 0; i < 4; i++ {
		liveSeq = append(liveSeq, idch{mustPub(linac, fmt.Sprintf("l-l-%d", i)), linac})
		liveSeq = append(liveSeq, idch{mustPub(ring, fmt.Sprintf("l-r-%d", i)), ring})
		if _, err := pubID(ignored, fmt.Sprintf("l-x-%d", i)); err != nil {
			cancel()
			return err
		}
	}
	if setupErr != nil {
		cancel()
		return setupErr
	}
	lastCursor, err := expectOrder(frames, liveSeq, "live interleave")
	if err != nil {
		cancel()
		return err
	}
	cancel()

	// --- resume across a disconnect with one global cursor --------------------
	gap := []idch{
		{mustPub(ring, "g-r-1"), ring},
		{mustPub(linac, "g-l-1"), linac},
		{mustPub(ring, "g-r-2"), ring},
	}
	if setupErr != nil {
		return setupErr
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel2()
	frames2, err := openAggregateStream(ctx2, base, watched, lastCursor)
	if err != nil {
		return err
	}
	if _, err := expectOrder(frames2, gap, "resume"); err != nil {
		return err
	}

	// --- age linac past retention while ring stays inside its window ----------
	// Cursor at linac's very first event: once linac trims it, only linac is
	// expired. Ring's first event is newer than that cursor, so ring must not
	// be reported expired even though its earliest id is larger.
	anchorCursor := history[0].id
	var gone aggGoneBody
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := pubID(linac, fmt.Sprintf("age-%d", time.Now().UnixNano())); err != nil {
			return err
		}
		code, body, raw := getAggregateStatus(base, watched, anchorCursor)
		switch code {
		case http.StatusOK:
			continue
		case http.StatusGone:
			gone = body
			_ = raw
		default:
			return fmt.Errorf("unexpected aggregate probe status %d: %s", code, raw)
		}

		if b, flagged := gone.EarliestAvailable[ring]; flagged {
			return fmt.Errorf("ring first event is newer than cursor; it must not be flagged expired, got ring=%d: %s", b, raw)
		}
		linacEarliest, ok := gone.EarliestAvailable[linac]
		if !ok || linacEarliest <= anchorCursor {
			return fmt.Errorf("410 must pinpoint aged-out linac with boundary > cursor: %s", raw)
		}

		// --- cursorless reconnect: exactly every still-retained event --------
		expected := map[int64]string{}
		for _, ch := range watched {
			ids := published[ch]
			if retention > 0 && len(ids) > retention {
				ids = ids[len(ids)-retention:]
			}
			for _, id := range ids {
				expected[id] = ch
			}
		}
		ctx3, cancel3 := context.WithTimeout(context.Background(), 10*time.Second)
		frames3, err := openAggregateStream(ctx3, base, watched, 0)
		if err != nil {
			cancel3()
			return fmt.Errorf("cursorless resync: %w", err)
		}
		got := make(map[int64]string, len(expected))
		var prev int64
		for len(got) < len(expected) {
			select {
			case f, ok := <-frames3:
				if !ok {
					cancel3()
					return fmt.Errorf("resync stream closed early, got %d/%d", len(got), len(expected))
				}
				if f.Comment {
					continue
				}
				var d aggFrameData
				if err := json.Unmarshal([]byte(f.Data), &d); err != nil {
					cancel3()
					return fmt.Errorf("resync bad json: %w", err)
				}
				if f.ID <= prev {
					cancel3()
					return fmt.Errorf("resync non-monotonic: %d after %d", f.ID, prev)
				}
				wantCh, isExpected := expected[f.ID]
				if !isExpected {
					cancel3()
					return fmt.Errorf("resync delivered unexpected/aged id %d from %q (anchor %d must be gone)",
						f.ID, d.Channel, anchorCursor)
				}
				if d.Channel != wantCh {
					cancel3()
					return fmt.Errorf("resync id %d tagged %q, want %q", f.ID, d.Channel, wantCh)
				}
				prev = f.ID
				got[f.ID] = d.Channel
			case <-time.After(6 * time.Second):
				cancel3()
				return fmt.Errorf("resync timeout, got %d/%d retained events", len(got), len(expected))
			}
		}
		cancel3()
		return nil
	}
	return fmt.Errorf("retention boundary never reached on aggregate probe")
}

// openStream opens a single-channel SSE GET and returns a channel of
// dispatched frames. after==0 omits Last-Event-ID entirely.
func openStream(ctx context.Context, base, channel string, after int64) (<-chan sseEvent, error) {
	return openStreamRaw(ctx, base, "/streams/"+channel, after)
}

// openAggregateStream opens GET /streams with one repeated channel query
// parameter per watched channel.
func openAggregateStream(ctx context.Context, base string, channels []string, after int64) (<-chan sseEvent, error) {
	q := url.Values{}
	for _, c := range channels {
		q.Add("channel", c)
	}
	return openStreamRaw(ctx, base, "/streams?"+q.Encode(), after)
}

func openStreamRaw(ctx context.Context, base, rawPath string, after int64) (<-chan sseEvent, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+rawPath, nil)
	if err != nil {
		return nil, err
	}
	if after > 0 {
		req.Header.Set("Last-Event-ID", strconv.FormatInt(after, 10))
	}
	req.Header.Set("Accept", "text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("stream status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	out := make(chan sseEvent, 32)
	go func() {
		defer close(out)
		defer resp.Body.Close()
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
		var cur sseEvent
		var dataLines []string
		flush := func() {
			if cur.Event == "" && cur.ID == 0 && len(dataLines) == 0 {
				return
			}
			cur.Data = strings.Join(dataLines, "\n")
			select {
			case out <- cur:
			case <-ctx.Done():
			}
			cur = sseEvent{}
			dataLines = nil
		}
		for sc.Scan() {
			line := sc.Text()
			switch {
			case line == "":
				flush()
			case strings.HasPrefix(line, ":"):
				select {
				case out <- sseEvent{Comment: true}:
				case <-ctx.Done():
					return
				}
			case strings.HasPrefix(line, "id:"):
				cur.ID, _ = strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(line, "id:")), 10, 64)
			case strings.HasPrefix(line, "event:"):
				cur.Event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			case strings.HasPrefix(line, "data:"):
				dataLines = append(dataLines, strings.TrimPrefix(line, "data:"))
				if len(dataLines) > 0 {
					dataLines[len(dataLines)-1] = strings.TrimPrefix(dataLines[len(dataLines)-1], " ")
				}
			}
		}
	}()
	return out, nil
}
