// Command restart-smoke verifies durability across a server restart in two
// phases driven by the one-shot verify harness:
//
//	-phase before   publishes an anchor, ages it past retention and records
//	                the assigned ids and earliestAvailableId to a state file;
//	-phase after    (run after the process/container restarted reusing the
//	                same data volume) asserts ids, replay verdicts, conflict
//	                verdicts, the 410 cursor and id continuation are unchanged.
package main

import (
	"bytes"
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

type event struct {
	EventKey string `json:"eventKey"`
	Time     string `json:"time"`
	Severity string `json:"severity"`
	Message  string `json:"message"`
}

// state is serialised between the two phases.
type state struct {
	Channel           string `json:"channel"`
	AnchorKey         string `json:"anchorKey"`
	Anchor            event  `json:"anchor"`
	PreID             int64  `json:"preId"`
	AnchorID          int64  `json:"anchorId"`
	LastID            int64  `json:"lastId"`
	EarliestAvailable int64  `json:"earliestAvailable"`
}

func main() {
	base := flag.String("base", envOr("BASE_URL", "http://localhost:8080"), "service base URL")
	statePath := flag.String("state", "restart-smoke-state.json", "state file shared between phases")
	phase := flag.String("phase", "before", "before|after")
	retention := flag.Int("retention", 25, "configured RETENTION_LIMIT")
	channel := flag.String("channel", "", "channel name (before phase generates one when empty)")
	flag.Parse()

	if err := run(*base, *statePath, *phase, *retention, *channel); err != nil {
		fmt.Printf("[FAIL] restart-smoke %s: %v\n", *phase, err)
		os.Exit(1)
	}
	fmt.Printf("[ OK ] restart-smoke %s\n", *phase)
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func run(base, statePath, phase string, retention int, channel string) error {
	switch phase {
	case "before":
		return before(base, statePath, retention, channel)
	case "after":
		return after(base, statePath)
	default:
		return fmt.Errorf("unknown phase %q", phase)
	}
}

func mkEvent(key, sev, msg string) event {
	return event{
		EventKey: key,
		Time:     time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC).Format(time.RFC3339),
		Severity: sev,
		Message:  msg,
	}
}

func post(base, ch string, evs []event) (int, []byte) {
	body, _ := json.Marshal(evs)
	resp, err := http.Post(base+"/events/"+ch, "application/json", bytes.NewReader(body))
	if err != nil {
		return -1, []byte(err.Error())
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

func postOne(base, ch string, e event) (int64, bool, int, error) {
	code, raw := post(base, ch, []event{e})
	if code != http.StatusOK {
		return 0, false, code, fmt.Errorf("status %d: %s", code, raw)
	}
	var out struct {
		Results []struct {
			ID     int64 `json:"id"`
			Replay bool  `json:"replay"`
		} `json:"results"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return 0, false, code, err
	}
	return out.Results[0].ID, out.Results[0].Replay, code, nil
}

func streamStatus(base, ch string, lastID int64) (int, []byte) {
	req, _ := http.NewRequest(http.MethodGet, base+"/streams/"+ch, nil)
	req.Header.Set("Last-Event-ID", strconv.FormatInt(lastID, 10))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return -1, []byte(err.Error())
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return http.StatusOK, nil
	}
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

// aggStreamStatus probes the aggregate multi-channel endpoint with a resume
// cursor and returns the HTTP status plus (for non-200) the body.
func aggStreamStatus(base string, channels []string, lastID int64) (int, []byte) {
	q := make(url.Values)
	for _, c := range channels {
		q.Add("channel", c)
	}
	req, _ := http.NewRequest(http.MethodGet, base+"/streams?"+q.Encode(), nil)
	req.Header.Set("Last-Event-ID", strconv.FormatInt(lastID, 10))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return -1, []byte(err.Error())
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return http.StatusOK, nil
	}
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

// aggGoneBody mirrors the aggregate expired-cursor 410 response.
type aggGoneBody struct {
	Error    string `json:"error"`
	Channels []struct {
		Channel           string `json:"channel"`
		EarliestAvailable int64  `json:"earliestAvailableId"`
		Expired           bool   `json:"expired"`
	} `json:"channels"`
}

// checkAggregateGone asserts that resuming the aggregate stream over
// ch (plus an empty channel) from cursor fails with a 410 whose per-channel
// boundaries pinpoint ch at the expected earliestAvailableId.
func checkAggregateGone(base, ch string, cursor, wantEarliest int64) error {
	unused := ch + "-unused"
	code, raw := aggStreamStatus(base, []string{ch, unused}, cursor)
	if code != http.StatusGone {
		return fmt.Errorf("aggregate expired cursor: want 410, got %d: %s", code, raw)
	}
	var body aggGoneBody
	if err := json.Unmarshal(raw, &body); err != nil {
		return fmt.Errorf("aggregate 410 body not JSON: %s", raw)
	}
	bounds := map[string]struct {
		earliest int64
		expired  bool
	}{}
	for _, b := range body.Channels {
		bounds[b.Channel] = struct {
			earliest int64
			expired  bool
		}{b.EarliestAvailable, b.Expired}
	}
	main, ok := bounds[ch]
	if !ok || !main.expired || main.earliest != wantEarliest {
		return fmt.Errorf("aggregate 410 boundary for %s: want expired earliestAvailableId=%d, body=%s",
			ch, wantEarliest, raw)
	}
	empty, ok := bounds[unused]
	if !ok || empty.expired || empty.earliest != 0 {
		return fmt.Errorf("aggregate 410 boundary for empty channel %s wrong: %s", unused, raw)
	}
	return nil
}

func before(base, statePath string, retention int, channel string) error {
	if channel == "" {
		channel = fmt.Sprintf("restart-%d", time.Now().UnixNano())
	}
	st := state{Channel: channel, AnchorKey: "anchor"}
	st.Anchor = mkEvent(st.AnchorKey, "critical", "permanent beam stop")

	// A pre-anchor gives the console a resume cursor strictly below the
	// anchor: once the anchor is trimmed, resuming from the pre-anchor is
	// impossible without loss, which the aggregate stream must reject.
	preID, replay, _, err := postOne(base, channel, mkEvent("pre-anchor", "info", "cursor position"))
	if err != nil {
		return fmt.Errorf("publish pre-anchor: %w", err)
	}
	if replay {
		return fmt.Errorf("pre-anchor unexpectedly replayed")
	}
	st.PreID = preID

	id, replay, _, err := postOne(base, channel, st.Anchor)
	if err != nil {
		return fmt.Errorf("publish anchor: %w", err)
	}
	if replay {
		return fmt.Errorf("anchor unexpectedly replayed")
	}
	st.AnchorID = id
	st.LastID = id

	// Publish `retention` more events so the anchor ages out of the window.
	for i := 0; i < retention; i++ {
		id, replay, _, err = postOne(base, channel, mkEvent(
			fmt.Sprintf("filler-%d", i), "info", "filler"))
		if err != nil {
			return err
		}
		if replay {
			return fmt.Errorf("filler %d replayed", i)
		}
		st.LastID = id
	}

	code, raw := streamStatus(base, channel, st.AnchorID)
	if code != http.StatusGone {
		return fmt.Errorf("anchor cursor: want 410, got %d: %s", code, raw)
	}
	var gone struct {
		EarliestAvailable int64 `json:"earliestAvailableId"`
	}
	if err := json.Unmarshal(raw, &gone); err != nil {
		return err
	}
	st.EarliestAvailable = gone.EarliestAvailable

	// The aggregate stream must reject the same lossy resume with a 410
	// whose per-channel boundary pinpoints this channel.
	if err := checkAggregateGone(base, channel, st.PreID, st.EarliestAvailable); err != nil {
		return err
	}

	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(statePath, b, 0o644); err != nil {
		return err
	}
	fmt.Printf("recorded: channel=%s preID=%d anchorID=%d lastID=%d earliestAvailable=%d\n",
		st.Channel, st.PreID, st.AnchorID, st.LastID, st.EarliestAvailable)
	return nil
}

func after(base, statePath string) error {
	b, err := os.ReadFile(statePath)
	if err != nil {
		return fmt.Errorf("read state: %w", err)
	}
	var st state
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}

	// 1. Exact replay of the (now retention-aged) anchor: same id, replay.
	id, replay, code, err := postOne(base, st.Channel, st.Anchor)
	if err != nil {
		return fmt.Errorf("replay anchor after restart: %w", err)
	}
	if id != st.AnchorID || !replay {
		return fmt.Errorf("after restart replay: id=%d replay=%v, want id=%d replay=true",
			id, replay, st.AnchorID)
	}

	// 2. Conflict conclusion for that key is unchanged and zero-write.
	changed := st.Anchor
	changed.Message = "permanent beam stop (tampered)"
	code, raw := post(base, st.Channel, []event{changed})
	if code != http.StatusConflict {
		return fmt.Errorf("after restart conflict: want 409, got %d: %s", code, raw)
	}
	if !strings.Contains(string(raw), st.AnchorKey) {
		return fmt.Errorf("409 body must pinpoint key: %s", raw)
	}

	// 3. Expired cursor still 410 with the same earliestAvailableId.
	code, raw = streamStatus(base, st.Channel, st.AnchorID)
	if code != http.StatusGone {
		return fmt.Errorf("after restart expired cursor: want 410, got %d: %s", code, raw)
	}
	var gone struct {
		EarliestAvailable int64 `json:"earliestAvailableId"`
	}
	if err := json.Unmarshal(raw, &gone); err != nil {
		return err
	}
	if gone.EarliestAvailable != st.EarliestAvailable {
		return fmt.Errorf("earliestAvailableId changed across restart: before=%d after=%d",
			st.EarliestAvailable, gone.EarliestAvailable)
	}

	// 3b. The aggregate stream's per-channel expiry boundary is likewise
	// unchanged after the restart.
	if err := checkAggregateGone(base, st.Channel, st.PreID, st.EarliestAvailable); err != nil {
		return fmt.Errorf("after restart: %w", err)
	}

	// 4. Numbering continues strictly above every pre-restart id.
	id, replay, _, err = postOne(base, st.Channel, mkEvent("after-restart", "warning", "new era"))
	if err != nil {
		return err
	}
	if replay {
		return fmt.Errorf("post-restart event must be fresh")
	}
	if id != st.LastID+1 {
		return fmt.Errorf("id continuation: got %d, want %d", id, st.LastID+1)
	}
	return nil
}
