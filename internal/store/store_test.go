package store

import (
	"errors"
	"path/filepath"
	"testing"
)

func ev(key, sev, msg string) Event {
	return Event{EventKey: key, Time: "2026-10-05T12:00:00Z", Severity: sev, Message: msg}
}

func TestAppendFreshIDsGlobalAndOrdered(t *testing.T) {
	s := mustOpen(t, 0)

	r1, err := s.Append("linac", []Event{ev("a", "critical", "m1"), ev("b", "warning", "m2")})
	if err != nil {
		t.Fatal(err)
	}
	if r1[0].ID != 1 || r1[1].ID != 2 || r1[0].Replay || r1[1].Replay {
		t.Fatalf("unexpected first results: %+v", r1)
	}
	r2, err := s.Append("booster", []Event{ev("c", "info", "m3")})
	if err != nil {
		t.Fatal(err)
	}
	if r2[0].ID != 3 || r2[0].Replay {
		t.Fatalf("global id must continue across channels: %+v", r2)
	}
}

func TestExactRetryReplays(t *testing.T) {
	s := mustOpen(t, 0)
	e := ev("trip-7", "critical", "dump")

	r1, err := s.Append("ch", []Event{e})
	if err != nil {
		t.Fatal(err)
	}
	r2, err := s.Append("ch", []Event{e, e}) // retries, incl. in-batch dup
	if err != nil {
		t.Fatal(err)
	}
	if r2[0].ID != r1[0].ID || !r2[0].Replay || r2[1].ID != r1[0].ID || !r2[1].Replay {
		t.Fatalf("identical retry must replay same id: %+v vs %+v", r1, r2)
	}
	if s.NextID() != r1[0].ID+1 {
		t.Fatalf("replay must not allocate ids, nextID=%d", s.NextID())
	}
	snap := s.Snapshot("ch")
	if len(snap) != 1 {
		t.Fatalf("replay must not append records, got %d", len(snap))
	}
}

func TestConflictRejectsWholeBatchZeroWrites(t *testing.T) {
	s := mustOpen(t, 0)
	if _, err := s.Append("ch", []Event{ev("k1", "critical", "v1"), ev("k2", "info", "v2")}); err != nil {
		t.Fatal(err)
	}

	bad := ev("k1", "critical", "CHANGED")
	_, err := s.Append("ch", []Event{bad, ev("k3", "info", "v3")})
	var ce *ConflictError
	if !errors.As(err, &ce) {
		t.Fatalf("want ConflictError, got %v", err)
	}
	if len(ce.Items) != 1 || ce.Items[0].Index != 0 || ce.Items[0].EventKey != "k1" {
		t.Fatalf("conflict must pinpoint item: %+v", ce.Items)
	}
	if ce.Items[0].Existing == nil || ce.Items[0].Existing.Message != "v1" {
		t.Fatalf("conflict must echo existing record: %+v", ce.Items[0].Existing)
	}

	// Zero writes: k3 never appeared, nextID unchanged, k1 intact.
	if s.NextID() != 3 {
		t.Fatalf("nextID=%d, want 3 after rejected batch", s.NextID())
	}
	if got := s.Snapshot("ch"); len(got) != 2 || got[0].Message != "v1" {
		t.Fatalf("store mutated by conflict: %+v", got)
	}
}

func TestInBatchDuplicateWithDifferentContentConflicts(t *testing.T) {
	s := mustOpen(t, 0)
	a := ev("dup", "info", "first")
	b := ev("dup", "info", "second")
	if _, err := s.Append("ch", []Event{a, b}); err == nil {
		t.Fatal("differing duplicates within one batch must conflict")
	}
	if len(s.Snapshot("ch")) != 0 {
		t.Fatal("nothing may be written")
	}
}

func TestRetentionAndGoneCursor(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "data"), 3)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := s.Append("ch", []Event{ev(
			string(rune('a'+i)), "info", "m")}); err != nil {
			t.Fatal(err)
		}
	}
	snap := s.Snapshot("ch")
	if len(snap) != 3 {
		t.Fatalf("retention=3, got %d rows", len(snap))
	}
	// Cursor at id 1 (aged out) -> Gone with earliestAvailableId=3.
	res := s.ReadHistory("ch", 1)
	if !res.Gone || res.EarliestAvailable != 3 {
		t.Fatalf("want Gone earliest=3, got %+v", res)
	}
	// Cursor at earliest-1 is fine (== resume boundary? strictly older), id 2
	// also aged out: Gone.
	if r := s.ReadHistory("ch", 2); !r.Gone {
		t.Fatal("cursor 2 with earliest 3 must be Gone")
	}
	// Cursor inside the window returns the suffix and never replays trimmed.
	r := s.ReadHistory("ch", 3)
	if r.Gone || len(r.Events) != 2 || r.Events[0].ID != 4 {
		t.Fatalf("resume from 3: %+v", r)
	}
	// Idempotency index still knows aged keys: identical replay works and a
	// changed aged key still conflicts.
	if rr, err := s.Append("ch", []Event{ev("a", "info", "m")}); err != nil || !rr[0].Replay {
		t.Fatalf("aged identical key should replay: %+v err=%v", rr, err)
	}
	if _, err := s.Append("ch", []Event{ev("a", "info", "new")}); err == nil {
		t.Fatal("aged key with new content must still conflict")
	}
}

func TestRestartPreservesIDsReplayAndConflict(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")

	s, err := Open(dir, 2) // retention 2
	if err != nil {
		t.Fatal(err)
	}
	var firstID int64
	for i := 0; i < 3; i++ {
		r, err := s.Append("ch", []Event{ev(string(rune('k'+i)), "critical", "orig")})
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			firstID = r[0].ID
		}
	}
	if firstID != 1 {
		t.Fatalf("first id = %d", firstID)
	}

	// Reopen: ids, replay verdicts and conflicts must be unchanged.
	s2, err := Open(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	if got := s2.NextID(); got != 4 {
		t.Fatalf("after restart nextID=%d want 4", got)
	}
	// Aged-out key (k) replays identically from the rebuilt WAL index.
	r, err := s2.Append("ch", []Event{ev("k", "critical", "orig")})
	if err != nil || !r[0].Replay || r[0].ID != firstID {
		t.Fatalf("post-restart replay: %+v err=%v", r, err)
	}
	// Same conflict verdict as before restart.
	if _, err := s2.Append("ch", []Event{ev("k", "critical", "tampered")}); err == nil {
		t.Fatal("conflict verdict must survive restart")
	}
	// Live window retained and Gone behaves identically.
	res := s2.ReadHistory("ch", 1)
	if !res.Gone || res.EarliestAvailable != 2 {
		t.Fatalf("post-restart window: %+v", res)
	}
	// Fresh allocation continues above every id ever assigned.
	r2, err := s2.Append("ch", []Event{ev("post-restart", "info", "x")})
	if err != nil {
		t.Fatal(err)
	}
	if r2[0].ID != 4 {
		t.Fatalf("new id after restart = %d want 4", r2[0].ID)
	}
}

func TestMixedReplayAndFreshInOneBatch(t *testing.T) {
	s := mustOpen(t, 0)
	old := ev("old", "critical", "established")
	r0, err := s.Append("ch", []Event{old})
	if err != nil {
		t.Fatal(err)
	}
	// Several newer keys so the replay id is strictly below the fresh id
	// allocated later in the same batch.
	for i := 0; i < 3; i++ {
		if _, err := s.Append("ch", []Event{ev(string(rune('p'+i)), "info", "x")}); err != nil {
			t.Fatal(err)
		}
	}
	// Batch: [old-key replay, brand-new key]. Results must map per index even
	// though the ids are not ascending.
	r, err := s.Append("ch", []Event{old, ev("brand-new", "warning", "y")})
	if err != nil {
		t.Fatal(err)
	}
	if len(r) != 2 {
		t.Fatalf("want 2 results, got %d", len(r))
	}
	if r[0].ID != r0[0].ID || !r[0].Replay {
		t.Fatalf("index 0 must replay old id %d, got %+v", r0[0].ID, r[0])
	}
	if r[1].Replay || r[1].ID != 5 {
		t.Fatalf("index 1 must be fresh id 5, got %+v", r[1])
	}
	if s.NextID() != 6 {
		t.Fatalf("only one new id allocated, nextID=%d want 6", s.NextID())
	}
}

func TestReadMultiHistoryMergesAndExpiresExactly(t *testing.T) {
	s := mustOpen(t, 3) // retention 3 per channel

	// Interleave global ids across channels.
	for _, tc := range []struct {
		ch  string
		key string
	}{
		{"a", "a1"}, // id 1
		{"b", "b1"}, // id 2
		{"a", "a2"}, // id 3
		{"b", "b2"}, // id 4
		{"a", "a3"}, // id 5
		{"a", "a4"}, // id 6 -> a trims id 1
		{"a", "a5"}, // id 7 -> a trims id 3
	} {
		if _, err := s.Append(tc.ch, []Event{ev(tc.key, "info", "m")}); err != nil {
			t.Fatal(err)
		}
	}
	// a: trimmed {1,3}, retained {5,6,7}; b: trimmed {}, retained {2,4}.

	// No cursor: every retained event of the selected channels, id-ordered.
	res := s.ReadMultiHistory([]string{"a", "b"}, 0)
	if res.Gone {
		t.Fatalf("no cursor must never be Gone: %+v", res)
	}
	var got []int64
	for _, e := range res.Events {
		got = append(got, e.ID)
	}
	want := []int64{2, 4, 5, 6, 7}
	if len(got) != len(want) {
		t.Fatalf("merged ids = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("merged ids = %v, want %v", got, want)
		}
	}
	// Frames carry their source channel.
	byID := map[int64]string{}
	for _, e := range res.Events {
		byID[e.ID] = e.Channel
	}
	if byID[2] != "b" || byID[4] != "b" || byID[5] != "a" || byID[6] != "a" || byID[7] != "a" {
		t.Fatalf("channel tags wrong: %v", byID)
	}

	// Cursor 3 sits below b's earliest retained id (4) yet loses nothing:
	// a's trimmed ids 1,3 are <= 3 and b trimmed nothing. Must NOT be Gone.
	res = s.ReadMultiHistory([]string{"a", "b"}, 3)
	if res.Gone {
		t.Fatalf("cursor 3 loses nothing, must not be Gone: %+v", res)
	}
	if len(res.Events) != 4 || res.Events[0].ID != 4 || res.Events[3].ID != 7 {
		t.Fatalf("cursor 3 events = %+v", res.Events)
	}

	// Cursor 2: a trimmed id 3 (> 2) which the console never saw -> Gone,
	// pinpointing only channel a.
	res = s.ReadMultiHistory([]string{"a", "b"}, 2)
	if !res.Gone || len(res.Expired) != 1 || res.Expired[0] != "a" {
		t.Fatalf("cursor 2: want Gone expired=[a], got %+v", res)
	}
	if res.Earliest["a"] != 5 || res.Earliest["b"] != 2 {
		t.Fatalf("per-channel earliest wrong: %v", res.Earliest)
	}

	// Unselected channels contribute nothing.
	res = s.ReadMultiHistory([]string{"b"}, 0)
	if len(res.Events) != 2 || res.Events[0].Channel != "b" {
		t.Fatalf("single selection leaked other channels: %+v", res.Events)
	}

	// Unknown/empty channels report earliest 0 and cannot expire a cursor.
	res = s.ReadMultiHistory([]string{"never-seen"}, 99)
	if res.Gone || res.Earliest["never-seen"] != 0 || len(res.Events) != 0 {
		t.Fatalf("empty channel: %+v", res)
	}
}

func TestMultiTrimBoundarySurvivesRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")

	s, err := Open(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ ch, key string }{
		{"x", "x1"}, // id 1
		{"y", "y1"}, // id 2
		{"x", "x2"}, // id 3
		{"x", "x3"}, // id 4 -> x trims 1
		{"x", "x4"}, // id 5 -> x trims 3
	} {
		if _, err := s.Append(tc.ch, []Event{ev(tc.key, "info", "m")}); err != nil {
			t.Fatal(err)
		}
	}
	// x: trimmed {1,3}, retained {4,5}; y: retained {2}.
	before := s.ReadMultiHistory([]string{"x", "y"}, 2)
	if !before.Gone || len(before.Expired) != 1 || before.Expired[0] != "x" {
		t.Fatalf("pre-restart: %+v", before)
	}

	s2, err := Open(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	after := s2.ReadMultiHistory([]string{"x", "y"}, 2)
	if !after.Gone || len(after.Expired) != 1 || after.Expired[0] != "x" {
		t.Fatalf("post-restart expiry boundary changed: %+v", after)
	}
	if after.Earliest["x"] != before.Earliest["x"] || after.Earliest["y"] != before.Earliest["y"] {
		t.Fatalf("earliest changed across restart: before=%v after=%v", before.Earliest, after.Earliest)
	}
	// And the boundary case stays lossless after restart too.
	if res := s2.ReadMultiHistory([]string{"x", "y"}, 3); res.Gone {
		t.Fatalf("post-restart cursor 3 must remain lossless: %+v", res)
	}
}

func mustOpen(t *testing.T, retention int) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "data"), retention)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
