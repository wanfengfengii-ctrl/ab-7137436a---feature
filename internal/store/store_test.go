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

func mustOpen(t *testing.T, retention int) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "data"), retention)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestReadHistoryManyFirstIDSurvivesRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")

	// First phase: linac starts at id 1 and is trimmed; ring starts later.
	s, err := Open(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append("linac", []Event{
		ev("l1", "critical", "1"),
		ev("l2", "info", "2"),
		ev("l3", "info", "3"), // linac window trimmed to [2,3]
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append("ring", []Event{ev("r1", "info", "1")}); err != nil { // id 4
		t.Fatal(err)
	}

	// Reopen: firstID must be rebuilt from the WAL and the aggregate verdict
	// unchanged. Cursor 1 predates linac (expired) but not ring (first id 4).
	s2, err := Open(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	res := s2.ReadHistoryMany([]string{"linac", "ring"}, 1)
	if res.Expired["linac"] != 2 {
		t.Fatalf("after restart linac expired boundary = %+v, want 2", res.Expired)
	}
	if _, ringExpired := res.Expired["ring"]; ringExpired {
		t.Fatalf("after restart ring (first id 4 > cursor 1) must not be expired: %+v", res.Expired)
	}
}

func TestReadHistoryManyMergesInGlobalIDOrder(t *testing.T) {
	s := mustOpen(t, 0)

	// Interleave commits across three channels; global ids are allocated in
	// commit order across all channels.
	mustAppend := func(ch, key string) int64 {
		r, err := s.Append(ch, []Event{ev(key, "info", key)})
		if err != nil {
			t.Fatal(err)
		}
		return r[0].ID
	}
	idLinac1 := mustAppend("linac", "l1")
	idRing1 := mustAppend("ring", "r1")
	idRing2 := mustAppend("ring", "r2")
	idBooster := mustAppend("booster", "b1")
	idLinac2 := mustAppend("linac", "l2")

	res := s.ReadHistoryMany([]string{"linac", "ring", "booster"}, 0)
	if len(res.Expired) != 0 {
		t.Fatalf("no retention, unexpected expired: %+v", res.Expired)
	}
	want := []struct {
		id      int64
		channel string
	}{
		{idLinac1, "linac"},
		{idRing1, "ring"},
		{idRing2, "ring"},
		{idBooster, "booster"},
		{idLinac2, "linac"},
	}
	if len(res.Events) != len(want) {
		t.Fatalf("merged events = %d, want %d", len(res.Events), len(want))
	}
	var prev int64
	for i, w := range want {
		got := res.Events[i]
		if got.Event.ID != w.id || got.Channel != w.channel {
			t.Fatalf("merged[%d] = id %d channel %q, want id %d channel %q",
				i, got.Event.ID, got.Channel, w.id, w.channel)
		}
		if got.Event.ID <= prev {
			t.Fatalf("merge not strictly id-ordered at %d: %d after %d", i, got.Event.ID, prev)
		}
		prev = got.Event.ID
	}
	if len(res.EarliestAvailable) != 3 ||
		res.EarliestAvailable["linac"] != idLinac1 ||
		res.EarliestAvailable["ring"] != idRing1 ||
		res.EarliestAvailable["booster"] != idBooster {
		t.Fatalf("earliest boundaries = %+v", res.EarliestAvailable)
	}
}

func TestReadHistoryManyCursorAndUnselectedChannels(t *testing.T) {
	s := mustOpen(t, 0)
	mustAppend := func(ch, key string) int64 {
		r, err := s.Append(ch, []Event{ev(key, "info", key)})
		if err != nil {
			t.Fatal(err)
		}
		return r[0].ID
	}
	l1 := mustAppend("linac", "l1")
	mustAppend("unselected", "x") // global id 2 must never surface
	r1 := mustAppend("ring", "r1")
	l2 := mustAppend("linac", "l2")

	// Resume strictly above l1: only ring r1 and linac l2 remain, in id order.
	res := s.ReadHistoryMany([]string{"linac", "ring", "never-written"}, l1)
	if len(res.Events) != 2 {
		t.Fatalf("want 2 merged events above cursor, got %d", len(res.Events))
	}
	if res.Events[0].Channel != "ring" || res.Events[0].Event.ID != r1 {
		t.Fatalf("first = %+v", res.Events[0])
	}
	if res.Events[1].Channel != "linac" || res.Events[1].Event.ID != l2 {
		t.Fatalf("second = %+v", res.Events[1])
	}
	if _, ok := res.EarliestAvailable["never-written"]; ok {
		t.Fatal("empty channel must not impose an earliest boundary")
	}
	for _, ce := range res.Events {
		if ce.Channel == "unselected" {
			t.Fatal("event from an unselected channel leaked into the merge")
		}
	}
}

func TestReadHistoryManyExpiredPerChannel(t *testing.T) {
	// retention 3: aging one channel out must be reported per channel even when
	// another selected channel is still fully servable.
	s := mustOpen(t, 3)
	mustAppend := func(ch, key string) int64 {
		r, err := s.Append(ch, []Event{ev(key, "info", key)})
		if err != nil {
			t.Fatal(err)
		}
		return r[0].ID
	}
	old := mustAppend("linac", "old")
	mustAppend("ring", "r1") // global id 2; ring stays well inside its window
	// Three unique linac fills: linac ends with ids [1,3,4,5], trimmed to the
	// retention window [3,4,5], so the oldest retained id is 3.
	for i := 0; i < 3; i++ {
		mustAppend("linac", "fill-"+string(rune('a'+i)))
	}

	// Cursor 1: linac had id 1 which aged out (expired). Ring's FIRST event is
	// id 2, i.e. it had nothing at/before the cursor — a later-starting channel
	// must not make the aggregate resume "gone".
	res := s.ReadHistoryMany([]string{"linac", "ring"}, old)
	if len(res.Expired) != 1 {
		t.Fatalf("want exactly one expired channel, got %+v", res.Expired)
	}
	if res.Expired["linac"] != 3 {
		t.Fatalf("linac earliest = %d, want 3 (%+v)", res.Expired["linac"], res.Expired)
	}
	if _, ringExpired := res.Expired["ring"]; ringExpired {
		t.Fatal("ring's first event is newer than the cursor; it must not be flagged expired")
	}
	// Despite linac being expired, the merged read still reflects the store
	// state (the caller turns Expired into a 410 rather than using Events):
	// ring id 2 then linac's retained 3,4,5, all strictly above the cursor.
	wantMerged := []struct {
		id      int64
		channel string
	}{
		{2, "ring"}, {3, "linac"}, {4, "linac"}, {5, "linac"},
	}
	if len(res.Events) != len(wantMerged) {
		t.Fatalf("merged above cursor = %+v", res.Events)
	}
	for i, w := range wantMerged {
		if res.Events[i].Event.ID != w.id || res.Events[i].Channel != w.channel {
			t.Fatalf("merged[%d] = id %d %q, want id %d %q",
				i, res.Events[i].Event.ID, res.Events[i].Channel, w.id, w.channel)
		}
	}
	// EarliestAvailable still covers every non-empty selected channel.
	if res.EarliestAvailable["ring"] != 2 {
		t.Fatalf("ring boundary = %+v", res.EarliestAvailable)
	}

	// A cursor at/after both windows' earliest id is servable.
	okRes := s.ReadHistoryMany([]string{"linac", "ring"}, 3)
	if len(okRes.Expired) != 0 {
		t.Fatalf("in-window cursor must not expire: %+v", okRes.Expired)
	}

	// But a cursor that predates ring's first retained event WHILE ring had an
	// event at/before it is genuinely expired: publish one more linac fill so
	// ring's window starts after a cursor ring itself has already passed.
	mustAppend("ring", "r-later") // id 6; ring window [2,6]
	// Force ring's id 2 out of retention by adding two more ring events.
	mustAppend("ring", "r-later2") // id 7
	mustAppend("ring", "r-later3") // id 8 -> ring window [6,7,8], earliest 6
	gone := s.ReadHistoryMany([]string{"linac", "ring"}, 3)
	if gone.Expired["ring"] != 6 {
		t.Fatalf("ring should be expired at cursor 3 with earliest 6, got %+v", gone.Expired)
	}
}
