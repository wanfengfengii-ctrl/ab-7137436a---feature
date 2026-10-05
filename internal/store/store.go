// Package store implements the per-channel, append-only event log with
// globally ordered ids, idempotent batch ingestion and durable persistence.
//
// Concurrency model: a single RWMutex guards every mutation. Allocation of
// ids, conflict checks and appends for a batch happen in one critical
// section, so a POST is atomic: either the whole batch commits or nothing is
// written. Writes are serialised through one lock, so events receive ids in
// the exact order in which concurrent POSTs are acknowledged.
package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Event is one safety event as published on a channel.
type Event struct {
	EventKey string `json:"eventKey"`
	Time     string `json:"time"`
	Severity string `json:"severity"`
	Message  string `json:"message"`
}

// StoredEvent is an Event augmented with its globally unique, monotonic id.
type StoredEvent struct {
	ID       int64  `json:"id"`
	EventKey string `json:"eventKey"`
	Time     string `json:"time"`
	Severity string `json:"severity"`
	Message  string `json:"message"`
}

// ConflictItem pinpoints one event of a rejected batch whose eventKey was
// already known with different content.
type ConflictItem struct {
	Index    int          `json:"index"`
	EventKey string       `json:"eventKey"`
	Incoming Event        `json:"incoming"`
	Existing *StoredEvent `json:"existing"` // nil when the clash is inside the same batch
}

// ConflictError is returned when a batch contains an eventKey that already
// exists with different content. No event in the batch was written.
type ConflictError struct {
	Items []ConflictItem `json:"items"`
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("conflict on %d event key(s)", len(e.Items))
}

// AppendResult reports the outcome for one event of an accepted batch.
type AppendResult struct {
	ID     int64 `json:"id"`
	Replay bool  `json:"replay"`
}

type channelState struct {
	events []*StoredEvent // retained window, ordered by id
	// byKey is the durable idempotency index covering EVERY key ever seen,
	// including keys whose events have aged out of the retained window. It is
	// rebuilt from the full WAL on restart, so replay/conflict conclusions
	// survive both trimming and restarts.
	byKey map[string]*StoredEvent
}

// Store is the set of all channel logs plus the global id counter.
type Store struct {
	mu        sync.RWMutex
	dir       string
	nextID    int64
	seq       uint64 // monotonic commit counter for WAL file naming
	channels  map[string]*channelState
	retention int
}

// Open loads (or initialises) the durable store rooted at dir. retention is
// the per-channel RETENTION_LIMIT (<= 0 disables trimming).
func Open(dir string, retention int) (*Store, error) {
	if dir == "" {
		return nil, errors.New("store: data directory must not be empty")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("store: create data dir: %w", err)
	}
	s := &Store{
		dir:       dir,
		nextID:    1,
		channels:  make(map[string]*channelState),
		retention: retention,
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) ch(name string) *channelState {
	c, ok := s.channels[name]
	if !ok {
		c = &channelState{byKey: make(map[string]*StoredEvent)}
		s.channels[name] = c
	}
	return c
}

// Append atomically validates and stores a batch of events on channel.
//
// Semantics:
//   - every unseen eventKey gets a fresh global id in batch order;
//   - an eventKey already present with identical content replays its id;
//   - an eventKey known with different content, or a key duplicated with
//     differing content inside the batch, rejects the whole batch with a
//     *ConflictError and performs zero writes.
func (s *Store) Append(channel string, events []Event) ([]AppendResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	c := s.ch(channel)

	type planned struct {
		ev     Event
		id     int64
		replay bool
	}
	plan := make([]planned, len(events))

	// Indices of keys already seen earlier in *this* batch, so in-batch
	// duplicates with inconsistent content also constitute a 409.
	batchSeen := make(map[string]int)

	var conflicts []ConflictItem
	nextID := s.nextID

	for i, ev := range events {
		if prior, ok := batchSeen[ev.EventKey]; ok {
			if !eventsEqual(plan[prior].ev, ev) {
				conflicts = append(conflicts, ConflictItem{
					Index:    i,
					EventKey: ev.EventKey,
					Incoming: ev,
				})
			} else {
				// Reuse the earlier assignment in this batch and report it
				// as a replay so the duplicate entry allocates nothing.
				p := plan[prior]
				p.replay = true
				plan[i] = p
			}
			continue
		}
		batchSeen[ev.EventKey] = i

		if existing, ok := c.byKey[ev.EventKey]; ok {
			if eventsEqual(existing.toEvent(), ev) {
				plan[i] = planned{ev: ev, id: existing.ID, replay: true}
			} else {
				conflicts = append(conflicts, ConflictItem{
					Index:    i,
					EventKey: ev.EventKey,
					Incoming: ev,
					Existing: cloneStored(existing),
				})
			}
			continue
		}

		plan[i] = planned{ev: ev, id: nextID}
		nextID++
	}

	if len(conflicts) > 0 {
		return nil, &ConflictError{Items: conflicts}
	}

	// Commit phase: append in order, skipping replays/duplicates.
	committed := make([]*StoredEvent, 0, len(events))
	results := make([]AppendResult, len(events))
	committedKeys := make(map[string]bool)
	for i, p := range plan {
		results[i] = AppendResult{ID: p.id, Replay: p.replay}
		if p.replay || committedKeys[p.ev.EventKey] {
			continue
		}
		committedKeys[p.ev.EventKey] = true
		stored := &StoredEvent{
			ID:       p.id,
			EventKey: p.ev.EventKey,
			Time:     p.ev.Time,
			Severity: p.ev.Severity,
			Message:  p.ev.Message,
		}
		c.events = append(c.events, stored)
		c.byKey[p.ev.EventKey] = stored
		committed = append(committed, stored)
	}
	prevNextID := s.nextID
	s.nextID = nextID

	if err := s.writeWAL(channel, committed); err != nil {
		// Durability failure: roll back the in-memory commit so the service
		// never acknowledges events it could not survive a restart with.
		for _, e := range committed {
			delete(c.byKey, e.EventKey)
		}
		kept := c.events[:0]
		dropped := make(map[int64]bool, len(committed))
		for _, e := range committed {
			dropped[e.ID] = true
		}
		for _, e := range c.events {
			if !dropped[e.ID] {
				kept = append(kept, e)
			}
		}
		c.events = kept
		s.nextID = prevNextID
		return nil, fmt.Errorf("persist batch: %w", err)
	}

	s.trimLocked(channel)
	return results, nil
}

// ReadResult is what a resumed SSE stream needs to know.
type ReadResult struct {
	Events            []*StoredEvent // history after the cursor, in id order
	EarliestAvailable int64          // oldest retained id on the channel (0 if empty)
	LatestID          int64          // newest id on the channel (0 if empty)
	Gone              bool           // cursor precedes EarliestAvailable
}

// ReadHistory returns retained channel events with id strictly greater than
// afterID. If afterID is below the earliest retained id, Gone is set so the
// caller can answer HTTP 410.
func (s *Store) ReadHistory(channel string, afterID int64) ReadResult {
	s.mu.RLock()
	defer s.mu.RUnlock()

	c, ok := s.channels[channel]
	if !ok {
		return ReadResult{}
	}
	res := ReadResult{}
	if len(c.events) > 0 {
		res.EarliestAvailable = c.events[0].ID
		res.LatestID = c.events[len(c.events)-1].ID
	}
	if res.EarliestAvailable > 0 && afterID > 0 && afterID < res.EarliestAvailable {
		res.Gone = true
		return res
	}
	for _, e := range c.events {
		if e.ID > afterID {
			res.Events = append(res.Events, cloneStored(e))
		}
	}
	return res
}

// Snapshot returns a point-in-time copy of a channel's retained events.
func (s *Store) Snapshot(channel string) []*StoredEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.channels[channel]
	if !ok {
		return nil
	}
	out := make([]*StoredEvent, 0, len(c.events))
	for _, e := range c.events {
		out = append(out, cloneStored(e))
	}
	return out
}

// NextID returns the first id that has not been allocated yet.
func (s *Store) NextID() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nextID
}

// trimLocked drops the oldest events beyond RETENTION_LIMIT from the live
// readable window. The idempotency index byKey is deliberately NOT trimmed:
// the WAL retains the complete history and rebuilds it on restart, so the
// same retry always yields the same replay/conflict verdict. Reads expose
// only the retained window, which is also what powers the 410 cursor check.
func (s *Store) trimLocked(channel string) {
	if s.retention <= 0 {
		return
	}
	c := s.ch(channel)
	if len(c.events) <= s.retention {
		return
	}
	c.events = append([]*StoredEvent(nil), c.events[len(c.events)-s.retention:]...)
}

func eventsEqual(a, b Event) bool {
	return a.EventKey == b.EventKey &&
		a.Time == b.Time &&
		a.Severity == b.Severity &&
		a.Message == b.Message
}

func cloneStored(e *StoredEvent) *StoredEvent {
	if e == nil {
		return nil
	}
	cp := *e
	return &cp
}

func (e *StoredEvent) toEvent() Event {
	return Event{
		EventKey: e.EventKey,
		Time:     e.Time,
		Severity: e.Severity,
		Message:  e.Message,
	}
}

// ---- persistence -----------------------------------------------------------
//
// Each committed batch is one JSON-lines file:
//
//	wal-<seq>-<channel>.jsonl   one persistedEvent per line
//
// Files are written temp+fsync+rename, so startup never sees a torn batch.
// Recovery replays every WAL file in seq order, which reconstructs global
// ids, per-channel order and the idempotency index exactly; RETENTION_LIMIT
// is then reapplied in memory. Retention therefore never destroys replay
// history on disk, while reads only expose the newest RETENTION_LIMIT rows.

type persistedEvent struct {
	Channel  string `json:"channel"`
	ID       int64  `json:"id"`
	EventKey string `json:"eventKey"`
	Time     string `json:"time"`
	Severity string `json:"severity"`
	Message  string `json:"message"`
}

func (s *Store) writeWAL(channel string, committed []*StoredEvent) error {
	if len(committed) == 0 {
		return nil // pure replay batch: nothing new to durably record
	}
	s.seq++
	maxID := committed[len(committed)-1].ID
	name := filepath.Join(s.dir, fmt.Sprintf("wal-%013d-%013d.jsonl", s.seq, maxID))
	tmp := name + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	for _, e := range committed {
		if err := enc.Encode(persistedEvent{
			Channel:  channel,
			ID:       e.ID,
			EventKey: e.EventKey,
			Time:     e.Time,
			Severity: e.Severity,
			Message:  e.Message,
		}); err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, name); err != nil {
		return err
	}
	return syncDir(s.dir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// walFile pairs a parsed file name with its path for ordered replay.
type walFile struct {
	seq   uint64
	maxID int64
	path  string
}

func (s *Store) load() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	var wals []walFile
	for _, ent := range entries {
		name := ent.Name()
		if !strings.HasPrefix(name, "wal-") || !strings.HasSuffix(name, ".jsonl") {
			continue
		}
		body := strings.TrimSuffix(strings.TrimPrefix(name, "wal-"), ".jsonl")
		parts := strings.Split(body, "-")
		if len(parts) != 2 {
			continue
		}
		seq, err1 := strconv.ParseUint(parts[0], 10, 64)
		maxID, err2 := strconv.ParseInt(parts[1], 10, 64)
		if err1 != nil || err2 != nil {
			continue
		}
		wals = append(wals, walFile{seq: seq, maxID: maxID, path: filepath.Join(s.dir, name)})
	}
	sort.Slice(wals, func(i, j int) bool { return wals[i].seq < wals[j].seq })

	for _, wf := range wals {
		if err := s.replayFile(wf); err != nil {
			return fmt.Errorf("replay %s: %w", filepath.Base(wf.path), err)
		}
		s.seq = wf.seq
	}

	// Reapply retention on every loaded channel so a restart exposes exactly
	// the same live window as a continuously running process.
	for name := range s.channels {
		s.trimLocked(name)
		if len(s.channels[name].events) > 0 {
			if s.channels[name].events[len(s.channels[name].events)-1].ID >= s.nextID {
				s.nextID = s.channels[name].events[len(s.channels[name].events)-1].ID + 1
			}
		}
	}
	// nextID must advance past every id ever allocated, including trimmed
	// ones: derive it from WAL file name maxIDs as well.
	for _, wf := range wals {
		if wf.maxID >= s.nextID {
			s.nextID = wf.maxID + 1
		}
	}
	if s.seq == 0 {
		s.nextID = 1
	}
	return nil
}

func (s *Store) replayFile(wf walFile) error {
	data, err := os.ReadFile(wf.path)
	if err != nil {
		return err
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var pe persistedEvent
		if err := json.Unmarshal([]byte(line), &pe); err != nil {
			return err
		}
		c := s.ch(pe.Channel)
		stored := &StoredEvent{
			ID:       pe.ID,
			EventKey: pe.EventKey,
			Time:     pe.Time,
			Severity: pe.Severity,
			Message:  pe.Message,
		}
		c.events = append(c.events, stored)
		c.byKey[pe.EventKey] = stored
		if pe.ID >= s.nextID {
			s.nextID = pe.ID + 1
		}
	}
	return nil
}
