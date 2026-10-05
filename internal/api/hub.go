// Package api exposes the HTTP/SSE surface of the interlock event service.
package api

import "sync"

// hub wakes live SSE connections when something may have been committed on
// their channel. It deliberately carries no payload: every connection
// re-reads the store from its own id cursor, which is the single source of
// truth. That makes delivery exactly-once and strictly id-ordered even when
// concurrent POSTs are acknowledged out of commit order on their fan-out.
type hub struct {
	mu       sync.Mutex
	channels map[string]map[chan struct{}]struct{}
}

func newHub() *hub {
	return &hub{channels: make(map[string]map[chan struct{}]struct{})}
}

// subscribe returns a 1-buffered wakeup signal and registers it atomically.
func (h *hub) subscribe(channel string) chan struct{} {
	ch := make(chan struct{}, 1)
	h.mu.Lock()
	subs := h.channels[channel]
	if subs == nil {
		subs = make(map[chan struct{}]struct{})
		h.channels[channel] = subs
	}
	subs[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *hub) unsubscribe(channel string, ch chan struct{}) {
	h.mu.Lock()
	if subs, ok := h.channels[channel]; ok {
		delete(subs, ch)
		if len(subs) == 0 {
			delete(h.channels, channel)
		}
	}
	h.mu.Unlock()
}

// publish must be called AFTER the store commit is durable. A non-blocking
// send on a 1-buffered signal coalesces bursts and never stalls publishers;
// subscribers always re-read everything newer than their cursor.
func (h *hub) publish(channel string) {
	h.mu.Lock()
	subs := h.channels[channel]
	list := make([]chan struct{}, 0, len(subs))
	for ch := range subs {
		list = append(list, ch)
	}
	h.mu.Unlock()

	for _, ch := range list {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}
