package store

import (
	"context"
	"errors"
	"sync"

	"github.com/alwaysLinger/rbkv/pb"
	"github.com/dgraph-io/badger/v4"
	badgerpb "github.com/dgraph-io/badger/v4/pb"
)

var (
	ErrWatcherIDConflict     = errors.New("watcher already exists")
	ErrWatcherConsumeTooSlow = errors.New("watcher consume too slow")
	ErrWatcherClosed         = errors.New("watcher closed")
)

type watcherID = string

type watcher struct {
	id      watcherID
	filters []badgerpb.Match
	eventCh chan *pb.Event
	ctx     context.Context
}

type eventDispatcher struct {
	fsm DB

	mu       *sync.RWMutex
	watchers map[watcherID]*watcher
}

func newEventDispatcher(fsm DBFSM) *eventDispatcher {
	return &eventDispatcher{
		fsm:      fsm,
		mu:       new(sync.RWMutex),
		watchers: make(map[watcherID]*watcher),
	}
}

func (h *eventDispatcher) add(w *watcher) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.watchers[w.id]; ok {
		return ErrWatcherIDConflict
	}
	h.watchers[w.id] = w

	go func() {
		ctx, cancel := context.WithCancel(w.ctx)
		defer func() {
			cancel()
			close(w.eventCh)
			h.mu.Lock()
			delete(h.watchers, w.id)
			h.mu.Unlock()
		}()

		_ = h.fsm.DB().Subscribe(ctx, func(kv *badger.KVList) error {
			for _, pbKv := range kv.Kv {
				ckv := pbKv
				var eventType pb.Event_EventType
				if len(ckv.Value) == 0 {
					eventType = pb.Event_Delete
				} else {
					eventType = pb.Event_Put
				}
				evt := &pb.Event{
					Type:      eventType,
					Key:       ckv.Key,
					Value:     ckv.Value,
					Version:   ckv.Version,
					ExpireAt:  ckv.ExpiresAt,
					WatcherId: w.id,
				}
				select {
				case w.eventCh <- evt:
				case <-ctx.Done():
					return ctx.Err()
				default:
					return ErrWatcherConsumeTooSlow
				}
			}
			return nil
		}, w.filters)
	}()

	return nil
}
