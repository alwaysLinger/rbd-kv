package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	nerr "github.com/alwaysLinger/rbkv/error"
	"github.com/alwaysLinger/rbkv/pb"
	"github.com/dgraph-io/badger/v4"
	badgerpb "github.com/dgraph-io/badger/v4/pb"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

var (
	ErrKeyNotFound       = errors.New("key not found")
	ErrNotLeader         = errors.New("not leader")
	ErrLeaderShipChanged = errors.New("leader changed")
	ErrNoLeader          = errors.New("no leader found within the cluster for now")
	ErrPeerExists        = errors.New("peer already exists")

	ErrWatcherIDConflict     = errors.New("watcher already exists")
	ErrWatcherConsumeTooSlow = errors.New("watcher consume too slow")
	ErrWatcherClosed         = errors.New("watcher closed")
)

type Store interface {
	Get(ctx context.Context, cmd *pb.Command) ([]byte, error)
	Put(ctx context.Context, cmd *pb.Command) error
	Delete(ctx context.Context, cmd *pb.Command) error
	LeaderInfo() (string, string, uint64, error)
	Watch(ctx context.Context, cmd *pb.WatchRequest) (<-chan *pb.Event, error)
	AddPeer(ctx context.Context, id, addr string) error

	Runner
	Observer
}

type Runner interface {
	Open() error
	Close() error
}

type Observer interface {
	ObChan() chan raft.Observation
	WithRaft(r *raft.Raft)
}

type Node struct {
	fsm        *FSM
	raft       *raft.Raft
	opts       *storeOptions
	dispatcher *eventDispatcher
	obCh       chan raft.Observation

	mu             *sync.RWMutex
	kvConn         grpc.ClientConnInterface
	kvLeaderClient pb.RbdkvClient
}

type storeOptions struct {
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

func (n *Node) Get(ctx context.Context, cmd *pb.Command) ([]byte, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, n.opts.ReadTimeout)
		defer cancel()
	}

	valCh := make(chan any, 1)
	errCh := make(chan error, 1)

	go func() {
		if cmd.Rc == pb.Command_Serializable || cmd.Rc == pb.Command_RCUnknown {
			val, err := n.get(cmd.Key)
			if err != nil {
				errCh <- err
			} else {
				valCh <- val
			}
		} else {
			committedIndex, staleTerm := n.raft.CommitIndex(), n.raft.CurrentTerm()
			_, staleLeaderID := n.raft.LeaderWithID()
			<-n.fsm.WaitForIndexAlign(committedIndex)
			val, err := n.get(cmd.Key)
			if err != nil {
				errCh <- err
			} else {
				currentTerm := n.raft.CurrentTerm()
				_, currentLeaderID := n.raft.LeaderWithID()
				if staleTerm == currentTerm && staleLeaderID == currentLeaderID {
					valCh <- val
				} else {
					if addr, id, term, err := n.LeaderInfo(); err != nil {
						errCh <- err
					} else {
						errCh <- nerr.NewNodeError(ErrLeaderShipChanged, addr, id, term)
					}
				}
			}
		}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case val := <-valCh:
		if val, ok := val.([]byte); ok {
			return val, nil
		} else {
			return nil, fmt.Errorf("unexpected get value: %v", val)
		}
	case err := <-errCh:
		return nil, err
	}
}

func (n *Node) get(key []byte) ([]byte, error) {
	var val []byte
	err := n.fsm.DB().View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return ErrKeyNotFound
			}
			return err
		}
		val, err = item.ValueCopy(val)
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return val, nil
}

func (n *Node) Put(ctx context.Context, cmd *pb.Command) error {
	return n.propose(ctx, cmd)
}

func (n *Node) Delete(ctx context.Context, cmd *pb.Command) error {
	return n.propose(ctx, cmd)
}

func (n *Node) propose(ctx context.Context, cmd *pb.Command) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, n.opts.WriteTimeout)
		defer cancel()
	}

	timeout, err := durationFromCtx(ctx)
	if err != nil {
		return err
	}

	b, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}

	af := n.raft.Apply(b, timeout)
	if af.Error() != nil {
		if errors.Is(af.Error(), raft.ErrNotLeader) || errors.Is(af.Error(), raft.ErrLeadershipLost) {
			if addr, id, term, err := n.LeaderInfo(); err != nil {
				return err
			} else {
				return nerr.NewNodeError(ErrNotLeader, addr, id, term)
			}
		}
		return af.Error()
	}

	if err, ok := af.Response().(error); ok {
		return err
	} else {
		return nil
	}
}

func durationFromCtx(ctx context.Context) (time.Duration, error) {
	dl, ok := ctx.Deadline()
	if !ok {
		return 0, fmt.Errorf("not a timer ctx")
	}
	if d := time.Until(dl); d > 0 {
		return d, nil
	}
	return 0, fmt.Errorf("deadline exceed")
}

func (n *Node) LeaderInfo() (string, string, uint64, error) {
	addr, id := n.raft.LeaderWithID()
	if addr == "" {
		return "", "", 0, nerr.NewNodeError(ErrNoLeader, "unknown", "", 0)
	}
	return string(addr), string(id), n.raft.CurrentTerm(), nil
}

func (n *Node) Watch(ctx context.Context, cmd *pb.WatchRequest) (<-chan *pb.Event, error) {
	if cmd.LeaderRequired && n.raft.State() != raft.Leader {
		if addr, id, term, err := n.LeaderInfo(); err != nil {
			return nil, err
		} else {
			return nil, nerr.NewNodeError(ErrNotLeader, addr, id, term)
		}
	}

	w := n.newWatcher(ctx, cmd.WatcherId, cmd.Prefixes, int64(cmd.EventCapacity))
	if err := n.dispatcher.add(w); err != nil {
		return nil, err
	}

	return w.eventCh, nil
}

type watcherID = string

type watcher struct {
	id      watcherID
	filters []badgerpb.Match
	eventCh chan *pb.Event
	ctx     context.Context
}

func (n *Node) newWatcher(ctx context.Context, id watcherID, prefixes [][]byte, capacity int64) *watcher {
	filters := make([]badgerpb.Match, len(prefixes))
	for i := range prefixes {
		filters[i] = badgerpb.Match{Prefix: prefixes[i]}
	}
	return &watcher{
		id:      id,
		filters: filters,
		eventCh: make(chan *pb.Event, capacity),
		ctx:     ctx,
	}
}

type eventDispatcher struct {
	fsm *FSM

	mu       *sync.RWMutex
	watchers map[watcherID]*watcher
}

func newEventDispatcher(fsm *FSM) *eventDispatcher {
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

func (n *Node) AddPeer(ctx context.Context, id, addr string) error {
	timeout, err := durationFromCtx(ctx)
	if err != nil {
		return err
	}
	timeout = max(timeout, 10*time.Second)

	configFuture := n.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}

	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(id) || srv.Address == raft.ServerAddress(addr) {
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(id) {
				return nil
			}
			future := n.raft.RemoveServer(srv.ID, 0, timeout)
			if err := future.Error(); err != nil {
				return fmt.Errorf("node id: %s at %s %w", id, err, ErrPeerExists)
			}
		}
	}

	f := n.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, timeout)
	if f.Error() != nil {
		return f.Error()
	}
	return nil
}

func (n *Node) Open() error {
	_, addr := n.raft.LeaderWithID()
	n.setKVConn(addr)

	go func() {
		for {
			time.Sleep(10 * time.Millisecond)
			ai := n.raft.AppliedIndex()
			n.fsm.SyncAppliedIndex(ai)
		}
	}()

	return nil
}

func (n *Node) Close() error {
	return n.fsm.Close()
}

func (n *Node) ObChan() chan raft.Observation {
	return n.obCh
}

func (n *Node) WithRaft(r *raft.Raft) {
	n.raft = r
}

func (n *Node) setKVConn(addr raft.ServerID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if conn, err := grpc.Dial(string(addr), grpc.WithInsecure(), grpc.WithBlock()); err != nil {
		if n.kvConn != nil {
			if c, ok := n.kvConn.(*grpc.ClientConn); ok {
				_ = c.Close()
			}
		}
		n.kvLeaderClient = pb.NewRbdkvClient(conn)
	} else {
		n.kvConn, n.kvLeaderClient = nil, nil
	}
}

func NewStore(fsm *FSM) *Node {
	dispatcher := newEventDispatcher(fsm)
	opts := &storeOptions{
		ReadTimeout:  2000 * time.Millisecond,
		WriteTimeout: 3000 * time.Millisecond,
	}
	obCh := make(chan raft.Observation, 100)
	n := &Node{
		fsm:        fsm,
		opts:       opts,
		dispatcher: dispatcher,
		obCh:       obCh,
		mu:         new(sync.RWMutex),
	}
	go func() {
		for ob := range obCh {
			if e, ok := ob.Data.(raft.LeaderObservation); ok {
				n.setKVConn(e.LeaderID)
			}
		}
	}()
	return n
}
