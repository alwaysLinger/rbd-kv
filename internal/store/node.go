package store

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	raftbadger "github.com/alwaysLinger/raft-badgerdb"
	nerr "github.com/alwaysLinger/rbkv/error"
	"github.com/alwaysLinger/rbkv/internal/meta"
	"github.com/alwaysLinger/rbkv/pb"
	"github.com/dgraph-io/badger/v4"
	badgerpb "github.com/dgraph-io/badger/v4/pb"
	"github.com/hashicorp/raft"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

var (
	ErrKeyNotFound       = errors.New("key not found")
	ErrNotLeader         = errors.New("not leader")
	ErrLeaderShipChanged = errors.New("leader changed")
	ErrNoLeader          = errors.New("no leader found within the cluster for now")

	ErrWatcherIDConflict     = errors.New("watcher already exists")
	ErrWatcherConsumeTooSlow = errors.New("watcher consume too slow")
	ErrWatcherClosed         = errors.New("watcher closed")

	ErrLeaderConnChanged = errors.New("leader changed while forward cmd to leader")
)

type RaftNode interface {
	WithRaft(raftAddr, joinAddr, logAddr string) error
	AddPeer(ctx context.Context, id, addr string) error

	Run() error
	Close() error
}

type Store interface {
	Get(ctx context.Context, cmd *pb.Command) ([]byte, error)
	Put(ctx context.Context, cmd *pb.Command) error
	Delete(ctx context.Context, cmd *pb.Command) error
	LeaderInfo() (string, string, uint64, error)
	Watch(ctx context.Context, cmd *pb.WatchRequest) (<-chan *pb.Event, error)
	ClusterStats(ctx context.Context, req *pb.ClusterStatsRequest) (*pb.ClusterStatsResponse, error)
}

type Node struct {
	id         string
	isLeader   *atomic.Bool
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
			if _, id, _, err := n.LeaderInfo(); err != nil {
				errCh <- err
			} else {
				if n.isLeaderWithID(id) {
					staleTerm := n.raft.CurrentTerm()
					switch v := n.propose(ctx, cmd).(type) {
					case []byte:
						if _, id, term, err := n.LeaderInfo(); err != nil {
							errCh <- err
						} else {
							if n.isLeaderWithID(id) && staleTerm == term {
								valCh <- v
							} else {
								if addr, id, term, err := n.LeaderInfo(); err != nil {
									errCh <- err
								} else {
									errCh <- nerr.NewNodeError(ErrLeaderShipChanged, addr, id, term)
								}
							}
						}
					case error:
						errCh <- v
					default:
						valCh <- v
					}
				} else {
					resp, err := n.forwardToLeader(ctx, cmd)
					if err != nil {
						errCh <- err
					} else {
						valCh <- resp.Value
					}
				}
			}
		}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case val := <-valCh:
		switch v := val.(type) {
		case []byte:
			return v, nil
		case error:
			return nil, v
		default:
			return nil, fmt.Errorf("unexpected get value: %v", v)
		}
	case err := <-errCh:
		return nil, err
	}
}

func (n *Node) isLeaderWithID(id string) bool {
	return n.id == id
}

func (n *Node) forwardToLeader(ctx context.Context, cmd *pb.Command) (*pb.CommandResponse, error) {
	rctx, err := meta.RedirectCtx(ctx)
	if err != nil {
		return nil, err
	}

	n.mu.RLock()
	c := n.kvLeaderClient
	n.mu.RUnlock()
	if c == nil {
		_, id, _, err := n.LeaderInfo()
		if err != nil {
			return nil, err
		}
		if err := n.setKVConn(id); err != nil {
			return nil, err
		}
		n.mu.RLock()
		c = n.kvLeaderClient
		n.mu.RUnlock()
		if c == nil {
			return nil, ErrLeaderConnChanged
		}
	}

	return c.Execute(rctx, cmd)
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
	if _, id, _, err := n.LeaderInfo(); err != nil {
		return err
	} else {
		if n.isLeaderWithID(id) {
			if err, ok := n.propose(ctx, cmd).(error); ok {
				return err
			}
			return nil
		}
		if _, err := n.forwardToLeader(ctx, cmd); err != nil {
			return err
		}
		return nil
	}
}

func (n *Node) Delete(ctx context.Context, cmd *pb.Command) error {
	if _, id, _, err := n.LeaderInfo(); err != nil {
		return err
	} else {
		if n.isLeaderWithID(id) {
			if err, ok := n.propose(ctx, cmd).(error); ok {
				return err
			}
			return nil
		}
		if _, err := n.forwardToLeader(ctx, cmd); err != nil {
			return err
		}
		return nil
	}
}

func (n *Node) propose(ctx context.Context, cmd *pb.Command) any {
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

	return af.Response()
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
	if len(addr) == 0 {
		return "", "", 0, nerr.NewNodeError(ErrNoLeader, "unknown", "", 0)
	}
	return string(addr), string(id), n.raft.CurrentTerm(), nil
}

func (n *Node) ClusterStats(ctx context.Context, req *pb.ClusterStatsRequest) (*pb.ClusterStatsResponse, error) {
	var (
		cf                          raft.ConfigurationFuture
		leaderID                    string
		raftMeta                    map[string]string
		lsmSize, vlogSize, keyCount uint64
	)

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		cf = n.raft.GetConfiguration()
		return cf.Error()
	})
	g.Go(func() error {
		var err error
		_, leaderID, _, err = n.LeaderInfo()
		if err != nil && !errors.Is(err, ErrNoLeader) {
			return err
		}
		return nil
	})
	if req.WithRaft {
		g.Go(func() error {
			raftMeta = n.raft.Stats()
			return nil
		})
	}

	if r := req.WithFsm; r != nil {
		g.Go(func() error {
			var err error
			lsmSize, vlogSize, keyCount, err = n.fsm.Stats(r.ExactSize, r.WithKeyCount)
			if err != nil {
				return err
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	confs := cf.Configuration()
	if len(confs.Servers) == 0 {
		return nil, fmt.Errorf("no servers found in cluster config")
	}
	peers := make([]*pb.ClusterStatsResponse_Peer, len(confs.Servers))
	servers := confs.Clone()
	for i, _ := range peers {
		peer := new(pb.ClusterStatsResponse_Peer)
		peer.Id = string(servers.Servers[i].ID)
		peer.IsLearner = servers.Servers[i].Suffrage == raft.Nonvoter
		peer.IsLeader = string(servers.Servers[i].ID) == leaderID
		peer.IsLocal = peer.Id == n.id
		if peer.IsLocal {
			if req.WithRaft {
				peer.Raft = &pb.ClusterStatsResponse_RaftMeta{Meta: raftMeta}
			}
			if r := req.WithFsm; r != nil {
				peer.Fsm = &pb.ClusterStatsResponse_StorageStats{
					LsmSize:   lsmSize,
					VlogSize:  vlogSize,
					KeysCount: keyCount,
				}
			}
		}
		peers[i] = peer
	}

	return &pb.ClusterStatsResponse{Peers: peers}, nil
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
	if !n.isLeader.Load() {
		return ErrNotLeader
	}

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
				return fmt.Errorf("failed while removing node:%s at %s: %w", id, addr, err)
			}
		}
	}

	f := n.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, timeout)
	if f.Error() != nil {
		return f.Error()
	}
	return nil
}

func (n *Node) WithRaft(raftAddr, joinAddr, logAddr string) error {
	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(n.id)
	c.ShutdownOnRemove = false
	c.HeartbeatTimeout = 600 * time.Millisecond
	c.ElectionTimeout = 1500 * time.Millisecond
	c.LeaderLeaseTimeout = 500 * time.Millisecond
	if err := raft.ValidateConfig(c); err != nil {
		return err
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", raftAddr)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(raftAddr, tcpAddr, 3, 5*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	snapshots, err := raft.NewFileSnapshotStore(logAddr, 3, os.Stderr)
	if err != nil {
		return err
	}

	logDir := filepath.Join(logAddr, "raftlog")
	var logStore raft.LogStore
	var stableStore raft.StableStore
	var rs *raftbadger.Store
	if rs, err = raftbadger.NewStore(logDir, nil, nil); err != nil {
		return err
	} else {
		logStore, stableStore = rs, rs
	}

	observer := raft.NewObserver(n.obCh, true, func(o *raft.Observation) bool {
		switch o.Data.(type) {
		case raft.LeaderObservation, raft.PeerObservation:
			return true
		default:
			return false
		}
	})

	r, err := raft.NewRaft(c, n.fsm, logStore, stableStore, snapshots, transport)
	if err != nil {
		return err
	}
	n.raft = r
	go func() {
		for c := range n.raft.LeaderCh() {
			n.isLeader.Store(c)
		}
	}()

	r.RegisterObserver(observer)

	if len(joinAddr) == 0 {
		var recoverable bool
		var err error
		if recoverable, err = raft.HasExistingState(logStore, stableStore, snapshots); err != nil {
			log.Printf("%v, current node self recovery may fail, try bootstrap the cluster", err)
			recoverable = true
		}
		if recoverable && err == nil {
			return nil
		}

		conf := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      c.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		// any further attempts to bootstrap will return an error that can be safely ignored
		r.BootstrapCluster(conf)
	} else {
		var recoverable bool
		var err error
		if recoverable, err = raft.HasExistingState(logStore, stableStore, snapshots); err != nil {
			log.Printf("%v, current node self recovery may fail, try join the cluster", err)
			recoverable = true
		}
		if recoverable && err == nil {
			return nil
		}

		conn, err := grpc.Dial(joinAddr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(3*time.Second))
		if err != nil {
			return fmt.Errorf("%w, error occurred when trying to join the cluster, please retry with the latest leader address", err)
		}
		defer conn.Close()
		client := pb.NewRbdkvClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		req := &pb.JoinRequest{
			Id:   n.id,
			Addr: raftAddr,
		}
		defer cancel()
		_, err = client.Join(ctx, req)
		if err != nil {
			return err
		}
	}

	return nil
}

func (n *Node) Run() error {
	if _, id, _, err := n.LeaderInfo(); err == nil {
		if err := n.setKVConn(id); err != nil {
			return err
		}
	}
	return nil
}

func (n *Node) Close() error {
	return n.fsm.Close()
}

func NewNode(id string, fsm *FSM) *Node {
	dispatcher := newEventDispatcher(fsm)
	opts := &storeOptions{
		ReadTimeout:  2000 * time.Millisecond,
		WriteTimeout: 3000 * time.Millisecond,
	}
	obCh := make(chan raft.Observation, 100)
	n := &Node{
		id:         id,
		isLeader:   new(atomic.Bool),
		fsm:        fsm,
		opts:       opts,
		dispatcher: dispatcher,
		obCh:       obCh,
		mu:         new(sync.RWMutex),
	}
	go func() {
		for ob := range obCh {
			if e, ok := ob.Data.(raft.LeaderObservation); ok {
				_ = n.setKVConn(string(e.LeaderID))
			}
		}
	}()
	return n
}

func (n *Node) setKVConn(addr string) error {
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(1*time.Second))
	if err != nil {
		n.mu.Lock()
		if n.kvConn != nil {
			if c, ok := n.kvConn.(*grpc.ClientConn); ok {
				_ = c.Close()
			}
		}
		n.kvConn, n.kvLeaderClient = nil, nil
		n.mu.Unlock()
		return err
	}

	n.mu.Lock()
	if n.kvConn != nil {
		if c, ok := n.kvConn.(*grpc.ClientConn); ok {
			_ = c.Close()
		}
	}
	n.kvConn = conn
	n.kvLeaderClient = pb.NewRbdkvClient(conn)
	n.mu.Unlock()

	return nil
}
