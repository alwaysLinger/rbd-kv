package store

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	raftbadger "github.com/alwaysLinger/raft-badgerdb"
	nerr "github.com/alwaysLinger/rbkv/error"
	"github.com/alwaysLinger/rbkv/internal/log"
	"github.com/alwaysLinger/rbkv/internal/meta"
	"github.com/alwaysLinger/rbkv/pb"
	badgerpb "github.com/dgraph-io/badger/v4/pb"
	"github.com/hashicorp/raft"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

var (
	ErrNotLeader         = errors.New("not a leader")
	ErrSerializableRead  = errors.New("serializable read failed")
	ErrLinearRead        = errors.New("linear read failed")
	ErrLeaderShipChanged = errors.New("leader changed")
	ErrNoLeader          = errors.New("no leader found within the cluster for now")
	ErrLeaderConnChanged = errors.New("leader changed while forward cmd to leader")
)

type RaftNode interface {
	WithRaft(raftAddr, joinAddr, logAddr string, batchSize uint64) error
	AddPeer(ctx context.Context, id, addr string) error

	Run() error
	Close() error
}

type Store interface {
	Get(ctx context.Context, cmd *pb.Command) (Getter, error)
	Put(ctx context.Context, cmd *pb.Command) (uint64, error)
	Delete(ctx context.Context, cmd *pb.Command) (uint64, error)
	LeaderInfo() (string, string, uint64, error)
	Watch(ctx context.Context, cmd *pb.WatchRequest) (<-chan *pb.Event, error)
	ClusterStats(ctx context.Context, req *pb.ClusterStatsRequest) (*pb.ClusterStatsResponse, error)
}

type Node struct {
	id         string
	isLeader   atomic.Bool
	fsm        DBFSM
	logStore   *raftbadger.Store
	raft       *raft.Raft
	opts       *storeOptions
	dispatcher *eventDispatcher
	obCh       chan raft.Observation
	logger     log.Logger

	mu             *sync.RWMutex
	kvConn         grpc.ClientConnInterface
	kvLeaderClient pb.RbdkvClient
}

type storeOptions struct {
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

type Getter interface {
	Val() []byte
	Meta() UserMeta
	Version() uint64
}

type getter struct {
	val  []byte
	meta UserMeta
	ver  uint64
}

func (g getter) Val() []byte {
	return g.val
}

func (g getter) Meta() UserMeta {
	return g.meta
}

func (g getter) Version() uint64 {
	return g.ver
}

func (n *Node) get(key []byte, at uint64) (Getter, error) {
	val, um, ver, err := n.fsm.Get(key, at)
	if err != nil {
		return nil, err
	}
	return getter{
		val:  val,
		meta: um,
		ver:  ver,
	}, nil
}

func (n *Node) serializableGet(key []byte, at uint64) (Getter, error) {
	return n.get(key, at)
}

func (n *Node) linearGet(ctx context.Context, cmd *pb.Command) (Getter, error) {
	_, oldId, oldTerm, err := n.LeaderInfo()
	if err != nil {
		return nil, fmt.Errorf("leadership pre check failed: %w", err)
	}

	var get Getter
	var e error

	if !n.isLeaderWithID(oldId) {
		var resp *pb.CommandResponse
		resp, e = n.forwardToLeader(ctx, cmd)
		if e == nil {
			get = getter{val: resp.Value, meta: UserMeta(resp.Meta), ver: resp.Version}
		}
	} else {
		if _, ok := ctx.Deadline(); !ok {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, n.opts.WriteTimeout)
			defer cancel()
		}
		timeout, err := durationFromCtx(ctx)
		if err != nil {
			return nil, fmt.Errorf("parse timeout failed: %w", err)
		}
		bf := n.raft.Barrier(timeout)
		if err := bf.Error(); err != nil {
			return nil, fmt.Errorf("error occurred while waiting FSM applying logs aligned with inflight logs err: %w", err)
		}
		get, e = n.get(cmd.Kv.Key, cmd.Kv.Version)
	}

	_, id, term, err := n.LeaderInfo()
	if err != nil {
		return nil, fmt.Errorf("leadership post check failed: %w", err)
	}
	if oldId != id || oldTerm != term {
		return nil, ErrLeaderShipChanged
	}
	if e != nil {
		return nil, e
	}
	return get, nil
}

func (n *Node) Get(ctx context.Context, cmd *pb.Command) (Getter, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, n.opts.ReadTimeout)
		defer cancel()
	}

	valCh := make(chan any, 1)
	errCh := make(chan error, 1)

	go func() {
		var get Getter
		var err error
		if cmd.Rc == pb.Command_Linearizable {
			get, err = n.linearGet(ctx, cmd)
			if err != nil {
				err = fmt.Errorf("%w: %w", ErrLinearRead, err)
			}
		} else {
			get, err = n.serializableGet(cmd.Kv.Key, cmd.Kv.Version)
			if err != nil {
				err = fmt.Errorf("%w: %w", ErrSerializableRead, err)
			}
		}
		if err != nil {
			errCh <- err
			return
		}
		valCh <- get
		return
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case val := <-valCh:
		switch v := val.(type) {
		case Getter:
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

func (n *Node) Put(ctx context.Context, cmd *pb.Command) (uint64, error) {
	if _, id, _, err := n.LeaderInfo(); err != nil {
		return 0, err
	} else {
		if n.isLeaderWithID(id) {
			ret := n.propose(ctx, cmd)
			if err, ok := ret.(error); ok {
				return 0, err
			}
			return ret.(uint64), nil
		}
		if ret, err := n.forwardToLeader(ctx, cmd); err != nil {
			return 0, err
		} else {
			return ret.Version, nil
		}
	}
}

func (n *Node) Delete(ctx context.Context, cmd *pb.Command) (uint64, error) {
	if _, id, _, err := n.LeaderInfo(); err != nil {
		return 0, err
	} else {
		if n.isLeaderWithID(id) {
			ret := n.propose(ctx, cmd)
			if err, ok := ret.(error); ok {
				return 0, err
			}
			return ret.(uint64), nil
		}
		if ret, err := n.forwardToLeader(ctx, cmd); err != nil {
			return 0, err
		} else {
			return ret.Version, nil
		}
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

func (n *Node) WithRaft(raftAddr, joinAddr, logAddr string, batchSize uint64) error {
	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(n.id)
	c.ShutdownOnRemove = false
	c.HeartbeatTimeout = 2 * time.Second
	c.ElectionTimeout = 3 * time.Second
	c.LeaderLeaseTimeout = 1 * time.Second
	c.SnapshotThreshold = 1e6
	c.TrailingLogs = 1e5
	c.SnapshotInterval = 30 * time.Minute
	if batchSize > 0 {
		c.CommitTimeout = 10 * time.Millisecond
		c.MaxAppendEntries = int(batchSize)
		c.BatchApplyCh = true
	}
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
		n.logStore, logStore, stableStore = rs, rs, rs
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

	// must set heartbeat handler after new raft
	// transport.SetHeartbeatHandler(func(rpc raft.RPC) {})

	r.RegisterObserver(observer)

	if len(joinAddr) == 0 {
		var recoverable bool
		var err error
		if recoverable, err = raft.HasExistingState(logStore, stableStore, snapshots); err != nil {
			n.logger.Warn("node self recovery may fail, try bootstrap the cluster", log.Error(err))
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
			n.logger.Warn("node self recovery may fail, try bootstrap the cluster", log.Error(err))
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
	if n.isLeader.Load() {
		bf := n.raft.Barrier(30 * time.Second)
		if err := bf.Error(); err != nil {
			n.logger.Error("closing: error occurred while waiting FSM applying logs aligned with inflight logs", log.Error(err))
		}
	}
	sf := n.raft.Shutdown()
	if err := sf.Error(); err != nil {
		n.logger.Error("error occurred while shutting down raft", log.Error(err))
	}

	if err := n.logStore.Close(); err != nil {
		n.logger.Error("error occurred while closing raft log storage", log.Error(err))
	}
	return n.fsm.Close()
}

func NewNode(id string, fsm DBFSM, logger log.Logger) *Node {
	dispatcher := newEventDispatcher(fsm)
	opts := &storeOptions{
		ReadTimeout:  2000 * time.Millisecond,
		WriteTimeout: 3000 * time.Millisecond,
	}
	obCh := make(chan raft.Observation, 100)
	if logger == nil {
		logger = log.NopLogger
	}
	n := &Node{
		id:         id,
		fsm:        fsm,
		opts:       opts,
		dispatcher: dispatcher,
		obCh:       obCh,
		mu:         new(sync.RWMutex),
		logger:     logger,
	}
	go func() {
		for ob := range obCh {
			if e, ok := ob.Data.(raft.LeaderObservation); ok && len(e.LeaderID) != 0 {
				if err := n.setKVConn(string(e.LeaderID)); err != nil {
					n.logger.Error("failed to set KV connection to leader",
						log.String("leader_id", string(e.LeaderID)),
						log.Error(err))
				}
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
