package internal

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	raftbadger "github.com/alwaysLinger/raft-badgerdb"
	"github.com/alwaysLinger/rbkv/internal/service"
	storage "github.com/alwaysLinger/rbkv/internal/store"
	"github.com/alwaysLinger/rbkv/pb"
	"github.com/hashicorp/raft"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

var ErrOptsCheckFailed = errors.New("options check failed")

type Server struct {
	ops    *Options
	n      storage.Store
	ctlSvc *service.ControllerService
	kvSvc  *service.KVService
}

type Options struct {
	RaftAddr string
	GrpcAddr string
	AdvAddr  string
	JoinAddr string
	NodeID   string
	LogDir   string
	KVDir    string
}

func NewServer(opts *Options) (*Server, error) {
	if opts == nil {
		return nil, fmt.Errorf("opts can not be nil")
	}

	if err := checkOpts(opts); err != nil {
		return nil, err
	}

	fsm, err := storage.OpenFSM(filepath.Join(opts.KVDir, "kv"), nil, nil)
	if err != nil {
		return nil, err
	}

	n := storage.NewNode(fsm, opts.NodeID)
	r, err := newRaft(opts.RaftAddr, opts.NodeID, opts.LogDir, opts.JoinAddr, fsm, n.ObChan())
	if err != nil {
		return nil, err
	}
	n.WithRaft(r)

	kvSvc := service.NewKVService(n)
	s := &Server{
		ops:   opts,
		n:     n,
		kvSvc: kvSvc,
	}
	if opts.AdvAddr != "" {
		svc := service.NewControllerService(n)
		s.ctlSvc = svc
	}
	return s, nil
}

func checkOpts(opts *Options) error {
	if (opts.AdvAddr == "" && opts.JoinAddr == "") || (len(opts.AdvAddr) != 0 && len(opts.JoinAddr) != 0) {
		return fmt.Errorf("%w:advertise-addr and join-addr should not be set together, nor should they be set simultaneously", ErrOptsCheckFailed)
	}
	if len(opts.RaftAddr) == 0 {
		return fmt.Errorf("%w:raft-addr not found", ErrOptsCheckFailed)
	}
	if len(opts.NodeID) == 0 {
		return fmt.Errorf("%w:node-id not found", ErrOptsCheckFailed)
	}
	if len(opts.LogDir) == 0 {
		return fmt.Errorf("%w:raft log dir not found", ErrOptsCheckFailed)
	}
	if len(opts.KVDir) == 0 {
		return fmt.Errorf("%w:kv dir not found", ErrOptsCheckFailed)
	}
	return nil
}

func (s *Server) Run() error {
	if err := s.n.Open(); err != nil {
		_ = s.n.Close()
		return err
	}

	g, _ := errgroup.WithContext(context.Background())
	if s.ctlSvc != nil {
		g.Go(func() error {
			return s.ctlSvc.Run(s.ops.AdvAddr)
		})
	}
	g.Go(func() error {
		return s.kvSvc.Run(s.ops.GrpcAddr)
	})
	return g.Wait()
}

func (s *Server) Close() error {
	if err := s.kvSvc.Stop(); err != nil {
		return err
	}
	if s.ctlSvc != nil {
		if err := s.ctlSvc.Stop(); err != nil {
			return err
		}
	}
	if err := s.n.Close(); err != nil {
		return err
	}
	return nil
}

func newRaft(addr, nodeID, dir, joinAddr string, fsm raft.FSM, obCh chan raft.Observation) (*raft.Raft, error) {
	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(nodeID)

	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}
	transport, err := raft.NewTCPTransport(addr, tcpAddr, 3, 5*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}

	snapshots, err := raft.NewFileSnapshotStore(dir, 3, os.Stderr)
	if err != nil {
		return nil, err
	}

	logDir := filepath.Join(dir, "raftlog")
	var logStore raft.LogStore
	var stableStore raft.StableStore
	var rs *raftbadger.Store
	if rs, err = raftbadger.NewStore(logDir, nil, nil); err != nil {
		return nil, err
	} else {
		logStore, stableStore = rs, rs
	}

	observer := raft.NewObserver(obCh, true, func(o *raft.Observation) bool {
		switch o.Data.(type) {
		case raft.LeaderObservation, raft.PeerObservation:
			return true
		default:
			return false
		}
	})

	r, err := raft.NewRaft(c, fsm, logStore, stableStore, snapshots, transport)
	if err != nil {
		return nil, err
	}
	r.RegisterObserver(observer)

	if joinAddr == "" {
		conf := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      c.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		r.BootstrapCluster(conf)
	} else {
		conn, err := grpc.Dial(joinAddr, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			return nil, err
		}
		defer conn.Close()
		client := pb.NewControllerClient(conn)
		req := &pb.JoinRequest{
			Id:   nodeID,
			Addr: addr,
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err = client.Join(ctx, req)
		if err != nil {
			return nil, err
		}
	}

	return r, nil
}
