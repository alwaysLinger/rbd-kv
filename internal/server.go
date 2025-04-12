package internal

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/alwaysLinger/rbkv/internal/log"
	"github.com/alwaysLinger/rbkv/internal/service"
	storage "github.com/alwaysLinger/rbkv/internal/store"
	"golang.org/x/sync/errgroup"
)

const minRaftLogBatchSize uint64 = 10
const maxRaftLogBatchSize uint64 = 500

var ErrOptsCheckFailed = errors.New("options check failed")

type Server struct {
	ops    *Options
	n      storage.RaftNode
	kvSvc  *service.KVService
	logger log.Logger
}

type Options struct {
	RaftAddr    string
	GrpcAddr    string
	JoinAddr    string
	NodeID      string
	LogDir      string
	KVDir       string
	BatchSize   uint64
	VersionKeep int
}

func NewServer(opts *Options) (*Server, error) {
	if opts == nil {
		return nil, fmt.Errorf("opts can not be nil")
	}

	if err := checkOpts(opts); err != nil {
		return nil, err
	}

	logger, err := log.NewLogger()
	if err != nil {
		return nil, fmt.Errorf("init logger failed: %w", err)
	}

	var fsm storage.DBFSM
	if opts.BatchSize > 0 {
		fsm, err = storage.OpenBatchFSM(filepath.Join(opts.KVDir, "kv"), nil, opts.VersionKeep, logger)
	} else {
		fsm, err = storage.OpenFSM(filepath.Join(opts.KVDir, "kv"), nil, opts.VersionKeep, logger)
	}
	if err != nil {
		return nil, err
	}

	n := storage.NewNode(opts.NodeID, fsm, logger)
	if err := n.WithRaft(opts.RaftAddr, opts.JoinAddr, opts.LogDir, opts.BatchSize); err != nil {
		return nil, err
	}

	kvSvc := service.NewKVService(n, n)
	s := &Server{
		ops:    opts,
		n:      n,
		kvSvc:  kvSvc,
		logger: logger,
	}
	return s, nil
}

func checkOpts(opts *Options) error {
	if len(opts.GrpcAddr) == 0 {
		return fmt.Errorf("%w:grpc-addr not found", ErrOptsCheckFailed)
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
	if opts.BatchSize != 0 {
		opts.BatchSize = max(opts.BatchSize, minRaftLogBatchSize)
		opts.BatchSize = min(opts.BatchSize, maxRaftLogBatchSize)
	}
	if opts.VersionKeep < 0 {
		return fmt.Errorf("%w:version keep num less then 0", ErrOptsCheckFailed)
	}
	return nil
}

func (s *Server) Run() error {
	if err := s.n.Run(); err != nil {
		_ = s.n.Close()
		return err
	}
	g, _ := errgroup.WithContext(context.Background())
	g.Go(func() error {
		return s.kvSvc.Run(s.ops.GrpcAddr)
	})
	return g.Wait()
}

func (s *Server) Close() error {
	defer s.logger.Sync()
	if err := s.kvSvc.Stop(); err != nil {
		s.logger.Error("failed to stop KV service", log.Error(err))
		return err
	}
	if err := s.n.Close(); err != nil {
		s.logger.Error("failed to close node", log.Error(err))
		return err
	}
	return nil
}
