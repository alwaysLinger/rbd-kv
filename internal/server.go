package internal

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/alwaysLinger/rbkv/internal/service"
	storage "github.com/alwaysLinger/rbkv/internal/store"
	"golang.org/x/sync/errgroup"
)

var ErrOptsCheckFailed = errors.New("options check failed")

type Server struct {
	ops   *Options
	n     storage.RaftNode
	kvSvc *service.KVService
}

type Options struct {
	RaftAddr string
	GrpcAddr string
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

	fsm, err := storage.OpenFSM(filepath.Join(opts.KVDir, "kv"), nil)
	if err != nil {
		return nil, err
	}

	n := storage.NewNode(opts.NodeID, fsm)
	if err := n.WithRaft(opts.RaftAddr, opts.JoinAddr, opts.LogDir); err != nil {
		return nil, err
	}

	kvSvc := service.NewKVService(n, n)
	s := &Server{
		ops:   opts,
		n:     n,
		kvSvc: kvSvc,
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
	if err := s.kvSvc.Stop(); err != nil {
		return err
	}
	if err := s.n.Close(); err != nil {
		return err
	}
	return nil
}
