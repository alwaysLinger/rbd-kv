package service

import (
	"context"
	"fmt"
	"net"

	rbdkv "github.com/alwaysLinger/rbkv/internal/store"
	"github.com/alwaysLinger/rbkv/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type KVService struct {
	pb.UnimplementedRbdkvServer

	store  rbdkv.Store
	server *grpc.Server
}

func (s *KVService) Execute(ctx context.Context, command *pb.Command) (*pb.CommandResponse, error) {
	switch command.Op {
	case pb.Command_Get:
		val, err := s.store.Get(ctx, command)
		if err != nil {
			return nil, err
		}
		return &pb.CommandResponse{Value: val}, nil
	case pb.Command_Put:
		err := s.store.Put(ctx, command)
		if err != nil {
			return nil, err
		}
		return &pb.CommandResponse{}, nil
	case pb.Command_Delete:
		err := s.store.Delete(ctx, command)
		if err != nil {
			return nil, err
		}
		return &pb.CommandResponse{}, nil
	default:
		return nil, status.Error(codes.InvalidArgument, "operation not support")
	}
}

func (s *KVService) LeaderInfo(context.Context, *pb.LeaderRequest) (*pb.LeaderInfoResponse, error) {
	if addr, id, term, err := s.store.LeaderInfo(); err != nil {
		return nil, err
	} else {
		return &pb.LeaderInfoResponse{
			LeaderAddr: addr,
			LeaderId:   id,
			Term:       term,
		}, nil
	}
}

func (s *KVService) Watch(request *pb.WatchRequest, g grpc.ServerStreamingServer[pb.WatchResponse]) error {
	evtCh, err := s.store.Watch(g.Context(), request)
	if err != nil {
		return err
	}

	for evt := range evtCh {
		err := g.Send(&pb.WatchResponse{
			WatcherId: evt.WatcherId,
			Event:     evt,
		})
		if err != nil {
			return err
		}
	}

	return rbdkv.ErrWatcherClosed
}

func NewKVService(s rbdkv.Store) *KVService {
	return &KVService{
		UnimplementedRbdkvServer: pb.UnimplementedRbdkvServer{},
		store:                    s,
	}
}

func (s *KVService) Run(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.server = grpc.NewServer(
		grpc.ChainUnaryInterceptor(redirectServerUnaryInterceptor(1), errServerUnaryInterceptor()),
		grpc.StreamInterceptor(errServerStreamInterceptor()),
	)
	pb.RegisterRbdkvServer(s.server, s)
	if err := s.server.Serve(lis); err != nil {
		fmt.Printf("kv server stopped: %v\n", err)
		return err
	}
	return nil
}

func (s *KVService) Stop() error {
	s.server.GracefulStop()
	return nil
}
