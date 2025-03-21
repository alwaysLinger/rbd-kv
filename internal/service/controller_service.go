package service

import (
	"context"
	"fmt"
	"net"

	"github.com/alwaysLinger/rbkv/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

type NodeController interface {
	AddPeer(ctx context.Context, id, address string) error
}

type ControllerService struct {
	pb.UnimplementedControllerServer

	nc     NodeController
	server *grpc.Server
}

func (s *ControllerService) Join(ctx context.Context, request *pb.JoinRequest) (*pb.JoinResponse, error) {
	err := s.nc.AddPeer(ctx, request.Id, request.Addr)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, "not a leader")
	}
	return &pb.JoinResponse{}, nil
}

func NewControllerService(nc NodeController) *ControllerService {
	return &ControllerService{
		UnimplementedControllerServer: pb.UnimplementedControllerServer{},
		nc:                            nc,
	}
}

func (s *ControllerService) Run(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	s.server = grpc.NewServer(grpc.UnaryInterceptor(errServerUnaryInterceptor()))
	pb.RegisterControllerServer(s.server, s)
	reflection.Register(s.server)
	if err := s.server.Serve(lis); err != nil {
		fmt.Printf("controller server stopped: %v\n", err)
		return err
	}

	return nil
}

func (s *ControllerService) Stop() error {
	s.server.GracefulStop()
	return nil
}
