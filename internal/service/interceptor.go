package service

import (
	"context"
	"errors"

	nerr "github.com/alwaysLinger/rbkv/error"
	"github.com/alwaysLinger/rbkv/pb"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func errServerUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		if err = ctx.Err(); err != nil {
			return nil, status.Error(codes.DeadlineExceeded, err.Error())
		}

		ret, err := handler(ctx, req)
		if err != nil {
			var nodeErr *nerr.NodeError
			if errors.As(err, &nodeErr) {
				st, err := status.New(codes.FailedPrecondition, nodeErr.Error()).WithDetails(&pb.LeaderInfoResponse{
					LeaderAddr: nodeErr.LeaderAddr,
					LeaderId:   nodeErr.LeaderID,
					Term:       nodeErr.Term,
				})
				if err != nil {
					return nil, err
				}
				return nil, st.Err()
			}

			if errors.Is(err, raft.ErrEnqueueTimeout) {
				return nil, status.Error(codes.DeadlineExceeded, err.Error())
			}
			return nil, status.Error(codes.Internal, err.Error())
		}

		return ret, nil
	}
}

func errServerStreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := ss.Context().Err(); err != nil {
			return status.Error(codes.DeadlineExceeded, err.Error())
		}

		err := handler(srv, ss)
		if err != nil {
			var nodeErr *nerr.NodeError
			if errors.As(err, &nodeErr) {
				st, err := status.New(codes.FailedPrecondition, nodeErr.Error()).WithDetails(&pb.LeaderInfoResponse{
					LeaderAddr: nodeErr.LeaderAddr,
					LeaderId:   nodeErr.LeaderID,
					Term:       nodeErr.Term,
				})
				if err != nil {
					return err
				}
				return st.Err()
			}

			return status.Error(codes.Internal, err.Error())
		}

		return nil
	}
}
