package service

import (
	"context"
	"errors"

	nerr "github.com/alwaysLinger/rbkv/error"
	"github.com/alwaysLinger/rbkv/internal/meta"
	"github.com/alwaysLinger/rbkv/internal/store"
	"github.com/alwaysLinger/rbkv/pb"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func wrapNodeError(err error) error {
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
	return err
}

func errServerUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		if err = ctx.Err(); err != nil {
			return nil, status.Error(codes.DeadlineExceeded, err.Error())
		}

		resp, err = handler(ctx, req)
		if err != nil {
			if s, ok := status.FromError(err); ok {
				return nil, status.Error(s.Code(), s.Message())
			}
			if wrappedErr := wrapNodeError(err); !errors.Is(wrappedErr, err) {
				return nil, wrappedErr
			}
			if errors.Is(err, raft.ErrEnqueueTimeout) {
				return nil, status.Error(codes.DeadlineExceeded, err.Error())
			}
			if errors.Is(err, store.ErrKeyNotFound) {
				return nil, status.Error(codes.NotFound, err.Error())
			}
			return nil, status.Error(codes.Internal, err.Error())
		}
		return
	}
}

func errServerStreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := ss.Context().Err(); err != nil {
			return status.Error(codes.DeadlineExceeded, err.Error())
		}

		err := handler(srv, ss)
		if err != nil {
			if wrappedErr := wrapNodeError(err); !errors.Is(wrappedErr, err) {
				return wrappedErr
			}
			return status.Error(codes.Internal, err.Error())
		}
		return nil
	}
}

func redirectServerUnaryInterceptor(redirectLimit int) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		times, err := meta.RedirectTimes(ctx)
		if err != nil {
			return nil, status.Errorf(codes.Aborted, "extract redirect times failed: %v", err)
		}
		if times > redirectLimit {
			return nil, status.Errorf(codes.Aborted, "limit of redirect has been reached")
		}
		resp, err = handler(ctx, req)
		if err != nil {
			if wrappedErr := wrapNodeError(err); !errors.Is(wrappedErr, err) {
				return nil, wrappedErr
			}
			if errors.Is(err, raft.ErrEnqueueTimeout) {
				return nil, status.Error(codes.DeadlineExceeded, err.Error())
			}
			if errors.Is(err, store.ErrKeyNotFound) {
				return nil, status.Error(codes.NotFound, err.Error())
			}
			return nil, status.Error(codes.Internal, err.Error())
		}
		return
	}
}
