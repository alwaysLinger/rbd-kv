package meta

import (
	"context"
	"strconv"

	"google.golang.org/grpc/metadata"
)

const redirectTimes string = "redirect_times"

func RedirectTimes(ctx context.Context) (int, error) {
	m, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return 0, nil
	}
	if rts := m.Get(redirectTimes); len(rts) == 0 {
		return 0, nil
	} else {
		if t, err := strconv.Atoi(rts[0]); err != nil {
			return 0, err
		} else {
			return t, nil
		}
	}
}

func RedirectCtx(ctx context.Context) (context.Context, error) {
	times, err := RedirectTimes(ctx)
	if err != nil {
		return nil, err
	}
	md := metadata.New(map[string]string{
		redirectTimes: strconv.Itoa(times + 1),
	})
	if oldMD, ok := metadata.FromIncomingContext(ctx); ok {
		md = metadata.Join(oldMD.Copy(), md)
	}
	newCtx := metadata.NewOutgoingContext(ctx, md)
	return newCtx, nil
}
