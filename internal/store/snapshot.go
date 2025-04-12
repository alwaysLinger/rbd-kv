package store

import (
	"errors"
	"fmt"

	"github.com/alwaysLinger/rbkv/internal/log"
	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
)

var ErrSnapshotPersist = errors.New("snapshot persist failed")

type badgerSnapshot struct {
	db     *badger.DB
	ts     uint64
	logger log.Logger
}

func (bs *badgerSnapshot) Persist(sink raft.SnapshotSink) error {
	bs.logger.Info("start dumping from BadgerDB", log.Uint64("timestamp", bs.ts))
	lastIndex, err := bs.db.NewStreamAt(bs.ts).Backup(sink, 0)
	if err != nil {
		_ = sink.Cancel()
		bs.logger.Error("dump failed", log.Error(err))
		return fmt.Errorf("%w: %w", ErrSnapshotPersist, err)
	}
	bs.logger.Info("successfully dumped", log.Uint64("last_index", lastIndex))
	return sink.Close()
}

func (bs *badgerSnapshot) Release() {}
