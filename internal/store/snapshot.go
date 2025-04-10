package store

import (
	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
)

type badgerSnapshot struct {
	db *badger.DB
	ts uint64
}

func (bs *badgerSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := bs.db.NewStreamAt(bs.ts).Backup(sink, 0); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (bs *badgerSnapshot) Release() {}
