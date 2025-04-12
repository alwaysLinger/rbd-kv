package store

import (
	"errors"
	"fmt"
	"log"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
)

var ErrSnapshotPersist = errors.New("snapshot persist failed")

type badgerSnapshot struct {
	db *badger.DB
	ts uint64
}

func (bs *badgerSnapshot) Persist(sink raft.SnapshotSink) error {
	log.Printf("start dumping from BadgerDB at %d\n", bs.ts)
	lastIndex, err := bs.db.NewStreamAt(bs.ts).Backup(sink, 0)
	if err != nil {
		_ = sink.Cancel()
		log.Printf("dump failed: %v\n", err)
		return fmt.Errorf("%w: %w", ErrSnapshotPersist, err)
	}
	log.Printf("successfully dumped, last dumped log entry index is %d\n", lastIndex)
	return sink.Close()
}

func (bs *badgerSnapshot) Release() {}
