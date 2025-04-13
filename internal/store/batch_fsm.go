package store

import (
	"io"

	"github.com/alwaysLinger/rbkv/internal/log"
	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
)

type BatchFSM struct {
	fsm *FSM
}

func (b *BatchFSM) Get(key []byte, at uint64) ([]byte, UserMeta, uint64, error) {
	return b.fsm.Get(key, at)
}

func (b *BatchFSM) applyBatch(logs []*raft.Log) []any {
	ret := make([]any, len(logs))
	for i, l := range logs {
		ret[i] = b.fsm.apply(l)
	}
	return ret
}

func (b *BatchFSM) ApplyBatch(logs []*raft.Log) []interface{} {
	return b.applyBatch(logs)
}

func (b *BatchFSM) Apply(l *raft.Log) interface{} {
	return b.fsm.Apply(l)
}

func (b *BatchFSM) Snapshot() (raft.FSMSnapshot, error) {
	return b.fsm.Snapshot()
}

func (b *BatchFSM) Restore(snapshot io.ReadCloser) error {
	return b.fsm.Restore(snapshot)
}

func (b *BatchFSM) DB() *badger.DB {
	return b.fsm.DB()
}

func (b *BatchFSM) Stats(exact, withKeyCount bool) (lsmSize, vlogSize, keyCount uint64, err error) {
	return b.fsm.Stats(exact, withKeyCount)
}

func (b *BatchFSM) Close() error {
	return b.fsm.Close()
}

func OpenBatchFSM(dir string, opts *badger.Options, versionKept int, logger log.Logger) (*BatchFSM, error) {
	fsm, err := OpenFSM(dir, opts, versionKept, logger)
	if err != nil {
		return nil, err
	}
	return &BatchFSM{fsm: fsm}, nil
}
