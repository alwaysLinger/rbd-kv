package store

import (
	"io"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
)

type BatchFSM struct {
	fsm *FSM
}

func (b *BatchFSM) Get(key []byte, at uint64, linear bool) ([]byte, uint64, error) {
	return b.fsm.Get(key, at, linear)
}

func (b *BatchFSM) ReadAt(key []byte, at uint64) ([]byte, uint64, error) {
	return b.fsm.ReadAt(key, at)
}

func (b *BatchFSM) Write(kvs any) []any {
	return b.fsm.Write(kvs)
}

func (b *BatchFSM) SetAt(key, val []byte, ttl time.Duration, ts uint64) any {
	return b.fsm.SetAt(key, val, ttl, ts)
}

func (b *BatchFSM) Delete(key []byte, ts uint64) any {
	return b.fsm.Delete(key, ts)
}

func (b *BatchFSM) SyncCommittedIndex(commitIndex uint64) {
	b.fsm.SyncCommittedIndex(commitIndex)
}

func (b *BatchFSM) applyBatch(logs []*raft.Log) []any {
	return b.fsm.Write(logs)
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

func OpenBatchFSM(dir string, opts *badger.Options, versionKept int) (*BatchFSM, error) {
	fsm, err := OpenFSM(dir, opts, versionKept)
	if err != nil {
		return nil, err
	}
	return &BatchFSM{fsm: fsm}, nil
}
