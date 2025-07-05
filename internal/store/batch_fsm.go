package store

import (
	"fmt"
	"io"

	"github.com/alwaysLinger/rbkv/internal/log"
	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
)

type BatchFSM struct {
	fsm *FSM
	txn BatchTxn[*badger.Item, *batch]
	wb  *writeBatch
}

func (b *BatchFSM) Get(key []byte, at uint64) ([]byte, UserMeta, uint64, error) {
	return b.fsm.Get(key, at)
}

func (b *BatchFSM) applyBatch(logs []*raft.Log) []any {
	last := logs[len(logs)-1]
	if last.Index <= b.fsm.appliedIndex {
		return make([]any, len(logs))
	}

	defer func() {
		if b.fsm.appliedIndex < last.Index {
			b.fsm.appliedIndex = last.Index
		}
	}()

	items := b.wb.newBatch(logs, b.fsm.appliedIndex)
	wb := b.txn.WriteBatch(items)
	return wb.Flush(items)
}

func (b *BatchFSM) ApplyBatch(logs []*raft.Log) []any {
	if len(logs) == 1 {
		return []any{b.Apply(logs[0])}
	}
	return b.applyBatch(logs)
}

func (b *BatchFSM) Apply(l *raft.Log) any {
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

	b := &BatchFSM{fsm: fsm}
	batcher := newWriteBatch(b.DB(), fsm.oracle, b.syncConsistentIndex)
	b.txn = &batchFsmTxn[*batch]{Txn: fsm.txn, batcher: batcher}
	b.wb = batcher
	return b, nil
}

func (b *BatchFSM) syncConsistentIndex(ts uint64, wb *badger.WriteBatch) error {
	if err := wb.SetEntryAt(badger.NewEntry(consistentIndexKey, uint64ToBytes(ts)).WithDiscard(), ts); err != nil {
		return fmt.Errorf("%w: %w", ErrUpdateConsistentIndex, err)
	}
	return nil
}
