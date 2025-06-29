package store

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/alwaysLinger/rbkv/pb"
	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

var (
	ErrBatchUnmarshal = errors.New("failed to unmarshal raft log data")
	ErrBatchSetEntry  = errors.New("failed to set entry within batch")
	ErrBatchDelEntry  = errors.New("failed to delete entry within batch")
	ErrBatchFlush     = errors.New("failed to flush a batch")
)

type Batcher interface {
	Flush(*batch) []any
}

type writeBatch struct {
	db         *badger.DB
	oracle     oracle
	pool       *sync.Pool
	onEachItem func(ts uint64, wb *badger.WriteBatch) error
}

func (wb *writeBatch) Flush(b *batch) []any {
	defer b.release(wb.pool)

	ret := make([]any, len(b.items))
	bwb := wb.db.NewManagedWriteBatch()
	defer bwb.Cancel()

	for i, item := range b.items {
		if item.err != nil {
			ret[i] = item.err
			continue
		}

		if item.op != pb.Command_CmdUnknown {
			if item.op == pb.Command_Put {
				ent := badger.NewEntry(item.key, item.val).WithMeta(byte(item.meta))
				if item.ttl != 0 {
					ent.WithTTL(item.ttl)
				}
				if err := bwb.SetEntryAt(ent, item.ts); err != nil {
					ret[i] = fmt.Errorf("%w: key %s val %s at %d: %w", ErrBatchSetEntry, item.key, item.val, item.ts, err)
					continue
				} else {
					ret[i] = item.ts
				}
			} else {
				if err := bwb.DeleteAt(item.key, item.ts); err != nil {
					ret[i] = fmt.Errorf("%w: key %s at %d: %w", ErrBatchDelEntry, item.key, item.ts, err)
					continue
				} else {
					ret[i] = item.ts
				}
			}
			if wb.onEachItem != nil {
				if err := wb.onEachItem(item.ts, bwb); err != nil {
					ret[i] = err
				}
			}
		}
	}

	if err := bwb.Flush(); err != nil {
		err = fmt.Errorf("%w: %w", ErrBatchFlush, err)
		idx := slices.IndexFunc(b.items, func(item *batchItem) bool {
			return item.op != pb.Command_CmdUnknown
		})
		for i := idx; i < len(b.items); i++ {
			if ret[i] == nil {
				ret[i] = err
			}
		}
		return ret
	}
	return ret
}

func newWriteBatch(db *badger.DB, oracle oracle, onUpdate func(ts uint64, wb *badger.WriteBatch) error) *writeBatch {
	return &writeBatch{
		db:     db,
		oracle: oracle,
		pool: &sync.Pool{
			New: func() any {
				return &batchItem{}
			},
		},
		onEachItem: onUpdate,
	}
}

func (wb *writeBatch) getItem() *batchItem {
	return wb.pool.Get().(*batchItem)
}

type batchItem struct {
	op   pb.Command_OpType
	key  []byte
	val  []byte
	meta UserMeta
	ttl  time.Duration
	ts   uint64
	err  error
}

func (bi *batchItem) reset() {
	bi.op = pb.Command_CmdUnknown
	bi.key = nil
	bi.val = nil
	bi.meta = 0
	bi.ttl = 0
	bi.ts = 0
}

type batch struct {
	items []*batchItem
}

func (b *batch) release(pool *sync.Pool) {
	for _, item := range b.items {
		item.reset()
		pool.Put(item)
	}
}

func (wb *writeBatch) newBatch(logs []*raft.Log, appliedIndex uint64) *batch {
	items := make([]*batchItem, 0, len(logs))
	for _, log := range logs {
		item := wb.getItem()
		if log.Index > appliedIndex {
			if log.Type == raft.LogCommand {
				cmd := &pb.Command{}
				if err := proto.Unmarshal(log.Data, cmd); err != nil {
					item.err = fmt.Errorf("%w: %s: %w", ErrBatchUnmarshal, log.Data, err)
				} else {
					item.op = cmd.Op
					item.key = cmd.Kv.Key
					item.val = cmd.Kv.Value
					item.meta = UserMeta(cmd.Kv.Meta)
					if cmd.Kv.Ttl != nil {
						item.ttl = cmd.Kv.Ttl.AsDuration()
					}
					item.ts = wb.oracle.ts(log)
				}
			}
		}
		items = append(items, item)
	}

	return &batch{items: items}
}
