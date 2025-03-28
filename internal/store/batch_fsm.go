package store

import (
	"errors"
	"io"
	"log"
	"slices"
	"time"

	"github.com/alwaysLinger/rbkv/pb"
	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

type BatchFSM struct {
	fsm *FSM
}

func (b *BatchFSM) applyBatch(logs []*raft.Log) []any {
	ret := make([]any, len(logs))
	lastLog := logs[len(logs)-1]
	if b.fsm.appliedIndex >= lastLog.Index {
		return ret
	}

	i := slices.IndexFunc(logs, func(log *raft.Log) bool {
		return log.Type == raft.LogCommand
	})
	if i == -1 {
		// not much that we can do about this error
		err := b.fsm.db.Update(func(txn *badger.Txn) error {
			b.fsm.appliedIndex = lastLog.Index
			if err := txn.Set(consistentIndexKey, uint64ToBytes(lastLog.Index)); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			log.Printf("error occurred while update consistent index to BadgerDB: %v\n", err)
		}
		return ret
	}

	txn := b.fsm.db.NewTransaction(true)
	for i, l := range logs {
		if b.fsm.appliedIndex >= l.Index || l.Type != raft.LogCommand {
			continue
		}
		var cmd pb.Command
		if err := proto.Unmarshal(l.Data, &cmd); err != nil {
			ret[i] = err
			continue
		}
		if cmd.Op == pb.Command_Get {
			if item, err := txn.Get(cmd.Key); err != nil {
				if errors.Is(err, badger.ErrKeyNotFound) {
					ret[i] = ErrNotFound
				} else {
					ret[i] = err
				}
			} else {
				if err := item.Value(func(val []byte) error {
					ret[i] = append([]byte{}, val...)
					return nil
				}); err != nil {
					ret[i] = err
				}
			}
		} else {
			if cmd.Op == pb.Command_Put {
				ent := badger.NewEntry(cmd.Key, cmd.Value)
				if cmd.Ttl != 0 {
					ent.WithTTL(time.Duration(cmd.Ttl) * time.Second)
				}
				if err := txn.SetEntry(ent); err != nil {
					if errors.Is(err, badger.ErrTxnTooBig) {
						_ = txn.Commit()
						txn = b.fsm.db.NewTransaction(true)
						_ = txn.SetEntry(ent)
					} else {
						log.Printf("error occurred while batching set txn to BadgerDB: %v\n", err)
						ret[i] = err
					}
				}
			} else {
				if err := txn.Delete(cmd.Key); err != nil {
					if errors.Is(err, badger.ErrTxnTooBig) {
						_ = txn.Commit()
						txn = b.fsm.db.NewTransaction(true)
						_ = txn.Delete(cmd.Key)
					} else {
						log.Printf("error occurred while batching delete txn to BadgerDB: %v\n", err)
						ret[i] = err
					}
				}
			}
		}
	}

	b.fsm.appliedIndex = lastLog.Index
	if err := txn.Set(consistentIndexKey, uint64ToBytes(lastLog.Index)); err != nil {
		if errors.Is(err, badger.ErrTxnTooBig) {
			txn = b.fsm.db.NewTransaction(true)
			_ = txn.Set(consistentIndexKey, uint64ToBytes(lastLog.Index))
		} else {
			log.Printf("error occurred while update consistent index to BadgerDB: %v\n", err)
		}
	}
	if err := txn.Commit(); err != nil {
		// not much that we can do about this error
		log.Printf("error occurred while commit txn to BadgerDB: %v\n", err)
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
	return b.fsm.db
}

func (b *BatchFSM) Stats(exact, withKeyCount bool) (lsmSize, vlogSize, keyCount uint64, err error) {
	return b.fsm.Stats(exact, withKeyCount)
}

func (b *BatchFSM) Close() error {
	return b.fsm.Close()
}

func OpenBatchFSM(dir string, opts *badger.Options) (*BatchFSM, error) {
	fsm, err := OpenFSM(dir, opts)
	if err != nil {
		return nil, err
	}

	return &BatchFSM{fsm: fsm}, nil
}
