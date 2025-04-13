package store

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/alwaysLinger/rbkv/internal/log"
	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
)

var (
	ErrUpdateConsistentIndex = errors.New("error occurred while update consistent index")
	ErrKeyNotFound           = errors.New("key not found")
	ErrReadTxn               = errors.New("error occurred while read txn")
	ErrDeleteTxn             = errors.New("error occurred while delete txn")
	ErrUpdateTxn             = errors.New("error occurred while update txn")
	ErrCommitTxn             = errors.New("error occurred while commit txn")
)

type oracle interface {
	ts(log *raft.Log) uint64
}

type logOracle struct {
}

func (l *logOracle) ts(log *raft.Log) uint64 {
	return log.Index
}

type Txn interface {
	Read(key []byte) ([]byte, uint64, error)
	ReadAt(key []byte, at uint64) ([]byte, uint64, error)
	SetAt(key, val []byte, ttl time.Duration, ts uint64) any
	Delete(key []byte, ts uint64) any
}

type fsmTxn struct {
	db       *badger.DB
	onUpdate func(ts uint64, txn *badger.Txn) error
	logger   log.Logger
}

func (ft *fsmTxn) update(ts uint64, f func(txn *badger.Txn) error) error {
	if ts == 0 {
		panic("no ts provided")
	}
	txn := ft.db.NewTransactionAt(math.MaxUint64, true)
	defer txn.Discard()
	if ft.onUpdate != nil {
		if err := ft.onUpdate(ts, txn); err != nil {
			return nil
		}
	}
	if f != nil {
		err := f(txn)
		if errors.Is(err, badger.ErrTxnTooBig) {
			ft.logger.Warn("ErrTxnTooBig occurred while set a kv pair", log.Error(err))
		} else if err != nil {
			ft.logger.Error("error occurred while update txn", log.Error(err))
			return err
		}
	}
	if err := txn.CommitAt(ts, nil); err != nil {
		ft.logger.Error("error occurred while commit txn", log.Error(err))
		return fmt.Errorf("%w: %w", ErrCommitTxn, err)
	}
	return nil
}

func (ft *fsmTxn) nopUpdate(ts uint64) error {
	return ft.update(ts, nil)
}

func (ft *fsmTxn) Read(key []byte) ([]byte, uint64, error) {
	var val []byte
	var ver uint64
	err := ft.db.View(func(txn *badger.Txn) error {
		if item, err := txn.Get(key); err != nil {
			return err
		} else {
			if val, err = item.ValueCopy(val); err != nil {
				return err
			} else {
				ver = item.Version()
				return nil
			}
		}
	})
	if err != nil {
		return nil, 0, err
	}
	return val, ver, nil
}

func (ft *fsmTxn) ReadAt(key []byte, at uint64) ([]byte, uint64, error) {
	var val []byte
	txn := ft.db.NewTransactionAt(at, false)
	defer txn.Discard()
	item, err := txn.Get(key)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, 0, fmt.Errorf("%w: key %s at %d", ErrKeyNotFound, key, at)
		}
		return nil, 0, fmt.Errorf("%w: key %s at %d: %w", ErrReadTxn, key, at, err)
	}
	if val, err = item.ValueCopy(val); err != nil {
		return nil, 0, fmt.Errorf("%w: key %s at %d: %w", ErrReadTxn, key, at, err)
	}
	return val, item.Version(), nil
}

func (ft *fsmTxn) SetAt(key, val []byte, ttl time.Duration, ts uint64) any {
	err := ft.update(ts, func(txn *badger.Txn) error {
		ent := badger.NewEntry(key, val)
		if ttl != 0 {
			ent.WithTTL(ttl)
		}
		return txn.SetEntry(ent)
	})
	if err != nil {
		return fmt.Errorf("%w:key %s value %s at %d: %w", ErrUpdateTxn, key, val, ts, err)
	}
	return ts
}

func (ft *fsmTxn) Delete(key []byte, ts uint64) any {
	err := ft.update(ts, func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
	if err != nil {
		return fmt.Errorf("%w:key %s at %d: %w", ErrDeleteTxn, key, ts, err)
	}
	return ts
}
