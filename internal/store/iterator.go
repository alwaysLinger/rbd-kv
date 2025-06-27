package store

import (
	"math"

	"github.com/dgraph-io/badger/v4"
)

type Iterator interface {
	Iterate() error
}

type badgerDBIterator struct {
	db      *badger.DB
	prefix  []byte
	f       func(item *badger.Item) error
	reverse bool
	at      uint64
	opts    *badger.IteratorOptions
}

func (bit *badgerDBIterator) Iterate() error {
	if bit.at == 0 {
		bit.at = math.MaxUint64
	}

	txn := bit.db.NewTransactionAt(bit.at, false)
	defer txn.Discard()

	if bit.opts == nil {
		bit.opts = &badger.IteratorOptions{}
	}
	bit.opts.Reverse = bit.reverse
	it := txn.NewIterator(*bit.opts)
	defer it.Close()

	for it.Seek(bit.prefix); it.ValidForPrefix(bit.prefix); it.Next() {
		if err := bit.f(it.Item()); err != nil {
			return err
		}
	}
	return nil
}

func newBadgerDBIterator(db *badger.DB, prefix []byte, f func(item *badger.Item) error, reverse bool, at uint64) *badgerDBIterator {
	return &badgerDBIterator{
		db:      db,
		prefix:  prefix,
		f:       f,
		reverse: reverse,
		at:      at,
	}
}

func (bit *badgerDBIterator) withIterateOptions(opts *badger.IteratorOptions) *badgerDBIterator {
	bit.opts = opts
	return bit
}
