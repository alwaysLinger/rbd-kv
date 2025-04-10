package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alwaysLinger/rbkv/pb"
	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

const (
	backUpGoNum  = 16
	restoreGoNum = 16

	maxVersionKept = 1000
)

var (
	consistentIndexKey = []byte("m.!ci")

	ErrUpdateConsistentIndex = errors.New("error occurred while update consistent index")
	ErrNotFound              = errors.New("key not found")
	ErrReadTxn               = errors.New("error occurred while read txn")
	ErrDeleteTxn             = errors.New("error occurred while delete txn")
	ErrUpdateTxn             = errors.New("error occurred while update txn")
	ErrCommitTxn             = errors.New("error occurred while commit txn")
)

type DB interface {
	DB() *badger.DB
}

type Txn interface {
	ReadAt(key []byte, at uint64) ([]byte, uint64, error)
	Write(kvs any) []any
	SetAt(key, val []byte, ttl time.Duration, ts uint64) any
	Delete(key []byte, ts uint64) any
}

type DBFSM interface {
	SyncCommittedIndex(commitIndex uint64)
	Get(key []byte, at uint64, linear bool) ([]byte, uint64, error)
	Stats(exact, withKeyCount bool) (lsmSize, vlogSize, keyCount uint64, err error)
	Close() error

	DB
	Txn
	raft.FSM
}

type FSM struct {
	db           *badger.DB
	gcTicker     *time.Ticker
	appliedIndex atomic.Uint64

	mu             sync.Mutex
	cond           *sync.Cond
	committedIndex uint64
}

func OpenFSM(dir string, opts *badger.Options, versionKept int) (*FSM, error) {
	if len(dir) == 0 {
		dir = os.TempDir()
	}

	if opts == nil {
		options := badger.LSMOnlyOptions(dir).
			WithDetectConflicts(false).
			WithNumGoroutines(backUpGoNum).
			WithMetricsEnabled(false).
			WithLoggingLevel(badger.WARNING)
		opts = &options
	}

	if versionKept == 0 {
		versionKept = 50
	}
	*opts = (*opts).WithNumVersionsToKeep(min(versionKept, maxVersionKept))

	s := new(FSM)

	db, err := badger.OpenManaged(*opts)
	if err != nil {
		return nil, err
	}
	s.db = db
	s.cond = sync.NewCond(&s.mu)

	if err := s.loadAppliedIndexFromDB(); err != nil {
		return nil, err
	}
	go s.runGC()

	return s, nil
}

func (s *FSM) loadAppliedIndexFromDB() error {
	txn := s.db.NewTransactionAt(math.MaxUint64, false)
	defer txn.Discard()
	if item, err := txn.Get(consistentIndexKey); err != nil {
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		return nil
	} else {
		_ = item.Value(func(val []byte) error {
			s.appliedIndex.Store(binary.BigEndian.Uint64(val))
			return nil
		})
	}
	return nil
}

func (s *FSM) runGC() {
	s.gcTicker = time.NewTicker(time.Hour)
	for range s.gcTicker.C {
	again:
		err := s.db.RunValueLogGC(0.5)
		if err == nil {
			goto again
		}
	}
}

func (s *FSM) Close() error {
	if s.gcTicker != nil {
		s.gcTicker.Stop()
	}
	_ = s.db.Sync()
	return s.db.Close()
}

func (s *FSM) DB() *badger.DB {
	return s.db
}

func (s *FSM) SyncCommittedIndex(commitIndex uint64) {
	s.mu.Lock()
	s.committedIndex = commitIndex
	s.mu.Unlock()
	s.cond.Broadcast()
}

func (s *FSM) waitForIndexAligned() <-chan struct{} {
	s.mu.Lock()
	cidx := s.committedIndex
	s.mu.Unlock()
	if s.appliedIndex.Load() >= cidx {
		return nil
	}

	ch := make(chan struct{}, 1)
	go func() {
		s.mu.Lock()
		if s.appliedIndex.Load() >= s.committedIndex {
			s.mu.Unlock()
			close(ch)
			return
		}
		for s.appliedIndex.Load() < s.committedIndex {
			s.cond.Wait()
		}
		s.mu.Unlock()
		close(ch)
	}()
	return ch
}

func (s *FSM) Get(key []byte, at uint64, linear bool) ([]byte, uint64, error) {
	if at == 0 {
		at = math.MaxUint64
	}
	if !linear {
		return s.ReadAt(key, at)
	}
	if ch := s.waitForIndexAligned(); ch != nil {
		<-ch
	}
	return s.ReadAt(key, at)
}

func (s *FSM) ReadAt(key []byte, at uint64) ([]byte, uint64, error) {
	var val []byte
	txn := s.db.NewTransactionAt(at, false)
	item, err := txn.Get(key)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, 0, fmt.Errorf("%w: key %s at %d", ErrNotFound, key, at)
		}
		return nil, 0, fmt.Errorf("%w: key %s at %d: %w", ErrReadTxn, key, at, err)
	}
	if val, err = item.ValueCopy(val); err != nil {
		return nil, 0, fmt.Errorf("%w: key %s at %d: %w", ErrReadTxn, key, at, err)
	}
	return val, item.Version(), nil
}

func (s *FSM) Write(kvs any) []any {
	logs, ok := kvs.([]*raft.Log)
	if !ok {
		panic("type assertion failed")
	}
	ret := make([]any, len(logs))
	for i, l := range logs {
		ret[i] = s.Apply(l)
	}
	return ret
}

func (s *FSM) update(ts uint64, f func(txn *badger.Txn) error) error {
	if ts == 0 {
		return nil
	}
	txn := s.db.NewTransactionAt(ts, true)
	defer txn.Discard()

	err := f(txn)
	if errors.Is(err, badger.ErrTxnTooBig) {
		log.Printf("ErrTxnTooBig occurred while set a kv pair: %v\n", err)
	} else if err != nil {
		log.Printf("err occurred while update txn: %v\n", err)
		return err
	}
	if err := txn.CommitAt(ts, nil); err != nil {
		log.Printf("err occurred while commit txn: %v\n", err)
		return fmt.Errorf("%w: %w", ErrCommitTxn, err)
	}
	return nil
}

func (s *FSM) syncConsistentIndex(txn *badger.Txn, ts uint64) error {
	if err := txn.SetEntry(badger.NewEntry(consistentIndexKey, uint64ToBytes(ts)).WithDiscard()); err != nil {
		return fmt.Errorf("%w: %w", ErrUpdateConsistentIndex, err)
	}
	return nil
}

func (s *FSM) SetAt(key, val []byte, ttl time.Duration, ts uint64) any {
	err := s.update(ts, func(txn *badger.Txn) error {
		if err := s.syncConsistentIndex(txn, ts); err != nil {
			return err
		}
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

func (s *FSM) Delete(key []byte, ts uint64) any {
	err := s.update(ts, func(txn *badger.Txn) error {
		if err := s.syncConsistentIndex(txn, ts); err != nil {
			return err
		}
		return txn.Delete(key)
	})
	if err != nil {
		return fmt.Errorf("%w:key %s at %d: %w", ErrDeleteTxn, key, ts, err)
	}
	return ts
}

func uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}

func (s *FSM) Apply(log *raft.Log) interface{} {
	if s.appliedIndex.Load() >= log.Index {
		return nil
	} else {
		s.appliedIndex.Store(log.Index)
	}

	cmd := &pb.Command{}
	err := proto.Unmarshal(log.Data, cmd)
	if err != nil {
		return err
	}

	switch cmd.Op {
	case pb.Command_Put:
		var ttl time.Duration
		if cmd.Kv.Ttl != nil {
			ttl = cmd.Kv.Ttl.AsDuration()
		}
		return s.SetAt(cmd.Kv.Key, cmd.Kv.Value, ttl, log.Index)
	case pb.Command_Delete:
		return s.Delete(cmd.Kv.Key, log.Index)
	default:
		panic("not excepted command operation")
	}
}

func (s *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &badgerSnapshot{db: s.db, ts: s.appliedIndex.Load()}, nil
}

func (s *FSM) Restore(snapshot io.ReadCloser) error {
	if err := s.db.DropAll(); err != nil {
		return err
	}

	if err := s.db.Load(snapshot, restoreGoNum); err != nil {
		return err
	}

	if err := s.loadAppliedIndexFromDB(); err != nil {
		return err
	}

	return nil
}

func (s *FSM) Stats(exact, withKeyCount bool) (lsmSize, vlogSize, keyCount uint64, err error) {
	if exact {
		s1, s2 := s.db.Size()
		lsmSize, vlogSize = uint64(s1), uint64(s2)
	} else {
		lsmSize, vlogSize = s.db.EstimateSize(nil)
	}
	if withKeyCount {
		err = s.db.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.IteratorOptions{})
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				keyCount++
			}
			return nil
		})
		if err != nil {
			return 0, 0, 0, err
		}
		return
	}
	return
}
