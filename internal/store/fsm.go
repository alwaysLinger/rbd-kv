package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"time"

	"github.com/alwaysLinger/rbkv/internal/log"
	"github.com/alwaysLinger/rbkv/pb"
	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

const (
	backUpGoNum  = 64
	restoreGoNum = 64

	maxVersionKept = 1000
)

var (
	consistentIndexKey = []byte("m.!ci")

	ErrRestore = errors.New("restore failed")
)

type DB interface {
	DB() *badger.DB
}

type Getter interface {
	Val() []byte
	Version() uint64
}

type getter struct {
	val []byte
	ver uint64
}

func (g getter) Val() []byte {
	return g.val
}

func (g getter) Version() uint64 {
	return g.ver
}

type DBFSM interface {
	Get(key []byte, at uint64) (Getter, error)
	Stats(exact, withKeyCount bool) (lsmSize, vlogSize, keyCount uint64, err error)
	Close() error

	DB
	raft.FSM
}

type FSM struct {
	db           *badger.DB
	gcTicker     *time.Ticker
	appliedIndex uint64 // this field will never be accessed concurrently
	logger       log.Logger
	oracle       oracle
	txn          Txn
}

func OpenFSM(dir string, opts *badger.Options, versionKept int, logger log.Logger) (*FSM, error) {
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
	if logger == nil {
		logger = log.NopLogger
	}
	s.logger = logger
	s.oracle = &logOracle{}
	s.txn = &fsmTxn{
		db:       s.db,
		onUpdate: s.syncConsistentIndex,
		logger:   s.logger,
	}

	if err := s.loadAppliedIndex(); err != nil {
		return nil, err
	}
	go s.runGC()

	return s, nil
}

func (s *FSM) loadAppliedIndex() error {
	val, _, err := s.txn.Read(consistentIndexKey)
	if err != nil {
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		return nil
	}
	s.appliedIndex = binary.BigEndian.Uint64(val)
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

func (s *FSM) Get(key []byte, at uint64) (Getter, error) {
	if at == 0 {
		at = math.MaxUint64
	}
	return s.get(key, at)
}

func (s *FSM) get(key []byte, at uint64) (Getter, error) {
	val, ver, err := s.txn.ReadAt(key, at)
	if err != nil {
		return nil, err
	}
	return getter{
		val: val,
		ver: ver,
	}, nil
}

func (s *FSM) syncConsistentIndex(ts uint64, txn *badger.Txn) error {
	if err := txn.SetEntry(badger.NewEntry(consistentIndexKey, uint64ToBytes(ts)).WithDiscard()); err != nil {
		return fmt.Errorf("%w: %w", ErrUpdateConsistentIndex, err)
	}
	return nil
}

func (s *FSM) put(key, val []byte, ttl time.Duration, ts uint64) any {
	return s.txn.SetAt(key, val, ttl, ts)
}

func (s *FSM) del(key []byte, ts uint64) any {
	return s.txn.Delete(key, ts)
}

func uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}

func (s *FSM) apply(log *raft.Log) any {
	if s.appliedIndex >= log.Index {
		return nil
	}
	defer func() {
		if s.appliedIndex < log.Index {
			s.appliedIndex = log.Index
		}
	}()

	if log.Type != raft.LogCommand {
		if txn, ok := s.txn.(*fsmTxn); ok {
			_ = txn.nopUpdate(s.oracle.ts(log))
		}
		return nil
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
		return s.put(cmd.Kv.Key, cmd.Kv.Value, ttl, s.oracle.ts(log))
	case pb.Command_Delete:
		return s.del(cmd.Kv.Key, s.oracle.ts(log))
	default:
		panic("not excepted command operation")
	}
}

func (s *FSM) Apply(log *raft.Log) interface{} {
	return s.apply(log)
}

func (s *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &badgerSnapshot{db: s.db, ts: s.appliedIndex, logger: s.logger}, nil
}

func (s *FSM) Restore(snapshot io.ReadCloser) error {
	s.logger.Info("start restoring")
	if err := s.db.DropAll(); err != nil {
		s.logger.Error("restore failed", log.Error(err))
		return fmt.Errorf("%w: %w", ErrRestore, err)
	}
	if err := s.db.Load(snapshot, restoreGoNum); err != nil {
		s.logger.Error("restore failed", log.Error(err))
		return fmt.Errorf("%w: %w", ErrRestore, err)
	}
	if err := s.loadAppliedIndex(); err != nil {
		s.logger.Error("restore failed", log.Error(err))
		return err
	}
	s.logger.Info("successfully restored", log.Uint64("log_index", s.appliedIndex))
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
