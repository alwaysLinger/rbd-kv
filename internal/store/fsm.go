package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"time"

	"github.com/alwaysLinger/rbkv/internal/log"
	"github.com/alwaysLinger/rbkv/pb"
	"github.com/dgraph-io/badger/v4"
	copts "github.com/dgraph-io/badger/v4/options"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

var (
	consistentIndexKey = []byte("m.!ci")

	ErrRestore        = errors.New("restore failed")
	ErrApplyUnmarshal = errors.New("failed to unmarshal raft log data")
)

type UserMeta byte

type DB interface {
	DB() *badger.DB
}

type DBFSM interface {
	Get(key []byte, at uint64) ([]byte, UserMeta, uint64, error)
	Stats(exact, withKeyCount bool) (lsmSize, vlogSize, keyCount uint64, err error)
	Close() error

	DB
	raft.FSM
}

type Option func(*FSM)

func WithBackupGoNum(n int) Option {
	return func(fsm *FSM) {
		fsm.backupGoNum = n
	}
}

func WithRestoreGoNum(n int) Option {
	return func(fsm *FSM) {
		fsm.restoreGoNum = n
	}
}

type FSM struct {
	db           *badger.DB
	gcTicker     *time.Ticker
	appliedIndex uint64 // this field will never be accessed concurrently
	logger       log.Logger
	oracle       oracle
	txn          Txn[*badger.Item]
	backupGoNum  int
	restoreGoNum int
}

func OpenFSM(dir string, bopts *badger.Options, versionKept int, logger log.Logger, opts ...Option) (*FSM, error) {
	if len(dir) == 0 {
		dir = os.TempDir()
	}

	s := new(FSM)
	s.backupGoNum = 8
	s.restoreGoNum = 8
	for _, opt := range opts {
		opt(s)
	}

	if bopts == nil {
		options := badger.DefaultOptions(dir).
			WithDetectConflicts(false).
			WithBlockCacheSize(512 << 20).
			WithValueThreshold(4 << 10).
			WithNumVersionsToKeep(math.MaxInt).
			WithCompression(copts.ZSTD).
			WithNumGoroutines(s.backupGoNum).
			WithMetricsEnabled(false).
			WithLoggingLevel(badger.WARNING)
		bopts = &options
	}

	if versionKept != 0 {
		*bopts = (*bopts).WithNumVersionsToKeep(versionKept)
	}

	db, err := badger.OpenManaged(*bopts)
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
	val, _, _, err := s.txn.ReadAt(consistentIndexKey, math.MaxUint64)
	if err != nil {
		if !errors.Is(err, ErrKeyNotFound) {
			return err
		}
		return nil
	}
	s.appliedIndex = binary.BigEndian.Uint64(val)
	return nil
}

func (s *FSM) runGC() {
	s.gcTicker = time.NewTicker(time.Minute * 30)
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

func (s *FSM) Get(key []byte, at uint64) ([]byte, UserMeta, uint64, error) {
	if at == 0 {
		at = math.MaxUint64
	}
	return s.get(key, at)
}

func (s *FSM) get(key []byte, at uint64) ([]byte, UserMeta, uint64, error) {
	val, meta, ver, err := s.txn.ReadAt(key, at)
	if err != nil {
		return nil, 0, 0, err
	}
	return val, meta, ver, nil
}

func (s *FSM) syncConsistentIndex(ts uint64, txn *badger.Txn) error {
	if err := txn.SetEntry(badger.NewEntry(consistentIndexKey, uint64ToBytes(ts)).WithDiscard()); err != nil {
		return fmt.Errorf("%w: %w", ErrUpdateConsistentIndex, err)
	}
	return nil
}

func (s *FSM) put(key, val []byte, meta UserMeta, ttl time.Duration, ts uint64) any {
	return s.txn.SetAt(key, val, meta, ttl, ts)
}

func (s *FSM) del(key []byte, ts uint64) any {
	return s.txn.DeleteAt(key, ts)
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
		panic(fmt.Errorf("%w: %s: %w", ErrApplyUnmarshal, log.Data, err))
	}

	switch cmd.Op {
	case pb.Command_Put:
		var ttl time.Duration
		if cmd.Kv.Ttl != nil {
			ttl = cmd.Kv.Ttl.AsDuration()
		}
		return s.put(cmd.Kv.Key, cmd.Kv.Value, UserMeta(cmd.Kv.Meta), ttl, s.oracle.ts(log))
	case pb.Command_Delete:
		return s.del(cmd.Kv.Key, s.oracle.ts(log))
	default:
		panic("not excepted command operation")
	}
}

func (s *FSM) Apply(log *raft.Log) any {
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
	r := &yieldReader{
		snapshot:   snapshot,
		throughput: 300,
		it:         0,
	}
	if err := s.db.Load(r, s.restoreGoNum); err != nil {
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
		var prefix []byte
		it := s.txn.Iterator(prefix, func(_ *badger.Item) error {
			keyCount++
			return nil
		}, false, 0)
		if err = it.Iterate(); err != nil {
			return 0, 0, 0, err
		}
		return
	}
	return
}

type yieldReader struct {
	snapshot   io.Reader
	throughput uint64
	it         uint64
}

func (yr *yieldReader) Read(p []byte) (n int, err error) {
	yr.it++
	if yr.it%yr.throughput == 0 {
		runtime.Gosched()
	}
	return yr.snapshot.Read(p)
}
