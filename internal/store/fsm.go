package store

import (
	"io"
	"os"
	"sync"
	"time"

	"github.com/alwaysLinger/rbkv/pb"
	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

const (
	backUpGoNum  = 16
	restoreGoNum = 16
)

type applyFunc func(log *raft.Log) interface{}

type FSM struct {
	db *badger.DB
	applyFunc
	gcTicker *time.Ticker

	mu                *sync.Mutex
	appliedCond       *sync.Cond
	localAppliedIndex uint64
}

func OpenFSM(dir string, opts *badger.Options, applyLog applyFunc) (*FSM, error) {
	if dir == "" {
		dir = os.TempDir()
	}

	if opts == nil {
		options := badger.DefaultOptions(dir).WithNumGoroutines(backUpGoNum).WithLogger(nil)
		opts = &options
	}

	s := new(FSM)
	s.mu = new(sync.Mutex)
	s.appliedCond = sync.NewCond(s.mu)

	db, err := badger.Open(*opts)
	if err != nil {
		return nil, err
	}
	s.db = db

	if applyLog != nil {
		s.applyFunc = applyLog
	}

	go s.runGC()

	return s, nil
}

func (s *FSM) runGC() {
	s.gcTicker = time.NewTicker(time.Minute * 30)
	for range s.gcTicker.C {
	again:
		err := s.db.RunValueLogGC(0.7)
		if err == nil {
			goto again
		}
	}
}

func (s *FSM) Close() error {
	if s.gcTicker != nil {
		s.gcTicker.Stop()
	}
	return s.db.Close()
}

func (s *FSM) DB() *badger.DB {
	return s.db
}

func (s *FSM) Apply(log *raft.Log) interface{} {
	if s.applyFunc != nil {
		return s.applyFunc(log)
	}

	cmd := &pb.Command{}
	err := proto.Unmarshal(log.Data, cmd)
	if err != nil {
		return err
	}

	err = s.db.Update(func(txn *badger.Txn) error {
		if cmd.Op == pb.Command_Put {
			ent := badger.NewEntry(cmd.Key, cmd.Value)
			if cmd.Ttl != 0 {
				ent.WithTTL(time.Duration(cmd.Ttl) * time.Second)
			}
			err = txn.SetEntry(ent)
			if err != nil {
				return err
			}
			return nil
		} else {
			err = txn.Delete(cmd.Key)
			if err != nil {
				return err
			}
			return nil
		}
	})

	if err != nil {
		return err
	}

	s.updateAppliedIndex(log.Index)

	return nil
}

func (s *FSM) updateAppliedIndex(index uint64) {
	s.mu.Lock()
	if s.localAppliedIndex < index {
		s.localAppliedIndex = index
		s.appliedCond.Broadcast()
	}
	s.mu.Unlock()
}

func (s *FSM) SyncAppliedIndex(index uint64) {
	s.updateAppliedIndex(index)
}

func (s *FSM) appliedIndex() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.localAppliedIndex
}

func (s *FSM) WaitForIndexAlign(index uint64) <-chan struct{} {
	ch := make(chan struct{}, 1)

	go func() {
		if s.appliedIndex() >= index {
			ch <- struct{}{}
		}

		s.appliedCond.L.Lock()
		for s.localAppliedIndex < index {
			s.appliedCond.Wait()
		}
		s.appliedCond.L.Unlock()
		ch <- struct{}{}
	}()

	return ch
}

func (s *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &badgersnapshot{db: s.db}, nil
}

func (s *FSM) Restore(snapshot io.ReadCloser) error {
	if err := s.db.DropAll(); err != nil {
		return err
	}

	if err := s.db.Load(snapshot, restoreGoNum); err != nil {
		return err
	}

	return nil
}

type badgersnapshot struct {
	db *badger.DB
}

func (bs *badgersnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := bs.db.Backup(sink, 0); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (bs *badgersnapshot) Release() {}
