package store

import (
	"errors"
	"io"
	"os"
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

var ErrNotFound = errors.New("key not found")

type applyFunc func(log *raft.Log) interface{}

type FSM struct {
	db *badger.DB
	applyFunc
	gcTicker *time.Ticker
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

	if cmd.Op == pb.Command_Get {
		var val []byte
		err := s.db.View(func(txn *badger.Txn) error {
			if item, err := txn.Get(cmd.Key); err != nil {
				if errors.Is(err, badger.ErrKeyNotFound) {
					return ErrNotFound
				}
				return err
			} else {
				if val, err = item.ValueCopy(val); err != nil {
					return err
				} else {
					return nil
				}
			}
		})
		if err != nil {
			return err
		} else {
			return val
		}
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

	return nil
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
