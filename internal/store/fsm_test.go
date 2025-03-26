package store

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/alwaysLinger/rbkv/pb"
	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

type mockSnapshotSink struct {
	*bytes.Buffer
	cancelCalled bool
	closeCalled  bool
}

func (m *mockSnapshotSink) ID() string {
	return "mock-snapshot"
}

func (m *mockSnapshotSink) Cancel() error {
	m.cancelCalled = true
	return nil
}

func (m *mockSnapshotSink) Close() error {
	m.closeCalled = true
	return nil
}

func TestFSMSnapshotAndRestore(t *testing.T) {
	dir := filepath.Join("/tmp", "fsm-snapshot-test")
	defer os.RemoveAll(dir)

	fsm, err := OpenFSM(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer fsm.Close()

	// 写入一些测试数据
	testData := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("k1"), []byte("v1")},
		{[]byte("k2"), []byte("v2")},
	}

	for _, d := range testData {
		cmd := &pb.Command{
			Op:    pb.Command_Put,
			Key:   d.key,
			Value: d.value,
		}
		data, err := proto.Marshal(cmd)
		if err != nil {
			t.Fatal(err)
		}
		if c := fsm.Apply(&raft.Log{Data: data}); c != nil {
			t.Fatal(err)
		}
	}

	snapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	sink := &mockSnapshotSink{Buffer: new(bytes.Buffer)}
	if err := snapshot.Persist(sink); err != nil {
		t.Fatal(err)
	}

	if !sink.closeCalled {
		t.Error("snapshot sink Close() was not called")
	}

	restoreFSM, err := OpenFSM(filepath.Join(dir, "restore"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer restoreFSM.Close()

	if err := restoreFSM.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))); err != nil {
		t.Fatal(err)
	}

	for _, d := range testData {
		var val []byte
		err := restoreFSM.db.View(func(txn *badger.Txn) error {
			item, err := txn.Get(d.key)
			if err != nil {
				return err
			}
			val, err = item.ValueCopy(nil)
			return err
		})
		if err != nil {
			t.Errorf("failed to get key %s after restore: %v", d.key, err)
			continue
		}
		if !bytes.Equal(val, d.value) {
			t.Errorf("restored value mismatch for key %s: got %s, want %s", d.key, val, d.value)
		}
	}
}
