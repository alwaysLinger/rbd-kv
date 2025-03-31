package store

import (
	"bytes"
	"fmt"
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

	testData := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("k1"), []byte("v1")},
		{[]byte("k2"), []byte("v2")},
	}

	for i, d := range testData {
		cmd := &pb.Command{
			Op:    pb.Command_Put,
			Key:   d.key,
			Value: d.value,
		}
		data, err := proto.Marshal(cmd)
		if err != nil {
			t.Fatal(err)
		}
		if c := fsm.Apply(&raft.Log{Data: data, Index: uint64(i + 1)}); c != nil {
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
			t.Log(err)
			t.Errorf("failed to get key %s after restore: %v", d.key, err)
			continue
		}
		if !bytes.Equal(val, d.value) {
			t.Errorf("restored value mismatch for key %s: got %s, want %s", d.key, val, d.value)
		}
	}
}

func TestConsistentIndex(t *testing.T) {
	dir := filepath.Join("/tmp", "fsm-consistent-index-test")
	defer os.RemoveAll(dir)
	fsm, err := OpenFSM(dir, nil)
	if err != nil {
		t.Fatal(err)
	}

	numEntries := 5
	for i := 1; i <= numEntries; i++ {
		cmd := &pb.Command{
			Op:    pb.Command_Put,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}
		data, err := proto.Marshal(cmd)
		if err != nil {
			t.Fatal(err)
		}

		result := fsm.Apply(&raft.Log{Data: data, Index: uint64(i), Type: raft.LogCommand})
		if err, ok := result.(error); ok {
			t.Fatalf("failed to apply log %d: %v", i, err)
		}
	}

	if fsm.appliedIndex != uint64(numEntries) {
		t.Errorf("appliedIndex not updated correctly: expected %d, got %d", numEntries, fsm.appliedIndex)
	}

	oldCmd := &pb.Command{
		Op:    pb.Command_Put,
		Key:   []byte("old-key"),
		Value: []byte("old-value"),
	}
	oldData, _ := proto.Marshal(oldCmd)
	oldResult := fsm.Apply(&raft.Log{Data: oldData, Index: 2, Type: raft.LogCommand})

	if err, ok := oldResult.(error); ok {
		t.Errorf("applying old log should be ignored, but returned error: %v", err)
	}

	if fsm.appliedIndex != uint64(numEntries) {
		t.Errorf("appliedIndex changed after applying old log: expected %d, got %d", numEntries, fsm.appliedIndex)
	}

	var oldVal []byte
	err = fsm.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("old-key"))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}
		oldVal, err = item.ValueCopy(nil)
		return err
	})

	if err == nil && oldVal != nil {
		t.Errorf("old-key should not exist in database, but was found with value: %s", oldVal)
	}

	if err := fsm.Close(); err != nil {
		t.Fatal(err)
	}

	restartedFSM, err := OpenFSM(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer restartedFSM.Close()

	if restartedFSM.appliedIndex != uint64(numEntries) {
		t.Errorf("appliedIndex not restored correctly after restart: expected %d, got %d", numEntries, restartedFSM.appliedIndex)
	}

	newCmd := &pb.Command{
		Op:    pb.Command_Put,
		Key:   []byte("new-key"),
		Value: []byte("new-value"),
	}
	newData, _ := proto.Marshal(newCmd)
	newResult := restartedFSM.Apply(&raft.Log{Data: newData, Index: uint64(numEntries + 1), Type: raft.LogCommand})

	if err, ok := newResult.(error); ok {
		t.Errorf("failed to apply new log after restart: %v", err)
	}

	if restartedFSM.appliedIndex != uint64(numEntries+1) {
		t.Errorf("appliedIndex not updated correctly after restart: expected %d, got %d", numEntries+1, restartedFSM.appliedIndex)
	}

	var val []byte
	err = restartedFSM.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("new-key"))
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		return err
	})

	if err != nil {
		t.Errorf("failed to get newly written key: %v", err)
	} else if !bytes.Equal(val, []byte("new-value")) {
		t.Errorf("newly written value mismatch: expected %s, got %s", "new-value", val)
	}
}

func BenchmarkApply(b *testing.B) {
	dir := filepath.Join("/tmp", "apply")
	defer os.RemoveAll(dir)
	fsm, err := OpenFSM(dir, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer fsm.Close()

	cmd := &pb.Command{
		Op:    pb.Command_Put,
		Key:   []byte("testkey"),
		Value: []byte("testvalue"),
	}
	data, err := proto.Marshal(cmd)
	if err != nil {
		b.Fatal(err)
	}
	log := &raft.Log{Data: data, Type: raft.LogCommand}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		log.Index = uint64(i)
		if err, ok := fsm.Apply(log).(error); ok && err != nil {
			b.Fatal(err)
		}
	}
}
