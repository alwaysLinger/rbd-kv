package store

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
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
	fsm, err := OpenFSM(dir, nil, 0, nil)
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
			Op: pb.Command_Put,
			Kv: &pb.Command_KV{
				Key:   d.key,
				Value: d.value,
			},
		}
		data, err := proto.Marshal(cmd)
		if err != nil {
			t.Fatal(err)
		}
		c := fsm.Apply(&raft.Log{Data: data, Index: uint64(i + 1)})
		if err, ok := c.(error); ok {
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

	restoreFSM, err := OpenFSM(filepath.Join(dir, "restore"), nil, 0, nil)
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
	fsm, err := OpenFSM(dir, nil, 0, nil)
	if err != nil {
		t.Fatal(err)
	}

	numEntries := 5
	for i := 1; i <= numEntries; i++ {
		cmd := &pb.Command{
			Op: pb.Command_Put,
			Kv: &pb.Command_KV{
				Key:   []byte(fmt.Sprintf("key-%d", i)),
				Value: []byte(fmt.Sprintf("value-%d", i)),
			},
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
		Op: pb.Command_Put,
		Kv: &pb.Command_KV{
			Key:   []byte("old-key"),
			Value: []byte("old-value"),
		},
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

	restartedFSM, err := OpenFSM(dir, nil, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer restartedFSM.Close()

	if restartedFSM.appliedIndex != uint64(numEntries) {
		t.Errorf("appliedIndex not restored correctly after restart: expected %d, got %d", numEntries, restartedFSM.appliedIndex)
	}

	newCmd := &pb.Command{
		Op: pb.Command_Put,
		Kv: &pb.Command_KV{
			Key:   []byte("new-key"),
			Value: []byte("new-value"),
		},
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
	fsm, err := OpenFSM(dir, nil, 0, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer fsm.Close()

	cmd := &pb.Command{
		Op: pb.Command_Put,
		Kv: &pb.Command_KV{
			Key:   []byte("testkey"),
			Value: []byte("testvalue"),
		},
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

func TestFSMTxn(t *testing.T) {
	dir := filepath.Join("/tmp", "fsm-txn-test")
	fsm, err := OpenFSM(dir, nil, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		t.Logf("closing error: %v\n", fsm.Close())
		t.Logf("remove test dir error: %v\n", os.RemoveAll(dir))
	})

	t.Run("SetAt", func(t *testing.T) {
		t.Parallel()
		key := []byte("txn-key1")
		value := []byte("txn-value1")
		at := uint64(100)

		result := fsm.txn.SetAt(key, value, 0, 0, at)
		if err, ok := result.(error); ok && err != nil {
			t.Fatalf("SetAt failed: %v", err)
		}

		val, _, _, err := fsm.txn.ReadAt(key, at)
		if err != nil {
			t.Fatalf("ReadAt after SetAt failed: %v", err)
		}
		if !bytes.Equal(val, value) {
			t.Errorf("SetAt value mismatch: expected %s, got %s", value, val)
		}
	})

	t.Run("ReadAt", func(t *testing.T) {
		t.Parallel()
		key := []byte("txn-key2")
		value := []byte("txn-value2")
		at := uint64(200)

		result := fsm.txn.SetAt(key, value, 0, 0, at)
		if err, ok := result.(error); ok && err != nil {
			t.Fatalf("SetAt for ReadAt test failed: %v", err)
		}

		val, _, _, err := fsm.txn.ReadAt(key, at)
		if err != nil {
			t.Fatalf("ReadAt failed: %v", err)
		}
		if !bytes.Equal(val, value) {
			t.Errorf("ReadAt value mismatch: expected %s, got %s", value, val)
		}

		val, _, _, err = fsm.txn.ReadAt(key, math.MaxUint64)
		if err != nil {
			t.Fatalf("ReadAt failed: %v", err)
		}
		if !bytes.Equal(val, value) {
			t.Errorf("ReadAt value mismatch: expected %s, got %s", value, val)
		}

		val, _, _, err = fsm.txn.ReadAt(key, at-1)
		if !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("ReadAt before at failed: %v", err)
		}

		val, _, _, err = fsm.txn.ReadAt(key, at+1)
		if err != nil {
			t.Fatalf("ReadAt before at failed: %v", err)
		}
		if !bytes.Equal(val, value) {
			t.Errorf("ReadAt value mismatch: expected %s, got %s", value, val)
		}
	})

	t.Run("DeleteAt", func(t *testing.T) {
		t.Parallel()
		key := []byte("txn-key3")
		value := []byte("txn-value3")
		at := uint64(300)

		result := fsm.txn.SetAt(key, value, 0, 0, at)
		if err, ok := result.(error); ok && err != nil {
			t.Fatalf("SetAt for DeleteAt test failed: %v", err)
		}

		result = fsm.txn.DeleteAt(key, at+1)
		if err, ok := result.(error); ok && err != nil {
			t.Fatalf("DeleteAt failed: %v", err)
		}

		_, _, _, err = fsm.txn.ReadAt(key, at+2)
		if !errors.Is(err, ErrKeyNotFound) {
			t.Error("ReadAt should fail after DeleteAt")
		}
	})

	t.Run("Delete_MultiVersion", func(t *testing.T) {
		t.Parallel()
		key := []byte("txn-key4")
		value := []byte("txn-value4")
		value2 := []byte("txn-value4-v2")
		at := uint64(400)
		aat := at + 100

		err := fsm.txn.SetAt(key, value, 0, 0, at)
		if e, ok := err.(error); ok {
			t.Fatalf("SetAt for DeleteAt test failed: %v", e)
		}

		err = fsm.txn.SetAt(key, value2, 0, 0, aat)
		if e, ok := err.(error); ok {
			t.Fatalf("SetAt high version failed: %v", e)
		}

		val, _, _, err := fsm.txn.ReadAt(key, aat+1)
		if err != nil {
			t.Fatalf("ReadAt high version failed: %v", err)
		}
		if !bytes.Equal(val, value2) {
			t.Errorf("High version value mismatch: expected %s, got %s", value2, val)
		}

		err = fsm.txn.DeleteAt(key, aat+10)
		if e, ok := err.(error); ok {
			t.Fatalf("DeleteAt high version failed: %v", e)
		}

		_, _, _, err = fsm.txn.ReadAt(key, aat+20)
		if !errors.Is(err.(error), ErrKeyNotFound) {
			t.Error("ReadAt should fail after DeleteAt high version")
		}

		val, _, _, err = fsm.txn.ReadAt(key, at+1)
		if err != nil {
			t.Fatalf("ReadAt low version after high version delete failed: %v", err)
		}
		if !bytes.Equal(val, value) {
			t.Errorf("Low version value mismatch: expected %s, got %s", value, val)
		}
	})

	t.Run("SetAt_MultiVersion", func(t *testing.T) {
		t.Parallel()
		key := []byte("version-key")
		value1 := []byte("version-value1")
		value2 := []byte("version-value2")

		err := fsm.txn.SetAt(key, value1, 0, 0, 600)
		if e, ok := err.(error); ok {
			t.Fatalf("SetAt version 1 failed: %v", e)
		}

		err = fsm.txn.SetAt(key, value2, 0, 0, 700)
		if e, ok := err.(error); ok {
			t.Fatalf("SetAt version 2 failed: %v", e)
		}

		val1, _, _, err := fsm.txn.ReadAt(key, 650)
		if err != nil {
			t.Fatalf("ReadAt version 1 failed: %v", err)
		}
		if !bytes.Equal(val1, value1) {
			t.Errorf("Version 1 value mismatch: expected %s, got %s", value1, val1)
		}

		val2, _, _, err := fsm.txn.ReadAt(key, 750)
		if err != nil {
			t.Fatalf("ReadAt version 2 failed: %v", err)
		}
		if !bytes.Equal(val2, value2) {
			t.Errorf("Version 2 value mismatch: expected %s, got %s", value2, val2)
		}
	})

	t.Run("CommitOrderIndependence", func(t *testing.T) {
		t.Parallel()
		key := []byte("order-key")
		valueHigh := []byte("high-version-value")
		valueLow := []byte("low-version-value")
		highTs := uint64(900)
		lowTs := uint64(800)

		err := fsm.txn.SetAt(key, valueHigh, 0, 0, highTs)
		if e, ok := err.(error); ok {
			t.Fatalf("SetAt high version failed: %v", e)
		}

		val, _, _, err := fsm.txn.ReadAt(key, highTs+1)
		if err != nil {
			t.Fatalf("ReadAt high version failed: %v", err)
		}
		if !bytes.Equal(val, valueHigh) {
			t.Errorf("High version value mismatch: expected %s, got %s", valueHigh, val)
		}

		err = fsm.txn.SetAt(key, valueLow, 0, 0, lowTs)
		if e, ok := err.(error); ok {
			t.Fatalf("SetAt low version failed: %v", e)
		}

		val, _, _, err = fsm.txn.ReadAt(key, lowTs+1)
		if err != nil {
			t.Fatalf("ReadAt low version failed: %v", err)
		}
		if !bytes.Equal(val, valueLow) {
			t.Errorf("Low version value mismatch: expected %s, got %s", valueLow, val)
		}

		val, _, _, err = fsm.txn.ReadAt(key, highTs+1)
		if err != nil {
			t.Fatalf("ReadAt high version after low version commit failed: %v", err)
		}
		if !bytes.Equal(val, valueHigh) {
			t.Errorf("High version value mismatch after low version commit: expected %s, got %s", valueHigh, val)
		}

		val, _, _, err = fsm.txn.ReadAt(key, (lowTs+highTs)/2)
		if err != nil {
			t.Fatalf("ReadAt middle timestamp failed: %v", err)
		}
		if !bytes.Equal(val, valueLow) {
			t.Errorf("Middle timestamp value mismatch: expected %s, got %s", valueLow, val)
		}
	})
}

func commandStub(t *testing.T, op pb.Command_OpType, key, value string) []byte {
	t.Helper()
	cmd := &pb.Command{
		Op: op,
		Kv: &pb.Command_KV{
			Key:   []byte(key),
			Value: []byte(value),
		},
	}
	data, err := proto.Marshal(cmd)
	if err != nil {
		t.Fatalf("Failed to marshal command: %v", err)
	}
	return data
}
