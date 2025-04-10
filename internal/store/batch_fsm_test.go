package store

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/alwaysLinger/rbkv/pb"
	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

type logIndexStub struct {
	idx *atomic.Uint64
}

func (i *logIndexStub) index() uint64 {
	return i.idx.Add(1)
}

func newLogIndexStub(index uint64) *logIndexStub {
	at := new(atomic.Uint64)
	at.Store(index)
	return &logIndexStub{idx: at}
}

func TestBatchApply(t *testing.T) {
	dir := filepath.Join("/tmp", "batch-apply-test")
	defer os.RemoveAll(dir)
	batchFSM, err := OpenBatchFSM(dir, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer batchFSM.Close()

	idxStub := newLogIndexStub(0)

	batchSize := 10
	testData := make([]struct {
		key   []byte
		value []byte
	}, batchSize)

	logs := make([]*raft.Log, batchSize)

	for i := 0; i < batchSize; i++ {
		testData[i] = struct {
			key   []byte
			value []byte
		}{
			key:   []byte(fmt.Sprintf("batch-key-%d", i)),
			value: []byte(fmt.Sprintf("batch-value-%d", i)),
		}

		cmd := &pb.Command{
			Op: pb.Command_Put,
			Kv: &pb.Command_KV{
				Key:   testData[i].key,
				Value: testData[i].value,
			},
		}
		data, err := proto.Marshal(cmd)
		if err != nil {
			t.Fatal(err)
		}
		logs[i] = &raft.Log{
			Data:  data,
			Index: idxStub.index(),
			Type:  raft.LogCommand,
		}
	}

	ret := batchFSM.ApplyBatch(logs)
	if len(ret) != batchSize {
		t.Fatalf("excepted batch result dose not match batchSize, got:%v\n", len(ret))
	}

	// getLogs := make([]*raft.Log, batchSize)
	// for i := 0; i < batchSize; i++ {
	// 	getCmd := &pb.Command{
	// 		Op: pb.Command_Get,
	// 		Kv: &pb.Command_KV{
	// 			Key: testData[i].key,
	// 		},
	// 	}
	// 	data, err := proto.Marshal(getCmd)
	// 	if err != nil {
	// 		t.Fatal(err)
	// 	}
	// 	getLogs[i] = &raft.Log{
	// 		Data:  data,
	// 		Index: idxStub.index(),
	// 		Type:  raft.LogCommand,
	// 	}
	// }
	//
	// getResults := batchFSM.ApplyBatch(getLogs)
	// for i, result := range getResults {
	// 	if err, ok := result.(error); ok {
	// 		t.Errorf("Get operation returned error for key %s: %v", testData[i].key, err)
	// 		continue
	// 	}
	// 	val, ok := result.([]byte)
	// 	if !ok {
	// 		t.Errorf("Get operation result for key %s is not []byte, got %T", testData[i].key, result)
	// 		continue
	// 	}
	// 	if !bytes.Equal(val, testData[i].value) {
	// 		t.Errorf("Get value mismatch for key %s: got %s, want %s",
	// 			testData[i].key, val, testData[i].value)
	// 	}
	// }

	for _, d := range testData {
		var val []byte
		err := batchFSM.DB().View(func(txn *badger.Txn) error {
			item, err := txn.Get(d.key)
			if err != nil {
				return err
			}
			val, err = item.ValueCopy(nil)
			return err
		})
		if err != nil {
			t.Errorf("failed to get key %s after batch apply: %v", d.key, err)
			continue
		}
		if !bytes.Equal(val, d.value) {
			t.Errorf("value mismatch for key %s: got %s, want %s", d.key, val, d.value)
		}
	}

	mixedLogs := make([]*raft.Log, 3)
	putCmd := &pb.Command{
		Op: pb.Command_Put,
		Kv: &pb.Command_KV{
			Key:   []byte("mixed-key-1"),
			Value: []byte("mixed-value-1"),
		},
	}
	putData, _ := proto.Marshal(putCmd)
	mixedLogs[0] = &raft.Log{Data: putData, Index: idxStub.index(), Type: raft.LogCommand}

	delCmd := &pb.Command{
		Op: pb.Command_Delete,
		Kv: &pb.Command_KV{
			Key: testData[0].key,
		},
	}
	delData, _ := proto.Marshal(delCmd)
	mixedLogs[1] = &raft.Log{Data: delData, Index: idxStub.index(), Type: raft.LogCommand}

	putCmd2 := &pb.Command{
		Op: pb.Command_Put,
		Kv: &pb.Command_KV{
			Key:   []byte("mixed-key-2"),
			Value: []byte("mixed-value-2"),
		},
	}
	putData2, _ := proto.Marshal(putCmd2)
	mixedLogs[2] = &raft.Log{Data: putData2, Index: idxStub.index(), Type: raft.LogCommand}

	mixedResults := batchFSM.ApplyBatch(mixedLogs)

	for i, result := range mixedResults {
		if err, ok := result.(error); ok {
			t.Errorf("Mixed ApplyBatch returned error for log %d: %v", i, err)
		}
	}

	checkKeys := []struct {
		key         []byte
		value       []byte
		shouldExist bool
	}{
		{[]byte("mixed-key-1"), []byte("mixed-value-1"), true},
		{[]byte("mixed-key-2"), []byte("mixed-value-2"), true},
		{testData[0].key, testData[0].value, false},
	}

	for _, ck := range checkKeys {
		var val []byte
		err := batchFSM.DB().View(func(txn *badger.Txn) error {
			item, err := txn.Get(ck.key)
			if err != nil {
				return err
			}
			val, err = item.ValueCopy(nil)
			return err
		})

		if ck.shouldExist {
			if err != nil {
				t.Errorf("failed to get key %s after mixed batch: %v", ck.key, err)
				continue
			}
			if !bytes.Equal(val, ck.value) {
				t.Errorf("value mismatch for key %s: got %s, want %s", ck.key, val, ck.value)
			}
		} else {
			if err == nil {
				t.Errorf("key %s should have been deleted but still exists", ck.key)
			} else if err != badger.ErrKeyNotFound {
				t.Errorf("unexpected error for deleted key %s: %v", ck.key, err)
			}
		}
	}

	oldLog := &raft.Log{
		Data:  putData,
		Index: 1,
		Type:  raft.LogCommand,
	}

	oldResult := batchFSM.Apply(oldLog)
	if _, ok := oldResult.(error); ok {
		t.Errorf("Apply with old index should be ignored, not return error")
	}

	if batchFSM.fsm.appliedIndex.Load() != mixedLogs[2].Index {
		t.Errorf("appliedIndex not updated correctly: got %d, want %d",
			batchFSM.fsm.appliedIndex.Load(), mixedLogs[2].Index)
	}
}

func TestBatchAppliedIndex(t *testing.T) {
	dir := filepath.Join("/tmp", "batch-applied-index-test")
	defer os.RemoveAll(dir)
	batchFSM, err := OpenBatchFSM(dir, nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	initialIndex := batchFSM.fsm.appliedIndex.Load()
	if initialIndex != 0 {
		t.Errorf("initial appliedIndex should be 0, got %d", initialIndex)
	}

	idxStub := newLogIndexStub(0)
	batchSize := 5
	logs := make([]*raft.Log, batchSize)

	for i := 0; i < batchSize; i++ {
		cmd := &pb.Command{
			Op: pb.Command_Put,
			Kv: &pb.Command_KV{
				Key:   []byte(fmt.Sprintf("index-key-%d", i)),
				Value: []byte(fmt.Sprintf("index-value-%d", i)),
			},
		}
		data, err := proto.Marshal(cmd)
		if err != nil {
			t.Fatal(err)
		}
		logs[i] = &raft.Log{
			Data:  data,
			Index: idxStub.index(),
			Type:  raft.LogCommand,
		}
	}

	_ = batchFSM.ApplyBatch(logs)

	expectedIndex := logs[batchSize-1].Index
	if batchFSM.fsm.appliedIndex.Load() != expectedIndex {
		t.Errorf("appliedIndex incorrect after first batch: expected %d, got %d", expectedIndex, batchFSM.fsm.appliedIndex.Load())
	}

	secondBatchSize := 3
	secondLogs := make([]*raft.Log, secondBatchSize)

	for i := 0; i < secondBatchSize; i++ {
		cmd := &pb.Command{
			Op: pb.Command_Put,
			Kv: &pb.Command_KV{
				Key:   []byte(fmt.Sprintf("second-key-%d", i)),
				Value: []byte(fmt.Sprintf("second-value-%d", i)),
			},
		}
		data, err := proto.Marshal(cmd)
		if err != nil {
			t.Fatal(err)
		}
		secondLogs[i] = &raft.Log{
			Data:  data,
			Index: idxStub.index(),
			Type:  raft.LogCommand,
		}
	}

	_ = batchFSM.ApplyBatch(secondLogs)

	expectedIndex = secondLogs[secondBatchSize-1].Index
	if batchFSM.fsm.appliedIndex.Load() != expectedIndex {
		t.Errorf("appliedIndex incorrect after second batch: expected %d, got %d", expectedIndex, batchFSM.fsm.appliedIndex.Load())
	}

	oldLogs := make([]*raft.Log, 2)
	oldCmd := &pb.Command{
		Op: pb.Command_Put,
		Kv: &pb.Command_KV{
			Key:   []byte("old-key"),
			Value: []byte("old-value"),
		},
	}
	oldData, _ := proto.Marshal(oldCmd)

	oldLogs[0] = &raft.Log{Data: oldData, Index: 2, Type: raft.LogCommand}
	oldLogs[1] = &raft.Log{Data: oldData, Index: 3, Type: raft.LogCommand}

	_ = batchFSM.ApplyBatch(oldLogs)

	if batchFSM.fsm.appliedIndex.Load() != expectedIndex {
		t.Errorf("appliedIndex changed after applying old logs: expected %d, got %d", expectedIndex, batchFSM.fsm.appliedIndex.Load())
	}

	var oldVal []byte
	err = batchFSM.DB().View(func(txn *badger.Txn) error {
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
		t.Errorf("old-key should not exist in database, but found with value: %s", oldVal)
	}

	if err := batchFSM.Close(); err != nil {
		t.Fatal(err)
	}

	restartedFSM, err := OpenBatchFSM(dir, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer restartedFSM.Close()

	if restartedFSM.fsm.appliedIndex.Load() != expectedIndex {
		t.Errorf("appliedIndex not restored correctly after restart: expected %d, got %d", expectedIndex, restartedFSM.fsm.appliedIndex.Load())
	}

	newLogs := make([]*raft.Log, 1)
	newCmd := &pb.Command{
		Op: pb.Command_Put,
		Kv: &pb.Command_KV{
			Key:   []byte("new-key"),
			Value: []byte("new-value"),
		},
	}
	newData, _ := proto.Marshal(newCmd)
	newLogs[0] = &raft.Log{
		Data:  newData,
		Index: expectedIndex + 1,
		Type:  raft.LogCommand,
	}

	_ = restartedFSM.ApplyBatch(newLogs)

	if restartedFSM.fsm.appliedIndex.Load() != expectedIndex+1 {
		t.Errorf("appliedIndex not updated correctly after restart: expected %d, got %d", expectedIndex+1, restartedFSM.fsm.appliedIndex.Load())
	}

	var newVal []byte
	err = restartedFSM.DB().View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("new-key"))
		if err != nil {
			return err
		}
		newVal, err = item.ValueCopy(nil)
		return err
	})

	if err != nil {
		t.Errorf("failed to get newly written key: %v", err)
	} else if !bytes.Equal(newVal, []byte("new-value")) {
		t.Errorf("newly written value mismatch: expected %s, got %s", "new-value", newVal)
	}
}

func BenchmarkApplyBatch(b *testing.B) {
	dir := filepath.Join("/tmp", "bench-apply-batch")
	defer os.RemoveAll(dir)
	batchFSM, err := OpenBatchFSM(dir, nil, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer batchFSM.Close()

	batchSize := 100
	logs := make([]*raft.Log, batchSize)
	for j := 0; j < batchSize; j++ {
		cmd := &pb.Command{
			Op: pb.Command_Put,
			Kv: &pb.Command_KV{
				Key:   []byte(fmt.Sprintf("bench-key-%d", j)),
				Value: []byte("benchmark-value"),
			},
		}
		data, err := proto.Marshal(cmd)
		if err != nil {
			b.Fatal(err)
		}
		logs[j] = &raft.Log{
			Data: data,
			Type: raft.LogCommand,
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		baseIndex := uint64(i * batchSize)
		for j := 0; j < batchSize; j++ {
			logs[j].Index = baseIndex + uint64(j) + 1
		}
		_ = batchFSM.ApplyBatch(logs)
	}
}

func BenchmarkApplyBatchSizes(b *testing.B) {
	benchSizes := []int{10, 50, 100, 500, 1000}
	for _, size := range benchSizes {
		b.Run(fmt.Sprintf("Size-%d", size), func(b *testing.B) {
			dir := filepath.Join("/tmp", fmt.Sprintf("bench-batch-%d", size))
			defer os.RemoveAll(dir)
			batchFSM, err := OpenBatchFSM(dir, nil, 0)
			if err != nil {
				b.Fatal(err)
			}
			defer batchFSM.Close()

			logs := make([]*raft.Log, size)
			for j := 0; j < size; j++ {
				cmd := &pb.Command{
					Op: pb.Command_Put,
					Kv: &pb.Command_KV{
						Key:   []byte(fmt.Sprintf("bench-key-%d", j)),
						Value: []byte("benchmark-value"),
					},
				}
				data, err := proto.Marshal(cmd)
				if err != nil {
					b.Fatal(err)
				}
				logs[j] = &raft.Log{
					Data: data,
					Type: raft.LogCommand,
				}
			}

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				baseIndex := uint64(i * size)
				for j := 0; j < size; j++ {
					logs[j].Index = baseIndex + uint64(j) + 1
				}
				_ = batchFSM.ApplyBatch(logs)
			}
		})
	}
}

func BenchmarkApplyVsBatch(b *testing.B) {
	b.Run("SingleApply", func(b *testing.B) {
		dir := filepath.Join("/tmp", "bench-single-apply")
		defer os.RemoveAll(dir)
		fsm, err := OpenFSM(dir, nil, 0)
		if err != nil {
			b.Fatal(err)
		}
		defer fsm.Close()

		cmd := &pb.Command{
			Op: pb.Command_Put,
			Kv: &pb.Command_KV{
				Key:   []byte("single-key"),
				Value: []byte("single-value"),
			},
		}
		data, err := proto.Marshal(cmd)
		if err != nil {
			b.Fatal(err)
		}
		log := &raft.Log{
			Data: data,
			Type: raft.LogCommand,
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			log.Index = uint64(i + 1)
			_ = fsm.Apply(log)
		}
	})

	b.Run("BatchApply", func(b *testing.B) {
		dir := filepath.Join("/tmp", "bench-batch-apply")
		defer os.RemoveAll(dir)
		batchFSM, err := OpenBatchFSM(dir, nil, 0)
		if err != nil {
			b.Fatal(err)
		}
		defer batchFSM.Close()

		cmd := &pb.Command{
			Op: pb.Command_Put,
			Kv: &pb.Command_KV{
				Key:   []byte("batch-key"),
				Value: []byte("batch-value"),
			},
		}
		data, err := proto.Marshal(cmd)
		if err != nil {
			b.Fatal(err)
		}

		idxStub := newLogIndexStub(0)
		batchSize := 100
		logs := make([]*raft.Log, batchSize)
		for i := 0; i < batchSize; i++ {
			logs[i] = &raft.Log{
				Data:  data,
				Index: idxStub.index(),
				Type:  raft.LogCommand,
			}
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for j := 0; j < batchSize; j++ {
				logs[j].Index = idxStub.index()
			}
			_ = batchFSM.ApplyBatch(logs)
		}
	})
}
