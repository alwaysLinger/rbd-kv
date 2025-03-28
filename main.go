package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/alwaysLinger/rbkv/internal"
)

var (
	grpcAddr  string
	raftAddr  string
	joinAddr  string
	logDir    string
	kvDir     string
	batchSize uint64
)

func init() {
	flag.StringVar(&grpcAddr, "grpc-addr", "", "Set the GRPC bind address and also used as the node id")
	flag.StringVar(&raftAddr, "raft-addr", "", "Set Raft bind address")
	flag.StringVar(&joinAddr, "join-addr", "", "Set join address, if any")
	flag.StringVar(&logDir, "log-dir", "", "Set raft log and metadata storage dir")
	flag.StringVar(&kvDir, "kv-dir", "", "Set kv log storage dir")
	flag.Uint64Var(&batchSize, "batch-size", 0, "Size of apply channel batch, values <= 0 disable batching")
}

func main() {
	flag.Parse()

	opts := &internal.Options{
		RaftAddr:  raftAddr,
		GrpcAddr:  grpcAddr,
		JoinAddr:  joinAddr,
		NodeID:    grpcAddr,
		LogDir:    logDir,
		KVDir:     kvDir,
		BatchSize: batchSize,
	}

	server, err := internal.NewServer(opts)
	if err != nil {
		fmt.Fprint(os.Stderr, err)
		os.Exit(1)
	}

	notify := make(chan os.Signal, 1)
	signal.Notify(notify, syscall.SIGINT, syscall.SIGTERM)
	errCh := make(chan error, 1)

	go func() {
		errCh <- server.Run()
	}()

	select {
	case err := <-errCh:
		if err != nil {
			fmt.Fprintf(os.Stderr, "startup failed: %v\n", err)
			os.Exit(1)
		}
	case <-notify:
		log.Println("closing")
	}

	if err := server.Close(); err != nil {
		fmt.Fprint(os.Stderr, err)
		os.Exit(1)
	}

	os.Exit(0)
}
