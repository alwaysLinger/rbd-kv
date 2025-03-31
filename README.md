## RAFT BadgerDB distributed KV

[简体中文](README_CN.md)

## BadgerDB as Hashicorp/Raft FSM Implementation

This repository implements [hashicorp/raft](https://github.com/hashicorp/raft)'s FSM interface
using [BadgerDB](https://github.com/dgraph-io/badger) as the underlying storage engine. BadgerDB was
chosen for its combination of high performance and ease of implementation, making it an ideal choice for an FSM
implementation.

## Simple Implementation

1. **Thread-Safe FSM Apply**: Raft's single-threaded Apply design eliminates concerns about BadgerDB's transaction
   conflict detection
2. **High Performance I/O**: BadgerDB provides excellent read and write performance capabilities
3. **Comprehensive Snapshot Support**: Built-in backup and restore functionalities make FSM implementation
   straightforward

## Features

1. Strong consistency with Raft consensus protocol
2. Serializable and linearizable read
3. Watch mechanism for key events
4. SSD design friendly, FSM storage size unlimited, thanks to [BadgerDB](https://github.com/dgraph-io/badger)
5. Use grpc as client interface
6. Support [haschicorp/raft BatchingFSM](https://github.com/hashicorp/raft), with this feature enabled, you will get
   higher throughput but may increase log replication latency, potentially losing more raft logs that haven't been
   applied to the state machine which can be re-applied by WAL replay.

## Why?

1. **Why write this project?**  
   One of my application uses memory as a level2 cache, so I needed a general-purpose
   level1 cache (performance doesn't need to match in-memory databases like Redis, as most requests won't reach the
   database, but I still want it to be fast enough). However, I required better
   consistency and reliable KV storage, which led to the creation of this project.
2. **Why the FSM provides such a lazy compaction and persistence strategy?**   
   For a higher write throughput and there is also no need for aggressive compaction or syncing.
   Both [RAFT](https://github.com/hashicorp/raft)
   and [BadgerDB](https://github.com/dgraph-io/badger) provide WAL mechanisms, giving us reliable data safety
   guarantees. It's important to understand that RAFT only ensures log replication across the cluster, while reliable
   storage depends entirely on the FSM implementation. To achieve higher write throughput, I've implemented a relatively
   aggressive RAFT WAL persistence strategy (which still remains safe due to replication), while providing a more
   conservative FSM data compaction and persistence strategy. Since this project also uses BadgerDB as storage for RAFT
   logs and RAFT metadata, I believe these two strategies are relatively reasonable.

## Example

1. **When bootstrap a cluster for the first time, you need to explicitly specify the leader peer address to join. Like
   node2 and node3 use the --join-addr flag to specify the address:**  
   ./rbkv --grpc-addr=localhost:9501 --raft-addr=localhost:9601 --log-dir=/tmp/node1 --kv-dir=/tmp/node1  
   ./rbkv --grpc-addr=localhost:9502 --join-addr=localhost:9501 --raft-addr=localhost:9602 --log-dir=/tmp/node2
   --kv-dir=/tmp/node2  
   ./rbkv --grpc-addr=localhost:9503 --join-addr=localhost:9501 --raft-addr=localhost:9603 --log-dir=/tmp/node3
   --kv-dir=/tmp/node3


2. **Once a node has successfully joined a cluster, you don't need to specify the --join-addr flag ever again. The node
   will automatically rejoin the cluster using its stored state in any order:**  
   ./rbkv --grpc-addr=localhost:9502 --raft-addr=localhost:9602 --log-dir=/tmp/node2 --kv-dir=/tmp/node2  
   ./rbkv --grpc-addr=localhost:9503 --raft-addr=localhost:9603 --log-dir=/tmp/node3 --kv-dir=/tmp/node3  
   ./rbkv --grpc-addr=localhost:9501 --raft-addr=localhost:9601 --log-dir=/tmp/node1 --kv-dir=/tmp/node1

## TODO

1. Support multi keys transaction
2. Smarter client
3. Cmdline tool

