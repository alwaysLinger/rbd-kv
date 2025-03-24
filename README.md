## RAFT BadgerDB distributed KV

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

## Why?

One of my application uses memory as a level2 cache, so I needed a general-purpose level1 cache similar to Redis (
though
performance doesn't need to match Redis since most requests don't access the database). However, I required better
consistency and reliable KV storage, which led to the creation of this project.

## Example

1. When bootstrap a cluster for the first time, you need to explicitly specify the leader peer address to join. Like
   node2 and node3 use the --join-addr flag to specify the address:  
   ./rbkv --grpc-addr=localhost:9501 --raft-addr=localhost:9601 --log-dir=/tmp/node1 --kv-dir=/tmp/node1  
   ./rbkv --grpc-addr=localhost:9502 --join-addr=localhost:9501 --raft-addr=localhost:9602 --log-dir=/tmp/node2
   --kv-dir=/tmp/node2  
   ./rbkv --grpc-addr=localhost:9503 --join-addr=localhost:9501 --raft-addr=localhost:9603 --log-dir=/tmp/node3
   --kv-dir=/tmp/node3
2. Once a node has successfully joined a cluster, you don't need to specify the --join-addr flag ever again. The node
   will automatically rejoin the cluster using its stored state in any order:  
   ./rbkv --grpc-addr=localhost:9502 --raft-addr=localhost:9602 --log-dir=/tmp/node2 --kv-dir=/tmp/node2  
   ./rbkv --grpc-addr=localhost:9503 --raft-addr=localhost:9603 --log-dir=/tmp/node3 --kv-dir=/tmp/node3  
   ./rbkv --grpc-addr=localhost:9501 --raft-addr=localhost:9601 --log-dir=/tmp/node1 --kv-dir=/tmp/node1

## TODO

1. Support transaction
2. Smarter client
3. Cmdline tool

