## RAFT BadgerDB distributed KV

## BadgerDB as Hashicorp/Raft FSM Implementation

This repository implements Hashicorp/Raft's FSM interface using BadgerDB as the underlying storage engine. BadgerDB was
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
4. SSD design friendly, FSM storage size unlimited, thanks to BadgerDB

## Why?

One of my application uses memory as a level2 cache, so I needed a general-purpose level1 cache similar to Redis (
though
performance doesn't need to match Redis since most requests don't access the database). However, I required better
consistency and reliable KV storage, which led to the creation of this project.

## TODO

1. Support transaction
2. Smarter client
3. Cmdline tool

