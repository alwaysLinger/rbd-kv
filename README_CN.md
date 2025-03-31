## RAFT BadgerDB distributed KV

## BadgerDB 作为 Hashicorp/Raft FSM 实现

本项目使用 [BadgerDB](https://github.com/dgraph-io/badger)
作为底层存储引擎，实现了 [hashicorp/raft](https://github.com/hashicorp/raft) 的 FSM 接口。选择 BadgerDB
是因为它兼具高性能和易于实现的特点，是 FSM 实现的理想选择。

## 实现简单

1. **线程安全的 FSM Apply**：Raft 的线程安全的 Apply 设计使我们不必担心 BadgerDB 事务冲突检测
2. **高性能 I/O**：BadgerDB 提供了出色的读写性能
3. **全面的快照支持**：内置的备份和恢复功能使 FSM 实现变得简单直接

## 特性

1. 基于 Raft 共识协议的强一致性
2. 支持可序列化和线性化读取
3. 键事件监听机制
4. SSD 设计友好，FSM 存储大小不受限制，再次感谢 [BadgerDB](https://github.com/dgraph-io/badger)
5. 使用 gRPC 作为客户端接口
6. 支持 [haschicorp/raft BatchingFSM](https://github.com/hashicorp/raft)，启用此功能将获得更高的写吞吐，但可能增加日志复制延迟，可能会导致更多尚未应用到状态机的
   raft 日志丢失，这些日志可以通过 WAL 重放恢复。

## 为什么？

1. **为什么创建这个项目？**  
   我的一个应用程序使用内存作为二级缓存，因此我需要一个通用的一级缓存（性能不需要对标
   Redis等内存数据库，因为大多数请求根本不会抵达数据库，但我依然希望它足够快）。但我需要更好的一致性和可靠的 KV
   存储，这促使我创建了这个项目。

2. **为什么 FSM 提供这样一种懒惰的压缩和持久化策略？**   
   为了获得更高的写入吞吐量，并且也没有必要进行激进的压缩或同步。
   [RAFT](https://github.com/hashicorp/raft) 和 [BadgerDB](https://github.com/dgraph-io/badger) 都提供了 WAL
   机制，给我们可靠的数据安全保障。 RAFT 只确保了集群间的日志复制，而可靠的存储完全依赖于 FSM 的实现。为了实现更高的写入吞吐量，我实现了一种相对激进的
   RAFT WAL 持久化策略（由于多副本复制的存在，这仍然是安全的），同时为 FSM 数据提供了更为保守的压缩和持久化策略。由于本项目也使用
   BadgerDB 作为 RAFT 日志和 RAFT 元数据的存储，我认为这两种策略是相对合理的

## 示例

1. **首次引导集群时，需要明确指定要加入的领导者节点地址。例如，node2 和 node3 使用 --join-addr 标志指定地址：**  
   ./rbkv --grpc-addr=localhost:9501 --raft-addr=localhost:9601 --log-dir=/tmp/node1 --kv-dir=/tmp/node1  
   ./rbkv --grpc-addr=localhost:9502 --join-addr=localhost:9501 --raft-addr=localhost:9602 --log-dir=/tmp/node2
   --kv-dir=/tmp/node2  
   ./rbkv --grpc-addr=localhost:9503 --join-addr=localhost:9501 --raft-addr=localhost:9603 --log-dir=/tmp/node3
   --kv-dir=/tmp/node3

2. **一旦节点成功加入集群，就不再需要指定 --join-addr 标志。节点将使用其存储的状态自动以任意顺序重新加入集群：**  
   ./rbkv --grpc-addr=localhost:9502 --raft-addr=localhost:9602 --log-dir=/tmp/node2 --kv-dir=/tmp/node2  
   ./rbkv --grpc-addr=localhost:9503 --raft-addr=localhost:9603 --log-dir=/tmp/node3 --kv-dir=/tmp/node3  
   ./rbkv --grpc-addr=localhost:9501 --raft-addr=localhost:9601 --log-dir=/tmp/node1 --kv-dir=/tmp/node1

## 待办

1. 支持多键事务
2. 更智能的客户端
3. 命令行工具