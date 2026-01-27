# Narwhal Worker 模块学习笔记

## 一、Worker 整体架构

Worker 是 Narwhal 的**数据可用性层（Data Availability Layer）**的核心组件，负责：
- 接收客户端交易
- 将交易打包成 Batch
- 确保 Batch 被足够多的节点存储（2f+1 quorum）
- 向 Primary 报告已确认的 Batch Digest

### 1.1 Worker 的三个主要流水线

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              Worker 架构图                                        │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                   │
│  ┌─────────────────────────────────────────────────────────────────────────────┐ │
│  │ Pipeline 1: handle_primary_messages (来自 Primary 的消息)                    │ │
│  │                                                                               │ │
│  │   Primary ──TCP──> Receiver ──> PrimaryReceiverHandler                       │ │
│  │                                         │                                     │ │
│  │                     ┌───────────────────┴───────────────────┐                │ │
│  │                     ▼                                       ▼                │ │
│  │              Synchronizer                                 其他消息            │ │
│  │          (同步缺失的 Batch)                                                   │ │
│  └─────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                   │
│  ┌─────────────────────────────────────────────────────────────────────────────┐ │
│  │ Pipeline 2: handle_clients_transactions (来自客户端的交易) ★核心流水线★        │ │
│  │                                                                               │ │
│  │   Client ──TCP──> Receiver ──> TxReceiverHandler                             │ │
│  │                                       │                                       │ │
│  │                                       ▼                                       │ │
│  │                                  BatchMaker ──广播──> 其他 Workers            │ │
│  │                                       │                                       │ │
│  │                                       ▼                                       │ │
│  │                                 QuorumWaiter (等待 2f+1 ACK)                  │ │
│  │                                       │                                       │ │
│  │                                       ▼                                       │ │
│  │                                  Processor (哈希 + 存储)                      │ │
│  │                                       │                                       │ │
│  │                                       ▼                                       │ │
│  │                                   Primary (发送 Digest)                       │ │
│  └─────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                   │
│  ┌─────────────────────────────────────────────────────────────────────────────┐ │
│  │ Pipeline 3: handle_workers_messages (来自其他 Worker 的消息)                  │ │
│  │                                                                               │ │
│  │   Other Workers ──TCP──> Receiver ──> WorkerReceiverHandler                  │ │
│  │                                              │                                │ │
│  │                          ┌───────────────────┴───────────────────┐           │ │
│  │                          ▼                                       ▼           │ │
│  │                     Batch 消息                            BatchRequest       │ │
│  │                          │                                       │           │ │
│  │                          ▼                                       ▼           │ │
│  │                     Processor                                 Helper         │ │
│  │                   (存储别人的Batch)                      (响应数据请求)        │ │
│  └─────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 二、Pipeline 2: handle_clients_transactions 详解

这是 Worker 最核心的流水线，处理从客户端到 Primary 的完整数据流。

### 2.1 流水线组件

```
Client Transaction
       │
       ▼
┌─────────────┐     tx_channel      ┌─────────────┐
│  Receiver   │ ──────────────────> │  BatchMaker │
│ (TCP监听)   │                     │ (打包交易)   │
└─────────────┘                     └──────┬──────┘
                                           │
                                   ┌───────┴───────┐
                                   │               │
                                   ▼               ▼
                           广播到其他Worker   our_batch_channel
                                               │
                                               ▼
                                   ┌─────────────────┐
                                   │  QuorumWaiter   │
                                   │ (等待2f+1确认)   │
                                   └────────┬────────┘
                                            │
                                    quorum_batch_channel
                                            │
                                            ▼
                                   ┌─────────────┐     digest_channel    ┌─────────┐
                                   │  Processor  │ ───────────────────> │ Primary │
                                   │(哈希+存储)   │                      │  Core   │
                                   └─────────────┘                      └─────────┘
```

### 2.2 各组件详细说明

#### BatchMaker（批处理制造器）

**职责**：将零散的交易积累成 Batch，达到阈值后封装并广播。

**关键代码（seal 方法）**：
```rust
async fn seal(&mut self) {
    // 1. 序列化交易
    let message = WorkerMessage::Batch(Batch(self.current_batch.clone()));
    let serialized = bincode::serialize(&message).expect("Failed to serialize batch");

    // 2. 广播给所有其他 Worker（获取 CancelHandler）
    let handlers = self.network.broadcast(self.addresses.clone(), Bytes::from(serialized)).await;
    
    // 3. 把 (Batch, CancelHandlers) 发送给 QuorumWaiter
    self.tx_message
        .send((self.current_batch.drain(..).collect(), handlers))
        .await
        .expect("Failed to send batch");
}
```

**触发条件**：
- 累积交易大小 ≥ `batch_size` 阈值
- 超时时间到达（`max_batch_delay`）

---

#### QuorumWaiter（法定人数等待器）

**职责**：等待 2f+1 个节点确认收到 Batch，确保数据可用性。

**核心逻辑**：
```rust
// 使用 FuturesUnordered 并发等待所有 ACK
let mut wait_for_quorum: FuturesUnordered<_> = handlers
    .into_iter()
    .zip(addresses.into_iter())
    .map(|(handler, (name, _))| {
        let stake = self.committee.stake(&name);
        async move {
            let result = handler.await;  // 等待这个节点的 ACK
            (stake, result)
        }
    })
    .collect();

// 累积 stake 直到达到 quorum
let mut total_stake = self.stake;  // 包含自己的 stake
while let Some((stake, result)) = wait_for_quorum.next().await {
    if result.is_ok() {
        total_stake += stake;
        if total_stake >= self.committee.quorum_threshold() {
            // 达到 2f+1，可以继续！
            break;
        }
    }
}
```

**为什么需要 2f+1？**
- BFT 系统中最多有 f 个恶意节点
- 2f+1 确保至少有 f+1 个诚实节点存储了数据
- 即使 f 个恶意节点拒绝提供数据，仍有诚实节点可以响应

---

#### Processor（处理器）

**职责**：计算 Batch 哈希、持久化存储、向 Primary 报告。

**两种模式**：
```rust
match batch {
    OurBatch(batch) => {
        // 自己的 Batch：计算哈希 + 存储 + 发送 Digest 给 Primary
        let digest = serialized_batch.digest();
        self.store.write(digest.to_vec(), serialized_batch.to_vec()).await;
        self.tx_digest.send((digest, self.id)).await?;
    }
    
    OthersBatch(batch, _source) => {
        // 别人的 Batch：只存储（不发送给 Primary）
        let digest = serialized_batch.digest();
        self.store.write(digest.to_vec(), serialized_batch.to_vec()).await;
    }
}
```

---

## 三、Pipeline 3: handle_workers_messages 详解

处理来自其他 Worker 的消息，实现节点间数据同步。

### 3.1 消息类型

```rust
pub enum WorkerMessage {
    Batch(Batch),                           // 其他 Worker 广播的 Batch
    BatchRequest(Vec<Digest>, PublicKey),   // 请求特定的 Batch 数据
}
```

### 3.2 数据流

```
其他 Worker
     │
     │ TCP 连接
     ▼
┌───────────┐
│ Receiver  │
└─────┬─────┘
      │
      ▼
WorkerReceiverHandler::dispatch()
      │
      ├─────────────────────────────────┬──────────────────────────────┐
      │                                 │                              │
      ▼                                 ▼                              ▼
 Batch 消息                       BatchRequest                     发送 ACK
      │                                 │                              │
      ▼                                 ▼                              ▼
 Processor                           Helper                     writer.send(ack)
(own_batch=false)              (从 Store 读取数据)
      │                                 │
      ▼                                 ▼
 存储到 RocksDB                发送数据给请求者
```

### 3.3 Helper 组件

Helper 响应其他 Worker 的 BatchRequest：

```rust
async fn run(&mut self) {
    while let Some((digests, origin)) = self.rx_request.recv().await {
        for digest in digests {
            // 从本地存储读取
            if let Ok(Some(data)) = self.store.read(digest.to_vec()).await {
                // 发送给请求者
                self.network.send(origin, Bytes::from(data)).await;
            }
        }
    }
}
```

---

## 四、Batch 共识机制（完整流程）

### 4.1 从 Batch 到 Certificate 的完整路径

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                           Batch 共识完整流程                                       │
├──────────────────────────────────────────────────────────────────────────────────┤
│                                                                                    │
│  [Worker 层]                                                                       │
│                                                                                    │
│    Client Tx ──> BatchMaker ──> QuorumWaiter ──> Processor                        │
│                      │              │                │                             │
│                      │              │                │                             │
│                 广播 Batch      2f+1 ACK        计算 Digest                        │
│                      │              │                │                             │
│                      ▼              ▼                ▼                             │
│               其他 Workers     确保数据可用      发送给 Primary                     │
│                                                      │                             │
├──────────────────────────────────────────────────────┼─────────────────────────────┤
│                                                      │                             │
│  [Primary 层]                                        │                             │
│                                                      ▼                             │
│                                               ┌─────────────┐                      │
│                                               │  Proposer   │                      │
│                                               │ (收集Digest) │                      │
│                                               └──────┬──────┘                      │
│                                                      │                             │
│                                                      │ 积累足够 Digest              │
│                                                      ▼                             │
│                                               ┌─────────────┐                      │
│                                               │   Header    │                      │
│                                               │  (区块头)    │                      │
│                                               └──────┬──────┘                      │
│                                                      │                             │
│                                              广播给其他 Primary                     │
│                                                      │                             │
│                                                      ▼                             │
│                                               ┌─────────────┐                      │
│                                               │    Core     │                      │
│                                               │ (验证+投票)  │                      │
│                                               └──────┬──────┘                      │
│                                                      │                             │
│                                              收集 2f+1 Votes                       │
│                                                      │                             │
│                                                      ▼                             │
│                                               ┌─────────────┐                      │
│                                               │ Certificate │                      │
│                                               │ (最终证明)   │                      │
│                                               └──────┬──────┘                      │
│                                                      │                             │
├──────────────────────────────────────────────────────┼─────────────────────────────┤
│                                                      │                             │
│  [Consensus 层]                                      ▼                             │
│                                                                                    │
│                                           Certificate 进入 DAG                     │
│                                                      │                             │
│                                              Tusk 算法排序                          │
│                                                      │                             │
│                                                      ▼                             │
│                                               交易最终确认！                        │
│                                                                                    │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 为什么 Worker 的 2f+1 ACK 不是"共识"？

| 阶段 | 位置 | 作用 | 是否共识 |
|------|------|------|----------|
| Batch 广播 + ACK | Worker | 确保数据被足够节点**存储** | ❌ 只是数据可用性 |
| Header 投票 | Primary | 对区块头内容达成**一致** | ✅ 这是真正的共识 |
| Certificate 形成 | Primary | 证明 Header 被**认可** | ✅ 共识的证明 |

---

## 五、关键 Rust 异步模式

### 5.1 Tokio Channel 模式

```rust
// 创建 channel
let (tx_batch, rx_batch) = channel(CHANNEL_CAPACITY);

// 发送端
tx_batch.send(batch).await?;

// 接收端
while let Some(batch) = rx_batch.recv().await {
    // 处理 batch
}
```

### 5.2 TCP Stream 的 split() 模式

```rust
// 创建带帧的传输层
let transport = Framed::new(socket, LengthDelimitedCodec::new());

// 分割为读写两端
let (mut writer, mut reader) = transport.split();
//      ↑              ↑
//      │              └── 读取来自对方的消息
//      └────────────────── 发送消息给对方

// 可以同时进行读写
while let Some(frame) = reader.next().await {
    // 处理消息
    writer.send(ack).await;  // 发送响应
}
```

**为什么需要 split？**
- Rust 的借用规则不允许同时持有可变引用用于读和写
- split() 将一个双向流分成两个独立所有权的半双工流
- 允许并发读写，reader 等待下一条消息时 writer 可以发送响应

### 5.3 FuturesUnordered 模式

```rust
// 创建 Future 集合
let mut futures: FuturesUnordered<_> = handlers
    .into_iter()
    .map(|h| async move { h.await })
    .collect();

// "先完成先处理"
while let Some(result) = futures.next().await {
    // 处理第一个完成的 Future
}
```

### 5.4 CancelHandler 模式（带宽优化）

```rust
pub struct CancelHandler {
    receiver: oneshot::Receiver<()>,  // 接收成功信号
    abort_handle: AbortHandle,         // 取消发送任务
}

// QuorumWaiter 达到 2f+1 后
// 剩余的 CancelHandler 被 drop
// → abort_handle 自动取消重试任务
// → 节省网络带宽
```

---

## 六、handle_clients_transactions vs handle_workers_messages 对比

| 方面 | handle_clients_transactions | handle_workers_messages |
|------|---------------------------|------------------------|
| 数据来源 | 外部客户端 | 其他 Worker 节点 |
| 处理流程 | BatchMaker → QuorumWaiter → Processor | 直接到 Processor 或 Helper |
| 是否广播 | ✅ 需要广播给其他 Worker | ❌ 已经是接收方 |
| 是否等待 Quorum | ✅ 必须等待 2f+1 ACK | ❌ 直接存储 |
| 发送给 Primary | ✅ 发送 Digest (OurBatch) | ❌ 不发送 (OthersBatch) |
| 职责 | 数据分发者 | 数据存储者/响应者 |

---

## 七、核心数据结构

```rust
// 交易批次
pub struct Batch(pub Vec<Transaction>);
pub type Transaction = Vec<u8>;  // 交易就是字节数组

// Batch 的哈希值
pub type Digest = [u8; 32];  // SHA-512 的前 32 字节

// Worker 间消息
pub enum WorkerMessage {
    Batch(Batch),
    BatchRequest(Vec<Digest>, PublicKey),
}

// Primary 的区块头（包含多个 Worker 的 Digest）
pub struct Header {
    pub author: PublicKey,
    pub round: Round,
    pub payload: Vec<(Digest, WorkerId)>,  // 多个 Batch Digest
    pub parents: Vec<Digest>,               // DAG 父节点
    // ...
}
```

---

## 八、总结

Narwhal Worker 的核心设计理念：

1. **数据与共识分离**：Worker 只负责数据可用性，Primary 负责共识
2. **流水线架构**：通过 channel 连接各组件，实现高吞吐
3. **2f+1 数据保障**：在发送给 Primary 前确保数据已被足够节点存储
4. **Actor 模型**：每个组件独立运行，通过消息传递通信
5. **异步优先**：全面使用 Tokio 异步运行时，避免阻塞

这种架构使得 Narwhal 能够实现高达数十万 TPS 的吞吐量，同时保证 BFT 安全性。
