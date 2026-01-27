# handle_clients_transactions 详解

本文档递归地介绍 `handle_clients_transactions` 函数及其调用的所有组件，适合 Rust 新手阅读。

---

## 目录

1. [概述](#1-概述)
2. [入口函数: handle_clients_transactions](#2-入口函数-handle_clients_transactions)
3. [Rust 基础语法解释](#3-rust-基础语法解释)
4. [组件1: Receiver - 网络接收器](#4-组件1-receiver---网络接收器)
5. [组件2: TxReceiverHandler - 交易处理器](#5-组件2-txreceiverhandler---交易处理器)
6. [组件3: BatchMaker - 批次打包器](#6-组件3-batchmaker---批次打包器)
7. [组件4: QuorumWaiter - 法定人数等待器](#7-组件4-quorumwaiter---法定人数等待器)
8. [组件5: Processor - 批次处理器](#8-组件5-processor---批次处理器)
9. [组件6: PrimaryConnector - 主节点连接器](#9-组件6-primaryconnector---主节点连接器)
10. [完整数据流图](#10-完整数据流图)
11. [关键 Rust 概念总结](#11-关键-rust-概念总结)

---

## 1. 概述

`handle_clients_transactions` 是 Worker 节点处理客户端交易的核心函数。它建立了一条完整的数据处理管道：

```
Client → Receiver → BatchMaker → QuorumWaiter → Processor → PrimaryConnector → Primary
```

这条管道的目标是：
1. **接收**客户端发来的交易
2. **打包**交易成批次（batch）
3. **广播**批次给其他 Worker 并等待确认
4. **存储**批次并计算摘要（digest）
5. **发送**摘要给 Primary 节点

---

## 2. 入口函数: handle_clients_transactions

**文件位置**: `worker/src/worker.rs` 第 131-182 行

```rust
/// Spawn all tasks responsible to handle clients transactions.
fn handle_clients_transactions(&self, tx_primary: Sender<SerializedBatchDigestMessage>) {
    // 创建三个 channel（通道），用于组件之间的通信
    let (tx_batch_maker, rx_batch_maker) = channel(CHANNEL_CAPACITY);
    let (tx_quorum_waiter, rx_quorum_waiter) = channel(CHANNEL_CAPACITY);
    let (tx_processor, rx_processor) = channel(CHANNEL_CAPACITY);

    // 获取监听地址
    let mut address = self
        .committee
        .worker(&self.name, &self.id)
        .expect("Our public key or worker id is not in the committee")
        .transactions;
    address.set_ip("0.0.0.0".parse().unwrap());
    
    // 启动网络接收器
    Receiver::spawn(
        address,
        /* handler */ TxReceiverHandler { tx_batch_maker },
    );

    // 启动批次打包器
    BatchMaker::spawn(
        self.parameters.batch_size,
        self.parameters.max_batch_delay,
        /* rx_transaction */ rx_batch_maker,
        /* tx_message */ tx_quorum_waiter,
        /* workers_addresses */ ...
    );

    // 启动法定人数等待器
    QuorumWaiter::spawn(
        self.committee.clone(),
        /* stake */ self.committee.stake(&self.name),
        /* rx_message */ rx_quorum_waiter,
        /* tx_batch */ tx_processor,
    );

    // 启动批次处理器
    Processor::spawn(
        self.id,
        self.store.clone(),
        /* rx_batch */ rx_processor,
        /* tx_digest */ tx_primary,
        /* own_batch */ true,
    );
}
```

### 代码逐行解释

#### 2.1 函数签名

```rust
fn handle_clients_transactions(&self, tx_primary: Sender<SerializedBatchDigestMessage>)
```

- `fn`: Rust 的函数关键字
- `&self`: 表示这是一个方法（method），`&` 表示借用（borrow），不获取所有权
- `tx_primary`: 参数名，类型是 `Sender<...>`，这是一个发送端
- `Sender<SerializedBatchDigestMessage>`: 泛型类型，表示可以发送 `SerializedBatchDigestMessage` 类型消息的发送器

#### 2.2 创建 Channel

```rust
let (tx_batch_maker, rx_batch_maker) = channel(CHANNEL_CAPACITY);
```

- `let`: 变量声明关键字
- `(tx_batch_maker, rx_batch_maker)`: 元组解构（tuple destructuring），`channel()` 返回一个元组
- `channel(CHANNEL_CAPACITY)`: 创建一个 MPSC channel（多生产者单消费者通道）
  - `tx`: 发送端（transmitter）
  - `rx`: 接收端（receiver）
  - `CHANNEL_CAPACITY = 1000`: 通道缓冲区大小

**为什么用 Channel？**
Channel 是 Rust 异步编程中组件间通信的标准方式，类似于 Go 的 channel。它实现了：
- 线程安全的消息传递
- 异步非阻塞操作
- 背压（backpressure）控制

#### 2.3 获取网络地址

```rust
let mut address = self
    .committee
    .worker(&self.name, &self.id)
    .expect("Our public key or worker id is not in the committee")
    .transactions;
```

- `self.committee`: 访问 Worker 结构体的 `committee` 字段
- `.worker(&self.name, &self.id)`: 调用方法获取 worker 地址配置，返回 `Result` 类型
- `.expect("...")`: 如果是 `Err`，程序 panic 并打印消息；如果是 `Ok`，解包获取值
- `.transactions`: 访问返回结构体的 `transactions` 字段（客户端连接地址）

```rust
address.set_ip("0.0.0.0".parse().unwrap());
```

- `"0.0.0.0".parse()`: 将字符串解析为 IP 地址
- `.unwrap()`: 解包 Result，失败则 panic
- `.set_ip(...)`: 设置监听所有网络接口

---

## 3. Rust 基础语法解释

在深入各组件之前，先了解一些常见的 Rust 语法：

### 3.1 所有权与借用

```rust
fn example(data: String)       // 获取所有权，调用后原变量不可用
fn example(data: &String)      // 借用，只读访问
fn example(data: &mut String)  // 可变借用，可修改
```

### 3.2 Option 和 Result

```rust
// Option: 表示可能有值或没有值
enum Option<T> {
    Some(T),  // 有值
    None,     // 无值
}

// Result: 表示可能成功或失败
enum Result<T, E> {
    Ok(T),    // 成功
    Err(E),   // 失败
}

// 常见处理方式
value.unwrap()              // 失败则 panic
value.expect("error msg")   // 失败则 panic 并显示消息
value?                      // 失败则提前返回错误（需在返回 Result 的函数中使用）
```

### 3.3 async/await

```rust
async fn do_something() {
    // 异步函数，不会阻塞线程
    let result = some_async_operation().await;  // .await 等待完成
}
```

### 3.4 tokio::spawn

```rust
tokio::spawn(async move {
    // 启动一个新的异步任务
    // move: 将所有引用的变量移动到闭包中
});
```

### 3.5 模式匹配

```rust
match value {
    Some(x) => println!("Got {}", x),
    None => println!("Nothing"),
}

// 或使用 if let
if let Some(x) = value {
    println!("Got {}", x);
}
```

### 3.6 trait（特征）

```rust
// 定义 trait
trait MessageHandler {
    fn dispatch(&self, message: Bytes);
}

// 为结构体实现 trait
impl MessageHandler for TxReceiverHandler {
    fn dispatch(&self, message: Bytes) {
        // 具体实现
    }
}
```

---

## 4. 组件1: Receiver - 网络接收器

**文件位置**: `network/src/receiver.rs`

`Receiver` 是一个通用的 TCP 服务器，负责接收网络连接并分发消息。

### 4.1 结构体定义

```rust
pub struct Receiver<Handler: MessageHandler> {
    /// Address to listen to.
    address: SocketAddr,
    /// Struct responsible to define how to handle received messages.
    handler: Handler,
}
```

- `<Handler: MessageHandler>`: 泛型参数，`Handler` 必须实现 `MessageHandler` trait
- `address`: 监听的网络地址
- `handler`: 消息处理器

### 4.2 spawn 方法

```rust
impl<Handler: MessageHandler> Receiver<Handler> {
    pub fn spawn(address: SocketAddr, handler: Handler) {
        tokio::spawn(async move {
            Self { address, handler }.run().await;
        });
    }
```

- `impl<Handler: MessageHandler>`: 为泛型 `Receiver` 实现方法
- `tokio::spawn(async move { ... })`: 启动一个新的异步任务
- `Self { address, handler }`: 创建结构体实例
- `.run().await`: 调用 run 方法并等待

### 4.3 run 方法 - 主循环

```rust
async fn run(&self) {
    // 绑定 TCP 端口
    let listener = TcpListener::bind(&self.address)
        .await
        .expect("Failed to bind TCP port");

    loop {
        // 接受新连接
        let (socket, peer) = match listener.accept().await {
            Ok(value) => value,
            Err(e) => {
                warn!("{}", NetworkError::FailedToListen(e));
                continue;
            }
        };
        // 为每个连接启动一个处理器
        Self::spawn_runner(socket, peer, self.handler.clone()).await;
    }
}
```

**流程**:
1. `TcpListener::bind`: 绑定端口开始监听
2. `listener.accept().await`: 异步等待新连接
3. `spawn_runner`: 为每个连接创建独立的处理任务

### 4.4 spawn_runner - 连接处理器

```rust
async fn spawn_runner(socket: TcpStream, peer: SocketAddr, handler: Handler) {
    tokio::spawn(async move {
        // 使用长度前缀编码器包装 socket
        let transport = Framed::new(socket, LengthDelimitedCodec::new());
        let (mut writer, mut reader) = transport.split();
        
        // 持续读取消息
        while let Some(frame) = reader.next().await {
            match frame {
                Ok(message) => {
                    // 调用 handler 处理消息
                    handler.dispatch(&mut writer, message.freeze()).await;
                }
                Err(e) => {
                    warn!("{}", e);
                    return;
                }
            }
        }
    });
}
```

**关键点**:
- `Framed::new(socket, LengthDelimitedCodec::new())`: 将原始 TCP 流包装成消息帧
  - `LengthDelimitedCodec`: 使用长度前缀来分隔消息（先发送4字节长度，再发送内容）
- `transport.split()`: 分离读写端，可以同时读写
- `handler.dispatch()`: 调用具体的消息处理逻辑

---

## 5. 组件2: TxReceiverHandler - 交易处理器

**文件位置**: `worker/src/worker.rs` 第 234-251 行

### 5.1 结构体定义

```rust
#[derive(Clone)]
struct TxReceiverHandler {
    tx_batch_maker: Sender<Transaction>,
}
```

- `#[derive(Clone)]`: 自动实现 `Clone` trait，允许复制
- `tx_batch_maker`: 向 BatchMaker 发送交易的 channel

### 5.2 MessageHandler trait 实现

```rust
#[async_trait]
impl MessageHandler for TxReceiverHandler {
    async fn dispatch(&self, _writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>> {
        // 将交易发送给 batch maker
        self.tx_batch_maker
            .send(message.to_vec())
            .await
            .expect("Failed to send transaction");

        // 让出 CPU 给其他任务
        tokio::task::yield_now().await;
        Ok(())
    }
}
```

**逐行解释**:

```rust
#[async_trait]
```
- Rust 的 trait 默认不支持 async 方法，`#[async_trait]` 宏解决这个问题

```rust
async fn dispatch(&self, _writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>>
```
- `_writer`: 下划线前缀表示有意不使用这个参数
- `message: Bytes`: 收到的原始字节数据
- `Result<(), Box<dyn Error>>`: 返回成功(`()`)或任意错误类型

```rust
self.tx_batch_maker.send(message.to_vec()).await
```
- `message.to_vec()`: 将 `Bytes` 转换为 `Vec<u8>`
- `.send(...).await`: 异步发送到 channel

```rust
tokio::task::yield_now().await;
```
- 主动让出 CPU，给其他任务执行机会，提高并发效率

---

## 6. 组件3: BatchMaker - 批次打包器

**文件位置**: `worker/src/batch_maker.rs`

BatchMaker 将单个交易打包成批次，然后广播给其他 Worker。

### 6.1 类型定义

```rust
pub type Transaction = Vec<u8>;  // 交易就是字节数组
pub type Batch = Vec<Transaction>;  // 批次是交易的列表
```

### 6.2 结构体定义

```rust
pub struct BatchMaker {
    /// 期望的批次大小（字节）
    batch_size: usize,
    /// 最大等待时间（毫秒）
    max_batch_delay: u64,
    /// 接收交易的 channel
    rx_transaction: Receiver<Transaction>,
    /// 发送消息给 QuorumWaiter 的 channel
    tx_message: Sender<QuorumWaiterMessage>,
    /// 其他 worker 的网络地址
    workers_addresses: Vec<(PublicKey, SocketAddr)>,
    /// 当前正在组装的批次
    current_batch: Batch,
    /// 当前批次的大小
    current_batch_size: usize,
    /// 可靠网络发送器
    network: ReliableSender,
}
```

### 6.3 spawn 方法

```rust
pub fn spawn(
    batch_size: usize,
    max_batch_delay: u64,
    rx_transaction: Receiver<Transaction>,
    tx_message: Sender<QuorumWaiterMessage>,
    workers_addresses: Vec<(PublicKey, SocketAddr)>,
) {
    tokio::spawn(async move {
        Self {
            batch_size,
            max_batch_delay,
            rx_transaction,
            tx_message,
            workers_addresses,
            current_batch: Batch::with_capacity(batch_size * 2),
            current_batch_size: 0,
            network: ReliableSender::new(),
        }
        .run()
        .await;
    });
}
```

**注意**: `Batch::with_capacity(batch_size * 2)` 预分配内存，避免频繁扩容。

### 6.4 run 方法 - 核心循环

```rust
async fn run(&mut self) {
    // 创建定时器
    let timer = sleep(Duration::from_millis(self.max_batch_delay));
    tokio::pin!(timer);  // pin! 宏使 timer 可以在循环中重复使用

    loop {
        tokio::select! {
            // 分支1: 收到新交易
            Some(transaction) = self.rx_transaction.recv() => {
                self.current_batch_size += transaction.len();
                self.current_batch.push(transaction);
                
                // 如果批次够大，立即封装
                if self.current_batch_size >= self.batch_size {
                    self.seal().await;
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                }
            },

            // 分支2: 定时器触发
            () = &mut timer => {
                if !self.current_batch.is_empty() {
                    self.seal().await;  // 即使批次较小也封装
                }
                timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
            }
        }

        tokio::task::yield_now().await;
    }
}
```

**tokio::select! 宏**:
- 同时等待多个异步操作
- 哪个先完成就执行哪个分支
- 类似于 Go 的 `select` 语句

**两个触发条件**:
1. 批次大小达到 `batch_size`（默认 500KB）
2. 超时达到 `max_batch_delay`（默认 100ms）

### 6.5 seal 方法 - 封装并广播批次

```rust
async fn seal(&mut self) {
    // 清空当前批次大小
    self.current_batch_size = 0;
    
    // 取出所有交易
    let batch: Vec<_> = self.current_batch.drain(..).collect();
    
    // 序列化成 WorkerMessage::Batch
    let message = WorkerMessage::Batch(batch);
    let serialized = bincode::serialize(&message).expect("Failed to serialize");

    // 广播给所有其他 worker
    let (names, addresses): (Vec<_>, _) = self.workers_addresses.iter().cloned().unzip();
    let bytes = Bytes::from(serialized.clone());
    let handlers = self.network.broadcast(addresses, bytes).await;

    // 发送给 QuorumWaiter 进行确认
    self.tx_message
        .send(QuorumWaiterMessage {
            batch: serialized,
            handlers: names.into_iter().zip(handlers.into_iter()).collect(),
        })
        .await
        .expect("Failed to deliver batch");
}
```

**关键操作**:
1. `drain(..)`: 清空 Vec 并返回所有元素的迭代器
2. `bincode::serialize`: 将 Rust 结构体序列化为字节
3. `network.broadcast`: 广播给所有 worker 地址
4. 返回 `handlers`: 用于追踪每个广播的确认状态

---

## 7. 组件4: QuorumWaiter - 法定人数等待器

**文件位置**: `worker/src/quorum_waiter.rs`

等待足够多的 Worker 确认收到批次（2f+1 stake）。

### 7.1 QuorumWaiterMessage 定义

```rust
#[derive(Debug)]
pub struct QuorumWaiterMessage {
    /// 序列化的批次数据
    pub batch: SerializedBatchMessage,
    /// 每个广播的取消/确认句柄
    pub handlers: Vec<(PublicKey, CancelHandler)>,
}
```

- `CancelHandler`: 实际上是 `oneshot::Receiver<Bytes>`，用于接收 ACK

### 7.2 结构体定义

```rust
pub struct QuorumWaiter {
    /// 委员会信息（包含所有节点及其 stake）
    committee: Committee,
    /// 本节点的 stake（权重）
    stake: Stake,
    /// 输入 channel
    rx_message: Receiver<QuorumWaiterMessage>,
    /// 输出 channel
    tx_batch: Sender<SerializedBatchMessage>,
}
```

### 7.3 run 方法

```rust
async fn run(&mut self) {
    while let Some(QuorumWaiterMessage { batch, handlers }) = self.rx_message.recv().await {
        // 将所有 handler 放入 FuturesUnordered（无序并发等待）
        let mut wait_for_quorum: FuturesUnordered<_> = handlers
            .into_iter()
            .map(|(name, handler)| {
                let stake = self.committee.stake(&name);
                Self::waiter(handler, stake)
            })
            .collect();

        // 累加 stake，达到法定人数就通过
        let mut total_stake = self.stake;  // 从自己的 stake 开始
        while let Some(stake) = wait_for_quorum.next().await {
            total_stake += stake;
            if total_stake >= self.committee.quorum_threshold() {
                // 达到 2f+1，发送批次给 Processor
                self.tx_batch
                    .send(batch)
                    .await
                    .expect("Failed to deliver batch");
                break;
            }
        }
    }
}
```

**FuturesUnordered**:
- 同时等待多个 Future
- 哪个先完成就先返回哪个的结果
- 比按顺序等待高效得多

**法定人数 (Quorum)**:
- BFT 系统中，需要 2f+1 节点确认才能保证安全
- `quorum_threshold() = 2 * total_votes / 3 + 1`

### 7.4 waiter 辅助方法

```rust
async fn waiter(wait_for: CancelHandler, deliver: Stake) -> Stake {
    let _ = wait_for.await;  // 等待 ACK
    deliver  // 返回对应的 stake
}
```

简单地等待确认，然后返回对应节点的 stake 值。

---

## 8. 组件5: Processor - 批次处理器

**文件位置**: `worker/src/processor.rs`

对批次进行哈希、存储，然后发送摘要给 Primary。

### 8.1 类型定义

```rust
pub type SerializedBatchMessage = Vec<u8>;
```

### 8.2 spawn 方法（整个实现）

```rust
pub struct Processor;

impl Processor {
    pub fn spawn(
        id: WorkerId,
        mut store: Store,
        mut rx_batch: Receiver<SerializedBatchMessage>,
        tx_digest: Sender<SerializedBatchDigestMessage>,
        own_digest: bool,  // 是自己的批次还是别人的
    ) {
        tokio::spawn(async move {
            while let Some(batch) = rx_batch.recv().await {
                // 1. 计算哈希
                let digest = Digest(
                    Sha512::digest(&batch).as_slice()[..32]
                        .try_into()
                        .unwrap()
                );

                // 2. 存储到 RocksDB
                store.write(digest.to_vec(), batch).await;

                // 3. 构造消息
                let message = match own_digest {
                    true => WorkerPrimaryMessage::OurBatch(digest, id),
                    false => WorkerPrimaryMessage::OthersBatch(digest, id),
                };
                
                // 4. 序列化并发送给 Primary
                let message = bincode::serialize(&message)
                    .expect("Failed to serialize");
                tx_digest
                    .send(message)
                    .await
                    .expect("Failed to send digest");
            }
        });
    }
}
```

**关键步骤**:

1. **计算摘要**:
   ```rust
   Sha512::digest(&batch).as_slice()[..32]
   ```
   - 使用 SHA-512 哈希整个批次
   - 取前 32 字节作为摘要（256 位）

2. **存储批次**:
   ```rust
   store.write(digest.to_vec(), batch).await;
   ```
   - Key: 摘要
   - Value: 完整批次数据
   - 之后可以通过摘要找回原始数据

3. **区分消息类型**:
   - `OurBatch`: 我们自己创建的批次
   - `OthersBatch`: 从其他 Worker 收到的批次

---

## 9. 组件6: PrimaryConnector - 主节点连接器

**文件位置**: `worker/src/primary_connector.rs`

将批次摘要发送给 Primary 节点。

### 9.1 结构体定义

```rust
pub struct PrimaryConnector {
    /// Primary 的网络地址
    primary_address: SocketAddr,
    /// 接收摘要的 channel
    rx_digest: Receiver<SerializedBatchDigestMessage>,
    /// 简单网络发送器
    network: SimpleSender,
}
```

### 9.2 完整实现

```rust
impl PrimaryConnector {
    pub fn spawn(
        primary_address: SocketAddr, 
        rx_digest: Receiver<SerializedBatchDigestMessage>
    ) {
        tokio::spawn(async move {
            Self {
                primary_address,
                rx_digest,
                network: SimpleSender::new(),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        while let Some(digest) = self.rx_digest.recv().await {
            // 通过网络发送给 Primary
            self.network
                .send(self.primary_address, Bytes::from(digest))
                .await;
        }
    }
}
```

**SimpleSender vs ReliableSender**:
- `SimpleSender`: 发送后不等待确认，fire-and-forget
- `ReliableSender`: 发送后等待 ACK，失败会重试

Worker 到 Primary 使用 `SimpleSender`，因为：
- 它们通常在同一机器上（LAN）
- 即使丢失，后续的批次也会包含信息

---

## 10. 完整数据流图

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                                  CLIENT                                          │
│                              (发送交易)                                          │
└───────────────────────────────────┬─────────────────────────────────────────────┘
                                    │ TCP 连接
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Receiver (network/receiver.rs)                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  • TcpListener::bind(address)                                           │   │
│  │  • loop { listener.accept().await }                                     │   │
│  │  • spawn_runner → handler.dispatch()                                    │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
└───────────────────────────────────┬─────────────────────────────────────────────┘
                                    │ message: Bytes
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  TxReceiverHandler (worker/worker.rs)                                           │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  impl MessageHandler {                                                   │   │
│  │      fn dispatch(&self, message: Bytes) {                               │   │
│  │          self.tx_batch_maker.send(message.to_vec())                     │   │
│  │      }                                                                   │   │
│  │  }                                                                       │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
└───────────────────────────────────┬─────────────────────────────────────────────┘
                                    │ tx_batch_maker channel
                                    │ Transaction = Vec<u8>
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  BatchMaker (worker/batch_maker.rs)                                             │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  current_batch.push(transaction);                                        │   │
│  │                                                                          │   │
│  │  if current_batch_size >= batch_size || timer_expired {                  │   │
│  │      seal():                                                             │   │
│  │        1. bincode::serialize(WorkerMessage::Batch(batch))               │   │
│  │        2. network.broadcast(addresses, bytes)  ──────────────────────┐  │   │
│  │        3. tx_message.send(QuorumWaiterMessage { batch, handlers })   │  │   │
│  │  }                                                                    │  │   │
│  └───────────────────────────────────────────────────────────────────────│──┘   │
└───────────────────────────────────┬──────────────────────────────────────│──────┘
                                    │                                      │
                                    │ tx_quorum_waiter channel             │ 广播给其他 Workers
                                    │ QuorumWaiterMessage                  │
                                    ▼                                      ▼
┌─────────────────────────────────────────────────────────┐    ┌────────────────────┐
│  QuorumWaiter (worker/quorum_waiter.rs)                 │    │  Other Workers     │
│  ┌─────────────────────────────────────────────────┐   │    │  ┌──────────────┐  │
│  │  while stake < quorum_threshold() {              │   │    │  │ 存储 batch   │  │
│  │      stake += wait_for_ack().await;              │◀──────│  │ 发送 ACK     │  │
│  │  }                                               │   │    │  └──────────────┘  │
│  │  tx_batch.send(batch);                           │   │    └────────────────────┘
│  └─────────────────────────────────────────────────┘   │
└───────────────────────────────────┬─────────────────────┘
                                    │ tx_processor channel
                                    │ SerializedBatchMessage
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Processor (worker/processor.rs)                                                │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  1. digest = Sha512::digest(&batch)[..32]                               │   │
│  │  2. store.write(digest, batch)  ──────────────────────────┐             │   │
│  │  3. message = WorkerPrimaryMessage::OurBatch(digest, id)  │             │   │
│  │  4. tx_digest.send(serialize(message))                    │             │   │
│  └───────────────────────────────────────────────────────────│─────────────┘   │
└───────────────────────────────────┬──────────────────────────│─────────────────┘
                                    │                          │
                                    │ tx_primary channel       ▼
                                    │ SerializedBatchDigest    ┌────────────────┐
                                    ▼                          │   RocksDB      │
┌─────────────────────────────────────────────────────────┐   │  ┌──────────┐  │
│  PrimaryConnector (worker/primary_connector.rs)         │   │  │Key:Digest│  │
│  ┌─────────────────────────────────────────────────┐   │   │  │Val:Batch │  │
│  │  network.send(primary_address, digest)           │   │   │  └──────────┘  │
│  └─────────────────────────────────────────────────┘   │   └────────────────┘
└───────────────────────────────────┬─────────────────────┘
                                    │ TCP
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                                  PRIMARY                                         │
│                          (接收批次摘要，构建 DAG)                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 11. 关键 Rust 概念总结

### 11.1 Channel 通信模式

本代码大量使用 **MPSC Channel**（多生产者单消费者）：

```rust
let (tx, rx) = channel(capacity);
// tx: Sender - 可以 clone，多个生产者
// rx: Receiver - 只有一个消费者

tx.send(data).await;  // 发送（异步）
rx.recv().await;      // 接收（异步），返回 Option<T>
```

### 11.2 Actor 模式

每个组件都是一个 **Actor**：
- 有自己的状态（struct 字段）
- 通过 channel 接收消息
- 在独立的 tokio task 中运行
- 处理消息后可能发送消息给其他 Actor

```rust
// 标准 Actor 模式
impl Component {
    pub fn spawn(...) {
        tokio::spawn(async move {
            Self { ... }.run().await;
        });
    }

    async fn run(&mut self) {
        while let Some(msg) = self.rx.recv().await {
            // 处理消息
            self.tx.send(result).await;
        }
    }
}
```

### 11.3 tokio::select! 多路复用

同时等待多个异步操作：

```rust
tokio::select! {
    Some(msg) = rx.recv() => { /* 收到消息 */ },
    () = &mut timer => { /* 定时器触发 */ },
    result = async_operation => { /* 操作完成 */ },
}
```

### 11.4 错误处理策略

本项目采用"快速失败"策略：

```rust
.expect("error message")  // 失败就 panic
.unwrap()                 // 失败就 panic
```

这对于内部组件间通信是合理的——如果内部 channel 失败，说明系统出了严重问题。

---

## 总结

`handle_clients_transactions` 建立了一条完整的交易处理管道：

| 组件 | 职责 | 输入 | 输出 |
|------|------|------|------|
| **Receiver** | TCP 监听 | 网络连接 | Bytes |
| **TxReceiverHandler** | 转发交易 | Bytes | Transaction |
| **BatchMaker** | 打包+广播 | Transaction | QuorumWaiterMessage |
| **QuorumWaiter** | 等待确认 | QuorumWaiterMessage | SerializedBatch |
| **Processor** | 哈希+存储 | SerializedBatch | DigestMessage |
| **PrimaryConnector** | 发送给 Primary | DigestMessage | (网络) |

这种**管道模式**的优势：
1. 每个组件职责单一，易于理解和测试
2. 组件间通过 channel 解耦，可独立开发
3. 异步处理，高吞吐量
4. 使用 Tokio 运行时，高效利用 CPU
