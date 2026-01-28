// Copyright(C) Facebook, Inc. and its affiliates.
use crate::certificate_waiter::CertificateWaiter;
use crate::core::Core;
use crate::error::DagError;
use crate::garbage_collector::GarbageCollector;
use crate::header_waiter::HeaderWaiter;
use crate::helper::Helper;
use crate::messages::{Certificate, Header, Vote};
use crate::payload_receiver::PayloadReceiver;
use crate::proposer::Proposer;
use crate::synchronizer::Synchronizer;
use async_trait::async_trait;
use bytes::Bytes;
use config::{Committee, KeyPair, Parameters, WorkerId};
use crypto::{Digest, PublicKey, SignatureService};
use futures::sink::SinkExt as _;
use log::info;
use network::{MessageHandler, Receiver as NetworkReceiver, Writer};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};

/// Primary 中每个 channel 的默认容量
pub const CHANNEL_CAPACITY: usize = 1_000;

/// DAG 的轮次编号（Round number）
pub type Round = u64;

/// Primary 之间传递的消息类型
/// 这些消息用于 Primary 节点之间的通信，实现 DAG 共识
#[derive(Debug, Serialize, Deserialize)]
pub enum PrimaryMessage {
    /// Header 消息：包含 Batch Digests 和父证书引用的区块头
    Header(Header),
    /// Vote 消息：对某个 Header 的投票
    Vote(Vote),
    /// Certificate 消息：包含 Header + 2f+1 投票的共识证明
    Certificate(Certificate),
    /// CertificatesRequest 消息：请求缺失的证书
    /// (请求的证书 Digests 列表, 请求者的公钥)
    CertificatesRequest(Vec<Digest>, /* requestor */ PublicKey),
}

/// Primary 发送给 Worker 的消息类型
#[derive(Debug, Serialize, Deserialize)]
pub enum PrimaryWorkerMessage {
    /// 指示 Worker 需要从目标节点同步缺失的 Batch
    /// (缺失的 Batch Digests, 目标节点公钥)
    Synchronize(Vec<Digest>, /* target */ PublicKey),
    /// 指示 Worker 清理指定轮次之前的旧数据
    Cleanup(Round),
}

/// Worker 发送给 Primary 的消息类型
#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerPrimaryMessage {
    /// Worker 通知 Primary：它封装了一个新的 Batch（本地创建的）
    /// (Batch 的 Digest, Worker ID)
    OurBatch(Digest, WorkerId),
    /// Worker 通知 Primary：它接收到了其他节点的 Batch Digest（用于验证）
    /// (Batch 的 Digest, Worker ID)
    OthersBatch(Digest, WorkerId),
}

/// Primary 结构体
/// Primary 是 Narwhal 共识层的入口，负责协调所有子组件
pub struct Primary;

impl Primary {
    /// 启动 Primary 节点及其所有子组件
    /// 
    /// # 参数
    /// - keypair: 节点的公私钥对
    /// - committee: 委员会信息（包含所有节点的网络地址、权重等）
    /// - parameters: 系统参数（header 大小、GC 深度、同步延迟等）
    /// - store: 持久化存储
    /// - tx_consensus: 向共识层（Tusk）发送 Certificate 的通道
    /// - rx_consensus: 从共识层接收已排序的 Certificate 的通道
    pub fn spawn(
        keypair: KeyPair,
        committee: Committee,
        parameters: Parameters,
        store: Store,
        tx_consensus: Sender<Certificate>,
        rx_consensus: Receiver<Certificate>,
    ) {
        // ==================== 创建所有内部通信 Channel ====================
        
        // 接收其他 Worker 的 Batch Digest（用于验证 Header）
        let (tx_others_digests, rx_others_digests) = channel(CHANNEL_CAPACITY);
        
        // 接收本地 Worker 的 Batch Digest（用于创建新 Header）
        let (tx_our_digests, rx_our_digests) = channel(CHANNEL_CAPACITY);
        
        // Core 向 Proposer 发送父证书（达到 quorum 后）
        let (tx_parents, rx_parents) = channel(CHANNEL_CAPACITY);
        
        // Proposer 向 Core 发送新创建的 Header
        let (tx_headers, rx_headers) = channel(CHANNEL_CAPACITY);
        
        // Synchronizer 向 HeaderWaiter 发送需要同步的 Header
        let (tx_sync_headers, rx_sync_headers) = channel(CHANNEL_CAPACITY);
        
        // Synchronizer 向 CertificateWaiter 发送需要等待祖先的 Certificate
        let (tx_sync_certificates, rx_sync_certificates) = channel(CHANNEL_CAPACITY);
        
        // HeaderWaiter 回环发送准备好的 Header 给 Core
        let (tx_headers_loopback, rx_headers_loopback) = channel(CHANNEL_CAPACITY);
        
        // CertificateWaiter 回环发送准备好的 Certificate 给 Core
        let (tx_certificates_loopback, rx_certificates_loopback) = channel(CHANNEL_CAPACITY);
        
        // 接收来自其他 Primary 的消息（Header/Vote/Certificate）
        let (tx_primary_messages, rx_primary_messages) = channel(CHANNEL_CAPACITY);
        
        // 接收来自其他 Primary 的证书请求
        let (tx_cert_requests, rx_cert_requests) = channel(CHANNEL_CAPACITY);

        // 将参数写入日志
        parameters.log();

        // 解析本节点的公钥和私钥
        let name = keypair.name;
        let secret = keypair.secret;

        // 共识轮次的原子变量，用于所有任务同步最新的共识轮次
        // 仅用于垃圾回收（清理旧数据）
        // 只有 GarbageCollector 会写入这个变量
        let consensus_round = Arc::new(AtomicU64::new(0));

        // ==================== 启动网络接收器 ====================
        
        // 启动网络接收器，监听来自其他 Primary 的消息
        let mut address = committee
            .primary(&name)
            .expect("Our public key or worker id is not in the committee")
            .primary_to_primary;  // Primary 到 Primary 的通信地址
        address.set_ip("0.0.0.0".parse().unwrap());  // 监听所有网络接口
        NetworkReceiver::spawn(
            address,
            /* handler */
            PrimaryReceiverHandler {
                tx_primary_messages,  // 处理 Header/Vote/Certificate
                tx_cert_requests,     // 处理证书请求
            },
        );
        info!(
            "Primary {} listening to primary messages on {}",
            name, address
        );

        // 启动网络接收器，监听来自本地 Worker 的消息
        let mut address = committee
            .primary(&name)
            .expect("Our public key or worker id is not in the committee")
            .worker_to_primary;  // Worker 到 Primary 的通信地址
        address.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(
            address,
            /* handler */
            WorkerReceiverHandler {
                tx_our_digests,      // 处理本地 Worker 的 Batch Digest
                tx_others_digests,   // 处理其他节点 Worker 的 Batch Digest
            },
        );
        info!(
            "Primary {} listening to workers messages on {}",
            name, address
        );

        // ==================== 创建辅助组件 ====================
        
        // Synchronizer：提供辅助方法帮助 Core 检查依赖（父证书、payload）
        let synchronizer = Synchronizer::new(
            name,
            &committee,
            store.clone(),
            /* tx_header_waiter */ tx_sync_headers,
            /* tx_certificate_waiter */ tx_sync_certificates,
        );

        // SignatureService：用于对 Digest 进行签名
        let signature_service = SignatureService::new(secret);

        // ==================== 启动核心组件 ====================
        
        // Core：核心状态机，接收并处理来自其他 Primary 的 Header、Vote、Certificate
        Core::spawn(
            name,
            committee.clone(),
            store.clone(),
            synchronizer,
            signature_service.clone(),
            consensus_round.clone(),
            parameters.gc_depth,
            /* rx_primaries */ rx_primary_messages,           // 接收其他 Primary 的消息
            /* rx_header_waiter */ rx_headers_loopback,       // 接收 HeaderWaiter 回环的 Header
            /* rx_certificate_waiter */ rx_certificates_loopback,  // 接收 CertificateWaiter 回环的 Certificate
            /* rx_proposer */ rx_headers,                     // 接收 Proposer 创建的新 Header
            tx_consensus,                                     // 发送 Certificate 给 Consensus
            /* tx_proposer */ tx_parents,                     // 发送父证书给 Proposer
        );

        // GarbageCollector：跟踪最新的共识轮次，允许其他任务清理内部状态
        // 从 Consensus 接收已排序的 Certificate，更新 consensus_round
        GarbageCollector::spawn(&name, &committee, consensus_round.clone(), rx_consensus);

        // PayloadReceiver：接收来自其他 Worker 的 Batch Digest
        // 这些 Digest 仅用于验证 Header（确保我们有 payload 的记录）
        PayloadReceiver::spawn(store.clone(), /* rx_workers */ rx_others_digests);

        // HeaderWaiter：当 Synchronizer 由于缺少父证书或 Batch Digest 而无法验证 Header 时
        // HeaderWaiter 会与其他节点同步，等待回复，并在获得所有缺失数据后重新调度 Header 的执行
        HeaderWaiter::spawn(
            name,
            committee.clone(),
            store.clone(),
            consensus_round,
            parameters.gc_depth,
            parameters.sync_retry_delay,   // 重试同步请求前的延迟
            parameters.sync_retry_nodes,   // 重试时与多少个节点同步
            /* rx_synchronizer */ rx_sync_headers,
            /* tx_core */ tx_headers_loopback,
        );

        // CertificateWaiter：等待接收 Certificate 的所有祖先，然后将其回环到 Core 进行进一步处理
        CertificateWaiter::spawn(
            store.clone(),
            /* rx_synchronizer */ rx_sync_certificates,
            /* tx_core */ tx_certificates_loopback,
        );

        // Proposer：当 Core 收集到足够的父证书时，Proposer 生成包含来自 Worker 的新 Batch Digest 的新 Header
        // 然后将其发送回 Core
        Proposer::spawn(
            name,
            &committee,
            signature_service,
            parameters.header_size,        // Header 的 payload 大小阈值
            parameters.max_header_delay,   // 创建 Header 的最大延迟
            /* rx_core */ rx_parents,      // 接收父证书
            /* rx_workers */ rx_our_digests,  // 接收本地 Worker 的 Digest
            /* tx_core */ tx_headers,      // 发送新 Header 给 Core
        );

        // Helper：专门用于响应其他 Primary 的证书请求
        Helper::spawn(committee.clone(), store, rx_cert_requests);

        // 注意：此日志条目用于计算性能
        info!(
            "Primary {} successfully booted on {}",
            name,
            committee
                .primary(&name)
                .expect("Our public key or worker id is not in the committee")
                .primary_to_primary
                .ip()
        );
    }
}

/// 定义网络接收器如何处理来自其他 Primary 的消息
/// 
/// 这个 Handler 负责：
/// 1. 接收并反序列化消息
/// 2. 根据消息类型路由到不同的 channel
/// 3. 发送 ACK 给发送方
#[derive(Clone)]
struct PrimaryReceiverHandler {
    tx_primary_messages: Sender<PrimaryMessage>,  // 发送 Header/Vote/Certificate 给 Core
    tx_cert_requests: Sender<(Vec<Digest>, PublicKey)>,  // 发送证书请求给 Helper
}

#[async_trait]
impl MessageHandler for PrimaryReceiverHandler {
    /// 分发来自其他 Primary 的消息
    /// 
    /// # 参数
    /// - writer: TCP 连接的写入端，用于发送响应
    /// - serialized: 序列化的消息字节
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        // 立即回复 ACK（确认收到消息）
        let _ = writer.send(Bytes::from("Ack")).await;

        // 反序列化并解析消息
        match bincode::deserialize(&serialized).map_err(DagError::SerializationError)? {
            // 证书请求：路由到 Helper 进行处理
            PrimaryMessage::CertificatesRequest(missing, requestor) => self
                .tx_cert_requests
                .send((missing, requestor))
                .await
                .expect("Failed to send primary message"),
            // 其他消息（Header/Vote/Certificate）：路由到 Core 进行处理
            request => self
                .tx_primary_messages
                .send(request)
                .await
                .expect("Failed to send certificate"),
        }
        Ok(())
    }
}
/*
从**为完成共识做准备**的角度，这段代码是 **Primary 节点之间协作构建 DAG 和达成共识的消息接收入口**，它为共识流程提供了三个关键准备：

## 1. **确保可靠通信：立即回复 ACK**

```rust
let _ = writer.send(Bytes::from("Ack")).await;
```

**共识意义**：
- 这是 Narwhal "可靠广播" 的基础设施保障
- 发送方（通过 `ReliableSender`）在收到 ACK 前会不断重试，收到后才认为消息投递成功
- **为什么重要**：Header/Vote/Certificate 的丢失会导致：
  - Header 丢失 → 无法投票 → 该节点的提议被忽略
  - Vote 丢失 → 无法形成 Certificate → DAG 推进卡住
  - Certificate 丢失 → 其他节点无法引用为父证书 → DAG 断链

## 2. **处理共识核心消息流：构建 DAG**

```rust
request => self.tx_primary_messages.send(request).await
```

这个分支处理三类消息，它们是 DAG 共识的"心跳"：

### **Header（提议阶段）**
- **来源**：其他 Primary 创建新 Header 后广播
- **发往**：`Core` 状态机
- **Core 会做什么**：
  1. 验证 Header（签名、轮次、父证书、payload 可用性）
  2. 如果验证通过，**投票**（发送 `Vote` 回给 Header 作者）
  3. 存储 Header
- **共识准备**：这是"接收他人提议"的入口，为后续投票决策做准备

### **Vote（投票收集阶段）**
- **来源**：其他 Primary 验证我们的 Header 后发来的投票
- **发往**：`Core` → `VotesAggregator`
- **Core 会做什么**：
  1. 累积 votes，计算 stake 权重
  2. 当 stake ≥ `quorum_threshold()` 时，**组装 Certificate**
  3. 广播 Certificate 给所有 Primary
- **共识准备**：这是"收集投票、形成多数决"的关键，没有足够 Vote 就无法推进 DAG

### **Certificate（确认阶段）**
- **来源**：其他 Primary 形成 Certificate 后广播
- **发往**：`Core` → 最终到 Consensus
- **Core 会做什么**：
  1. 验证 Certificate（检查 2f+1 签名、Header 合法性）
  2. 存储 Certificate
  3. 检查是否收集到足够的 Certificate（上一轮 quorum）
  4. 如果是 → 通知 `Proposer` 可以推进到新轮次
  5. 将 Certificate 发送给 Consensus 模块排序
- **共识准备**：这是"DAG 层确认 + 轮次推进 + 输入排序"的触发点

## 3. **处理同步请求：确保数据完整性**

```rust
PrimaryMessage::CertificatesRequest(missing, requestor) => 
    self.tx_cert_requests.send((missing, requestor)).await
```

**共识意义**：
- **来源**：其他 Primary 发现它缺少某些 Certificate（可能是网络延迟、节点重启等）
- **发往**：`Helper` 组件
- **Helper 会做什么**：
  1. 从本地 store 读取被请求的 Certificate
  2. 发送给请求方
- **为什么关键**：
  - 如果节点缺失父证书，它就无法验证引用这些父证书的 Header
  - 如果节点缺失祖先证书，Consensus 就无法正确排序（需要完整的因果链）
  - 这是 Narwhal **"Pull 机制"** 的体现：正常路径是 Push（广播），但缺失时主动 Pull

## 在共识流程中的位置

```
其他 Primary 节点
    ↓ 广播 Header
⭐ PrimaryReceiverHandler::dispatch ⭐ 
    ↓ 路由到 Core
    ↓ 验证 → 投票
    ↓ 收集 Vote
    ↓ 形成 Certificate
    ↓ 广播 Certificate
⭐ PrimaryReceiverHandler::dispatch ⭐（再次接收）
    ↓ 路由到 Core
    ↓ 检查父证书齐全
    ↓ 通知 Proposer 推进轮次
    ↓ 发送到 Consensus 排序
    ↓
最终提交序列
```

## 总结

从共识准备角度，这段 `dispatch` 是 **Primary 节点间协作的"神经中枢"入口**：

1. **通信保障**：ACK 确保关键消息必达，避免 DAG 构建因消息丢失而停滞
2. **共识推进**：接收 Header → 投票 → 收集 Vote → 形成 Certificate → 推进轮次 → 输入排序，整个流程的每个阶段都依赖这个入口
3. **数据修复**：通过同步请求填补缺失的 Certificate，确保所有节点最终有一致的 DAG 视图

**如果这段代码失效，会导致**：
- 无法接收其他节点提议 → 无法参与投票 → 被孤立
- 无法接收投票 → 自己的 Header 永远无法形成 Certificate
- 无法接收 Certificate → DAG 视图不完整 → 共识输出错误
- 无法响应同步 → 网络分区或延迟时无法恢复

这就是为什么 `dispatch` 虽然看起来只是"消息路由"，但它是整个分布式共识能够运转的**必要基础设施**。
*/





/// 定义网络接收器如何处理来自 Worker 的消息
/// 
/// 这个 Handler 负责：
/// 1. 区分本地 Worker 和其他节点 Worker 的消息
/// 2. 将 OurBatch 路由到 Proposer（用于创建新 Header）
/// 3. 将 OthersBatch 路由到 PayloadReceiver（用于验证 Header）
#[derive(Clone)]
struct WorkerReceiverHandler {
    tx_our_digests: Sender<(Digest, WorkerId)>,       // 发送本地 Worker 的 Digest 给 Proposer
    tx_others_digests: Sender<(Digest, WorkerId)>,    // 发送其他节点的 Digest 给 PayloadReceiver
}

#[async_trait]
impl MessageHandler for WorkerReceiverHandler {
    /// 分发来自 Worker 的消息
    /// 
    /// # 参数
    /// - _writer: TCP 连接的写入端（Worker 消息不需要响应，所以加下划线前缀）
    /// - serialized: 序列化的消息字节
    /// 
    async fn dispatch(
        &self,
        _writer: &mut Writer,
        serialized: Bytes,
    ) -> Result<(), Box<dyn Error>> {
        // 反序列化并解析消息
        match bincode::deserialize(&serialized).map_err(DagError::SerializationError)? {
            // OurBatch：本地 Worker 创建的 Batch，发送给 Proposer 用于创建新 Header
            WorkerPrimaryMessage::OurBatch(digest, worker_id) => self
                .tx_our_digests
                .send((digest, worker_id))
                .await
                .expect("Failed to send workers' digests"),
            // OthersBatch：其他节点 Worker 的 Batch，发送给 PayloadReceiver 用于验证 Header
            WorkerPrimaryMessage::OthersBatch(digest, worker_id) => self
                .tx_others_digests
                .send((digest, worker_id))
                .await
                .expect("Failed to send workers' digests"),
        }
        Ok(())
    }
}


/*
从**为完成共识做准备**的角度，这段代码扮演的角色是：**将数据面（Worker 层）的成果桥接到元数据面（Primary DAG 层），为后续共识投票和排序提供必要的输入**。

## 具体做了什么准备工作

### 1. **接收并区分两类 Batch Digest**

```rust
match bincode::deserialize(&serialized) {
    WorkerPrimaryMessage::OurBatch(digest, worker_id) => ...
    WorkerPrimaryMessage::OthersBatch(digest, worker_id) => ...
}
```

这两类消息代表了不同的共识准备阶段：

#### **OurBatch（本地准备）**
- **来源**：本地 Worker 打包交易、可靠广播给其他节点、达到 quorum 可用性阈值后，向本地 Primary 报告
- **发往**：`Proposer`（通过 `tx_our_digests`）
- **作用**：Proposer 会累积这些 digest，当条件满足时（父证书齐全 + payload 足够 / 超时），创建新 Header
  - Header 里的 `payload: BTreeMap<Digest, WorkerId>` 就是从这里来的
  - 这个 Header 会被广播给其他 Primary，触发投票 → 形成 Certificate → 进入共识排序
- **共识意义**：**这是"我提议的数据"进入 DAG 的入口**

#### **OthersBatch（验证准备）**
- **来源**：其他节点的 Worker 广播 Batch 时，我们的 Worker 收到后也会告诉 Primary
- **发往**：`PayloadReceiver`（通过 `tx_others_digests`）
- **作用**：在 store 中记录 `(digest, worker_id)` 对，但不存实际数据（数据在 Worker 的 store 里）
  - 当收到其他 Primary 的 Header 时，`Synchronizer::missing_payload` 会检查：Header 声称的 payload digest 我们是否有记录
  - 如果没记录，说明对方引用了我们没见过的 batch，需要同步
- **共识意义**：**这是"验证他人提议"的依据，确保共识的数据可用性**

### 2. **为什么不回 ACK？**

注意参数是 `_writer`（下划线前缀），这段 `dispatch` 没有 `writer.send("Ack")`。原因是：

- Worker → Primary 这条通道用的是 **`SimpleSender`**（best-effort），发送端（`PrimaryConnector`）不需要等 ACK，只管发
- 对比：Primary ↔ Primary 用的是 **`ReliableSender`**（可靠投递），必须等 ACK 才算成功

这个设计是因为：
- Worker 和 Primary 是"同一节点的不同进程"（或同进程不同任务），网络可靠性高
- Primary 之间是"跨节点 WAN"，需要可靠投递保证

### 3. **在整个共识流程中的位置**

```
[Client] → [Worker] → [QuorumWaiter 达到可用性] → [Processor 存储] 
    → [PrimaryConnector 发送 OurBatch] → ⭐ 这段 dispatch ⭐ 
    → [Proposer 累积 digest] → [创建 Header] → [Core 广播 & 投票] 
    → [形成 Certificate] → [Consensus 排序] → [提交]
```

这段代码是 **Worker 数据面与 Primary 元数据面的桥梁**，没有它：
- Proposer 不知道有哪些 batch 可以放进 Header
- Synchronizer 无法验证其他节点 Header 的 payload 合法性
- 整个 DAG 的构建就会卡住

---

## 总结

从共识准备角度，这段代码完成了：
1. **输入准备**：把本地 Worker 产生的 batch digest 输送给 Proposer，作为创建 Header 的"弹药"
2. **验证准备**：记录其他节点的 batch digest，作为验证他们 Header 的"清单"
3. **架构解耦**：实现了 Narwhal 核心设计"数据与元数据分离"——Worker 负责大块数据传播，Primary 只处理轻量级哈希，但两者通过这个 dispatch 精确对接

没有这个桥接，Primary 就是"巧妇难为无米之炊"，无法启动 DAG 的构建和共识流程。


*/