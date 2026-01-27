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
