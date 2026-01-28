// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::Certificate;
use crate::primary::PrimaryWorkerMessage;
use bytes::Bytes;
use config::Committee;
use crypto::PublicKey;
use network::SimpleSender;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;

/// GarbageCollector：共识后清理机制，确保系统不会无限积累旧数据。
///
/// 从共识的角度理解 GarbageCollector：
///
/// **共识的三个阶段**：
/// 1. 准备阶段（Narwhal/Core）：构建 DAG，形成 Certificate
/// 2. 排序阶段（Tusk/Bullshark）：从 DAG 中提取线性顺序
/// 3. 清理阶段（GarbageCollector）：回收已提交轮次的资源 ← 我们在这里
///
/// **为什么需要垃圾回收？**
/// - DAG 会不断生长：每个轮次产生新的 Header、Vote、Certificate
/// - 如果不清理：内存和磁盘会被旧数据占满，最终耗尽资源
/// - 但不能随意删除：只能删除"已经被共识提交"的旧轮次数据
///
/// **核心机制**：
/// 1. 监听 Consensus 输出的已排序 Certificate
/// 2. 跟踪最高提交轮次（consensus_round）
/// 3. 通知 Primary 和 Worker："轮次 X 已提交，可以清理了"
/// 4. 各组件清理 X - gc_depth 之前的旧状态（保留最近 gc_depth 轮以应对分叉）
///
/// **安全性保证**：
/// - 只清理"已提交"的轮次：这些数据已经被排序，不会再被引用
/// - 保留 gc_depth 轮：防止网络延迟导致的"迟到消息"需要这些数据
/// - 原子更新：使用 AtomicU64 确保所有组件看到一致的 consensus_round
pub struct GarbageCollector {
    /// The current consensus round (used for cleanup).
    /// 当前共识已提交的最高轮次，所有组件通过这个共享变量同步清理进度。
    consensus_round: Arc<AtomicU64>,
    /// Receives the ordered certificates from consensus.
    /// 接收 Consensus 输出的已排序 Certificate（线性顺序）。
    rx_consensus: Receiver<Certificate>,
    /// The network addresses of our workers.
    /// 本地所有 Worker 的网络地址（用于通知清理）。
    addresses: Vec<SocketAddr>,
    /// A network sender to notify our workers of cleanup events.
    /// 网络发送器，用于广播清理消息给 Worker。
    network: SimpleSender,
}

impl GarbageCollector {
    /// 启动垃圾回收任务（独立的 tokio 任务）
    ///
    /// 参数说明：
    /// - name: 本节点的公钥（用于查找本地 Worker 地址）
    /// - committee: 委员会配置（包含所有节点和 Worker 的网络信息）
    /// - consensus_round: 共享的轮次计数器（所有组件通过它同步清理进度）
    /// - rx_consensus: Consensus 输出的 Certificate 流（已排序）
    ///
    /// 工作流程：
    /// 1. 从 committee 中查找本地所有 Worker 的地址
    /// 2. 创建 GarbageCollector 实例
    /// 3. 在独立的 tokio 任务中运行（不阻塞主流程）
    pub fn spawn(
        name: &PublicKey,
        committee: &Committee,
        consensus_round: Arc<AtomicU64>,
        rx_consensus: Receiver<Certificate>,
    ) {
        // 查找本地 Worker 地址：我们需要通知它们进行清理
        let addresses = committee
            .our_workers(name)
            .expect("Our public key or worker id is not in the committee")
            .iter()
            .map(|x| x.primary_to_worker)
            .collect();

        tokio::spawn(async move {
            Self {
                consensus_round,
                rx_consensus,
                addresses,
                network: SimpleSender::new(),
            }
            .run()
            .await;
        });
    }

    /// 主循环：监听 Consensus 输出，触发分布式清理
    ///
    /// 共识意义：这是"共识完成后"的资源回收阶段，确保系统可持续运行。
    ///
    /// 工作流程：
    /// 1. 接收 Consensus 输出的每个 Certificate（按线性顺序）
    /// 2. 跟踪最高提交轮次（last_committed_round）
    /// 3. 当轮次推进时，触发全局清理：
    ///    - 更新 consensus_round（Primary 的 Core、Proposer 等会读取它）
    ///    - 广播 Cleanup 消息给所有本地 Worker
    async fn run(&mut self) {
        // 跟踪已提交的最高轮次（初始为 0）
        let mut last_committed_round = 0;

        // ==================== 主循环：处理 Consensus 输出 ====================
        // 共识意义：Consensus 输出的 Certificate 流代表"最终确定的交易顺序"。
        // 每个 Certificate 都已经被排序、不可逆转，可以安全执行。
        while let Some(certificate) = self.rx_consensus.recv().await {
            // TODO [issue #9]: Re-include batch digests that have not been sequenced into our next block.

            let round = certificate.round();

            // ==================== 检测轮次推进 ====================
            // 为什么只在轮次推进时触发清理？
            // - Consensus 可能在同一轮输出多个 Certificate（DAG 的同层节点）
            // - 我们不需要为每个 Certificate 都触发清理，只需要在轮次变化时触发
            // - 这减少了清理的频率，避免过多的网络广播和状态更新
            if round > last_committed_round {
                last_committed_round = round;

                // ==================== 步骤 1：通知 Primary 清理 ====================
                // 共识意义：更新全局的 consensus_round，Primary 的各个组件会读取它。
                //
                // 谁会读取 consensus_round？
                // 1. Core::run() 主循环：
                //    - 每次处理消息后，检查 consensus_round
                //    - 如果 round > gc_depth，清理 round - gc_depth 之前的状态：
                //      * last_voted：防止重复投票的记录
                //      * processing：正在处理的 Header 集合
                //      * certificates_aggregators：轮次的证书聚合器
                //      * cancel_handlers：网络重传的取消句柄
                //
                // 2. Proposer：清理旧的提议状态
                // 3. Synchronizer：清理旧的同步请求
                //
                // 为什么使用 Relaxed 内存顺序？
                // - 清理不需要强同步：稍微延迟几毫秒清理不影响正确性
                // - Relaxed 性能最好，避免昂贵的内存屏障
                self.consensus_round.store(round, Ordering::Relaxed);

                // ==================== 步骤 2：通知 Worker 清理 ====================
                // 共识意义：Worker 存储着实际的交易数据（batch），也需要清理。
                //
                // Worker 收到 Cleanup(round) 后会做什么？
                // 1. 清理旧的 batch 数据：
                //    - 删除 round - gc_depth 之前的 batch（已经被 Consensus 排序）
                //    - 释放内存和磁盘空间
                //
                // 2. 清理同步状态：
                //    - 删除旧的同步请求和响应缓存
                //
                // 为什么使用 SimpleSender 而非 ReliableSender？
                // - 清理消息不是关键消息：丢失一次无所谓，下次轮次推进会再发
                // - 如果 Worker 暂时不可达，它重启后会自动从 Primary 重新同步清理进度
                // - SimpleSender 更高效，不需要等待 ACK
                //
                // 为什么只广播给本地 Worker？
                // - 每个 Primary 只负责清理自己的 Worker
                // - 其他节点的 GarbageCollector 会负责清理它们自己的 Worker
                // - 这是分布式系统的"各自清理"模式，避免跨节点协调
                let bytes = bincode::serialize(&PrimaryWorkerMessage::Cleanup(round))
                    .expect("Failed to serialize our own message");
                self.network
                    .broadcast(self.addresses.clone(), Bytes::from(bytes))
                    .await;
            }
        }
    }
}
