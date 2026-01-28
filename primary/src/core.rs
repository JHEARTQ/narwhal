// Copyright(C) Facebook, Inc. and its affiliates.
use crate::aggregators::{CertificatesAggregator, VotesAggregator};
use crate::error::{DagError, DagResult};
use crate::messages::{Certificate, Header, Vote};
use crate::primary::{PrimaryMessage, Round};
use crate::synchronizer::Synchronizer;
use async_recursion::async_recursion;
use bytes::Bytes;
use config::Committee;
use crypto::Hash as _;
use crypto::{Digest, PublicKey, SignatureService};
use log::{debug, error, warn};
use network::{CancelHandler, ReliableSender};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
/*
从为完成共识做准备的角度，Core 是 Narwhal 的核心状态机，它负责构建 DAG（有向无环图）并为最终的共识排序做好所有准备工作。
可以把它理解为"共识的准备车间"，完成从"原材料（Header）"到"半成品（Certificate）"再到"输入共识排序"的全流程
*/
#[cfg(test)]
#[path = "tests/core_tests.rs"]
pub mod core_tests;

/// Core 是 Narwhal 的核心状态机，负责构建 DAG 并为共识排序做准备。
///
/// 从"为完成共识做准备"的角度，Core 完成了以下关键职责：
///
/// 1. **数据可用性保障**：通过 Synchronizer 确保所有引用的数据（父证书、payload）都可用
/// 2. **多数决机制**：通过 VotesAggregator 和 CertificatesAggregator 实现 BFT 核心（收集 2f+1 权重）
/// 3. **防重放与一致性**：防止重复投票和重复处理，确保 DAG 结构正确
/// 4. **异步容错**：通过"挂起-同步-回环"机制，在网络不可靠时仍能构建完整 DAG
/// 5. **清晰输出**：将验证完毕、因果完整的 Certificate 流输出给 Consensus 进行排序
///
/// 工作流程：Header（提议）→ Vote（投票）→ Certificate（确认）→ 推进轮次 + 输入共识
pub struct Core {
    // ==================== 身份与基础设施 ====================
    /// The public key of this primary.
    name: PublicKey,
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    store: Store,
    /// Handles synchronization with other nodes and our workers.
    synchronizer: Synchronizer,
    /// Service to sign headers.
    signature_service: SignatureService,
    /// The current consensus round (used for cleanup).
    consensus_round: Arc<AtomicU64>,
    /// The depth of the garbage collector.
    gc_depth: Round,

    // ==================== 输入输出通道（四条输入 + 两条输出）====================
    /// Receiver for dag messages (headers, votes, certificates).
    rx_primaries: Receiver<PrimaryMessage>,
    /// Receives loopback headers from the `HeaderWaiter`.
    rx_header_waiter: Receiver<Header>,
    /// Receives loopback certificates from the `CertificateWaiter`.
    rx_certificate_waiter: Receiver<Certificate>,
    /// Receives our newly created headers from the `Proposer`.
    rx_proposer: Receiver<Header>,
    /// Output all certificates to the consensus layer.
    tx_consensus: Sender<Certificate>,
    /// Send valid a quorum of certificates' ids to the `Proposer` (along with their round).
    tx_proposer: Sender<(Vec<Digest>, Round)>,

    // ==================== 核心状态：投票聚合与轮次推进 ====================
    /// The last garbage collected round.
    gc_round: Round,
    /// The authors of the last voted headers (防止重复投票，确保每轮每个作者只投一次)。
    last_voted: HashMap<Round, HashSet<PublicKey>>,
    /// The set of headers we are currently processing (防止重复处理)。
    processing: HashMap<Round, HashSet<Digest>>,
    /// The last header we proposed (for which we are waiting votes).
    current_header: Header,
    /// Aggregates votes into a certificate (收集 2f+1 投票 → 形成 Certificate)。
    votes_aggregator: VotesAggregator,
    /// Aggregates certificates to use as parents for new headers (收集 2f+1 证书 → 推进轮次)。
    certificates_aggregators: HashMap<Round, Box<CertificatesAggregator>>,
    /// A network sender to send the batches to the other workers.
    network: ReliableSender,
    /// Keeps the cancel handlers of the messages we sent.
    cancel_handlers: HashMap<Round, Vec<CancelHandler>>,
}

impl Core {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        store: Store,
        synchronizer: Synchronizer,
        signature_service: SignatureService,
        consensus_round: Arc<AtomicU64>,
        gc_depth: Round,
        rx_primaries: Receiver<PrimaryMessage>,
        rx_header_waiter: Receiver<Header>,
        rx_certificate_waiter: Receiver<Certificate>,
        rx_proposer: Receiver<Header>,
        tx_consensus: Sender<Certificate>,
        tx_proposer: Sender<(Vec<Digest>, Round)>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                store,
                synchronizer,
                signature_service,
                consensus_round,
                gc_depth,
                rx_primaries,
                rx_header_waiter,
                rx_certificate_waiter,
                rx_proposer,
                tx_consensus,
                tx_proposer,
                gc_round: 0,
                last_voted: HashMap::with_capacity(2 * gc_depth as usize),
                processing: HashMap::with_capacity(2 * gc_depth as usize),
                current_header: Header::default(),
                votes_aggregator: VotesAggregator::new(),
                certificates_aggregators: HashMap::with_capacity(2 * gc_depth as usize),
                network: ReliableSender::new(),
                cancel_handlers: HashMap::with_capacity(2 * gc_depth as usize),
            }
            .run()
            .await;
        });
    }

    async fn process_own_header(&mut self, header: Header) -> DagResult<()> {
        // Reset the votes aggregator.
        self.current_header = header.clone();
        self.votes_aggregator = VotesAggregator::new();

        // Broadcast the new header in a reliable manner.
        let addresses = self
            .committee
            .others_primaries(&self.name)
            .iter()
            .map(|(_, x)| x.primary_to_primary)
            .collect();
        let bytes = bincode::serialize(&PrimaryMessage::Header(header.clone()))
            .expect("Failed to serialize our own header");
        let handlers = self.network.broadcast(addresses, Bytes::from(bytes)).await;
        self.cancel_handlers
            .entry(header.round)
            .or_insert_with(Vec::new)
            .extend(handlers);

        // Process the header.
        self.process_header(&header).await
    }

    /// 阶段 1：处理 Header（提议阶段）→ 投票
    ///
    /// 这是共识准备的第一步：验证他人的提议，并决定是否投票支持。
    ///
    /// 关键验证点：
    /// 1. 父证书是否齐全（通过 Synchronizer 检查，缺失则挂起等待同步）
    /// 2. 父证书是否达到 quorum 阈值（2f+1 权重）
    /// 3. 父证书轮次是否连续（round + 1）
    /// 4. Payload（batch digest）是否可用（通过 Synchronizer 检查）
    /// 5. 是否已经为该作者在该轮投过票（防止重复投票）
    ///
    /// 如果所有验证通过，创建 Vote 并发送给 Header 作者。
    #[async_recursion]
    async fn process_header(&mut self, header: &Header) -> DagResult<()> {
        debug!("Processing {:?}", header);

        // ==================== 步骤 0：标记"正在处理" ====================
        // 将这个 Header 加入 processing 集合，防止重复处理。
        // 这是幂等性保证：即使收到同一个 Header 多次（网络重传），也只处理一次。
        self.processing
            .entry(header.round)
            .or_insert_with(HashSet::new)
            .insert(header.id.clone());

        // ==================== 步骤 1：检查父证书是否齐全 ====================
        // 共识意义：DAG 的核心是"因果关系"，每个 Header 必须引用上一轮的 quorum 证书作为"父节点"。
        // 如果父证书缺失，说明我们的 DAG 视图不完整，需要先同步缺失的证书。
        //
        // Synchronizer::get_parents 的行为：
        // - 如果所有父证书都在本地 store → 返回完整的父证书列表
        // - 如果有任何父证书缺失 → 返回空 Vec，并触发后台同步：
        //   1. 向其他节点发送 CertificatesRequest
        //   2. 等待证书到达（通过 store.notify_read）
        //   3. 通过 HeaderWaiter 将此 Header 重新发送回 rx_header_waiter
        //
        // 这是 Narwhal "异步容错" 的核心机制：缺失依赖时挂起，同步完成后自动恢复。
        let parents = self.synchronizer.get_parents(header).await?;
        if parents.is_empty() {
            debug!("Processing of {} suspended: missing parent(s)", header.id);
            return Ok(()); // 挂起当前处理，等待 HeaderWaiter 回环
        }

        // ==================== 步骤 2：验证父证书（BFT 核心） ====================
        // 共识意义：这是 Byzantine Fault Tolerance 的核心验证逻辑。
        //
        // 验证 1：轮次连续性
        // - Header 声称自己是 round r，那它的父证书必须都来自 round r-1
        // - 这确保 DAG 按轮次分层，防止"跨轮引用"导致的混乱
        //
        // 验证 2：Quorum 阈值（2f+1 权重）
        // - 累计所有父证书作者的 stake 权重
        // - 必须 >= committee.quorum_threshold()（通常是 2/3 总权重）
        // - 这确保：至少有 f+1 个诚实节点认可了这些父证书，满足 BFT 安全性
        //
        // 如果验证失败 → 拒绝 Header，不投票
        let mut stake = 0;
        for x in parents {
            // 检查：父证书的轮次 + 1 == 当前 Header 的轮次
            ensure!(
                x.round() + 1 == header.round,
                DagError::MalformedHeader(header.id.clone())
            );
            stake += self.committee.stake(&x.origin());
        }
        // 检查：累计权重是否达到 quorum（2f+1）
        ensure!(
            stake >= self.committee.quorum_threshold(),
            DagError::HeaderRequiresQuorum(header.id.clone())
        );

        // ==================== 步骤 3：检查 Payload 是否可用 ====================
        // 共识意义：Narwhal 的核心设计是"数据与元数据分离"。
        // - Header 只包含 batch digest（哈希），实际数据在 Worker 的 store 里
        // - 这里检查：Header 声称的所有 batch digest，我们是否有对应的记录
        //
        // Synchronizer::missing_payload 的行为：
        // - 检查 header.payload 中的每个 (digest, worker_id) 对
        // - 在本地 store 中查找 key = [digest + worker_id]
        // - 如果有任何 digest 缺失 → 返回 true，并触发后台同步：
        //   1. 向 Worker 发送 PrimaryWorkerMessage::Synchronize
        //   2. Worker 向其他节点请求 batch
        //   3. 数据到达后，通过 HeaderWaiter 回环重新处理此 Header
        //
        // 为什么需要 worker_id？
        // - 防止攻击：恶意节点可能声称"batch X 来自 worker 1"，但实际上 X 在 worker 2
        // - 我们必须验证：digest 确实来自声称的 worker，确保数据来源可信
        if self.synchronizer.missing_payload(header).await? {
            debug!("Processing of {} suspended: missing payload", header);
            return Ok(()); // 挂起当前处理，等待 Worker 同步完成后 HeaderWaiter 回环
        }

        // ==================== 步骤 4：存储 Header ====================
        // 所有验证都通过了，将 Header 持久化到 RocksDB。
        // 这样即使节点崩溃重启，DAG 结构也不会丢失。
        let bytes = bincode::serialize(header).expect("Failed to serialize header");
        self.store.write(header.id.to_vec(), bytes).await;

        // ==================== 步骤 5：决定是否投票 ====================
        // 共识意义：投票是 BFT 共识的核心动作，但必须防止"重复投票"。
        //
        // 防御机制：last_voted 记录了"每一轮我们已经为哪些作者投过票"
        // - 同一个作者在同一轮可能发多个 Header（恶意或网络重传）
        // - 我们只为每个作者在每一轮投一次票
        // - 这防止了"双重投票攻击"：恶意节点利用重复投票突破 quorum 阈值
        //
        // insert() 返回 true → 第一次见到该作者在该轮的 Header，可以投票
        // insert() 返回 false → 已经投过票了，忽略
        if self
            .last_voted
            .entry(header.round)
            .or_insert_with(HashSet::new)
            .insert(header.author)
        {
            // 创建 Vote：对 Header 的 digest 签名，表示"我认可这个 Header"
            let vote = Vote::new(header, &self.name, &mut self.signature_service).await;
            debug!("Created {:?}", vote);

            // 特殊情况：如果这是我们自己的 Header（自投票）
            // 直接调用 process_vote，无需网络发送
            if vote.origin == self.name {
                self.process_vote(vote)
                    .await
                    .expect("Failed to process our own vote");
            } else {
                // 正常情况：发送 Vote 给 Header 作者
                // 使用 ReliableSender 确保投票不会丢失（关键消息）
                let address = self
                    .committee
                    .primary(&header.author)
                    .expect("Author of valid header is not in the committee")
                    .primary_to_primary;
                let bytes = bincode::serialize(&PrimaryMessage::Vote(vote))
                    .expect("Failed to serialize our own vote");
                let handler = self.network.send(address, Bytes::from(bytes)).await;

                // 保存 cancel handler：如果这一轮被 GC 了，可以取消重传
                self.cancel_handlers
                    .entry(header.round)
                    .or_insert_with(Vec::new)
                    .push(handler);
            }
        }
        Ok(())
    }

    /// 阶段 2：处理 Vote（投票收集阶段）→ 形成 Certificate
    ///
    /// 这是共识准备的第二步：收集他人对我们提议的投票，达到多数决。
    ///
    /// 核心职责：
    /// 1. 聚合投票：累计来自不同节点的 Vote，跟踪 stake 权重总和
    /// 2. 触发 quorum：当权重 >= 2f+1 时，达成多数决，形成 Certificate
    /// 3. 广播确认：立即将 Certificate 广播给所有节点，确保 DAG 一致性
    /// 4. 本地推进：处理新形成的 Certificate，推进自己的 DAG 视图
    ///
    /// 这是 BFT 多数决机制的核心实现：从"个体认可"到"集体确认"的关键转变。
    #[async_recursion]
    async fn process_vote(&mut self, vote: Vote) -> DagResult<()> {
        debug!("Processing {:?}", vote);

        // ==================== 步骤 1：聚合投票 → 检测 quorum ====================
        // 共识意义：这是 BFT 多数决机制的核心逻辑。
        //
        // VotesAggregator::append 的工作流程：
        // 1. 验证 Vote 签名：确保投票来自声称的节点
        // 2. 验证 Vote 针对的 Header：必须是 current_header（我们当前提议的 Header）
        // 3. 检查重复投票：同一个节点不能对同一个 Header 投两次票
        // 4. 累加 stake 权重：将投票者的权重加到总权重中
        // 5. 检查 quorum：如果总权重 >= committee.quorum_threshold()（2f+1）
        //    → 组装 Certificate：将 current_header + 所有 votes + signatures 打包
        //
        // 为什么是 2f+1？（f = 最多容忍的 Byzantine 节点数）
        // - 假设总共有 3f+1 个节点（总权重归一化）
        // - 最多 f 个节点是恶意的
        // - 2f+1 个投票意味着：至少有 (2f+1) - f = f+1 个诚实节点投了赞成票
        // - 任何两个 quorum 集合至少有 1 个诚实节点重叠
        // - 这确保了 BFT 安全性：恶意节点无法同时为两个冲突的 Header 形成 Certificate
        //
        // 返回值：
        // - Some(certificate)：达到 quorum，成功组装了 Certificate
        // - None：还未达到 quorum，继续等待更多投票
        if let Some(certificate) =
            self.votes_aggregator
                .append(vote, &self.committee, &self.current_header)?
        {
            debug!("Assembled {:?}", certificate);

            // ==================== 步骤 2：广播 Certificate ====================
            // 共识意义：一旦我们形成了 Certificate，必须立即告知所有节点。
            //
            // 为什么必须广播？
            // 1. DAG 一致性：所有节点必须看到相同的 DAG 结构
            //    - 如果只有我们知道这个 Certificate，其他节点的 DAG 会缺失节点
            //    - 这会导致后续轮次的 Header 引用不一致
            //
            // 2. 轮次推进：其他节点需要这个 Certificate 来推进轮次
            //    - 每个节点在收集到 quorum 个 Certificate 后才能进入下一轮
            //    - 如果我们不广播，其他节点会卡住等待
            //
            // 3. 共识输入：Consensus 模块需要完整的 Certificate 流来排序
            //    - 缺失的 Certificate 会导致排序不完整
            //
            // 为什么使用 ReliableSender？
            // - Certificate 是关键消息，丢失会阻塞整个系统
            // - ReliableSender 会自动重传，直到收到 ACK
            // - 保存 cancel_handlers：如果这一轮被 GC，可以取消重传避免浪费
            let addresses = self
                .committee
                .others_primaries(&self.name)
                .iter()
                .map(|(_, x)| x.primary_to_primary)
                .collect();
            let bytes = bincode::serialize(&PrimaryMessage::Certificate(certificate.clone()))
                .expect("Failed to serialize our own certificate");
            let handlers = self.network.broadcast(addresses, Bytes::from(bytes)).await;
            self.cancel_handlers
                .entry(certificate.round())
                .or_insert_with(Vec::new)
                .extend(handlers);

            // ==================== 步骤 3：本地处理 Certificate ====================
            // 共识意义：我们自己也需要处理这个新形成的 Certificate。
            //
            // 为什么需要处理？
            // 1. 更新 CertificatesAggregator：
            //    - 将这个 Certificate 加入到"本轮收集的证书"集合中
            //    - 如果我们收集到了 quorum 个本轮证书 → 通知 Proposer 进入下一轮
            //
            // 2. 输入 Consensus：
            //    - 将 Certificate 发送给 Consensus 模块进行排序
            //    - Consensus 会按照 DAG 拓扑顺序输出交易
            //
            // 3. 触发依赖处理：
            //    - 其他节点可能在等待这个 Certificate（通过 CertificateWaiter）
            //    - 一旦存储，会触发 store.notify_read，解锁等待的处理
            //
            // 递归性：process_certificate 可能触发新的 Header 处理（通过 HeaderWaiter）
            // 这是 DAG 构建的自然展开过程：Certificate 到位 → 解锁依赖 → 继续扩展 DAG
            self.process_certificate(certificate)
                .await
                .expect("Failed to process valid certificate");
        }
        Ok(())
    }

    /// 阶段 3：处理 Certificate（确认阶段）→ 推进轮次 + 输入共识
    ///
    /// 这是共识准备的最后一步：处理已确认的 Certificate，为共识排序做好输入准备。
    ///
    /// 核心职责：
    /// 1. 确保内嵌 Header 已处理（数据完整性）
    /// 2. 验证 DAG 祖先链完整（因果一致性）
    /// 3. 持久化 Certificate（可靠性）
    /// 4. 聚合本轮 Certificate，达到 quorum 后推进轮次（活性）
    /// 5. 输出到 Consensus 模块进行最终排序（共识完成）
    ///
    /// 这个函数是 DAG 构建的终点，也是共识排序的起点。
    #[async_recursion]
    async fn process_certificate(&mut self, certificate: Certificate) -> DagResult<()> {
        debug!("Processing {:?}", certificate);

        // ==================== 步骤 1：处理内嵌的 Header ====================
        // 共识意义：Certificate 是"Header + 2f+1 个投票签名"的组合体。
        //
        // 为什么需要处理内嵌的 Header？
        // - Certificate 可能通过多种途径到达：
        //   1. 我们自己组装的（process_vote 中）
        //   2. 其他节点广播给我们的
        //   3. 同步机制拉取的（CertificateWaiter）
        //
        // - 如果是途径 2 或 3，我们可能还没处理过这个 Header
        // - 但我们不需要完整验证（父证书、payload）：
        //   既然这个 Header 已经获得了 2f+1 投票 → 至少 f+1 个诚实节点验证过它
        //   → 可以信任它的合法性（BFT 安全性保证）
        //
        // 检查逻辑：
        // - 查看 processing 集合，如果已经包含这个 Header → 说明我们投过票，一定处理过了
        // - 否则 → 调用 process_header 处理（但可能仍然缺失依赖，挂起等待同步）
        if !self
            .processing
            .get(&certificate.header.round)
            .map_or_else(|| false, |x| x.contains(&certificate.header.id))
        {
            // 注意：process_header 仍可能因为缺失父证书或 payload 而挂起
            // 但这没关系，因为 Certificate 本身会被 CertificateWaiter 重新触发
            self.process_header(&certificate.header).await?;
        }

        // ==================== 步骤 2：验证祖先 Certificate 链 ====================
        // 共识意义：这是 DAG 因果一致性的核心保障。
        //
        // Synchronizer::deliver_certificate 的验证逻辑：
        // 1. 递归检查：这个 Certificate 的所有祖先（父证书、祖父证书、……）是否都在本地 store
        // 2. 如果缺失任何祖先 → 返回 false，并触发后台同步：
        //    - 向其他节点发送 CertificatesRequest（带上缺失的 digest 列表）
        //    - 等待祖先 Certificate 到达
        //    - 通过 CertificateWaiter 将当前 Certificate 回环重新处理
        //
        // 为什么必须等待祖先齐全？
        // - DAG 的本质是"偏序关系"：后继节点必须等待前驱节点
        // - 如果祖先缺失，我们无法确定这个 Certificate 在 DAG 中的正确位置
        // - Consensus 排序时需要完整的 DAG 拓扑，缺失节点会导致排序错误
        //
        // 这是"异步容错"的另一个体现：缺失祖先时挂起，同步完成后恢复。
        if !self.synchronizer.deliver_certificate(&certificate).await? {
            debug!(
                "Processing of {:?} suspended: missing ancestors",
                certificate
            );
            return Ok(()); // 挂起，等待 CertificateWaiter 回环
        }

        // ==================== 步骤 3：持久化 Certificate ====================
        // 所有依赖都齐全了，将 Certificate 存储到 RocksDB。
        //
        // 共识意义：
        // 1. 可靠性：节点崩溃重启后，DAG 结构不会丢失
        // 2. 同步支持：其他节点请求 Certificate 时，我们可以提供
        // 3. 触发通知：store.write 会触发 store.notify_read
        //    → 解锁其他等待这个 Certificate 的处理（CertificateWaiter）
        let bytes = bincode::serialize(&certificate).expect("Failed to serialize certificate");
        self.store.write(certificate.digest().to_vec(), bytes).await;

        // ==================== 步骤 4：聚合 Certificate → 推进轮次 ====================
        // 共识意义：这是 DAG "活性"（liveness）的关键机制。
        //
        // CertificatesAggregator::append 的工作流程：
        // 1. 将 Certificate 加入到"本轮收集的证书"集合中
        // 2. 累加这些证书作者的 stake 权重
        // 3. 检查：权重是否 >= quorum_threshold()（2f+1）
        // 4. 如果达到 quorum → 返回 Some(parents)：
        //    - parents：本轮所有 Certificate 的 digest 列表
        //    - 这些 digest 将作为下一轮 Header 的"父节点引用"
        //
        // 为什么需要 quorum？
        // - 每一轮必须等待"大多数节点完成上一轮"才能推进
        // - 这确保 DAG 的"层次分明"：所有节点基本同步地推进轮次
        // - 如果没有 quorum 限制：
        //   * 快节点可能冲到很前面
        //   * 慢节点落后很多
        //   * DAG 会变得稀疏、不连通，影响共识效率
        //
        // 通知 Proposer：
        // - 一旦达到 quorum，立即通知 Proposer："可以创建新 Header 了"
        // - Proposer 会用 parents 作为父节点引用，创建新一轮的 Header
        // - 这触发了新的"提议-投票-确认"循环，DAG 不断向前生长
        if let Some(parents) = self
            .certificates_aggregators
            .entry(certificate.round())
            .or_insert_with(|| Box::new(CertificatesAggregator::new()))
            .append(certificate.clone(), &self.committee)?
        {
            // 通知 Proposer：进入下一轮，这是 DAG 活性的引擎
            self.tx_proposer
                .send((parents, certificate.round()))
                .await
                .expect("Failed to send certificate");
        }

        // ==================== 步骤 5：输出到 Consensus 层 ====================
        // 共识意义：这是 Narwhal（mempool）与 Tusk/Bullshark（共识）的接口。
        //
        // 职责划分：
        // - Narwhal（Core + Primary + Worker）：
        //   * 负责"共识前的准备"：数据可用性、DAG 构建、BFT 验证
        //   * 输出：一个可靠的、因果完整的 Certificate 流
        //
        // - Tusk/Bullshark（Consensus）：
        //   * 负责"最终排序"：从 DAG 中提取线性顺序（total order）
        //   * 使用 DAG 的拓扑结构和随机信标，决定哪些 Certificate 先提交、哪些后提交
        //   * 输出：有序的交易流（可以发送给执行层）
        //
        // 为什么是这样的分层设计？
        // 1. 解耦：数据传播（Narwhal）与排序决策（Consensus）分离
        // 2. 高吞吐：Narwhal 可以并行构建 DAG，不受排序算法限制
        // 3. 灵活性：可以更换不同的排序算法（Tusk/Bullshark/其他），无需改动 Narwhal
        //
        // 容错处理：
        // - 如果 Consensus 通道满了或崩溃 → 记录 warn 日志
        // - 但不会阻塞 DAG 构建：Core 继续处理其他消息
        // - 这确保 Narwhal 层的"活性"不受 Consensus 层的影响
        let id = certificate.header.id.clone();
        if let Err(e) = self.tx_consensus.send(certificate).await {
            warn!(
                "Failed to deliver certificate {} to the consensus: {}",
                id, e
            );
        }
        Ok(())
    }

    fn sanitize_header(&mut self, header: &Header) -> DagResult<()> {
        ensure!(
            self.gc_round <= header.round,
            DagError::TooOld(header.id.clone(), header.round)
        );

        // Verify the header's signature.
        header.verify(&self.committee)?;

        // TODO [issue #3]: Prevent bad nodes from sending junk headers with high round numbers.

        Ok(())
    }

    fn sanitize_vote(&mut self, vote: &Vote) -> DagResult<()> {
        ensure!(
            self.current_header.round <= vote.round,
            DagError::TooOld(vote.digest(), vote.round)
        );

        // Ensure we receive a vote on the expected header.
        ensure!(
            vote.id == self.current_header.id
                && vote.origin == self.current_header.author
                && vote.round == self.current_header.round,
            DagError::UnexpectedVote(vote.id.clone())
        );

        // Verify the vote.
        vote.verify(&self.committee).map_err(DagError::from)
    }

    fn sanitize_certificate(&mut self, certificate: &Certificate) -> DagResult<()> {
        ensure!(
            self.gc_round <= certificate.round(),
            DagError::TooOld(certificate.digest(), certificate.round())
        );

        // Verify the certificate (and the embedded header).
        certificate.verify(&self.committee).map_err(DagError::from)
    }

    /// 主循环：监听四条输入通道，统一处理所有消息
    ///
    /// 输入来源：
    /// 1. rx_primaries: 来自其他 Primary 的网络消息（Header/Vote/Certificate/同步请求）
    /// 2. rx_header_waiter: HeaderWaiter 回环（依赖同步完成后重新处理的 Header）
    /// 3. rx_certificate_waiter: CertificateWaiter 回环（祖先到齐后重新处理的 Certificate）
    /// 4. rx_proposer: Proposer 创建的新 Header（本地发起的提议）
    ///
    /// 处理流程：
    /// - 接收消息 → sanitize 验证 → process 处理 → 输出（投票/Certificate/通知）
    /// - 如果依赖缺失 → 挂起 → Waiter 等待同步 → 回环重新处理
    /// - 定期垃圾回收：清理已提交轮次的旧状态
    ///
    /// 这个循环是 DAG 构建和共识准备的心跳，每次迭代都在推进系统向前发展。
    pub async fn run(&mut self) {
        loop {
            let result = tokio::select! {
                // We receive here messages from other primaries.
                // 1. 来自其他 Primary 的消息（网络）
                Some(message) = self.rx_primaries.recv() => {
                    match message {
                        PrimaryMessage::Header(header) => {
                            match self.sanitize_header(&header) {
                                Ok(()) => self.process_header(&header).await,
                                error => error
                            }

                        },
                        PrimaryMessage::Vote(vote) => {
                            match self.sanitize_vote(&vote) {
                                Ok(()) => self.process_vote(vote).await,
                                error => error
                            }
                        },
                        PrimaryMessage::Certificate(certificate) => {
                            match self.sanitize_certificate(&certificate) {
                                Ok(()) =>  self.process_certificate(certificate).await,
                                error => error
                            }
                        },
                        _ => panic!("Unexpected core message")
                    }
                },

                // We receive here loopback headers from the `HeaderWaiter`. Those are headers for which we interrupted
                // execution (we were missing some of their dependencies) and we are now ready to resume processing.
                // 2. 来自 HeaderWaiter 的回环（同步完成后重新处理）
                Some(header) = self.rx_header_waiter.recv() => self.process_header(&header).await,

                // We receive here loopback certificates from the `CertificateWaiter`. Those are certificates for which
                // we interrupted execution (we were missing some of their ancestors) and we are now ready to resume
                // processing.
                // 3. 来自 CertificateWaiter 的回环（祖先到齐后重新处理）
                Some(certificate) = self.rx_certificate_waiter.recv() => self.process_certificate(certificate).await,

                // We also receive here our new headers created by the `Proposer`.
                // 4. 来自 Proposer 的新 Header（本地创建）
                Some(header) = self.rx_proposer.recv() => self.process_own_header(header).await,
            };
            match result {
                Ok(()) => (),
                Err(DagError::StoreError(e)) => {
                    error!("{}", e);
                    panic!("Storage failure: killing node.");
                }
                Err(e @ DagError::TooOld(..)) => debug!("{}", e),
                Err(e) => warn!("{}", e),
            }

            // Cleanup internal state.
            let round = self.consensus_round.load(Ordering::Relaxed);
            if round > self.gc_depth {
                let gc_round = round - self.gc_depth;
                self.last_voted.retain(|k, _| k >= &gc_round);
                self.processing.retain(|k, _| k >= &gc_round);
                self.certificates_aggregators.retain(|k, _| k >= &gc_round);
                self.cancel_handlers.retain(|k, _| k >= &gc_round);
                self.gc_round = gc_round;
            }
        }
    }
}
