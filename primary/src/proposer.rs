// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::{Certificate, Header};
use crate::primary::Round;
use config::{Committee, WorkerId};
use crypto::Hash as _;
use crypto::{Digest, PublicKey, SignatureService};
use log::debug;
#[cfg(feature = "benchmark")]
use log::info;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/proposer_tests.rs"]
pub mod proposer_tests;

/// Proposer：DAG 的"提议引擎"，驱动系统不断生长新节点。
///
/// 从共识的角度理解 Proposer：
///
/// **共识流程中的位置**：
/// 1. Core（准备）：接收来自网络的 Header/Vote/Certificate，构建 DAG 左半部分
/// 2. Proposer（驱动）← 我们在这里
///    - 监听 Core 的信号："本轮 quorum 个 Certificate 已到位"
///    - 汇聚来自 Worker 的 batch 数据
///    - 创建新 Header，驱动 DAG 继续生长
/// 3. Consensus（排序）：从完整的 DAG 中提取交易序列
///
/// **核心职责**：
/// 1. **轮次推进**：当 Core 汇聚到 quorum 个证书 → 进入下一轮
/// 2. **批数据汇聚**：从 Worker 接收 batch digest，缓冲到达规模
/// 3. **头部创建**：当数据量或延迟满足条件 → 创建新 Header 并广播
/// 4. **吞吐控制**：通过 max_header_delay 平衡吞吐和延迟
///
/// **设计哲学**：
/// - "推拉结合"：推（来自 Core 的轮次信号）+ 拉（来自 Worker 的数据）
/// - 异步驱动：不同 Worker 的数据到达时间可以不同，Proposer 缓冲等待
/// - 活性保证：即使没有新数据，max_header_delay 也确保定期推进
pub struct Proposer {
    // ==================== 身份与基础设施 ====================
    /// 本节点的公钥（用于签名 Header）。
    name: PublicKey,
    /// 签名服务（对新创建的 Header 签名）。
    signature_service: SignatureService,
    /// Header payload 的目标大小（单位：字节）。
    /// 当 batch 数据积累到这个大小时，可以创建新 Header。
    header_size: usize,
    /// 最大等待延迟（单位：毫秒）。即使没有数据，超过这个时间也要创建 Header。
    /// 这确保了\"活性\"（liveness）：即使网络不活跃，系统也能定期推进。
    max_header_delay: u64,

    // ==================== 输入输出通道 ====================
    /// 来自 Core 的信号通道：(parents_digests, round)
    /// - Core 汇聚到 quorum 个本轮 Certificate 时，发送此消息
    /// - 这是 DAG 活性的关键信号：告诉 Proposer \"可以进入下一轮了\"。
    rx_core: Receiver<(Vec<Digest>, Round)>,
    /// 来自 Worker 的数据流：(batch_digest, worker_id)
    /// - Worker 将新的 batch 数据存储到本地后，发送此消息
    /// - 这是 mempool 层的输入：系统中未确认的交易。
    rx_workers: Receiver<(Digest, WorkerId)>,
    /// 输出通道，将新创建的 Header 发送给 Core
    /// Core 会广播 Header、收集投票、组装 Certificate、推进 DAG
    tx_core: Sender<Header>,

    // ==================== 状态：轮次、父节点、缓冲数据 ====================
    /// 当前 DAG 的轮次。每当 Core 发来 quorum 信号时，会推进到 round + 1。
    round: Round,
    /// 缓冲当前轮次可用的父节点（Certificate digest）。
    /// - 初始化为 genesis Certificate（系统启动时的虚拟 Certificate）
    /// - 当 Core 发来新信号时，更新为新一轮的 parents
    last_parents: Vec<Digest>,
    /// 缓冲来自 Worker 的 batch digest（还未被放入任何 Header）。
    /// - 当 Header 创建时，将这些 digest 作为 payload，然后清空此缓冲
    /// - 这是\"mempool\"的核心：未确认的交易数据
    digests: Vec<(Digest, WorkerId)>,
    /// 当前缓冲的 payload 大小（字节）。当达到 header_size 时，触发创建新 Header。
    /// 这是吞吐控制的关键指标。
    payload_size: usize,
}

impl Proposer {
    /// 启动 Proposer 任务（独立的 tokio 任务）
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: &Committee,
        signature_service: SignatureService,
        header_size: usize,
        max_header_delay: u64,
        rx_core: Receiver<(Vec<Digest>, Round)>,
        rx_workers: Receiver<(Digest, WorkerId)>,
        tx_core: Sender<Header>,
    ) {
        let genesis = Certificate::genesis(committee)
            .iter()
            .map(|x| x.digest())
            .collect();

        tokio::spawn(async move {
            Self {
                name,
                signature_service,
                header_size,
                max_header_delay,
                rx_core,
                rx_workers,
                tx_core,
                round: 1,
                last_parents: genesis,
                digests: Vec::with_capacity(2 * header_size),
                payload_size: 0,
            }
            .run()
            .await;
        });
    }

    /// 创建并发送新 Header
    ///
    /// 共识意义：这是 DAG 生长的原始动力，每个 Header 代表一个新的\"提议\"。
    ///
    /// 步骤 1：组装 Header
    /// - 输入 1：当前轮次号（round）
    /// - 输入 2：缓冲的 batch digest（self.digests） 作为 Header 的 payload
    /// - 输入 3：缓冲的父节点（self.last_parents） 作为 Header 的 parent references
    /// - 输入 4：本节点的公钥和签名密钥 对 Header 签名
    async fn make_header(&mut self) {
        // ==================== 组装 Header ====================
        let header = Header::new(
            self.name,
            self.round,
            self.digests.drain(..).collect(),
            self.last_parents.drain(..).collect(),
            &mut self.signature_service,
        )
        .await;
        debug!("Created {:?}", header);

        #[cfg(feature = "benchmark")]
        for digest in header.payload.keys() {
            // NOTE: This log entry is used to compute performance.
            info!("Created {} -> {:?}", header, digest);
        }

        // ==================== 发送到 Core ====================
        // 这个 Header 会进入 Core 的处理流程：
        // 1. 广播给所有 Primary
        // 2. 收集投票 → 形成 Certificate
        // 3. 投入 DAG
        self.tx_core
            .send(header)
            .await
            .expect("Failed to send header");
    }

    /// 主循环：驱动 DAG 生长的\"心跳\"
    ///
    /// 共识意义：Proposer 是 DAG 活性的引擎。
    /// - 如果没有 Proposer 定期创建 Header → DAG 会停止生长
    /// - 没有 DAG 节点 → Consensus 无法排序
    /// - 系统活性（liveness）会丧失
    ///
    /// 工作流程（\"推拉模型\"）：
    /// 1. 拉（从 Core）：监听轮次推进信号 → 更新 last_parents
    /// 2. 推（从 Worker）：接收 batch 数据 → 缓冲 digests
    /// 3. 创建（本地）：当条件满足 → 创建新 Header 并发送
    /// 4. 循环往复：新 Header → Core 处理 → 形成 Certificate → Core 发来新信号
    pub async fn run(&mut self) {
        debug!("Dag starting at round {}", self.round);

        let timer = sleep(Duration::from_millis(self.max_header_delay));
        tokio::pin!(timer);

        loop {
            // ==================== 检查是否可以创建新 Header ====================
            let enough_parents = !self.last_parents.is_empty();
            let enough_digests = self.payload_size >= self.header_size;
            let timer_expired = timer.is_elapsed();
            if (timer_expired || enough_digests) && enough_parents {
                // ==================== 创建并发送新 Header ====================
                self.make_header().await;
                self.payload_size = 0;

                // ==================== 重启 Timer ====================
                let deadline = Instant::now() + Duration::from_millis(self.max_header_delay);
                timer.as_mut().reset(deadline);
            }

            tokio::select! {
                // ==================== 事件 1：来自 Core 的轮次推进信号 ====================
                Some((parents, round)) = self.rx_core.recv() => {
                    if round < self.round {
                        continue;
                    }

                    // Advance to the next round.
                    // 当前 Proposer 的轮次 = Core 报告的轮次 + 1
                    self.round = round + 1;
                    debug!("Dag moved to round {}", self.round);

                    // Signal that we have enough parent certificates to propose a new header.
                    // 下次创建 Header 时，会将这些 parents 作为\"父节点引用\"放入新 Header
                    self.last_parents = parents;
                }
                // ==================== 事件 2：来自 Worker 的 Batch 数据 ====================
                Some((digest, worker_id)) = self.rx_workers.recv() => {
                    // ==================== 缓冲 Batch Digest ====================
                    self.payload_size += digest.size();
                    self.digests.push((digest, worker_id));
                }
                // ==================== 事件 3：延迟计时器 ====================
                () = &mut timer => {
                    // 计时器到期，继续循环
                }
            }
        }
    }
}
