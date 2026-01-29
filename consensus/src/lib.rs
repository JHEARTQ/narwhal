// Copyright(C) Facebook, Inc. and its affiliates.
use config::{Committee, Stake};
use crypto::Hash as _;
use crypto::{Digest, PublicKey};
use log::{debug, info, log_enabled, warn};
use primary::{Certificate, Round};
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/consensus_tests.rs"]
pub mod consensus_tests;

/// DAG 在内存中的表示
/// 
/// 结构：HashMap<Round, HashMap<PublicKey, (Digest, Certificate)>>
/// - 外层 HashMap：按轮次（Round）组织
/// - 内层 HashMap：每个轮次中，按节点公钥（PublicKey）组织
/// - 值：(Certificate 的 Digest, Certificate 本身)
/// 
/// 示例：
/// ```
/// Round 3: { NodeA -> (digest_A3, cert_A3), NodeB -> (digest_B3, cert_B3) }
/// Round 2: { NodeA -> (digest_A2, cert_A2), NodeC -> (digest_C2, cert_C2) }
/// Round 1: { NodeA -> (digest_A1, cert_A1), NodeB -> (digest_B1, cert_B1) }
/// ```
type Dag = HashMap<Round, HashMap<PublicKey, (Digest, Certificate)>>;

/// 共识层的状态（需要持久化以支持崩溃恢复）
/// 
/// State 是 Tusk 共识算法的核心数据结构，维护：
/// 1. **DAG 的内存视图**：所有尚未被垃圾回收的 Certificate
/// 2. **提交进度追踪**：每个节点已提交到哪一轮，防止重复提交
/// 3. **垃圾回收边界**：决定哪些旧数据可以被清理
/// 
/// 共识过程：
/// - Primary 不断发送新 Certificate 给 Consensus
/// - Consensus 将 Certificate 加入 DAG
/// - 当满足条件时（leader 有足够支持），提交一批 Certificate
/// - update() 更新提交进度，清理旧数据
struct State {
    /// 全局最新的已提交轮次
    /// 
    /// 定义：所有节点中，最大的 last_committed[node] 值
    /// 
    /// 作用：
    /// 1. **垃圾回收边界**：轮次 < last_committed_round - gc_depth 的数据可以清理
    /// 2. **Leader 选举起点**：只考虑 > last_committed_round 的轮次
    /// 3. **进度指示器**：表示全局共识已推进到哪一轮
    /// 
    /// 为什么是"最大值"？
    /// - 不同节点的提交进度可能不同（网络延迟、消息乱序）
    /// - 取最大值确保：只有当所有节点都推进后，才能安全清理旧数据
    last_committed_round: Round,
    
    /// 每个节点（authority）的最新已提交轮次
    /// 
    /// 键：节点公钥（PublicKey）
    /// 值：该节点最后一次被提交的 Certificate 的轮次
    /// 
    /// 作用：
    /// 1. **防止重复提交**：order_dag() 中跳过已提交的 Certificate
    /// 2. **追踪各节点进度**：哪些节点活跃，哪些节点落后
    /// 3. **清理 DAG**：可以安全删除 <= last_committed[node] 轮次的数据
    /// 
    /// 示例：
    /// ```
    /// last_committed = {
    ///   NodeA: 10,  // NodeA 的第 10 轮 Certificate 已提交
    ///   NodeB: 9,   // NodeB 的第 9 轮 Certificate 已提交
    ///   NodeC: 10,  // NodeC 的第 10 轮 Certificate 已提交
    /// }
    /// → last_committed_round = max(10, 9, 10) = 10
    /// ```
    last_committed: HashMap<PublicKey, Round>,
    
    /// DAG 的内存副本（仅保留未提交的 Certificate）
    /// 
    /// 存储规则：
    /// 1. 新 Certificate 到达 → 加入对应的 round 和 origin
    /// 2. Certificate 被提交 → 可能被清理（取决于 gc_depth）
    /// 3. 轮次过旧 → 被垃圾回收删除
    /// 
    /// 为什么需要保留已提交的数据一段时间？
    /// - order_dag() 需要遍历父证书来展开子 DAG
    /// - 如果立即删除，可能导致后续 leader 的依赖缺失
    /// - gc_depth 控制保留的深度（通常 10-50 轮）
    dag: Dag,
}

impl State {
    /// 创建初始状态（从 Genesis 开始）
    /// 
    /// # 参数
    /// - genesis: 创世区块（每个节点一个虚拟的 Round 0 Certificate）
    /// 
    /// # Genesis 的作用
    /// Genesis Certificate 是共识的"起点"，解决"第一轮 Header 引用谁作为父节点"的问题：
    /// - 所有节点的 Round 1 Header 都引用 Genesis 作为父证书
    /// - Genesis 不需要真实的交易数据，只是占位符
    /// - 每个节点有自己的 Genesis Certificate（origin 不同）
    /// 
    /// # 初始化逻辑
    /// 1. 将 genesis 转换为 HashMap<PublicKey, (Digest, Certificate)>
    /// 2. last_committed 初始化为 {NodeA: 0, NodeB: 0, ...}（所有节点都在轮次 0）
    /// 3. dag 初始化为 {0: genesis_map}（只有轮次 0 的数据）
    /// 4. last_committed_round = 0（全局进度为 0）
    fn new(genesis: Vec<Certificate>) -> Self {
        // 将 genesis Vec 转换为 HashMap，键是节点公钥
        let genesis = genesis
            .into_iter()
            .map(|x| (x.origin(), (x.digest(), x)))
            .collect::<HashMap<_, _>>();

        Self {
            last_committed_round: 0,
            // 初始化每个节点的提交进度为轮次 0
            last_committed: genesis.iter().map(|(x, (_, y))| (*x, y.round())).collect(),
            // DAG 只包含轮次 0（genesis）
            dag: [(0, genesis)].iter().cloned().collect(),
        }
    }

    /// 更新状态并清理旧数据（垃圾回收）
    /// 
    /// # 参数
    /// - certificate: 刚刚被提交（commit）的 Certificate
    /// - gc_depth: 垃圾回收深度（保留多少轮未被清理）
    /// 
    /// # 调用时机
    /// 每次提交一个 Certificate 后立即调用，确保状态同步更新。
    /// 
    /// # 三个核心操作
    /// 
    /// ## 1. 更新节点的提交进度
    /// ```rust
    /// self.last_committed.entry(certificate.origin())
    ///     .and_modify(|r| *r = max(*r, certificate.round()))
    ///     .or_insert_with(|| certificate.round());
    /// ```
    /// 
    /// - 如果节点已存在 → 更新为 max(旧值, 新轮次)
    /// - 如果节点不存在 → 插入新轮次（理论上不会发生，因为 genesis 已初始化所有节点）
    /// 
    /// **为什么用 max？**
    /// - 提交顺序可能乱序：先提交轮次 10，后提交轮次 8
    /// - max 确保记录的是真正的"最新"轮次
    /// 
    /// ## 2. 更新全局最新提交轮次
    /// ```rust
    /// let last_committed_round = *self.last_committed.values().max().unwrap();
    /// self.last_committed_round = last_committed_round;
    /// ```
    /// 
    /// - 取所有节点的 last_committed 的最大值
    /// - 这是垃圾回收的"水位线"：只有当所有节点都推进后，才能安全清理
    /// 
    /// **示例**：
    /// ```
    /// last_committed = { A: 10, B: 12, C: 11 }
    /// → last_committed_round = 12
    /// → 可以清理 < 12 - gc_depth 的轮次
    /// ```
    /// 
    /// ## 3. 垃圾回收 DAG（两个维度）
    /// ```rust
    /// for (name, round) in &self.last_committed {
    ///     self.dag.retain(|r, authorities| {
    ///         authorities.retain(|n, _| n != name || r >= round);
    ///         !authorities.is_empty() && r + gc_depth >= last_committed_round
    ///     });
    /// }
    /// ```
    /// 
    /// ### 维度 1：按节点清理（内层 retain）
    /// `authorities.retain(|n, _| n != name || r >= round)`
    /// 
    /// - 删除条件：节点 n == name **且** 轮次 r < round
    /// - 保留条件：节点 n != name **或** 轮次 r >= round
    /// 
    /// **解读**：
    /// - 对于每个节点 name，删除它在 < last_committed[name] 轮次的所有 Certificate
    /// - 其他节点的 Certificate 不受影响
    /// 
    /// **示例**：
    /// ```
    /// last_committed[NodeA] = 10
    /// dag[8] 包含 NodeA、NodeB、NodeC
    /// → 删除 dag[8][NodeA]（因为 8 < 10）
    /// → 保留 dag[8][NodeB] 和 dag[8][NodeC]
    /// ```
    /// 
    /// ### 维度 2：按轮次清理（外层 retain）
    /// `!authorities.is_empty() && r + gc_depth >= last_committed_round`
    /// 
    /// - 删除条件 1：authorities 为空（该轮次所有节点的 Certificate 都被清理了）
    /// - 删除条件 2：轮次过旧（r + gc_depth < last_committed_round）
    /// 
    /// **示例**：
    /// ```
    /// last_committed_round = 50
    /// gc_depth = 10
    /// 
    /// Round 41: 41 + 10 = 51 >= 50 → 保留 ✅
    /// Round 40: 40 + 10 = 50 >= 50 → 保留 ✅
    /// Round 39: 39 + 10 = 49 < 50  → 删除 ❌
    /// ```
    /// 
    /// # 为什么需要垃圾回收？
    /// 1. **内存限制**：DAG 会无限增长，必须定期清理
    /// 2. **性能优化**：遍历 DAG 时跳过不必要的旧数据
    /// 3. **防止重复提交**：已提交的 Certificate 不应再被处理
    /// 
    /// # gc_depth 的权衡
    /// - **太小**：可能导致依赖缺失（order_dag 时找不到父证书）
    /// - **太大**：占用过多内存，降低性能
    /// - **典型值**：10-50 轮（取决于网络延迟和节点数量）
    fn update(&mut self, certificate: &Certificate, gc_depth: Round) {
        // 步骤 1：更新该节点的最新提交轮次
        self.last_committed
            .entry(certificate.origin())
            .and_modify(|r| *r = max(*r, certificate.round()))
            .or_insert_with(|| certificate.round());

        // 步骤 2：计算全局最新提交轮次（所有节点的最大值）
        let last_committed_round = *self.last_committed.values().max().unwrap();
        self.last_committed_round = last_committed_round;

        // 步骤 3：垃圾回收 DAG（精细化清理策略）
        //
        // ==================== 为什么需要这样的垃圾回收？====================
        //
        // 问题场景：不同节点的提交进度不一致
        //
        // 假设系统状态如下：
        // ```
        // last_committed = {
        //   NodeA: 10,  // NodeA 已提交到轮次 10
        //   NodeB: 7,   // NodeB 只提交到轮次 7
        //   NodeC: 9,   // NodeC 提交到轮次 9
        // }
        // last_committed_round = 10（所有节点的最大值）
        // gc_depth = 5
        //
        // DAG 内容：
        // Round 8: { NodeA: cert_A8, NodeB: cert_B8, NodeC: cert_C8 }
        // Round 7: { NodeA: cert_A7, NodeB: cert_B7, NodeC: cert_C7 }
        // Round 6: { NodeA: cert_A6, NodeB: cert_B6, NodeC: cert_C6 }
        // ```
        //
        // ❌ 错误策略 1：按全局轮次删除
        // ```rust
        // // 直接删除 < last_committed_round 的所有轮次
        // self.dag.retain(|r, _| r >= &last_committed_round);
        // ```
        // 后果：
        // - 删除了 Round 6、7、8、9（都 < 10）
        // - 但 NodeB 在轮次 8、9 的 Certificate 还没提交！
        // - NodeC 在轮次 10 的 Certificate 还没提交！
        // - order_dag() 遍历父证书时会找不到数据 → 崩溃 ❌
        //
        // ❌ 错误策略 2：保守策略（保留所有未提交的）
        // ```rust
        // let min_committed = self.last_committed.values().min().unwrap();
        // self.dag.retain(|r, _| r >= min_committed);
        // ```
        // 后果：
        // - 必须保留 >= 7 的所有轮次（因为 NodeB 最慢）
        // - 即使 NodeA 在轮次 7、8、9 都已提交，也无法清理
        // - 内存占用过高，垃圾回收效率低 ❌
        //
        // ✅ 正确策略：精细化清理（当前实现）
        // 针对每个节点，只删除它已提交的 Certificate，其他节点不受影响。
        //
        // 执行过程（按节点逐个清理）：
        //
        // 1️⃣ 处理 NodeA（last_committed[NodeA] = 10）
        //    Round 8: 删除 NodeA，保留 NodeB、NodeC
        //    Round 7: 删除 NodeA，保留 NodeB、NodeC
        //    Round 6: 删除 NodeA，保留 NodeB、NodeC
        //    → 删除 NodeA 在 < 10 轮次的所有 Certificate
        //
        // 2️⃣ 处理 NodeB（last_committed[NodeB] = 7）
        //    Round 6: 删除 NodeB，保留 NodeC（NodeA 已在步骤 1 删除）
        //    → Round 7、8 的 NodeB 保留（因为 7 >= 7）
        //
        // 3️⃣ 处理 NodeC（last_committed[NodeC] = 9）
        //    Round 8: 删除 NodeC（NodeA 已删除，NodeB 保留）
        //    Round 7: 删除 NodeC（NodeA 已删除，NodeB 保留）
        //    Round 6: 删除 NodeC（NodeA、NodeB 已删除）
        //    → Round 6 现在为空，整个轮次被删除
        //
        // 最终 DAG 状态：
        // ```
        // Round 8: { NodeB: cert_B8 }  // 只保留 NodeB 未提交的
        // Round 7: { NodeB: cert_B7 }  // 只保留 NodeB 未提交的
        // ```
        //
        // ==================== 两层 retain 的含义 ====================
        //
        // 外层：for (name, round) in &self.last_committed
        // - 遍历每个节点及其已提交轮次
        // - 对 DAG 的每个轮次执行 retain（保留/删除判断）
        //
        // 内层 retain 1：authorities.retain(|n, _| n != name || r >= round)
        // - 针对当前处理的节点 name 和轮次 round
        // - 删除条件：n == name && r < round
        //   → 当前节点在该轮次的 Certificate 已提交，删除！
        // - 保留条件：n != name || r >= round
        //   → 其他节点的 Certificate，或当前节点未提交的，保留！
        //
        // 内层 retain 2：!authorities.is_empty() && r + gc_depth >= last_committed_round
        // - 检查 1：authorities 不为空
        //   → 如果该轮次所有节点的 Certificate 都被删除了，整个轮次删除
        // - 检查 2：轮次不能太旧
        //   → 即使有未提交的，如果超出 gc_depth 范围，也必须删除（防止内存泄漏）
        //
        // ==================== 为什么这样设计？====================
        //
        // 1. **精确性**：只删除已提交的，不误删未提交的
        // 2. **效率性**：尽可能释放内存，不保守浪费
        // 3. **安全性**：确保 order_dag 遍历时数据完整
        // 4. **边界保护**：gc_depth 防止无限保留导致内存溢出
        //
        for (name, round) in &self.last_committed {
            self.dag.retain(|r, authorities| {
                // 3.1: 删除该节点在已提交轮次之前的所有 Certificate
                authorities.retain(|n, _| n != name || r >= round);
                // 3.2: 如果该轮次没有任何 Certificate，或轮次过旧，删除整个轮次
                //
                // ==================== 为什么需要 r + gc_depth >= last_committed_round？====================
                //
                // 这是一个**强制边界保护**，防止某个节点长期落后导致内存泄漏。
                //
                // 问题场景：节点严重落后
                //
                // 假设系统运行了很久，状态如下：
                // ```
                // last_committed = {
                //   NodeA: 100,  // 正常节点
                //   NodeB: 100,  // 正常节点
                //   NodeC: 20,   // 严重落后的节点（可能故障、网络分区、或者崩溃后重启）
                // }
                // last_committed_round = 100
                // gc_depth = 10
                //
                // DAG 内容（简化）：
                // Round 95: { NodeA: cert_A95, NodeB: cert_B95 }
                // Round 90: { NodeA: cert_A90, NodeB: cert_B90 }
                // ...
                // Round 25: { NodeA: cert_A25, NodeB: cert_B25, NodeC: cert_C25 }
                // Round 24: { NodeA: cert_A24, NodeB: cert_B24, NodeC: cert_C24 }
                // ...
                // Round 21: { NodeA: cert_A21, NodeB: cert_B21, NodeC: cert_C21 }
                // ```
                //
                // ❌ 如果只有内层 retain（没有 gc_depth 边界）：
                //
                // 处理 NodeC（last_committed[NodeC] = 20）时：
                // ```rust
                // authorities.retain(|n, _| n != name || r >= round);
                // // 对于 NodeC，保留所有 >= 20 的轮次
                // ```
                //
                // 结果：
                // - Round 21-100 的 NodeC Certificate 全部保留（因为都 >= 20）
                // - 这是 **80 轮** 的数据！
                // - 但 gc_depth = 10 的意图是：只保留最近 10 轮
                //
                // 后果：
                // 1. **内存泄漏**：NodeC 的数据无法清理，占用大量内存
                // 2. **性能下降**：order_dag 遍历时需要处理大量旧数据
                // 3. **无限增长**：如果 NodeC 一直不恢复，数据会无限累积
                //
                // ✅ 有了 gc_depth 边界保护后：
                //
                // 第二层检查：r + gc_depth >= last_committed_round
                //
                // ```
                // last_committed_round = 100
                // gc_depth = 10
                //
                // Round 91: 91 + 10 = 101 >= 100 → 保留 ✅
                // Round 90: 90 + 10 = 100 >= 100 → 保留 ✅
                // Round 89: 89 + 10 = 99  < 100  → 删除 ❌ （即使 NodeC 还没提交！）
                // ...
                // Round 21: 21 + 10 = 31  < 100  → 删除 ❌ （强制清理）
                // ```
                //
                // 最终结果：
                // - 只保留 Round 90-100（最近 10 轮左右）
                // - NodeC 在 Round 21-89 的 Certificate 被**强制删除**
                // - 内存占用控制在合理范围
                //
                // ==================== 两层防护的协同作用 ====================
                //
                // 第一层（内层 retain）：精细化清理
                // - 删除已提交的 Certificate
                // - 保留未提交的 Certificate
                // - 目标：避免重复提交，节省内存
                //
                // 第二层（外层 retain）：强制边界
                // - 删除过旧的轮次（即使有未提交的 Certificate）
                // - 防止落后节点导致内存泄漏
                // - 目标：保证系统健壮性
                //
                // ==================== 实际场景 ====================
                //
                // 场景 1：正常运行（所有节点同步）
                // ```
                // last_committed = { A: 100, B: 100, C: 100 }
                // last_committed_round = 100
                // gc_depth = 10
                //
                // → 内层 retain：删除 < 100 的所有 Certificate
                // → 外层 retain：保留 >= 90 的轮次
                // → 结果：只保留 Round 100 及部分 Round 99、98...（未提交的）
                // ```
                //
                // 场景 2：某节点落后（NodeC 落后）
                // ```
                // last_committed = { A: 100, B: 100, C: 95 }
                // last_committed_round = 100
                // gc_depth = 10
                //
                // → 内层 retain：
                //   - NodeA、NodeB：删除 < 100
                //   - NodeC：删除 < 95，保留 >= 95
                // → 外层 retain：强制删除 < 90 的所有轮次
                // → 结果：保留 Round 90-100，NodeC 在 Round 95-100 有数据
                // ```
                //
                // 场景 3：某节点严重落后（NodeC 严重落后，上面的例子）
                // ```
                // last_committed = { A: 100, B: 100, C: 20 }
                // last_committed_round = 100
                // gc_depth = 10
                //
                // → 内层 retain：NodeC 会保留 >= 20 的所有数据
                // → 外层 retain：强制删除 < 90，包括 NodeC 的未提交数据
                // → 结果：NodeC 在 Round 21-89 的数据被牺牲，防止内存泄漏
                // ```
                //
                // ==================== 为什么可以"牺牲"落后节点的数据？====================
                //
                // 1. **共识已经推进**：全局已经提交到 Round 100，过旧的数据不影响共识安全性
                // 2. **节点可以重新同步**：NodeC 恢复后可以从其他节点获取数据
                // 3. **系统健壮性优先**：防止单个故障节点拖垮整个系统
                // 4. **gc_depth 是安全边界**：超出这个范围的数据对当前共识无用
                //
                // 这就是为什么需要这个看似"强硬"的边界检查：
                // **保证在任何情况下，内存占用都不会超过 gc_depth 轮次的数据。**
                //
                !authorities.is_empty() && r + gc_depth >= last_committed_round
            });
        }
    }
}

pub struct Consensus {
    /// 委员会信息（包含验证者列表、权益权重等）
    committee: Committee,
    /// 垃圾回收深度（保留多少轮已提交的数据）
    gc_depth: Round,

    /// 接收来自 Primary 的新 Certificate
    /// Primary 保证在发送 Certificate 之前，已经发送了它所有的祖先（因果一致性）
    rx_primary: Receiver<Certificate>,
    /// 将排序后的 Certificate 发送回 Primary
    /// 作用 1：通知 Primary 清理已处理的请求
    /// 作用 2：共识反馈回路
    tx_primary: Sender<Certificate>,
    /// 将排序后的 Certificate 输出给应用层（执行层）
    /// 这是共识的最终输出流
    tx_output: Sender<Certificate>,

    /// 创世证书集合（硬编码的起始点）
    genesis: Vec<Certificate>,
}

impl Consensus {
    pub fn spawn(
        committee: Committee,
        gc_depth: Round,
        rx_primary: Receiver<Certificate>,
        tx_primary: Sender<Certificate>,
        tx_output: Sender<Certificate>,
    ) {
        tokio::spawn(async move {
            Self {
                committee: committee.clone(),
                gc_depth,
                rx_primary,
                tx_primary,
                tx_output,
                genesis: Certificate::genesis(&committee),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        // 初始化共识状态（仅在此处可变，其他部分不可变）
        // State 包含 DAG 的内存视图和提交进度
        let mut state = State::new(self.genesis.clone());

        // 持续从 Primary 接收 Certificate
        while let Some(certificate) = self.rx_primary.recv().await {
            debug!("Processing {:?}", certificate);
            let round = certificate.round();

            // 1. 将新 Certificate 加入本地 DAG 存储
            // 此时还不确定它是否能被提交
            state
                .dag
                .entry(round)
                .or_insert_with(HashMap::new)
                .insert(certificate.origin(), (certificate.digest(), certificate));

            // 2. 尝试提交：寻找一个新的 Leader
            // Tusk 算法的核心：利用 DAG 的随机性进行 Leader 选举
            //
            // 规则：
            // - 需要至少 r 轮的 2f+1 个 Certificate 才能揭示 r 轮的随机数 Coin
            // - 有了 r 轮的 Coin，决定 r-2 轮的 Leader
            // - 所以从 round - 1 开始检查（确保 round - 1 有可能达到 2f+1）
            let r = round - 1;

            // 规则：只在偶数轮选举 Leader
            // 规则：前 4 轮用于启动，跳过
            if r % 2 != 0 || r < 4 {
                continue;
            }

            // 目标：确定 round - 2 的 Leader
            // 如果这个 Leader 已经在更早的时候被提交了，就跳过
            let leader_round = r - 2;
            if leader_round <= state.last_committed_round {
                continue;
            }
            // 计算 Leader 是谁，并检查 DAG 中是否真的有这个 Leader 的 Certificate
            let (leader_digest, leader) = match self.leader(leader_round, &state.dag) {
                Some(x) => x,
                None => continue,
            };

            // 3. 检查 Leader 是否获得足够支持（2f+1 票）
            // 
            // 投票规则：
            // - 候选人：第 r-2 轮的 Leader
            // - 投票者：第 r-1 轮的 Certificate
            // - 赞成票：如果投票者在 `parents` 中引用了 Leader
            //
            // 统计 r-1 轮中引用了 Leader 的节点的总权益（Stake）
            let stake: Stake = state
                .dag
                .get(&(r - 1))
                .expect("We should have the whole history by now")
                .values()
                .filter(|(_, x)| x.header.parents.contains(&leader_digest))
                .map(|(_, x)| self.committee.stake(&x.origin()))
                .sum();

            // 如果支持票不足 2f+1，该 Leader 本轮无法当选（但可能在未来通过链式规则被间接提交）
            if stake < self.committee.validity_threshold() {
                debug!("Leader {:?} does not have enough support", leader);
                continue;
            }

            // 4. Leader 提交逻辑
            debug!("Leader {:?} has enough support", leader);
            let mut sequence = Vec::new();
            
            // Tusk 提交规则是递归的：
            // - 当前 Leader 成功提交
            // - 它可能引用了之前的 Leader（未提交的）
            // - `order_leaders` 找出一串相连的 Leaders 链：Leader[r] -> Leader[r-2] -> ...
            // - 按照时间顺序（从旧到新）依次提交
            for leader in self.order_leaders(leader, &state).iter().rev() {
                // 对于每个 Leader，提交它引用的整个子图（Causal History）
                // `order_dag` 使用 DFS 遍历所有依赖的 Certificate
                for x in self.order_dag(leader, &state) {
                    // 更新状态（提交进度），并触发垃圾回收
                    state.update(&x, self.gc_depth);

                    // 加入提交序列
                    sequence.push(x);
                }
            }

            // Log the latest committed round of every authority (for debug).
            if log_enabled!(log::Level::Debug) {
                for (name, round) in &state.last_committed {
                    debug!("Latest commit of {}: Round {}", name, round);
                }
            }

            // 5. 输出结果
            // 按照顺序输出 Certificate
            for certificate in sequence {
                #[cfg(not(feature = "benchmark"))]
                info!("Committed {}", certificate.header);

                #[cfg(feature = "benchmark")]
                for digest in certificate.header.payload.keys() {
                    // NOTE: This log entry is used to compute performance.
                    info!("Committed {} -> {:?}", certificate.header, digest);
                }

                // 发送回 Primary 做清理
                self.tx_primary
                    .send(certificate.clone())
                    .await
                    .expect("Failed to send certificate to primary");

                // 发送给应用层执行
                if let Err(e) = self.tx_output.send(certificate).await {
                    warn!("Failed to output certificate: {}", e);
                }
            }
        }
    }

    /// Returns the certificate (and the certificate's digest) originated by the leader of the
    /// specified round (if any).
    /// 返回指定轮次的 Leader（如果有的话）
    /// 
    /// 这里的 Leader 选举是确定性的：
    /// - 根据 round 计算随机数 coin（或者简单的 round robin）
    /// - 映射到验证者集合中的某一个
    fn leader<'a>(&self, round: Round, dag: &'a Dag) -> Option<&'a (Digest, Certificate)> {
        // TODO: We should elect the leader of round r-2 using the common coin revealed at round r.
        // At this stage, we are guaranteed to have 2f+1 certificates from round r (which is enough to
        // compute the coin). We currently just use round-robin.
        // TODO: 使用 r+2 轮产生的 Random Coin（共享随机数）来选举 r 轮的 Leader。
        // 目前为了测试，使用 Round-Robin（轮询）方式。
        #[cfg(test)]
        let coin = 0;
        #[cfg(not(test))]
        let coin = round;

        // Elect the leader.
        // 选举 Leader
        // 1. 获取所有验证者公钥并排序（确保所有节点视角一致）
        let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
        keys.sort();
        // 2. 取模选出 Leader
        let leader = keys[coin as usize % self.committee.size()];

        // Return its certificate and the certificate's digest.
        // 返回 Leader 的 Certificate（如果我们在 DAG 中收到了它）
        dag.get(&round).map(|x| x.get(&leader)).flatten()
    }

    /// Order the past leaders that we didn't already commit.
    /// 找出需要提交的 Leader 链（Previous Leaders）
    /// 
    /// 如果当前的 Leader 成功被提交，它可能会"拉取"之前的 Leader 一起提交。
    /// 规则：
    /// - 从 current_leader 开始，向前回溯（step_by(2)）
    /// - 如果发现前一个 Leader（prev_leader）与当前 Leader 之间有路径（linked），则将 prev_leader 加入提交队列
    /// - 继续向前回溯，直到找不到连接，或者到达已提交边界
    fn order_leaders(&self, leader: &Certificate, state: &State) -> Vec<Certificate> {
        let mut to_commit = vec![leader.clone()];
        let mut leader = leader;
        // 从上一个 Leader 轮次开始回溯
        // Tusk 中每隔 2 轮尝试选举一次 Leader
        for r in (state.last_committed_round + 2..leader.round())
            .rev()
            .step_by(2)
        {
            // Get the certificate proposed by the previous leader.
            // 获取该轮次的潜在 Leader
            let (_, prev_leader) = match self.leader(r, &state.dag) {
                Some(x) => x,
                None => continue,
            };

            // Check whether there is a path between the last two leaders.
            // 检查当前 Leader 是否强引用了 prev_leader
            // 如果连接成功，说明 prev_leader 也在本轮可以被安全提交
            if self.linked(leader, prev_leader, &state.dag) {
                to_commit.push(prev_leader.clone());
                leader = prev_leader;
            }
        }
        // 返回顺序：[CurrentLeader, PrevLeader1, PrevLeader2...]
        to_commit
    }

    /// Checks if there is a path between two leaders.
    /// 检查两个 Certificate 之间是否存在因果路径（Strong Path）
    /// 
    /// 从 leader 开始，逐层向下遍历 DAG，看是否能到达 prev_leader。
    /// 因为 DAG 是逐层构建的，我们只需要每层检查 parents 引用。
    fn linked(&self, leader: &Certificate, prev_leader: &Certificate, dag: &Dag) -> bool {
        let mut parents = vec![leader];
        // 从 current_leader 轮次倒退回 prev_leader 轮次
        for r in (prev_leader.round()..leader.round()).rev() {
            parents = dag
                .get(&(r))
                .expect("We should have the whole history by now")
                .values()
                // 筛选出被上一层 parents 引用的 Certificate
                .filter(|(digest, _)| parents.iter().any(|x| x.header.parents.contains(digest)))
                .map(|(_, certificate)| certificate)
                .collect();
        }
        // 如果最终 parents 集合包含了 prev_leader，说明有路径
        parents.contains(&prev_leader)
    }

    /// Flatten the dag referenced by the input certificate. This is a classic depth-first search (pre-order):
    /// https://en.wikipedia.org/wiki/Tree_traversal#Pre-order
    /// 铺平 DAG：找出 leader 所引用的所有未提交的 Certificate
    /// 
    /// 使用标准的 DFS（深度优先搜索）遍历
    /// 目标：将 DAG 的偏序关系（Partial Order）转化为全序关系（Total Order）
    fn order_dag(&self, leader: &Certificate, state: &State) -> Vec<Certificate> {
        debug!("Processing sub-dag of {:?}", leader);
        let mut ordered = Vec::new();
        let mut already_ordered = HashSet::new();

        let mut buffer = vec![leader];
        while let Some(x) = buffer.pop() {
            debug!("Sequencing {:?}", x);
            ordered.push(x.clone());
            // 遍历所有父节点
            for parent in &x.header.parents {
                let (digest, certificate) = match state
                    .dag
                    .get(&(x.round() - 1))
                    .map(|x| x.values().find(|(x, _)| x == parent))
                    .flatten()
                {
                    Some(x) => x,
                    None => continue, // We already ordered or GC up to here.
                                      // 如果父节点找不到了（可能已提交或被 GC），跳过
                };

                // We skip the certificate if we (1) already processed it or (2) we reached a round that we already
                // committed for this authority.
                // 跳过已经处理过的，或者已经提交过的 Certificate
                let mut skip = already_ordered.contains(&digest);
                skip |= state
                    .last_committed
                    .get(&certificate.origin())
                    .map_or_else(|| false, |r| r == &certificate.round());
                if !skip {
                    buffer.push(certificate);
                    already_ordered.insert(digest);
                }
            }
        }

        // Ensure we do not commit garbage collected certificates.
        // 最终过滤：再次确保不包含过旧的数据（GC 保护）
        ordered.retain(|x| x.round() + self.gc_depth >= state.last_committed_round);

        // Ordering the output by round is not really necessary but it makes the commit sequence prettier.
        // 排序：虽然逻辑上 DFS 已经确定了因果顺序，但按轮次排序会让输出更美观、确定性更强
        ordered.sort_by_key(|x| x.round());
        ordered
    }
}
