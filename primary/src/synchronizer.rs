// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::DagResult;
use crate::header_waiter::WaiterMessage;
use crate::messages::{Certificate, Header};
use config::Committee;
use crypto::Hash as _;
use crypto::{Digest, PublicKey};
use std::collections::HashMap;
use store::Store;
use tokio::sync::mpsc::Sender;

/// Synchronizer：DAG 同步的"守门员"，负责依赖检查和异步容错。
///
/// 从共识的角度理解 Synchronizer：
///
/// **核心职责**：
/// 1. **依赖检查**：验证 Header/Certificate 的所有依赖是否本地可用
/// 2. **缺失检测**：如果有依赖缺失，识别出具体缺失的数据
/// 3. **触发同步**：将缺失数据的请求发送给 Waiter，启动后台同步
/// 4. **延迟处理**：暂停当前 Header 处理，等待同步完成后由 Waiter 回环
///
/// **设计意义（异步容错的核心机制）**：
/// - 网络不可靠：消息可能延迟或乱序到达
/// - 节点异步：不同节点的数据到达速度不同
/// - 解决方案："挂起-同步-回环"模式
///   1. 遇到依赖缺失 → 暂停处理（返回 empty/false）
///   2. 主动发起同步请求（向其他节点拉取数据）
///   3. 数据到达 store 后 → Waiter 触发回环处理
///   4. 重新处理时依赖已齐全 → 正常执行
///
/// **两层依赖**：
/// 1. **Payload 依赖**（missing_payload）：
///    - Header 声称包含的 batch digest
///    - 这些数据存储在本地 Worker
/// 2. **DAG 依赖**（get_parents 和 deliver_certificate）：
///    - Header/Certificate 的父节点（上一轮的 Certificate）
pub struct Synchronizer {
    /// 本节点的公钥（用于判断是否是自己的 Header）。
    name: PublicKey,
    /// 持久化存储（RocksDB）。
    store: Store,
    /// 向 HeaderWaiter 发送同步请求。
    tx_header_waiter: Sender<WaiterMessage>,
    /// 向 CertificateWaiter 发送同步请求。
    tx_certificate_waiter: Sender<Certificate>,
    /// Genesis Certificate 的缓存（及其 digest）。
    /// - 系统启动时的虚拟 Certificate（代表轮次 0）
    /// - 所有节点的第一轮 Header 都会引用 genesis 作为父节点
    genesis: Vec<(Digest, Certificate)>,
}

impl Synchronizer {
    /// 创建 Synchronizer 实例
    pub fn new(
        name: PublicKey,
        committee: &Committee,
        store: Store,
        tx_header_waiter: Sender<WaiterMessage>,
        tx_certificate_waiter: Sender<Certificate>,
    ) -> Self {
        Self {
            name,
            store,
            tx_header_waiter,
            tx_certificate_waiter,
            genesis: Certificate::genesis(committee)
                .into_iter()
                .map(|x| (x.digest(), x))
                .collect(),
        }
    }

    /// 检查 Header 的 payload（batch 数据）是否齐全
    ///
    /// 共识意义（数据可用性）：
    /// - Narwhal 分离了"数据"和"元数据"：数据在 Worker，元数据在 Primary
    /// - 必须验证所有 batch 都可用，否则无法执行交易
    /// - 返回值：
    ///   - false：所有数据都本地可用，可以继续处理
    ///   - true：有数据缺失，已触发同步请求，需要等待回环
    pub async fn missing_payload(&mut self, header: &Header) -> DagResult<bool> {
        // ==================== 步骤 1：自己提议的 Header 无需检查 ====================
        if header.author == self.name {
            return Ok(false);
        }

        // ==================== 步骤 2：逐个检查 Batch 数据是否本地可用 ====================
        // 为什么同时验证 worker_id？防御恶意节点冒充：
        // - 恶意节点可能声称"batch X 来自 worker 1"，但实际上 X 在 worker 2
        // - 我们检查 (digest, worker_id) 对，确保数据来自正确的来源
        let mut missing = HashMap::new();
        for (digest, worker_id) in header.payload.iter() {
            let key = [digest.as_ref(), &worker_id.to_le_bytes()].concat();
            if self.store.read(key).await?.is_none() {
                missing.insert(digest.clone(), *worker_id);
            }
        }

        // ==================== 步骤 3：如果数据齐全，继续处理 ====================
        if missing.is_empty() {
            return Ok(false);
        }

        // ==================== 步骤 4：如果有缺失，触发异步同步 ====================
        // 异步容错的关键机制：
        // 1. 发送 SyncBatches 请求给 HeaderWaiter
        // 2. HeaderWaiter 会通知 Worker 向其他节点请求数据
        // 3. 数据到达 store 后，通过 store.notify_read 触发
        // 4. HeaderWaiter 将此 Header 回环到 Core 的 rx_header_waiter
        // 5. Core 重新处理此 Header 时，missing.is_empty() 为 true，继续处理
        self.tx_header_waiter
            .send(WaiterMessage::SyncBatches(missing, header.clone()))
            .await
            .expect("Failed to send sync batch request");
        Ok(true) // 返回 true：有缺失，已触发同步
    }

    /// 获取 Header 的所有父节点（上一轮的 Certificate）
    ///
    /// 共识意义（DAG 因果性）：
    /// - DAG 是有向无环图：每个 Header 必须引用某些 Certificate 作为父节点
    /// - 父节点来自上一轮：Header 轮次为 r，父节点都来自轮次 r-1
    /// - 必须齐全所有父节点，否则 DAG 拓扑不完整
    /// - 返回值：
    ///   - Vec[Certificate]：所有父节点齐全，直接返回
    ///   - Vec[]（空）：有父节点缺失，已触发同步请求
    pub async fn get_parents(&mut self, header: &Header) -> DagResult<Vec<Certificate>> {
        // ==================== 步骤 1：遍历所有父节点 digest ====================
        let mut missing = Vec::new();
        let mut parents = Vec::new();
        for digest in &header.parents {
            // ==================== 步骤 1a：检查是否是 genesis ====================
            // Genesis Certificate 无需从 store 读取，直接从缓存返回
            // 为什么特殊处理？Genesis 是虚拟证书，所有节点天生就有
            if let Some(genesis) = self
                .genesis
                .iter()
                .find(|(x, _)| x == digest)
                .map(|(_, x)| x)
            {
                parents.push(genesis.clone());
                continue;
            }

            // ==================== 步骤 1b：从 store 读取 Certificate ====================
            match self.store.read(digest.to_vec()).await? {
                Some(certificate) => parents.push(bincode::deserialize(&certificate)?),
                None => missing.push(digest.clone()), // 缺失的父节点记录下来
            };
        }

        // ==================== 步骤 2：如果所有父节点都齐全，直接返回 ====================
        if missing.is_empty() {
            return Ok(parents);
        }

        // ==================== 步骤 3：如果有缺失，触发异步同步 ====================
        // 异步容错的另一种体现：
        // 1. 发送 SyncParents 请求给 HeaderWaiter
        // 2. HeaderWaiter 会向其他 Primary 请求缺失的 Certificate
        // 3. 数据到达 store 后，通过 store.notify_read 触发
        // 4. HeaderWaiter 将此 Header 回环到 Core 的 rx_header_waiter
        // 5. Core 重新处理此 Header 时，missing.is_empty() 为 true，继续处理
        self.tx_header_waiter
            .send(WaiterMessage::SyncParents(missing, header.clone()))
            .await
            .expect("Failed to send sync parents request");
        Ok(Vec::new()) // 返回空向量：有缺失，已触发同步
    }

    /// 检查 Certificate 的所有祖先是否本地可用
    ///
    /// 共识意义（DAG 完整性）：
    /// - Certificate 内嵌了一个 Header，而 Header 有 parents
    /// - 这些 parents 的 parents 就是\"祖先\"
    /// - 必须递归验证所有祖先都可用，才能确保 DAG 拓扑完整
    /// - 返回值：
    ///   - true：所有祖先都可用，可以继续处理
    ///   - false：有祖先缺失，已触发同步请求，需要等待回环
    pub async fn deliver_certificate(&mut self, certificate: &Certificate) -> DagResult<bool> {
        // ==================== 检查所有父节点的祖先 ====================
        for digest in &certificate.header.parents {
            // Genesis 特殊处理：无需检查祖先
            if self.genesis.iter().any(|(x, _)| x == digest) {
                continue;
            }

            // ==================== 检查祖先 Certificate 是否在 store 中 ====================
            if self.store.read(digest.to_vec()).await?.is_none() {
                // ==================== 有祖先缺失，触发异步同步 ====================
                // CertificateWaiter 会：
                // 1. 向其他 Primary 请求缺失的 Certificate
                // 2. 数据到达 store 后，重新处理此 Certificate
                self.tx_certificate_waiter
                    .send(certificate.clone())
                    .await
                    .expect("Failed to send sync certificate request");
                return Ok(false); // 返回 false：有缺失，已触发同步
            };
        }
        Ok(true) // 返回 true：所有祖先都可用
    }
}
