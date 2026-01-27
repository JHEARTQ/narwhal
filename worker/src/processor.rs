// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::SerializedBatchDigestMessage;
use config::WorkerId;
use crypto::Digest;
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use primary::WorkerPrimaryMessage;
use std::convert::TryInto;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/processor_tests.rs"]
pub mod processor_tests;

/// Indicates a serialized `WorkerMessage::Batch` message.
pub type SerializedBatchMessage = Vec<u8>;

/// Hashes and stores batches, it then outputs the batch's digest.
/// Processor (处理器) 的核心职责：
/// 1. 计算 Batch 的哈希摘要 (Digest)。
/// 2. 将 Batch 数据持久化存储到本地数据库中。
/// 3. 将计算出的 Digest 发送给 Primary 节点（以便 Primary 将其加入 DAG）。
pub struct Processor;

impl Processor {
    /// 启动 Processor 任务。
    pub fn spawn(
        // Our worker's id.
        id: WorkerId,
        // The persistent storage.
        // 本地持久化存储 (通常是 RocksDB)。
        mut store: Store,
        // Input channel to receive batches.
        // 输入管道：接收序列化后的 Batch 数据。
        // 数据来源可能是 QuorumWaiter (自己的 Batch) 或者 网络接收 (因为同步或其他 Worker 广播过来的 Batch)。
        mut rx_batch: Receiver<SerializedBatchMessage>,
        // Output channel to send out batches' digests.
        // 输出管道：发送给 PrimaryConnecter，最终由它通过网络发给 Primary 节点。
        tx_digest: Sender<SerializedBatchDigestMessage>,
        // Whether we are processing our own batches or the batches of other nodes.
        // 标记位：
        // true: 处理的是自己生成的 Batch (OurBatch)。Primary 会将其以此 Worker 的名义加入 DAG。
        // false: 处理的是从其他 Worker 接收的 Batch (OthersBatch)。Primary 可能只需要知道其可用性。
        own_digest: bool,
    ) {
        tokio::spawn(async move {
            while let Some(batch) = rx_batch.recv().await {
                // Hash the batch.
                // 1. 计算哈希：使用 SHA-512 计算整个 Batch 的哈希值，并截取前 32 字节作为 Digest。
                let digest = Digest(Sha512::digest(&batch).as_slice()[..32].try_into().unwrap());

                // Store the batch.
                // 2. 持久化存储：Narwhal 架构的核心 —— "数据与元数据分离"。
                // Worker 负责存数据 (Write Path)，Primary 只负责处理哈希元数据。
                // 只有虽然写入硬盘成功后，才会通知 Primary。
                store.write(digest.to_vec(), batch).await;

                // Deliver the batch's digest.
                // 3. 构建消息通知 Primary。
                let message = match own_digest {
                    true => WorkerPrimaryMessage::OurBatch(digest, id),
                    false => WorkerPrimaryMessage::OthersBatch(digest, id),
                };
                // 4. 序列化消息并发送给 Primary 连接器。
                let message = bincode::serialize(&message)
                    .expect("Failed to serialize our own worker-primary message");
                tx_digest
                    .send(message)
                    .await
                    .expect("Failed to send digest");
            }
        });
    }
}
