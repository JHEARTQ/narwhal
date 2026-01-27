// Copyright(C) Facebook, Inc. and its affiliates.
use crate::quorum_waiter::QuorumWaiterMessage;
use crate::worker::WorkerMessage;
use bytes::Bytes;
#[cfg(feature = "benchmark")]
use crypto::Digest;
use crypto::PublicKey;
#[cfg(feature = "benchmark")]
use ed25519_dalek::{Digest as _, Sha512};
#[cfg(feature = "benchmark")]
use log::info;
use network::ReliableSender;
#[cfg(feature = "benchmark")]
use std::convert::TryInto as _;
use std::net::SocketAddr;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/batch_maker_tests.rs"]
pub mod batch_maker_tests;

pub type Transaction = Vec<u8>;
pub type Batch = Vec<Transaction>;

/// Assemble clients transactions into batches.
pub struct BatchMaker {
    /// The preferred batch size (in bytes).
    batch_size: usize,
    /// The maximum delay after which to seal the batch (in ms).
    max_batch_delay: u64,
    /// Channel to receive transactions from the network.
    rx_transaction: Receiver<Transaction>,
    /// Output channel to deliver sealed batches to the `QuorumWaiter`.
    tx_message: Sender<QuorumWaiterMessage>,
    /// The network addresses of the other workers that share our worker id.
    workers_addresses: Vec<(PublicKey, SocketAddr)>,
    /// Holds the current batch.
    current_batch: Batch,
    /// Holds the size of the current batch (in bytes).
    current_batch_size: usize,
    /// A network sender to broadcast the batches to the other workers.
    network: ReliableSender,
}

impl BatchMaker {
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

    /// Main loop receiving incoming transactions and creating batches.
    async fn run(&mut self) {
        let timer = sleep(Duration::from_millis(self.max_batch_delay));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                // Assemble client transactions into batches of preset size.
                Some(transaction) = self.rx_transaction.recv() => {
                    self.current_batch_size += transaction.len();
                    self.current_batch.push(transaction);
                    if self.current_batch_size >= self.batch_size {
                        self.seal().await;
                        timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                    }
                },

                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer => {
                    if !self.current_batch.is_empty() {
                        self.seal().await;
                    }
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                }
            }

            // Give the change to schedule other tasks.
            tokio::task::yield_now().await;
        }
    }

    /// Seal and broadcast the current batch.
    /// "封存" (Seal) 并广播当前累积的 Batch。
    /// 这个函数在两种情况下被调用：1. 累积的交易大小达到 batch_size 上限；2. 距离上一次封存超过了 max_batch_delay 时间。
    ///
    /// 核心流程：
    /// 1. 序列化 (Serialize)：将所有累积的交易 (Transaction) 从 current_batch 中取出，打包成一个 WorkerMessage::Batch 并序列化。
    /// 2. 广播 (Broadcast)：通过网络层 (ReliableSender) 将这个 Batch 广播给委员会中所有其他节点的同号 Worker。
    ///    - 这里的广播是“可靠广播”，如果不成功会不断重试，直到被取消。
    ///    - 返回的 handlers 是控制这些后台重试任务的句柄（可以用来取消任务）。
    /// 3. 移交 (Handoff)：将序列化的 Batch 数据以及刚才拿到的 handlers 发送给下一阶段的 [QuorumWaiter]。
    ///    - BatchMaker 不负责等待确认，它把“等待 2f+1 个确认”以及“取消多余重传”的工作交给了 QuorumWaiter。
    ///
    /// 副作用：该函数执行后，`current_batch` 会被清空，准备收集下一个 Batch。
    async fn seal(&mut self) {
        #[cfg(feature = "benchmark")]
        let size = self.current_batch_size;

        // Look for sample txs (they all start with 0) and gather their txs id (the next 8 bytes).
        // [Benchmark 特性] 提取样本交易 ID，用于后续性能追踪。
        // 这里的逻辑是：如果交易数据的第一个字节是 0，就被视为“样本交易”，接下来的 8 字节是它的 ID。
        #[cfg(feature = "benchmark")]
        let tx_ids: Vec<_> = self
            .current_batch
            .iter()
            .filter(|tx| tx[0] == 0u8 && tx.len() > 8)
            .filter_map(|tx| tx[1..9].try_into().ok())
            .collect();

        // Serialize the batch.
        // 1. 重置当前 Batch 大小计数器
        self.current_batch_size = 0;
        // 2. 将 current_batch 中的所有交易“抽干”(drain)并移动到新的局部变量 `batch` 中
        let batch: Vec<_> = self.current_batch.drain(..).collect();
        // 3. 包装成 WorkerMessage 协议格式
        let message = WorkerMessage::Batch(batch);
        // 4. 使用 bincode 序列化为字节流
        let serialized = bincode::serialize(&message).expect("Failed to serialize our own batch");

        #[cfg(feature = "benchmark")]
        {
            // NOTE: This is one extra hash that is only needed to print the following log entries.
            // [Benchmark 特性] 计算哈希并打印日志，方便分析系统吞吐量和延迟。
            let digest = Digest(
                Sha512::digest(&serialized).as_slice()[..32]
                    .try_into()
                    .unwrap(),
            );

            for id in tx_ids {
                // NOTE: This log entry is used to compute performance.
                info!(
                    "Batch {:?} contains sample tx {}",
                    digest,
                    u64::from_be_bytes(id)
                );
            }

            // NOTE: This log entry is used to compute performance.
            info!("Batch {:?} contains {} B", digest, size);
        }

        // Broadcast the batch through the network.
        // 5. 准备广播目标：解压 worker_addresses 获取所有对等 Worker 的 (公钥, 地址) 列表
        let (names, addresses): (Vec<_>, _) = self.workers_addresses.iter().cloned().unzip();
        let bytes = Bytes::from(serialized.clone());
        // 6. 执行“可靠广播” (Reliable Broadcast)
        // self.network.broadcast 会向所有地址发送消息，并为每个目标启动一个重试任务。
        // 它返回一组 `CancelHandler`，每个 handler 对应一个目标的后台重试任务。
        let handlers = self.network.broadcast(addresses, bytes).await;

        // Send the batch through the deliver channel for further processing.
        // 7. 将序列化后的 Batch 和对应的重试任务句柄打包发送给 QuorumWaiter。
        // QuorumWaiter 会在收到足够多的 ACK 后，利用这些 handlers 来取消剩余的重试任务，从而节省带宽（取消不再需要的重试）。
        self.tx_message
            .send(QuorumWaiterMessage {
                batch: serialized,
                handlers: names.into_iter().zip(handlers.into_iter()).collect(),
            })
            .await
            .expect("Failed to deliver batch");
    }
}
