// Copyright(C) Facebook, Inc. and its affiliates.
use crate::processor::SerializedBatchMessage;
use config::{Committee, Stake};
use crypto::PublicKey;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use network::CancelHandler;
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/quorum_waiter_tests.rs"]
pub mod quorum_waiter_tests;

#[derive(Debug)]
pub struct QuorumWaiterMessage {
    /// A serialized `WorkerMessage::Batch` message.
    /// 序列化后的 Batch 消息体。
    pub batch: SerializedBatchMessage,
    /// The cancel handlers to receive the acknowledgements of our broadcast.
    /// 一组控制句柄。
    /// 每个句柄对应一个发往其他 Worker 的后台传输任务。
    /// 我们可以通过 await 这个句柄来等待对方的 ACK，或者通过 drop 句柄来取消后台任务。
    pub handlers: Vec<(PublicKey, CancelHandler)>,
}

/// The QuorumWaiter waits for 2f authorities to acknowledge reception of a batch.
/// QuorumWaiter (法定人数等待器) 的核心职责：
/// 确保一个 Batch 已经被足够多的节点（2f+1 权重）接收，从而满足数据可用性要求。
/// 
/// 流程：
/// 1. 从 BatchMaker 接收刚广播出去的 Batch 和对应的监听句柄 (handlers)。
/// 2. 监听这些 handlers，收集来自其他 Worker 的 ACK 确认。
/// 3. 一旦收集到的 Stake（权益/权重）总和达到法定阈值 (Quorum Threshold，通常是 2/3)，
///    就认为这个 Batch 已经"安全"了。
/// 4. 将 Batch 转发给 Processor 进行存储，并停止等待剩余的 ACK。
pub struct QuorumWaiter {
    /// The committee information.
    /// 委员会信息，用于查询节点的 Stake 权重和计算 Quorum 阈值。
    committee: Committee,
    /// The stake of this authority.
    /// 自身节点的 Stake 权重（因为自己肯定存了即便没发给自己，也要算在内）。
    stake: Stake,
    /// Input Channel to receive commands.
    /// 接收来自 BatchMaker 的消息管道。
    rx_message: Receiver<QuorumWaiterMessage>,
    /// Channel to deliver batches for which we have enough acknowledgements.
    /// 当收集齐 Quorum 后，将 Batch 发往处理器的管道。
    tx_batch: Sender<SerializedBatchMessage>,
}

impl QuorumWaiter {
    /// Spawn a new QuorumWaiter.
    /// 启动 QuorumWaiter 任务。
    pub fn spawn(
        committee: Committee,
        stake: Stake,
        rx_message: Receiver<QuorumWaiterMessage>,
        tx_batch: Sender<Vec<u8>>,
    ) {
        tokio::spawn(async move {
            Self {
                committee,
                stake,
                rx_message,
                tx_batch,
            }
            .run()
            .await;
        });
    }

    /// Helper function. It waits for a future to complete and then delivers a value.
    /// 辅助函数：将等待 ACK 的 Future 和该节点对应的 Stake 绑定在一起。
    /// 当 `wait_for` (CancelHandler) 完成时（意味着收到了 ACK），返回这个节点的 Stake。
    async fn waiter(wait_for: CancelHandler, deliver: Stake) -> Stake {
        // 等待 ReliableSender 的后台任务通知我们"ACK已收到"。
        let _ = wait_for.await;
        // 返回该节点的权重。
        deliver
    }

    /// Main loop.
    async fn run(&mut self) {
        // 循环处理每一个从 BatchMaker 发来的新 Batch 任务
        while let Some(QuorumWaiterMessage { batch, handlers }) = self.rx_message.recv().await {
            // 1. 将所有节点的等待任务转换为一个并发流 (FuturesUnordered)。
            // 这样我们可以同时等待所有节点的 ACK，而不是一个接一个等。
            //
            // 步骤详解：
            // - handlers.into_iter(): 取得所有权，开始遍历每个 (节点名, 句柄)。
            // - map(...): 对每个句柄进行转换：
            //     1. 查表：根据节点名(name) 查找它在委员会中的权重(stake)。
            //     2. 包装：调用 Self::waiter(handler, stake) 创建一个 Future。
            //        这个 Future 的作用是："等待 handler 完成(即收到ACK)，然后返回 stake"。
            // - collect(): 将这些 Future 收集到 FuturesUnordered 集合中。
            //   FuturesUnordered 是一个特殊的集合，它允许里面的任务并发执行，且 next() 会返回"先完成的那个结果"。
            let mut wait_for_quorum: FuturesUnordered<_> = handlers
                .into_iter()
                .map(|(name, handler)| {
                    let stake = self.committee.stake(&name);
                    Self::waiter(handler, stake)
                })
                .collect();

            // Wait for the first 2f nodes to send back an Ack. Then we consider the batch
            // delivered and we send its digest to the primary (that will include it into
            // the dag). This should reduce the amount of synching.
            // 2. 初始化已收集的 Stake，先加上自己的 Stake。
            let mut total_stake = self.stake;
            
            // 3. 循环等待，直到收到足够的 ACK。
            while let Some(stake) = wait_for_quorum.next().await {
                total_stake += stake;
                
                // 4. 检查是否达到法定阈值 (Quorum)。
                if total_stake >= self.committee.quorum_threshold() {
                    // 5. 达到阈值！将 Batch 发送给 Processor 进行后续处理（持久化、通知 Primary）。
                    self.tx_batch
                        .send(batch)
                        .await
                        .expect("Failed to deliver batch");
                    
                    // 6. 关键点：跳出循环。
                    // 此时 `wait_for_quorum` 那个集合会被 Drop 掉。
                    // 集合中所有剩余的 `handler` 也会被 Drop。
                    // 这会触发 ReliableSender 的机制：感知到 `receiver` 被 Drop，
                    // 从而取消那些还在后台拼命重试的网络发送任务，节省带宽。
                    break;
                }
            }
        }
    }
}
