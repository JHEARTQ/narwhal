// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::{Round, WorkerMessage};
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey};
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, error};
use network::SimpleSender;
use primary::PrimaryWorkerMessage;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use store::{Store, StoreError};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/synchronizer_tests.rs"]
pub mod synchronizer_tests;

/// Resolution of the timer managing retrials of sync requests (in ms).
const TIMER_RESOLUTION: u64 = 1_000;

// The `Synchronizer` is responsible to keep the worker in sync with the others.
pub struct Synchronizer {
    /// The public key of this authority.
    name: PublicKey,
    /// The id of this worker.
    id: WorkerId,
    /// The committee information.
    committee: Committee,
    // The persistent storage.
    store: Store,
    /// The depth of the garbage collection.
    gc_depth: Round,
    /// The delay to wait before re-trying to send sync requests.
    sync_retry_delay: u64,
    /// Determine with how many nodes to sync when re-trying to send sync-requests. These nodes
    /// are picked at random from the committee.
    sync_retry_nodes: usize,
    /// Input channel to receive the commands from the primary.
    rx_message: Receiver<PrimaryWorkerMessage>,
    /// A network sender to send requests to the other workers.
    network: SimpleSender,
    /// Loosely keep track of the primary's round number (only used for cleanup).
    round: Round,
    /// Keeps the digests (of batches) that are waiting to be processed by the primary. Their
    /// processing will resume when we get the missing batches in the store or we no longer need them.
    /// It also keeps the round number and a timestamp (`u128`) of each request we sent.
    pending: HashMap<Digest, (Round, Sender<()>, u128)>,
}

impl Synchronizer {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        id: WorkerId,
        committee: Committee,
        store: Store,
        gc_depth: Round,
        sync_retry_delay: u64,
        sync_retry_nodes: usize,
        rx_message: Receiver<PrimaryWorkerMessage>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                id,
                committee,
                store,
                gc_depth,
                sync_retry_delay,
                sync_retry_nodes,
                rx_message,
                network: SimpleSender::new(),
                round: Round::default(),
                pending: HashMap::new(),
            }
            .run()
            .await;
        });
    }

    /// Helper function. It waits for a batch to become available in the storage
    /// and then delivers its digest.
    async fn waiter(
        missing: Digest,
        mut store: Store,
        deliver: Digest,
        mut handler: Receiver<()>,
    ) -> Result<Option<Digest>, StoreError> {
        tokio::select! {
            result = store.notify_read(missing.to_vec()) => {
                result.map(|_| Some(deliver))
            }
            _ = handler.recv() => Ok(None),
        }
    }

    /// Main loop listening to the primary's messages.
    /// 这个函数是 Worker 同步器的核心循环，主要负责三件事：
    /// 1. 处理来自 Primary 的同步指令（Synchronize）和清理指令（Cleanup）。
    /// 2. 监听后台等待任务（waiter）的完成情况，即等待缺失的数据写入存储。
    /// 3. 管理重试定时器，对超时的同步请求进行广播重试。
    async fn run(&mut self) {
        // FuturesUnordered 用于并发管理多个等待任务（waiter），任何一个任务完成都会在这里被感知。
        let mut waiting = FuturesUnordered::new();

        // 创建一个定时器，默认为1秒触发一次，用于检查超时的同步请求。
        let timer = sleep(Duration::from_millis(TIMER_RESOLUTION));
        tokio::pin!(timer);

        loop {
            // tokio::select! 宏允许我们同时监听多个异步事件，哪个先发生就处理哪个。
            tokio::select! {
                // 分支 1: 处理来自 Primary 节点的消。
                // 只有当接收到 Primary 的消息时才会执行此分支。
                Some(message) = self.rx_message.recv() => match message {
                    // 同步请求：Primary 告诉我们需要同步哪些 digest，并建议向哪个 target 节点请求。
                    PrimaryWorkerMessage::Synchronize(digests, target) => {
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("Failed to measure time")
                            .as_millis();

                        let mut missing = Vec::new();
                        for digest in digests {
                            // 如果这个 digest 已经在等待同步的列表中了，就跳过，避免重复请求。
                            if self.pending.contains_key(&digest) {
                                continue;
                            }

                            // 再次检查本地存储，也许在 Primary 发送命令的同时数据已经到了。
                            match self.store.read(digest.to_vec()).await {
                                Ok(None) => {
                                    // 确实缺失，加入待请求列表。
                                    missing.push(digest.clone());
                                    debug!("Requesting sync for batch {}", digest);
                                },
                                Ok(Some(_)) => {
                                    // 运气不错，数据已经到了，无需请求。
                                },
                                Err(e) => {
                                    error!("{}", e);
                                    continue;
                                }
                            }

                            // 创建一个 waiter 任务：
                            // 它会挂起等待存储层通知这个 digest 对应的数据被写入。
                            // 任务被放入 `waiting` 集合中并发执行。
                            let deliver = digest.clone();
                            let (tx_cancel, rx_cancel) = channel(1);
                            let fut = Self::waiter(digest.clone(), self.store.clone(), deliver, rx_cancel);
                            waiting.push(fut);
                            // 记录 pending 状态，包含发起时间戳，用于后续超时检查。
                            self.pending.insert(digest, (self.round, tx_cancel, now));
                        }

                        // 向指定的目标 Worker 发送 BatchRequest 请求数据。
                        // 如果从这个节点获取失败，我们将依赖定时器触发的重试机制去向更多节点请求。
                        let address = match self.committee.worker(&target, &self.id) {
                            Ok(address) => address.worker_to_worker,
                            Err(e) => {
                                error!("The primary asked us to sync with an unknown node: {}", e);
                                continue;
                            }
                        };
                        let message = WorkerMessage::BatchRequest(missing, self.name);
                        let serialized = bincode::serialize(&message).expect("Failed to serialize our own message");
                        self.network.send(address, Bytes::from(serialized)).await;
                    },
                    // 清理请求：Primary 通知了最新的共识轮次。
                    PrimaryWorkerMessage::Cleanup(round) => {
                        // 更新本地轮次记录。
                        self.round = round;

                        // 如果还没超过垃圾回收深度，不需要清理。
                        if self.round < self.gc_depth {
                            continue;
                        }

                        // 计算过期的轮次阈值。
                        let mut gc_round = self.round - self.gc_depth;
                        // 遍历所有正在等待的同步请求，如果请求的轮次太老了，就发送取消信号。
                        for (r, handler, _) in self.pending.values() {
                            if r <= &gc_round {
                                let _ = handler.send(()).await;
                            }
                        }
                        // 从 pending 列表中移除过期的条目。
                        self.pending.retain(|_, (r, _, _)| r > &mut gc_round);
                    }
                },

                // 分支 2: 处理 waiter 任务的完成。
                // 当通过 store.notify_read 等到了数据，或者任务被取消时，会进入此分支。
                Some(result) = waiting.next() => match result {
                    Ok(Some(digest)) => {
                        // 数据成功同步并写入存储！从 pending 列表中移除。
                        self.pending.remove(&digest);
                    },
                    Ok(None) => {
                        // waiter 任务被取消了（通常是因为垃圾回收）。
                    },
                    Err(e) => error!("{}", e)
                },

                // 分支 3: 定时器触发。
                // 用于处理同步请求的重试机制。
                () = &mut timer => {
                    // 我们最初只向 Primary 建议的一个节点发送了请求。
                    // 现在的策略是：如果那个节点由于某种原因没有发给我们，
                    // 我们就不再信任单一节点，而是向多个随机节点广播请求。
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Failed to measure time")
                        .as_millis();

                    let mut retry = Vec::new();
                    // 检查所有 pending 的请求，看它们是否超时。
                    for (digest, (_, _, timestamp)) in &self.pending {
                        if timestamp + (self.sync_retry_delay as u128) < now {
                            debug!("Requesting sync for batch {} (retry)", digest);
                            retry.push(digest.clone());
                        }
                    }
                    if !retry.is_empty() {
                        // 随机选择 sync_retry_nodes 个其他 Worker 进行广播请求。
                        let addresses = self.committee
                            .others_workers(&self.name, &self.id)
                            .iter().map(|(_, address)| address.worker_to_worker)
                            .collect();
                        let message = WorkerMessage::BatchRequest(retry, self.name);
                        let serialized = bincode::serialize(&message).expect("Failed to serialize our own message");
                        self.network
                            .lucky_broadcast(addresses, Bytes::from(serialized), self.sync_retry_nodes)
                            .await;
                    }

                    // 重置定时器，进入下一轮等待。
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(TIMER_RESOLUTION));
                },
            }
        }
    }
}
