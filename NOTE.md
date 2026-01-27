阅读源码学习DAG共识之Narwhal and Tusk

我们将从数据流的角度来学习源码。

1. 客户端在发布交易后，首先会被Work收集。Work通过如下代码来处理接收到的交易：

```
fn handle_clients_transactions(&self, tx_primary: Sender<SerializedBatchDigestMessage>) {
        let (tx_batch_maker, rx_batch_maker) = channel(CHANNEL_CAPACITY);
        let (tx_quorum_waiter, rx_quorum_waiter) = channel(CHANNEL_CAPACITY);
        let (tx_processor, rx_processor) = channel(CHANNEL_CAPACITY);

        // We first receive clients' transactions from the network.
        let mut address = self
            .committee
            .worker(&self.name, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .transactions;
        address.set_ip("0.0.0.0".parse().unwrap());
        Receiver::spawn(
            address,
            /* handler */ TxReceiverHandler { tx_batch_maker },
        );

        // The transactions are sent to the `BatchMaker` that assembles them into batches. It then broadcasts
        // (in a reliable manner) the batches to all other workers that share the same `id` as us. Finally, it
        // gathers the 'cancel handlers' of the messages and send them to the `QuorumWaiter`.
        BatchMaker::spawn(
            self.parameters.batch_size,
            self.parameters.max_batch_delay,
            /* rx_transaction */ rx_batch_maker,
            /* tx_message */ tx_quorum_waiter,
            /* workers_addresses */
            self.committee
                .others_workers(&self.name, &self.id)
                .iter()
                .map(|(name, addresses)| (*name, addresses.worker_to_worker))
                .collect(),
        );

        // The `QuorumWaiter` waits for 2f authorities to acknowledge reception of the batch. It then forwards
        // the batch to the `Processor`.
        QuorumWaiter::spawn(
            self.committee.clone(),
            /* stake */ self.committee.stake(&self.name),
            /* rx_message */ rx_quorum_waiter,
            /* tx_batch */ tx_processor,
        );

        // The `Processor` hashes and stores the batch. It then forwards the batch's digest to the `PrimaryConnector`
        // that will send it to our primary machine.
        Processor::spawn(
            self.id,
            self.store.clone(),
            /* rx_batch */ rx_processor,
            /* tx_digest */ tx_primary,
            /* own_batch */ true,
        );

        info!(
            "Worker {} listening to client transactions on {}",
            self.id, address
        );
    }
```



        let (tx_batch_maker, rx_batch_maker) = channel(CHANNEL_CAPACITY);
        let (tx_quorum_waiter, rx_quorum_waiter) = channel(CHANNEL_CAPACITY);
        let (tx_processor, rx_processor) = channel(CHANNEL_CAPACITY);

这三行代码定义了Narwhal Worker节点处理客户端交易时的**核心流水线**。

可以把这三个 Channel 想象成工厂流水线上的三条传送带，它们依次连接了不同的处理车间（组件）。数据流向如下：

**`Network (Client)` -> 传送带1 -> `BatchMaker` -> 传送带2 -> `QuorumWaiter` -> 传送带3 -> `Processor`**

下面是具体的解释：

### 1. `(tx_batch_maker, rx_batch_maker)`：原料传送带
*   **用途**：**接收原始交易**。
*   **连接**：从 **网络接收端 (`TxReceiverHandler`)** 传向 **打包器 (`BatchMaker`)**。
*   **工作原理**：
    *   Worker 在网络端口收到客户端发来的零散交易。
    *   通过 `tx` 发送到通道。
    *   `BatchMaker` 从 `rx` 取出这些交易，把它们“攒”起来，打包成一个大的 `Batch`（批次）。

### 2. `(tx_quorum_waiter, rx_quorum_waiter)`：广播确认传送带
*   **用途**：**等待共识（Quorum）**。
*   **连接**：从 **打包器 (`BatchMaker`)** 传向 **仲裁等待器 (`QuorumWaiter`)**。
*   **工作原理**：
    *   当 `BatchMaker` 生成了一个 Batch 后，它会广播给其他节点的 Worker。
    *   它通过这个通道把任务句柄发给 `QuorumWaiter`。
    *   `QuorumWaiter` 的工作就是“盯着”这个 Batch，直到收到足够多（超过 $2f+1$）的其他节点的确认（Ack），证明数据已经安全备份了。

### 3. `(tx_processor, rx_processor)`：存储处理传送带
*   **用途**：**落盘与汇报**。
*   **连接**：从 **仲裁等待器 (`QuorumWaiter`)** 传向 **处理器 (`Processor`)**。
*   **工作原理**：
    *   一旦 `QuorumWaiter` 确认数据已经安全（达成了 Quorum），它就判定这个 Batch 有效。
    *   它通过这个通道把 Batch 发给 `Processor`。
    *   `Processor` 负责最后的工作：把 Batch 保存到本地数据库（Store），计算哈希 ID，然后把这个 ID 汇报给自己的 Primary 节点（也就是常说的“Header”构造过程的基础）。

### 总结
这三个 Channel 串联起了一个完整的生命周期：
**收单（交易）** $\to$ **打包（Batch） & 广播** $\to$ **确认安全（Quorum）** $\to$ **存盘 & 上报**。