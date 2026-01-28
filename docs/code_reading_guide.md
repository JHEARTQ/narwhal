# Narwhal 代码阅读指南（结合 Paper.md）

> 目标：把 [Paper.md](../Paper.md) 里的概念（DAG mempool、证书、同步、GC、共识排序）逐一映射到本仓库的 Rust 实现，并给出一条“从入口到端到端链路”的阅读路线。
>
> 说明：仓库 README 指向论文 *Narwhal and Tusk*。本仓库的 `consensus` 实现采用了“偶数轮 leader + f+1 支持”的简化/变体逻辑，并在源码中明确留有 `TODO`（例如 common coin）。因此：本指南以代码为准，同时标注与 `Paper.md` 描述的差异点。

---

## 1. 快速总览：组件分工（从 Paper.md 到代码）

Paper.md 的核心主张是“传播与排序解耦”：

- **Narwhal（mempool / 数据可用性层）**：把交易可靠传播、可用性证明、因果依赖（DAG）维护好。
- **Tusk（共识排序层）**：只对“轻量元数据”（证书/哈希）排序，交易本体按需从 Narwhal 的存储中取回。

在本仓库中，对应的代码结构是：

- **node（二进制入口）**：启动 primary / worker / consensus 任务。
- **worker（数据面）**：接收客户端交易，打包成 batch，在 worker 间可靠扩散并达成“可用性阈值”，把 batch digest 上报给本机 primary。
- **primary（元数据面 / DAG）**：把来自 worker 的 batch digest 组装进 header，跨 primary 广播 header、收集 vote 形成 certificate；维护 DAG 轮次推进；向共识层输出 certificate；并在缺失数据时驱动同步。
- **consensus（排序层）**：从 primary 接收证书流，基于 DAG 结构选 leader 并输出“提交序列”；向 primary 反馈用于 GC。

配套基础设施：

- **network**：TCP + length-delimited framing；`ReliableSender` 用于“必须送达/必须 ACK”的消息；`SimpleSender` 用于 best-effort（同步请求/通知类）。
- **store**：RocksDB + 异步命令通道；支持 `notify_read`（等待某个 key 写入后再返回）。
- **crypto**：ed25519 签名、批量验签、异步签名服务 `SignatureService`。
- **config**：committee/地址/参数/keypair 的 JSON 导入导出。

---

## 2. 论文术语 ↔ 代码实体：对照表

| Paper.md/论文概念 | 代码里的实体 | 关键字段/职责 |
|---|---|---|
| 轮次 Round | `type Round = u64` | primary/consensus/GC 都以 round 为主时间轴 |
| 区块/Block（携带 payload 引用 + parents） | `primary::messages::Header` | `payload: BTreeMap<Digest, WorkerId>` + `parents: BTreeSet<Digest>` |
| 可用性证书 Certificate | `primary::messages::Certificate` | `header + votes(>= quorum_threshold)` |
| 投票 Vote | `primary::messages::Vote` | 对某 header 的签名投票 |
| batch（交易批） | `worker::batch_maker::Batch` | `Vec<Transaction>`；通过 worker-to-worker 广播 |
| 交易 Transaction | `type Transaction = Vec<u8>` | demo/benchmark 级别表示 |
| “推 + 拉”可靠传播/同步 | `ReliableSender` + waiter/synchronizer | 关键：`HeaderWaiter`、`Worker::Synchronizer`、`store.notify_read` |
| 垃圾回收 GC depth | `Parameters::gc_depth` + `GarbageCollector` | 共识 round 推进后，通知 primary/worker 清理 |

入口导出的核心类型：

- `primary::Header` / `primary::Certificate`：对外暴露的 DAG 元数据。
- `worker::Worker`：启动 worker。
- `consensus::Consensus`：排序/提交输出。

---

## 3. 推荐阅读顺序（按“从可运行入口到关键链路”）

### 3.1 从可运行入口开始：node

建议先读：

1. [node/src/main.rs](../node/src/main.rs)
   - CLI：`run primary` / `run worker`。
   - `Primary::spawn(...)`、`Consensus::spawn(...)`、`Worker::spawn(...)` 的启动关系。
   - primary 与 consensus 之间的两条 channel：`tx_new_certificates`（primary→consensus）与 `tx_feedback`（consensus→primary/GC）。

### 3.2 配置与门限：config

2. [config/src/lib.rs](../config/src/lib.rs)
   - `Committee`：stake、阈值：`quorum_threshold()`（约 2f+1 权重）与 `validity_threshold()`（约 f+1 权重）。
   - `Parameters`：header/batch/同步/GC 等关键参数。

### 3.3 密码学与异步签名：crypto

3. [crypto/src/lib.rs](../crypto/src/lib.rs)
   - `Digest` / `Hash` trait。
   - `Signature` 与 `Signature::verify_batch`（证书验签关键）。
   - `SignatureService`：tokio task 持有私钥，外部通过 channel 请求签名。

### 3.4 网络层：network

4. [network/src/receiver.rs](../network/src/receiver.rs)
   - `Receiver::spawn`：对每条 TCP 连接 spawn runner。
   - `MessageHandler`：上层通过 handler 做“反序列化 + 路由 + 可选 ACK”。

5. [network/src/reliable_sender.rs](../network/src/reliable_sender.rs)
   - `ReliableSender::send/broadcast`：必须 ACK 才算成功，否则自动重试。
   - `CancelHandler`：上层 drop 掉 handler = 取消重试。

6. [network/src/simple_sender.rs](../network/src/simple_sender.rs)
   - best-effort：适合同步请求/通知。

### 3.5 存储与“等待写入”：store

7. [store/src/lib.rs](../store/src/lib.rs)
   - 所有读写都走一个异步命令通道（避免多线程 RocksDB 并发复杂度）。
   - `notify_read(key)`：如果 key 不存在，则把 oneshot sender 放入 obligations 队列；写入时统一唤醒。

### 3.6 Worker：交易进入系统的第一站

8. [worker/src/worker.rs](../worker/src/worker.rs)
   - 三条主线：
     - `handle_primary_messages`：接收 primary→worker 的同步/清理命令。
     - `handle_clients_transactions`：客户端交易入口流水线。
     - `handle_workers_messages`：worker↔worker 传播 batch 与响应请求。

9. [worker/src/batch_maker.rs](../worker/src/batch_maker.rs)
   - 把交易按大小或超时 seal 成 batch。
   - 通过 `ReliableSender::broadcast` 推给同 id 的其他 worker。

10. [worker/src/quorum_waiter.rs](../worker/src/quorum_waiter.rs)
    - 关键：收集 ACK 的 stake，达到 `committee.quorum_threshold()` 后才认为 batch 具有可用性。
    - 达成 quorum 后 drop 掉剩余 CancelHandler，从而取消多余重试。

11. [worker/src/processor.rs](../worker/src/processor.rs)
    - hash + store batch（数据面落盘）。
    - 把 digest 通过 `PrimaryConnector` 上报 primary。

12. [worker/src/synchronizer.rs](../worker/src/synchronizer.rs)
    - 处理 primary 发来的 `PrimaryWorkerMessage::Synchronize/Cleanup`。
    - 发 `WorkerMessage::BatchRequest` 拉取缺失 batch；用 `store.notify_read` 等待缺失数据到齐。

### 3.7 Primary：把 batch digest 变成 DAG 元数据，并产生证书

13. [primary/src/primary.rs](../primary/src/primary.rs)
    - 这是最好的“架构总线图”：把 core/proposer/waiter/synchronizer/GC/helper 这些组件连起来。
    - 消息枚举：`PrimaryMessage`（primary↔primary）、`WorkerPrimaryMessage`（worker→primary）、`PrimaryWorkerMessage`（primary→worker）。

14. [primary/src/messages.rs](../primary/src/messages.rs)
    - `Header`：payload+parents+签名。
    - `Vote`：对 header 的签名。
    - `Certificate`：header + votes（>= quorum）。

15. [primary/src/core.rs](../primary/src/core.rs)
    - 核心状态机：处理 `Header/Vote/Certificate`。
    - 关键流程：
      - `process_header`：先要 parents（证书）齐，再要 payload（batch digest 记录）齐，然后才能投票。
      - `process_vote`：`VotesAggregator` 达到 quorum 后组装 certificate 并广播。
      - `process_certificate`：确保证书祖先齐（`deliver_certificate`），再存储、推进 round（把 parents 发给 proposer），并把证书发往 consensus。

16. [primary/src/proposer.rs](../primary/src/proposer.rs)
    - 等到“上一轮证书达到 quorum” + “payload 累积到阈值/超时”，就创建新 header。

17. [primary/src/synchronizer.rs](../primary/src/synchronizer.rs)
    - `get_parents` / `missing_payload` / `deliver_certificate`：缺啥就把 header/cert 交给 waiter。

18. [primary/src/header_waiter.rs](../primary/src/header_waiter.rs)
    - 对缺失 parents（证书）与缺失 payload（batch digest 记录）分别发起同步。
    - 使用 `store.notify_read` 等依赖到齐，再把 header loopback 给 core 继续处理。

19. [primary/src/certificate_waiter.rs](../primary/src/certificate_waiter.rs)
    - 等证书所有父证书到齐，再 loopback 给 core。

20. [primary/src/garbage_collector.rs](../primary/src/garbage_collector.rs)
    - 从 consensus 收到“已提交证书”序列，更新 `consensus_round`，并通知 workers cleanup。

### 3.8 Consensus：对证书 DAG 做确定性排序

21. [consensus/src/lib.rs](../consensus/src/lib.rs)
    - 本 crate 维护一个内存 DAG（按 round、按作者索引）。
    - 逻辑要点：
      - 仅在偶数轮选 leader（`if r % 2 != 0 || r < 4 { continue; }`）。
      - `leader(round)` 目前是 round-robin（测试下固定 coin=0），源码有 `TODO` 提示未来用 common coin。
      - leader 需要在子层（r-1）获得至少 `validity_threshold()`（约 f+1）stake 的引用支持。
      - 一旦能提交 leader，会回溯 `order_leaders`，并对每个 leader 做一次 `order_dag`（DFS 展开依赖）形成提交序列。
    - 输出：
      - `tx_primary`：反馈给 primary/GC 用于清理。
      - `tx_output`：应用层输出（node 中 `analyze` 目前空实现）。

---

## 4. 两条“端到端主链路”（建议按这个顺序跟日志/断点）

### 4.1 交易进入系统：Client → Worker → Primary

1. Client 把交易发到 worker 的 `transactions` 端口。
2. `TxReceiverHandler` 把 bytes 送入 `BatchMaker`。
3. `BatchMaker::seal`：打包成 `WorkerMessage::Batch`，通过 `ReliableSender` 广播到同 id 的其他 worker。
4. `QuorumWaiter`：等待 ACK 累积 stake 达到 quorum，认为 batch 达到可用性。
5. `Processor`：
   - hash batch 得到 digest。
   - `store.write(digest, batch)` 落盘。
   - 发送 `WorkerPrimaryMessage::{OurBatch|OthersBatch}` 给 primary。

这里对应 Paper.md 的“Worker 负责数据面、Primary 只处理 hash 元数据”。

### 4.2 形成 DAG 元数据与证书：Primary → Primary → Certificate → Consensus → GC

1. primary 收到 `OurBatch(digest, worker_id)` 后，`Proposer` 把 digest 累积到 payload。
2. 当 `Core` 收到足够的 parent certificates（上一轮 quorum）后，`Proposer` 创建 `Header`（带 payload + parents）并签名。
3. `Core::process_own_header`：可靠广播 header 到其他 primaries，然后本地继续 `process_header`。
4. 其他 primary 收到 header：检查 parents + payload 是否齐全（缺就同步），齐全则投票 `Vote` 回发 header 作者。
5. header 作者 `VotesAggregator` 达到 quorum 后组装 `Certificate`，可靠广播证书。
6. 所有 primary 把证书写入 store，并把证书发送给 `consensus` crate。
7. `Consensus` 从证书流中选 leader、输出提交序列；通过 `tx_primary` 回传给 primary，触发 `GarbageCollector` 更新 round 并通知 worker 清理。

---

## 5. 同步（Pull）机制在代码里怎么落地？

Paper.md 里强调“推 + 拉”。在代码里可以从两个角度理解：

- **推（push）**：正常路径尽量广播（header/certificate 的 reliable broadcast；batch 的 reliable broadcast）。
- **拉（pull）**：任何缺失都走“同步器 + waiter + store.notify_read”的闭环。

关键闭环：

- primary 侧：
  - `Synchronizer::get_parents` 缺证书 → `HeaderWaiter::SyncParents` → `PrimaryMessage::CertificatesRequest` → `Helper` 回发证书 → store 写入 → waiter 唤醒 → header loopback。
  - `Synchronizer::missing_payload` 缺 payload 记录 → `HeaderWaiter::SyncBatches` → `PrimaryWorkerMessage::Synchronize` 通知 worker 去拉 batch。
- worker 侧：
  - `Synchronizer` 收到 `PrimaryWorkerMessage::Synchronize(digests, target)` → 给 target worker 发 `WorkerMessage::BatchRequest` → 其他 worker 用 `Helper` 从 store 读 batch 回发 → 本地 `Processor` 写入 store → waiter（`notify_read`）唤醒。

这套设计把“等待依赖到齐”从核心状态机里剥离出去，核心只要在依赖齐全时继续处理。

---

## 6. 与 Paper.md 的差异与对应读法（避免读歪）

- Paper.md 描述的 Tusk 有 wave/随机硬币/递归回溯等机制；而本仓库的 [consensus/src/lib.rs](../consensus/src/lib.rs) 采用了“偶数轮 leader + f+1 支持”的提交规则，并显式写了 `TODO: use common coin`。
- 因此阅读建议是：
  1) 先按本仓库 `consensus` 的实现理解“怎么从 DAG 里挑 leader 并输出提交序列”；
  2) 再回头对照 Paper.md 的 wave/coin 机制，理解它们在理论上如何提升异步活性与抗对手能力；
  3) 最后看源码 TODO 位置，理解未来要替换/增强的点在哪里。

---

## 7. 调试与观察建议（读代码时最有用的几个抓手）

- 日志级别：运行二进制时加 `-vvv`（或更多）可以看到 `debug/trace`。
- 关键日志点：
  - worker：`BatchMaker::seal`（benchmark feature 下还会打印样本交易/批大小）。
  - primary：`Core` 的 `Processing ...` 日志能看到 header/vote/cert 的流转。
  - consensus：`Committed ...` 日志（feature=benchmark 下会输出 `Committed header -> digest` 便于统计）。
- 常见“卡住”的原因：同步器在等依赖（parents/payload/batch），通常可以从 waiter 的 debug 日志与 store 的 `notify_read` 行为定位。
- 想快速理解链路时，建议把断点打在：
  - worker：`BatchMaker::seal`、`QuorumWaiter::run` 达到 quorum 的分支、`Processor` 写入 store 后。
  - primary：`Proposer::make_header`、`Core::process_header/process_vote/process_certificate`。
  - consensus：`leader`、`order_leaders`、`order_dag`。

---

## 8. 进一步阅读：测试与 benchmark

- 单元测试分布在各 crate 的 `src/tests/` 下（例如：primary/worker/network/crypto/store）。
- benchmark 脚本在 [benchmark/](../benchmark/)：用于部署、生成配置、收集性能日志。

---

## 9. 你可以接下来怎么用这份指南

- 只想跑通系统：从 node 入口看 CLI → 看 config 的 committee/keypair → 跟一次“交易→batch→header→cert→commit”。
- 想改协议：优先读 primary 的 `Core/Proposer/Synchronizer` 与 consensus 的 leader/commit 逻辑。
- 想改吞吐：优先看 worker pipeline 的 batch size、max delay、以及 network 的可靠发送机制。
