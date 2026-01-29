// Copyright(C) Facebook, Inc. and its affiliates.
use anyhow::{Context, Result};
use clap::{crate_name, crate_version, App, AppSettings, ArgMatches, SubCommand};
use config::Export as _;
use config::Import as _;
use config::{Committee, KeyPair, Parameters, WorkerId};
use consensus::Consensus;
use env_logger::Env;
use primary::{Certificate, Primary};
use store::Store;
use tokio::sync::mpsc::{channel, Receiver};
use worker::Worker;

/// The default channel capacity.
pub const CHANNEL_CAPACITY: usize = 1_000;

#[tokio::main]
async fn main() -> Result<()> {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .about("A research implementation of Narwhal and Tusk.")
        .args_from_usage("-v... 'Sets the level of verbosity'")
        .subcommand(
            SubCommand::with_name("generate_keys")
                .about("Print a fresh key pair to file")
                .args_from_usage("--filename=<FILE> 'The file where to print the new key pair'"),
        )
        .subcommand(
            SubCommand::with_name("run")
                .about("Run a node")
                .args_from_usage("--keys=<FILE> 'The file containing the node keys'")
                .args_from_usage("--committee=<FILE> 'The file containing committee information'")
                .args_from_usage("--parameters=[FILE] 'The file containing the node parameters'")
                .args_from_usage("--store=<PATH> 'The path where to create the data store'")
                .subcommand(SubCommand::with_name("primary").about("Run a single primary"))
                .subcommand(
                    SubCommand::with_name("worker")
                        .about("Run a single worker")
                        .args_from_usage("--id=<INT> 'The worker id'"),
                )
                .setting(AppSettings::SubcommandRequiredElseHelp),
        )
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .get_matches();

    let log_level = match matches.occurrences_of("v") {
        0 => "error",
        1 => "warn",
        2 => "info",
        3 => "debug",
        _ => "trace",
    };
    let mut logger = env_logger::Builder::from_env(Env::default().default_filter_or(log_level));
    #[cfg(feature = "benchmark")]
    logger.format_timestamp_millis();
    logger.init();

    match matches.subcommand() {
        ("generate_keys", Some(sub_matches)) => KeyPair::new()
            .export(sub_matches.value_of("filename").unwrap())
            .context("Failed to generate key pair")?,
        ("run", Some(sub_matches)) => run(sub_matches).await?,
        _ => unreachable!(),
    }
    Ok(())
}

// Runs either a worker or a primary.
/// 运行节点的主逻辑（Worker 或 Primary）
///
/// # 架构概览
/// Narwhal 节点可以是：
/// - **Primary**: 维护 Mempool 协议头（Header）和共识（Consensus）。
/// - **Worker**: 负责交易数据的存储和广播。
async fn run(matches: &ArgMatches<'_>) -> Result<()> {
    let key_file = matches.value_of("keys").unwrap();
    let committee_file = matches.value_of("committee").unwrap();
    let parameters_file = matches.value_of("parameters");
    let store_path = matches.value_of("store").unwrap();

    // Read the committee and node's keypair from file.
    // 读取密钥对和委员会配置
    let keypair = KeyPair::import(key_file).context("Failed to load the node's keypair")?;
    let committee =
        Committee::import(committee_file).context("Failed to load the committee information")?;

    // Load default parameters if none are specified.
    let parameters = match parameters_file {
        Some(filename) => {
            Parameters::import(filename).context("Failed to load the node's parameters")?
        }
        None => Parameters::default(),
    };

    // Make the data store.
    // 初始化持久化存储（RocksDB）
    let store = Store::new(store_path).context("Failed to create a store")?;

    // Channels the sequence of certificates.
    // 共识输出通道：Consensus -> Client/Application
    // 经过共识排序后的 Certificate 会被发送到这里
    let (tx_output, rx_output) = channel(CHANNEL_CAPACITY);

    // Check whether to run a primary, a worker, or an entire authority.
    match matches.subcommand() {
        // Spawn the primary and consensus core.
        // 启动 Primary 节点（包含 Primary 核心和 Consensus 模块）
        ("primary", _) => {
            // Channel 1: Primary -> Consensus
            // Primary 将收集齐 2f+1 签名的完整 Certificate 发送给 Consensus
            let (tx_new_certificates, rx_new_certificates) = channel(CHANNEL_CAPACITY);

            // Channel 2: Consensus -> Primary
            // Consensus 将已经排序（Commit）的 Certificate 发回 Primary
            // 用途：Primary 收到后可以进行垃圾回收（GC），清理过旧的历史数据
            let (tx_feedback, rx_feedback) = channel(CHANNEL_CAPACITY);

            // 1. 启动 Primary 模块
            // 职责：广播 Header，收集 Vote，生成 Certificate，构建 DAG
            Primary::spawn(
                keypair,
                committee.clone(),
                parameters.clone(),
                store,
                /* tx_consensus */ tx_new_certificates,
                /* rx_consensus */ rx_feedback,
            );

            // 2. 启动 Consensus 模块
            // 职责：接收 Certificate，运行 Tusk 算法选 Leader，输出确定的提交顺序
            Consensus::spawn(
                committee,
                parameters.gc_depth,
                /* rx_primary */ rx_new_certificates,
                /* tx_primary */ tx_feedback,
                tx_output,
            );
        }

        // Spawn a single worker.
        // 启动 Worker 节点
        ("worker", Some(sub_matches)) => {
            let id = sub_matches
                .value_of("id")
                .unwrap()
                .parse::<WorkerId>()
                .context("The worker id must be a positive integer")?;
            Worker::spawn(keypair.name, id, committee, parameters, store);
        }
        _ => unreachable!(),
    }

    // Analyze the consensus' output.
    // 将共识输出流传递给分析函数（应用层）
    analyze(rx_output).await;

    // If this expression is reached, the program ends and all other tasks terminate.
    unreachable!();
}

/// 处理共识输出流
///
/// 当共识模块确定了 Certificate 的线性顺序后，会通过 channel 发送到这里。
/// 这是连接"共识层"与"执行层/应用层"的接口。
async fn analyze(mut rx_output: Receiver<Certificate>) {
    while let Some(_certificate) = rx_output.recv().await {
        // NOTE: Here goes the application logic.
        // 这里实现具体的业务逻辑。
        // 例如：
        // 1. 解析 certificate 中的 payload (Batches)
        // 2. 从 Worker 获取实际交易数据
        // 3. 按顺序执行交易
    }
}
