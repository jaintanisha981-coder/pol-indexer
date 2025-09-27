use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use dotenvy::dotenv;
use ethers::prelude::*;
use ethers::types::{Filter, H160, H256, U256};
use tokio::sync::Mutex;

const POL_CONTRACT: &str = "0x455e53cbb86018ac2b8092fdcd39d8444affc3f6";
const BINANCE_ADDRESSES: [&str; 6] = [
    "0xF977814e90dA44bFA03b6295A0616a897441aceC",
    "0xe7804c37c13166fF0b37F5aE0BB07A3aEbb6e245",
    "0x505e71695E9bc45943c58adEC1650577BcA68fD9",
    "0x290275e3db66394C52272398959845170E4DCb88",
    "0xD5C08681719445A5Fdce2Bda98b341A49050d821",
    "0x082489A616aB4D46d1947eE3F912e080815b08DA",
];

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok(); // load .env file if present
    let rpc = std::env::var("POLYGON_RPC").unwrap_or_else(|_| "https://polygon-rpc.com".to_string());
    let provider = Provider::<Http>::try_from(rpc)?;
    let provider = Arc::new(provider);

    let binance: Vec<H160> = BINANCE_ADDRESSES
        .iter()
        .map(|s| s.parse::<H160>().expect("bad address"))
        .collect();

    let running_total = Arc::new(Mutex::new(U256::zero()));

    let transfer_topic: H256 = H256::from_slice(ethers::utils::keccak256("Transfer(address,address,uint256)").as_slice());

    let mut last_block = provider.get_block_number().await?.as_u64();

    // Ensure DB file / schema exists (runs schema.sql)
    initialize_db().await?;

    loop {
        let latest = provider.get_block_number().await?.as_u64();
        if latest > last_block {
            for block_num in (last_block + 1)..=latest {
                if let Err(e) = process_block(
                    block_num,
                    provider.clone(),
                    transfer_topic,
                    &binance,
                    running_total.clone(),
                )
                .await
                {
                    eprintln!("Error processing block {}: {:?}", block_num, e);
                }
            }
            last_block = latest;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn initialize_db() -> Result<()> {
    let schema = include_str!("../schema.sql").to_string();
    tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
        let conn = rusqlite::Connection::open("pol_indexer.sqlite3")?;
        conn.execute_batch(&schema)?;
        Ok(())
    })
    .await??;
    Ok(())
}

async fn process_block(
    block_number: u64,
    provider: Arc<Provider<Http>>,
    transfer_topic: H256,
    binance: &Vec<H160>,
    running_total: Arc<Mutex<U256>>,
) -> Result<()> {
    let block = provider
        .get_block(block_number)
        .await?
        .ok_or_else(|| anyhow::anyhow!("block not found"))?;
    let ts = block.timestamp.as_u64() as i64;

    {
        let block_hash = block.hash.unwrap_or_default();
        let block_num = block_number as i64;
        tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
            let conn = rusqlite::Connection::open("pol_indexer.sqlite3")?;
            conn.execute(
                "INSERT OR IGNORE INTO blocks(block_number, block_hash, timestamp) VALUES(?1, ?2, ?3)",
                rusqlite::params![block_num, format!("{:?}", block_hash), ts],
            )?;
            Ok(())
        })
        .await??;
    }

    let pol_addr: H160 = POL_CONTRACT.parse()?;
    let filter = Filter::new()
        .address(pol_addr)
        .from_block(block_number)
        .to_block(block_number)
        .topic0(ValueOrArray::Value(transfer_topic));

    let logs = provider.get_logs(&filter).await?;
    for log in logs.into_iter() {
        if log.topics.len() < 3 {
            continue;
        }
        let from = H160::from_slice(&log.topics[1].as_fixed_bytes()[12..]);
        let to = H160::from_slice(&log.topics[2].as_fixed_bytes()[12..]);
        let amount = U256::from_big_endian(&log.data.0);

        let tx_hash = log.transaction_hash.unwrap_or_default();
        let log_index = log.log_index.unwrap_or_default().as_u64() as i64;
        let blocknum_i64 = block_number as i64;

        let tx_hash_s = format!("{:?}", tx_hash);
        let pol_s = format!("{:?}", pol_addr); 
        let from_s = format!("{:?}", from);
        let to_s = format!("{:?}", to);
        let amount_s = amount.to_string();
        let ts_copy = ts;

        // first DB write
        let pol_s_clone1 = pol_s.clone();
        tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
            let conn = rusqlite::Connection::open("pol_indexer.sqlite3")?;
            conn.execute(
                "INSERT OR IGNORE INTO token_transfers(tx_hash, block_number, log_index, token_address, \"from\", \"to\", amount, decimals, timestamp)
                 VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                rusqlite::params![tx_hash_s, blocknum_i64, log_index, pol_s_clone1, from_s, to_s, amount_s, 18i64, ts_copy],
            )?;
            Ok(())
        })
        .await??;

        // update running total
        let from_is_bin = binance.iter().any(|a| *a == from);
        let to_is_bin = binance.iter().any(|a| *a == to);

        if to_is_bin && !from_is_bin {
            let mut guard = running_total.lock().await;
            *guard = (*guard) + amount;
        } else if from_is_bin && !to_is_bin {
            let mut guard = running_total.lock().await;
            *guard = (*guard).checked_sub(amount).unwrap_or(U256::zero());
        }

        // snapshot DB write
        let pol_s_clone2 = pol_s.clone();
        let snapshot = running_total.lock().await.clone();
        tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
            let conn = rusqlite::Connection::open("pol_indexer.sqlite3")?;
            conn.execute(
                "INSERT INTO net_flows(token_address, exchange_label, snapshot_time, cumulative_amount_text) VALUES(?1, ?2, ?3, ?4)",
                rusqlite::params![pol_s_clone2, "Binance", ts_copy, snapshot.to_string()],
            )?;
            Ok(())
        })
        .await??;
    }

    Ok(())
}
