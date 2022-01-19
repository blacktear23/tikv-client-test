use std::time::Duration;
use tikv_client::{TransactionClient, Transaction, RetryOptions, Backoff, CheckLevel, TransactionOptions};
use tokio::runtime::Runtime;

async fn tikv_test() {
    let pd_addrs = vec![String::from("127.0.0.1:2379")];
    let conn = TransactionClient::new(pd_addrs, None).await.unwrap();
    let mut txn = conn.begin_pessimistic().await.unwrap();
    let _ = txn.batch_get(vec![String::from("key1"), String::from("key2")]).await.unwrap();
}

pub async fn sleep(ms: u32) {
    tokio::time::sleep(Duration::from_millis(ms as u64)).await;
}

async fn tikv_transaction1(txn: Transaction) {
    let mut txn = txn;
    println!("txn1 start Lock");
    match txn.lock_keys(vec!["tk2".to_owned()]).await {
        Ok(_) => {}
        Err(err) => {
            println!("txn2 lock key error: {}", err.to_string());
            txn.rollback().await.unwrap();
            return;
        }
    };
    println!("txn1 start put");
    match txn.put("tk2".to_owned(), "tv1".to_owned()).await {
        Ok(_) => {}
        Err(err) => {
            println!("txn1 {:}", err.to_string());
            txn.rollback().await.unwrap();
            return;
        }
    }
    sleep(30000).await;
    txn.commit().await.unwrap();
    println!("txn1 Commit");
}

async fn tikv_transaction2(txn: Transaction) {
    let mut txn = txn;
    println!("txn2 start Lock");
    match txn.lock_keys(vec!["tk2".to_owned()]).await{
        Ok(_) => {}
        Err(err) => {
            println!("txn2 lock key error: {}", err.to_string());
            txn.rollback().await.unwrap();
            return;
        }
    }
    println!("txn2 start put");
    match txn.put("tk2".to_owned(), "tv2".to_owned()).await {
        Ok(_) => {}
        Err(err) => {
            println!("txn2 {:}", err.to_string());
            txn.rollback().await.unwrap();
            return;
        }
    }
    sleep(1000).await;
    txn.commit().await.unwrap();
    println!("txn2 Commit");
}

pub fn get_transaction_option() -> TransactionOptions {
    let opts = TransactionOptions::new_pessimistic();
    let mut retry_opts = RetryOptions::default_pessimistic();
    retry_opts.lock_backoff = Backoff::full_jitter_backoff(2, 500, 1000);
    retry_opts.region_backoff = Backoff::full_jitter_backoff(2, 500, 1000);
    opts.drop_check(CheckLevel::Warn)
        .use_async_commit()
        .try_one_pc()
        .retry_options(retry_opts)
}

#[tokio::main]
async fn main() {
    let runtime = Runtime::new().unwrap();
    let pd_addrs = vec![String::from("127.0.0.1:2379")];
    let conn = TransactionClient::new(pd_addrs.clone(), None).await.unwrap();
    let conn2 = TransactionClient::new(pd_addrs.clone(), None).await.unwrap();
    let txn1 = conn.begin_with_options(get_transaction_option()).await.unwrap();
    runtime.spawn(async move {
        // tikv_test().await;
        tikv_transaction1(txn1).await;
    });
    sleep(1000).await;
    let txn2 = conn2.begin_with_options(get_transaction_option()).await.unwrap();
    runtime.spawn(async move {
        tikv_transaction2(txn2).await;
    });
    sleep(40000).await;
    println!("Finished");
    runtime.shutdown_background();
}
