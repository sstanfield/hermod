#![feature(async_await)]

use std::io;

use futures::executor::ThreadPool;
use futures::future::join;

use std::clone::Clone;
use std::sync::Arc;

use msg_async::*;

fn main() -> io::Result<()> {
    let mut threadpool = ThreadPool::new()?;
    let broker_manager = Arc::new(BrokerManager::new(threadpool.clone()));

    threadpool.run(join(
        start_pub_empty(threadpool.clone(), broker_manager.clone()),
        start_sub_empty(threadpool.clone(), broker_manager.clone()),
    ));
    Ok(())
}
