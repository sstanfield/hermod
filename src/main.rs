#![feature(async_await)]

use std::io;

use futures::channel::mpsc;
use futures::executor::ThreadPool;
use futures::task::SpawnExt;

use msg_async::*;

fn main() -> io::Result<()> {
    let (tx, rx) = mpsc::channel::<BrokerMessage>(10);
    let mut threadpool = ThreadPool::new()?;
    let tx2 = tx.clone();
    let pub_pool = threadpool.clone();
    threadpool.spawn(start_pub_empty(tx2, pub_pool)).unwrap();
    //let child = thread::spawn(move || {
    //    executor::block_on(start_pub(tx2, pub_pool)).unwrap();
    //});
    let sub_pool = threadpool.clone();
    threadpool
        .spawn(start_sub_empty(tx.clone(), sub_pool))
        .unwrap();
    threadpool.run(new_message_broker_concurrent(rx));
    //child.join().unwrap();
    //child_broker.join().unwrap();
    Ok(())
}
