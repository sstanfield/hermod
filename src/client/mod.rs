use std::sync::Arc;

use futures::channel::mpsc;
use futures::executor::ThreadPool;
use futures::io::AsyncReadExt;
use futures::task::SpawnExt;
use futures::StreamExt;

use romio::{TcpListener, TcpStream};

use super::broker::*;
use super::types::*;

use log::info;

pub mod read_input;
use crate::read_input::*;
pub mod message_core;
use crate::message_core::*;
pub mod protocol;
use crate::protocol::*;

pub async fn start_client(
    mut threadpool: ThreadPool,
    io_pool: ThreadPool,
    broker_manager: Arc<BrokerManager>,
) {
    let mut listener = TcpListener::bind(&"127.0.0.1:7878".parse().unwrap())
        .expect("Unable to bind to 127.0.0.1:7878");
    let mut incoming = listener.incoming();

    info!("Client listening on 127.0.0.1:7878");
    let mut connections = 0;

    while let Some(stream) = incoming.next().await {
        threadpool
            .spawn(new_client(
                stream.unwrap(),
                connections,
                broker_manager.clone(),
                threadpool.clone(),
                io_pool.clone(),
            ))
            .unwrap();
        connections += 1;
    }
}

async fn new_client(
    stream: TcpStream,
    idx: u64,
    broker_manager: Arc<BrokerManager>,
    mut threadpool: ThreadPool,
    io_pool: ThreadPool,
) {
    let addr = stream.peer_addr().unwrap();
    let (reader, writer) = stream.split();
    info!("Accepting sub stream from: {}", addr);
    let (broker_tx, rx) = mpsc::channel::<ClientMessage>(1000);
    //let (broker_tx, rx) = mpsc::unbounded::<ClientMessage>();
    let codec = ClientCodec::new();
    // Do this so when message_incoming completes client_incoming is dropped and the connection closes.
    let _client = threadpool
        .spawn_with_handle(client_incoming(broker_tx.clone(), reader, codec.clone()))
        .unwrap();
    let mut mc = MessageCore::new(broker_tx, rx, idx, broker_manager, io_pool, codec);
    mc.message_incoming(writer).await;

    info!("Closing sub stream from: {}", addr);
}
