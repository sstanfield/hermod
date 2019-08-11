use std::net::SocketAddr;
use std::sync::Arc;

use futures::channel::mpsc;
use futures::executor::ThreadPool;
use futures::io::AsyncReadExt;
use futures::lock::Mutex;
use futures::task::SpawnExt;
use futures::StreamExt;

use romio::{TcpListener, TcpStream};

use super::broker::*;
use common::types::*;

use log::{error, info};

pub mod read_input;
use crate::read_input::*;
pub mod message_core;
use crate::message_core::*;

pub struct ClientManager {
    count: Arc<Mutex<usize>>,
}

impl Default for ClientManager {
    fn default() -> Self {
        ClientManager {
            count: Arc::new(Mutex::new(0)),
        }
    }
}

impl ClientManager {
    pub fn new() -> ClientManager {
        Default::default()
    }

    pub async fn inc_clients(&self) {
        let mut count = self.count.lock().await;
        *count += 1;
    }

    pub async fn dec_clients(&self) {
        let mut count = self.count.lock().await;
        if *count > 0 {
            *count -= 1;
        }
    }

    pub async fn is_shutdown(&self) {
        let mut going_down = true;
        while going_down {
            let count = self.count.lock().await;
            if *count == 0 {
                going_down = false;
            }
        }
    }
}

pub async fn start_client(
    mut threadpool: ThreadPool,
    client_manager: Arc<ClientManager>,
    broker_manager: Arc<BrokerManager>,
    decoder_factory: ProtocolDecoderFactory,
    encoder_factory: ProtocolEncoderFactory,
    bind_addr: SocketAddr,
) {
    match TcpListener::bind(&bind_addr) {
        Ok(mut listener) => {
            let mut incoming = listener.incoming();

            info!("Client listening on {}", bind_addr);
            let mut connections = 0;

            while let Some(stream) = incoming.next().await {
                threadpool
                    .spawn(new_client(
                        stream.unwrap(),
                        connections,
                        Arc::clone(&client_manager),
                        broker_manager.clone(),
                        threadpool.clone(),
                        decoder_factory,
                        encoder_factory,
                    ))
                    .unwrap();
                connections += 1;
            }
        }
        Err(err) => {
            error!("Unable to bind to address {}, {}", bind_addr, err);
        }
    }
}

async fn new_client(
    stream: TcpStream,
    idx: u64,
    client_manager: Arc<ClientManager>,
    broker_manager: Arc<BrokerManager>,
    mut threadpool: ThreadPool,
    decoder_factory: ProtocolDecoderFactory,
    encoder_factory: ProtocolEncoderFactory,
) {
    client_manager.inc_clients().await;
    let addr = stream.peer_addr().unwrap();
    let (reader, writer) = stream.split();
    info!("Accepting sub stream from: {}", addr);
    let (broker_tx, rx) = mpsc::channel::<ClientMessage>(1000);
    // Do this so when message_incoming completes client_incoming is dropped and the connection closes.
    let _client = threadpool
        .spawn_with_handle(client_incoming(broker_tx.clone(), reader, decoder_factory))
        .unwrap();
    let client_name_unique = format!("Client_{}:{}", idx, addr);
    let mut mc = MessageCore::new(
        broker_tx,
        rx,
        &client_name_unique,
        broker_manager,
        encoder_factory,
        writer,
    );
    mc.message_incoming().await;

    info!("Closing sub stream from: {}", addr);
    client_manager.dec_clients().await;
}
