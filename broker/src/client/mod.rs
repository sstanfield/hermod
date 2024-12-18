use std::net::SocketAddr;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

use futures::channel::mpsc;
use futures::lock::Mutex;

use tokio::net::{TcpListener, TcpStream};

use super::broker::*;
use common::types::*;
use common::util::*;

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

    pub async fn is_shutdown(&self, timeout_ms: u128) {
        // Note this not a well behaved async function but it is only intended
        // to be run at shutdown and on the ctrlc signal thread.
        let start_time = get_epoch_ms();
        let mut going_down = true;
        while going_down {
            let count = self.count.lock().await;
            if *count == 0 {
                going_down = false;
            }
            let time = get_epoch_ms();
            if (time - start_time) > timeout_ms {
                error!("Timed out waiting for clients to shutdown!");
                going_down = true;
            }
            if !going_down {
                sleep(Duration::from_millis(200));
            }
        }
    }
}

pub async fn start_client(
    client_manager: Arc<ClientManager>,
    broker_manager: Arc<BrokerManager>,
    decoder_factory: ProtocolServerDecoderFactory,
    encoder_factory: ProtocolServerEncoderFactory,
    bind_addr: SocketAddr,
) {
    match TcpListener::bind(&bind_addr).await {
        Ok(listener) => {
            info!("Client listening on {}", bind_addr);
            let mut connections = 0;

            while let Ok((stream, _peer_addr)) = listener.accept().await {
                tokio::task::spawn(new_client(
                    stream,
                    connections,
                    Arc::clone(&client_manager),
                    broker_manager.clone(),
                    decoder_factory,
                    encoder_factory,
                ));
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
    decoder_factory: ProtocolServerDecoderFactory,
    encoder_factory: ProtocolServerEncoderFactory,
) {
    client_manager.inc_clients().await;
    let addr = stream.peer_addr().unwrap();
    let (reader, writer) = stream.into_split();
    info!("Accepting sub stream from: {}", addr);
    let (broker_tx, rx) = mpsc::channel::<ClientMessage>(1000);
    // Do this so when message_incoming completes client_incoming is dropped and the connection closes.
    tokio::task::spawn(client_incoming(broker_tx.clone(), reader, decoder_factory));
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
