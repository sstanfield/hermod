use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;

use futures::channel::mpsc;
use futures::executor::ThreadPool;
use futures::lock::Mutex;
use futures::sink::SinkExt;
use futures::task::SpawnExt;
use futures::StreamExt;

use super::types::*;

use log::{error, info};

pub struct BrokerManager {
    brokers: Mutex<(
        HashMap<TopicPartition, mpsc::Sender<BrokerMessage>>,
        ThreadPool,
    )>,
}

impl BrokerManager {
    pub fn new(threadpool: ThreadPool) -> BrokerManager {
        BrokerManager {
            brokers: Mutex::new((HashMap::new(), threadpool)),
        }
    }

    pub async fn get_broker_tx(
        &self,
        tp: TopicPartition,
    ) -> Result<mpsc::Sender<BrokerMessage>, ()> {
        let mut data = self.brokers.lock().await;
        let partition = tp.partition;
        let topic = tp.topic.clone(); // Use for error logging.
        let brokers = &mut data.0;
        if brokers.contains_key(&tp) {
            return Ok(brokers.get(&tp).unwrap().clone());
        }
        let (tx, rx) = mpsc::channel::<BrokerMessage>(10);
        brokers.insert(tp.clone(), tx.clone());
        drop(brokers);
        let threadpool = &mut data.1;
        if let Err(_) = threadpool.spawn(new_message_broker(rx, tp)) {
            error!(
                "Got error spawning task for partion {}, topic {}",
                partition, topic
            );
            Err(())
        } else {
            Ok(tx.clone())
        }
    }
}

async fn new_message_broker(mut rx: mpsc::Receiver<BrokerMessage>, tp: TopicPartition) {
    info!(
        "Broker starting for partition {}, topic {}.",
        tp.partition, tp.topic
    );
    let log_file_name = format!("{}.{}.log", tp.topic, tp.partition);
    let log_file = OpenOptions::new()
        .read(false)
        .write(true)
        .append(true)
        .create(true)
        .open(&log_file_name);
    if let Err(_) = log_file {
        error!("Failed to open log file {}", log_file_name);
        return;
    }
    let mut log_file = log_file.unwrap();
    let mut client_tx: HashMap<String, mpsc::Sender<ClientMessage>> = HashMap::new();
    let mut sequence = 0;
    let mut message = rx.next().await;
    while message.is_some() {
        let mes = message.clone().unwrap();
        match mes {
            BrokerMessage::Message(mut message) => {
                message.sequence = sequence;

                let v = format!("{{ \"topic\": \"{}\", \"payload_size\": {}, \"checksum\": \"{}\", \"sequence\": {} }}",
                                   message.topic, message.payload_size, message.checksum, message.sequence);
                if let Err(_) = log_file.write(v.as_bytes()) {
                    error!("Failed to write message to log file: {}", log_file_name);
                }
                if let Err(_) = log_file.write(&message.payload) {
                    error!("Failed to write payload to log file: {}", log_file_name);
                }
                if let Err(_) = log_file.flush() {
                    error!("Failed to flush log file: {}", log_file_name);
                }

                sequence += 1;
                for tx in client_tx.values_mut() {
                    if let Err(_) = tx.send(ClientMessage::Message(message.clone())).await {
                        // XXX Revome bad client.
                    }
                }
            }
            BrokerMessage::NewClient(client_name, tx) => {
                client_tx.insert(client_name.to_string(), tx.clone());
            }
            BrokerMessage::CloseClient(client_name) => {
                client_tx.remove(&client_name);
            }
        };
        message = rx.next().await;
    }
    info!(
        "Broker ending for partition {}, topic {}.",
        tp.partition, tp.topic
    );
}
