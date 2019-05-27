use std::collections::HashMap;

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
    let mut client_tx: HashMap<String, mpsc::Sender<ClientMessage>> = HashMap::new();
    let mut sequence = 0;
    let mut message = rx.next().await;
    while message.is_some() {
        let mes = message.clone().unwrap();
        match mes {
            BrokerMessage::Message(mut message) => {
                message.sequence = sequence;
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
