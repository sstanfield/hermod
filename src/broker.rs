use std::collections::HashMap;

use futures::channel::mpsc;
use futures::executor::ThreadPool;
use futures::lock::Mutex;
use futures::sink::SinkExt;
use futures::task::SpawnExt;
use futures::StreamExt;

use super::msglog::*;
use super::types::*;

use log::{error, info};

use serde_json;

#[derive(Deserialize)]
struct OffsetRecord {
    group_id: String,
    partition: u64,
    topic: String,
    offset: u64,
}

pub struct BrokerManager {
    brokers: Mutex<(
        HashMap<TopicPartition, mpsc::Sender<BrokerMessage>>,
        ThreadPool,
        mpsc::Sender<BrokerMessage>,
    )>,
}

impl BrokerManager {
    pub fn new(mut threadpool: ThreadPool) -> BrokerManager {
        let tp = TopicPartition {
            topic: "__topic_online".to_string(),
            partition: 0,
        };
        let (topic_online_tx, rx) = mpsc::channel::<BrokerMessage>(10);
        if let Err(_) = threadpool.spawn(new_message_broker(rx, tp.clone())) {
            error!(
                "Got error spawning task for partion {}, topic {}",
                tp.partition, tp.topic
            );
        }
        BrokerManager {
            brokers: Mutex::new((HashMap::new(), threadpool, topic_online_tx)),
        }
    }

    pub async fn expand_topics(&self, topic_name: String) -> Vec<String> {
        let mut result = Vec::<String>::new();
        if topic_name.trim().ends_with("*") {
            let len = topic_name.len();
            let mut data = self.brokers.lock().await;
            let brokers = &mut data.0;
            if len == 1 {
                for key in brokers.keys() {
                    result.push(key.topic.clone());
                }
            } else {
                for key in brokers.keys() {
                    if key.topic.starts_with(&topic_name[..len - 1]) {
                        result.push(key.topic.clone());
                    }
                }
            }
        } else {
            result.push(topic_name);
        }
        result
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
        if let Err(_) = threadpool.spawn(new_message_broker(rx, tp.clone())) {
            error!(
                "Got error spawning task for partion {}, topic {}",
                partition, topic
            );
            Err(())
        } else {
            drop(threadpool);
            let topic_online_tx = &mut data.2;
            let payload = format!(
                "{{\"partition\": {}, \"topic\": \"{}\"}}",
                tp.partition, tp.topic
            )
            .into_bytes();
            let message = Message {
                message_type: MessageType::Message,
                topic: "__topic_online".to_string(),
                payload_size: payload.len(),
                checksum: "".to_string(),
                sequence: 0,
                payload,
            };
            if let Err(error) = topic_online_tx.send(BrokerMessage::Message(message)).await {
                error!("Error sending to broker: {}", error);
            }

            Ok(tx.clone())
        }
    }
}

async fn new_message_broker(mut rx: mpsc::Receiver<BrokerMessage>, tp: TopicPartition) {
    info!(
        "Broker starting for partition {}, topic {}.",
        tp.partition, tp.topic
    );

    let msg_log = MessageLog::new(&tp, tp.topic.starts_with("__consumer_offsets"));
    if let Err(error) = msg_log {
        error!("Failed to open the message log {}", error);
        return;
    }
    let mut msg_log = msg_log.unwrap();
    let mut client_tx: HashMap<String, mpsc::Sender<ClientMessage>> = HashMap::new();
    let mut message = rx.next().await;
    while message.is_some() {
        let mes = message.clone().unwrap();
        match mes {
            BrokerMessage::Message(mut message) => {
                message.sequence = msg_log.offset();

                if let Err(error) = msg_log.append(&message) {
                    error!("Failed to message log {}", error);
                    return;
                }

                for tx in client_tx.values_mut() {
                    if let Err(_) = tx.send(ClientMessage::Message(message.clone())).await {
                        // XXX Remove bad client.
                    }
                }
            }
            BrokerMessage::NewClient(client_name, group_id, mut tx) => {
                let commit_topic = format!(
                    "__consumer_offsets-{}-{}-{}",
                    group_id, tp.topic, tp.partition
                );
                let tp_offset = TopicPartition {
                    topic: commit_topic,
                    partition: tp.partition,
                };
                let offset_log = MessageLog::new(&tp_offset, false);
                if offset_log.is_ok() {
                    let mut offset_log = offset_log.unwrap();
                    let offset_message = offset_log.get_message(0);
                    if offset_message.is_ok() {
                        let offset_message = offset_message.unwrap();
                        let record: OffsetRecord =
                            serde_json::from_slice(&offset_message.payload[..]).unwrap(); // Don't unwrap...
                        let info = msg_log.get_index(record.offset).unwrap(); // XXX don't unwrap...
                        if let Err(_) = tx
                            .send(ClientMessage::MessageBatch(
                                msg_log.log_file_name(),
                                info.position,
                                msg_log.log_file_end() - info.position,
                            ))
                            .await
                        {
                            // XXX Remove bad client.
                        }
                    } else {
                        error!(
                            "Error retrieving consumer offset: {}",
                            offset_message.unwrap_err()
                        );
                    }
                } else {
                    error!(
                        "Error retrieving consumer offset log: {}",
                        offset_log.unwrap_err()
                    );
                }
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
