// XXX This is producing false positives on async fns with reference params.
// Turn back on when it works...
#![allow(clippy::needless_lifetimes)]

use std::collections::HashMap;

use futures::channel::mpsc;
use futures::executor::ThreadPool;
use futures::lock::Mutex;
use futures::sink::SinkExt;
use futures::task::SpawnExt;
use futures::StreamExt;

use super::msglog::*;
use common::types::*;

use crc::{crc32, Hasher32};
use log::{error, info};

use serde_json;

#[derive(Deserialize)]
struct OffsetRecord {
    offset: u64,
}

struct ClientInfo {
    tx: mpsc::Sender<ClientMessage>,
    group_id: String,
    sub_type: SubType,
    is_internal: bool,
}

#[derive(Clone)]
pub enum BrokerMessage {
    Message {
        message: Message,
    },
    NewClient {
        client_name: String,
        group_id: String,
        tx: mpsc::Sender<ClientMessage>,
        is_internal: bool,
    },
    FetchPolicy {
        client_name: String,
        sub_type: SubType,
        position: TopicPosition,
    },
    Fetch {
        client_name: String,
        position: TopicPosition,
    },
    CloseClient {
        client_name: String,
    },
}

struct BrokerData {
    brokers: HashMap<TopicPartition, mpsc::Sender<BrokerMessage>>,
    threadpool: ThreadPool,
    topic_online_tx: mpsc::Sender<BrokerMessage>,
}

pub struct BrokerManager {
    brokers: Mutex<BrokerData>,
}

impl BrokerManager {
    pub fn new(mut threadpool: ThreadPool) -> BrokerManager {
        let tp = TopicPartition {
            topic: "__topic_online".to_string(),
            partition: 0,
        };
        let (topic_online_tx, rx) = mpsc::channel::<BrokerMessage>(1000);
        //let (topic_online_tx, rx) = mpsc::unbounded::<BrokerMessage>();
        if let Err(err) = threadpool.spawn(new_message_broker(rx, tp.clone())) {
            error!(
                "Got error spawning task for partion {}, topic {}, error: {}",
                tp.partition, tp.topic, err
            );
        }
        let mut hm = HashMap::new();
        hm.insert(tp, topic_online_tx.clone());
        BrokerManager {
            brokers: Mutex::new(BrokerData {
                brokers: hm,
                threadpool,
                topic_online_tx,
            }),
        }
    }

    pub async fn expand_topics(&self, topic_name: String) -> Vec<String> {
        let mut result = Vec::<String>::new();
        if topic_name.trim().ends_with('*') {
            let len = topic_name.len();
            let data = self.brokers.lock().await;
            if len == 1 {
                for key in data.brokers.keys() {
                    result.push(key.topic.clone());
                }
            } else {
                for key in data.brokers.keys() {
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
        if data.brokers.contains_key(&tp) {
            return Ok(data.brokers.get(&tp).unwrap().clone());
        }
        let (tx, rx) = mpsc::channel::<BrokerMessage>(1000);
        data.brokers.insert(tp.clone(), tx.clone());
        if let Err(err) = data.threadpool.spawn(new_message_broker(rx, tp.clone())) {
            error!(
                "Got error spawning task for partion {}, topic {}, error: {}",
                partition, topic, err
            );
            Err(())
        } else {
            let payload = format!(
                "{{\"partition\": {}, \"topic\": \"{}\"}}",
                tp.partition, tp.topic
            )
            .into_bytes();
            let mut digest = crc32::Digest::new(crc32::IEEE);
            digest.write(&payload);
            let crc = digest.sum32();
            let message = Message {
                message_type: MessageType::Message,
                topic: "__topic_online".to_string(),
                partition: 0,
                payload_size: payload.len(),
                crc,
                sequence: 0,
                payload,
            };
            if let Err(error) = data
                .topic_online_tx
                .send(BrokerMessage::Message { message })
                .await
            {
                error!("Error sending to broker: {}", error);
            }

            Ok(tx.clone())
        }
    }
}

async fn fetch(
    offset: u64,
    msg_log: &MessageLog,
    client_tx: &mut mpsc::Sender<ClientMessage>,
    client_name: &str,
) -> bool {
    let mut success = true;
    let info = msg_log.get_index(offset);
    if info.is_ok() {
        let info = info.unwrap();
        if let Err(error) = client_tx
            .send(ClientMessage::MessageBatch {
                file_name: msg_log.log_file_name(),
                start: info.position,
                length: msg_log.log_file_end() - info.position,
            })
            .await
        {
            success = false;
            error!(
                "Error fetching ({}) for client {}, dropping: {}",
                offset, client_name, error
            );
        }
    }
    success
}

async fn fetch_with_pos(
    tp: &TopicPartition,
    msg_log: &MessageLog,
    client: &mut ClientInfo,
    client_name: &str,
    position: TopicPosition,
) -> bool {
    let mut bad_client = false;
    match position {
        TopicPosition::Earliest => {
            bad_client = !fetch(0, &msg_log, &mut client.tx, &client_name).await;
        }
        TopicPosition::Latest => {}
        TopicPosition::Current => {
            let commit_topic = format!(
                "__consumer_offsets-{}-{}-{}",
                client.group_id, tp.topic, tp.partition
            );
            let tp_offset = TopicPartition {
                topic: commit_topic,
                partition: tp.partition,
            };
            let offset_log = MessageLog::new(&tp_offset, true);
            if offset_log.is_ok() {
                let mut offset_log = offset_log.unwrap();
                let offset_message = offset_log.get_message(0);
                if offset_message.is_ok() {
                    let offset_message = offset_message.unwrap();
                    let record: OffsetRecord =
                        serde_json::from_slice(&offset_message.payload[..]).unwrap(); // XXX TODO Don't unwrap...
                    bad_client =
                        !fetch(record.offset + 1, &msg_log, &mut client.tx, &client_name).await;
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
        }
        TopicPosition::Offset { offset } => {
            bad_client = !fetch(offset, &msg_log, &mut client.tx, &client_name).await;
        }
    }
    bad_client
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
    let mut client_tx: HashMap<String, ClientInfo> = HashMap::new();
    let mut message = rx.next().await;
    while message.is_some() {
        let mes = message.clone().unwrap();
        match mes {
            BrokerMessage::Message { mut message } => {
                message.sequence = msg_log.offset();

                if let Err(error) = msg_log.append(&message) {
                    error!("Failed to write message to log {}", error);
                    return;
                }

                let mut bad_clients = Vec::<String>::new();
                for tx_key in client_tx.keys() {
                    let client = client_tx.get(tx_key).unwrap();
                    let mut tx = client.tx.clone();
                    let is_internal = client.is_internal;
                    let message = if is_internal {
                        ClientMessage::InternalMessage {
                            message: message.clone(),
                        }
                    } else {
                        match client.sub_type {
                            SubType::Stream => ClientMessage::Message {
                                message: message.clone(),
                            },
                            SubType::Fetch => ClientMessage::MessagesAvailable {
                                topic: tp.topic.clone(),
                                partition: tp.partition,
                            },
                        }
                    };
                    if let Err(err) = tx.send(message).await {
                        error!("Error writiing to client, will close. {}", err);
                        bad_clients.push(tx_key.clone());
                    }
                }
                for bad_client in bad_clients {
                    client_tx.remove(&bad_client);
                }
            }
            BrokerMessage::NewClient {
                client_name,
                group_id,
                tx,
                is_internal,
            } => {
                client_tx.insert(
                    client_name.to_string(),
                    ClientInfo {
                        tx: tx.clone(),
                        group_id,
                        sub_type: SubType::Fetch,
                        is_internal,
                    },
                );
            }
            BrokerMessage::FetchPolicy {
                client_name,
                sub_type,
                position,
            } => {
                if let Some(mut client) = client_tx.get_mut(&client_name) {
                    client.sub_type = sub_type;
                    if sub_type == SubType::Stream {
                        let bad_client =
                            fetch_with_pos(&tp, &msg_log, client, &client_name, position).await;
                        if bad_client {
                            client_tx.remove(&client_name);
                        }
                    }
                }
            }
            BrokerMessage::Fetch {
                client_name,
                position,
            } => {
                if let Some(client) = client_tx.get_mut(&client_name) {
                    let bad_client =
                        fetch_with_pos(&tp, &msg_log, client, &client_name, position).await;
                    if bad_client {
                        client_tx.remove(&client_name);
                    }
                }
            }
            BrokerMessage::CloseClient { client_name } => {
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
