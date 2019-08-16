use std::cell::Cell;
use std::collections::HashMap;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

use futures::channel::mpsc;
use futures::executor::ThreadPool;
use futures::lock::Mutex;
use futures::sink::SinkExt;
use futures::task::SpawnExt;
use futures::StreamExt;

use super::msglog::*;
use common::types::*;
use common::util::*;

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
    needs_fetch: Cell<bool>,
}

#[derive(Clone)]
pub enum BrokerMessage {
    Message {
        message: Message,
    },
    MessageBatch {
        messages: Vec<Message>,
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
    Shutdown,
}

struct BrokerData {
    brokers: HashMap<TopicPartition, mpsc::Sender<BrokerMessage>>,
    threadpool: ThreadPool,
    topic_online_tx: mpsc::Sender<BrokerMessage>,
    is_shutdown: bool,
}

pub struct BrokerManager {
    brokers: Mutex<BrokerData>,
    count: Arc<Mutex<usize>>,
    log_dir: String,
}

impl BrokerManager {
    pub fn new(mut threadpool: ThreadPool, log_dir: &str) -> BrokerManager {
        let log_dir = log_dir.to_string();
        let tp = TopicPartition {
            topic: "__topic_online".to_string(),
            partition: 0,
        };
        let count = Arc::new(Mutex::new(0));
        let (topic_online_tx, rx) = mpsc::channel::<BrokerMessage>(1000);
        //let (topic_online_tx, rx) = mpsc::unbounded::<BrokerMessage>();
        if let Err(err) = threadpool.spawn(new_message_broker(
            rx,
            tp.clone(),
            log_dir.clone(),
            count.clone(),
        )) {
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
                is_shutdown: false,
            }),
            count,
            log_dir,
        }
    }

    pub async fn expand_topics(&self, topic_name: String) -> Vec<String> {
        let mut result = Vec::<String>::new();
        if topic_name.trim().ends_with('*') {
            let len = topic_name.len();
            let data = self.brokers.lock().await;
            if data.is_shutdown {
                return result;
            }
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
        let partition = tp.partition;
        let topic = tp.topic.clone(); // Use for error logging.
        let mut data = self.brokers.lock().await;
        if data.is_shutdown {
            info!("Tried to get a broker after shutdown!");
            return Err(());
        }
        if data.brokers.contains_key(&tp) {
            return Ok(data.brokers.get(&tp).unwrap().clone());
        }
        let (tx, rx) = mpsc::channel::<BrokerMessage>(1000);
        data.brokers.insert(tp.clone(), tx.clone());
        if let Err(err) = data.threadpool.spawn(new_message_broker(
            rx,
            tp.clone(),
            self.log_dir.clone(),
            self.count.clone(),
        )) {
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

    pub async fn shutdown(&self, timeout_ms: u128) {
        // Note this not a well behaved async function but it is only intended
        // to be run at shutdown and on the ctrlc signal thread.
        let mut data = self.brokers.lock().await;
        if data.is_shutdown {
            info!("Tried to shutdown while shutting down!");
        } else {
            let start_time = get_epoch_ms();
            data.is_shutdown = true;
            let keys: Vec<TopicPartition> = data.brokers.keys().cloned().collect();
            for broker_key in keys {
                let tx = data.brokers.get_mut(&broker_key).unwrap();
                info!(
                    "Closing broker for {}/{}.",
                    broker_key.topic, broker_key.partition
                );
                if let Err(err) = tx.try_send(BrokerMessage::Shutdown) {
                    error!(
                        "Error closing to broker {}/{}. {}",
                        broker_key.topic, broker_key.partition, err
                    );
                }
            }
            let mut going_down = true;
            while going_down {
                let count = self.count.lock().await;
                if *count == 0 {
                    going_down = false;
                }
                let time = get_epoch_ms();
                if (time - start_time) > timeout_ms {
                    error!("Timed out waiting for brokers to shutdown!");
                    going_down = true;
                }
                if !going_down {
                    sleep(Duration::from_millis(200));
                }
            }
        }
    }
}

fn fetch(
    offset: u64,
    msg_log: &MessageLog,
    client_tx: &mut mpsc::Sender<ClientMessage>,
    client_name: &str,
) -> bool {
    let mut success = true;
    let info = msg_log.get_index(offset);
    if let Ok(info) = info {
        if let Err(error) = client_tx.try_send(ClientMessage::MessageBatch {
            file_name: msg_log.log_file_name(),
            start: info.position,
            length: msg_log.log_file_end() - info.position,
        }) {
            success = false;
            error!(
                "Error fetching ({}) for client {}, dropping: {}",
                offset, client_name, error
            );
        }
    }
    success
}

fn fetch_with_pos(
    tp: &TopicPartition,
    msg_log: &MessageLog,
    client: &mut ClientInfo,
    client_name: &str,
    position: TopicPosition,
    log_dir: &str,
) -> bool {
    let mut bad_client = false;
    match position {
        TopicPosition::Earliest => {
            bad_client = !fetch(0, &msg_log, &mut client.tx, &client_name);
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
            let offset_log = MessageLog::new(&tp_offset, true, log_dir);
            if let Ok(mut offset_log) = offset_log {
                let offset_message = offset_log.get_message(0);
                if let Ok(offset_message) = offset_message {
                    let record: OffsetRecord =
                        serde_json::from_slice(&offset_message.payload[..]).unwrap(); // XXX TODO Don't unwrap...
                    bad_client = !fetch(record.offset + 1, &msg_log, &mut client.tx, &client_name);
                } else {
                    info!(
                        "Issue retrieving consumer offset (probably not committed): {}, using 0.",
                        offset_message.unwrap_err()
                    );
                    bad_client = !fetch(0, &msg_log, &mut client.tx, &client_name);
                }
            } else {
                error!(
                    "Error retrieving consumer offset log: {}",
                    offset_log.unwrap_err()
                );
            }
        }
        TopicPosition::Offset { offset } => {
            bad_client = !fetch(offset, &msg_log, &mut client.tx, &client_name);
        }
    }
    bad_client
}

fn handle_message(
    mut message: Message,
    tp: &TopicPartition,
    msg_log: &mut MessageLog,
    client_tx: &mut HashMap<String, ClientInfo>,
) {
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
        let mut send_msg = true;
        let message = if is_internal {
            ClientMessage::InternalMessage {
                message: message.clone(),
            }
        } else {
            match client.sub_type {
                SubType::Stream => ClientMessage::Message {
                    message: message.clone(),
                },
                SubType::Fetch => {
                    if !client.needs_fetch.get()
                        && message.message_type != MessageType::BatchMessage
                    {
                        client.needs_fetch.set(true);
                    } else {
                        send_msg = false;
                    }
                    ClientMessage::MessagesAvailable {
                        topic: tp.topic.clone(),
                        partition: tp.partition,
                    }
                }
            }
        };
        if send_msg {
            if let Err(err) = tx.try_send(message) {
                error!("Error writing to client, will close. {}", err);
                bad_clients.push(tx_key.clone());
            }
        }
    }
    for bad_client in bad_clients {
        client_tx.remove(&bad_client);
    }
}

async fn new_message_broker(
    mut rx: mpsc::Receiver<BrokerMessage>,
    tp: TopicPartition,
    log_dir: String,
    count: Arc<Mutex<usize>>,
) {
    info!(
        "Broker starting for partition {}, topic {}.",
        tp.partition, tp.topic
    );
    {
        let mut count = count.lock().await;
        *count += 1;
    }

    let mut running = true;
    let msg_log = MessageLog::new(&tp, tp.topic.starts_with("__consumer_offsets"), &log_dir);
    if let Err(error) = msg_log {
        error!("Failed to open the message log {}", error);
        return;
    }
    let mut msg_log = msg_log.unwrap();
    let mut client_tx: HashMap<String, ClientInfo> = HashMap::new();
    let mut message = rx.next().await;
    while message.is_some() && running {
        let mes = message.clone().unwrap();
        match mes {
            BrokerMessage::Message { message } => {
                handle_message(message, &tp, &mut msg_log, &mut client_tx);
            }
            BrokerMessage::MessageBatch { messages } => {
                for message in messages {
                    handle_message(message, &tp, &mut msg_log, &mut client_tx);
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
                        needs_fetch: Cell::new(true),
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
                            fetch_with_pos(&tp, &msg_log, client, &client_name, position, &log_dir);
                        client.needs_fetch.set(false);
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
                        fetch_with_pos(&tp, &msg_log, client, &client_name, position, &log_dir);
                    client.needs_fetch.set(false);
                    if bad_client {
                        client_tx.remove(&client_name);
                    }
                }
            }
            BrokerMessage::CloseClient { client_name } => {
                client_tx.remove(&client_name);
            }
            BrokerMessage::Shutdown => {
                for tx_key in client_tx.keys() {
                    let client = client_tx.get(tx_key).unwrap();
                    let mut tx = client.tx.clone();
                    if !tx.is_closed() {
                        info!("Closing client {}.", tx_key);
                        if let Err(err) = tx.try_send(ClientMessage::Over) {
                            error!("Error writing to client {} while closing. {}", tx_key, err);
                        }
                        tx.close_channel();
                    }
                }
                running = false;
            }
        };
        if running {
            message = rx.next().await;
        }
    }
    info!(
        "Broker ending for partition {}, topic {}.",
        tp.partition, tp.topic
    );
    {
        let mut count = count.lock().await;
        *count -= 1;
    }
}
