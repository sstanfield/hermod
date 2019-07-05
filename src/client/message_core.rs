use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

use futures::channel::mpsc;
use futures::executor::ThreadPool;
use futures::io::{AsyncWriteExt, WriteHalf};
use futures::sink::SinkExt;
use futures::task::SpawnExt;
use futures::StreamExt;

use romio::TcpStream;

use super::super::broker::*;
use super::super::types::*;
use super::protocol::*;

use log::{error, info};

macro_rules! get_broker_tx {
    ($self:expr, $partition:expr, $topic:expr, $tx:expr) => {
        let tp = TopicPartition {
            partition: $partition,
            topic: $topic,
        };
        $tx = $self.broker_tx_cache.entry(tp.clone()).or_insert(
            $self
                .broker_manager
                .get_broker_tx(tp.clone())
                .await
                .unwrap(),
        );
    };
}

macro_rules! new_client {
    ($self:expr, $partition:expr, $topic:expr, $internal:expr) => {
        let tx: &mut mpsc::Sender<BrokerMessage>;
        get_broker_tx!($self, $partition, $topic, tx);
        if let Err(_) = tx
            .send(BrokerMessage::NewClient(
                $self.client_name.to_string(),
                $self.group_id.clone().unwrap(),
                $self.broker_tx.clone(),
                $internal,
            ))
            .await
        {
            error!("Error sending message to broker, close client.");
            $self.running = false;
        }
    };
}

macro_rules! send_ok {
    ($self:expr, $writer:expr) => {
        let data = $self.client_codec.encode(ClientMessage::StatusOk);
        if let Err(err) = $writer.write_all(&data).await {
                error!("Error writing to client, closing: {}", err);
                $self.running = false;
        }
        //send_str!($self, $writer, "{\"Status\":{\"status\":\"OK\"}}");
    };
    ($self:expr, $writer:expr, $count:expr) => {
        let data  = $self.client_codec.encode(ClientMessage::StatusOkCount { count: $count});
        if let Err(err) = $writer.write_all(&data).await {
                error!("Error writing to client, closing: {}", err);
                $self.running = false;
        }
    };
}

macro_rules! send_err {
    ($self:expr, $writer:expr, $code:expr, $message:expr) => {
        let data = $self.client_codec.encode(ClientMessage::StatusError($code, $message));
        if let Err(err) = $writer.write_all(&data).await {
                error!("Error writing to client, closing: {}", err);
                $self.running = false;
        }
    };
}

macro_rules! send_msg {
    ($self:expr, $writer:expr, $message:expr) => {
        let data = $self.client_codec.encode(ClientMessage::Message($message));
        if let Err(err) = $writer.write_all(&data).await {
                error!("Error writing to client, closing: {}", err);
                $self.running = false;
        }
    };
}

pub struct MessageCore {
    broker_tx: mpsc::Sender<ClientMessage>,
    rx: mpsc::Receiver<ClientMessage>,
    broker_manager: Arc<BrokerManager>,
    io_pool: ThreadPool,
    broker_tx_cache: HashMap<TopicPartition, mpsc::Sender<BrokerMessage>>,
    client_name: String,
    group_id: Option<String>,
    running: bool,
    client_codec: ClientCodec,
}

impl MessageCore {
    pub fn new(
        broker_tx: mpsc::Sender<ClientMessage>,
        rx: mpsc::Receiver<ClientMessage>,
        idx: u64,
        broker_manager: Arc<BrokerManager>,
        io_pool: ThreadPool,
        client_codec: ClientCodec,
    ) -> MessageCore {
        let broker_tx_cache: HashMap<TopicPartition, mpsc::Sender<BrokerMessage>> = HashMap::new();
        let client_name = format!("Client_{}", idx);
        let group_id: Option<String> = None;
        MessageCore {
            broker_tx,
            rx,
            broker_manager,
            io_pool,
            broker_tx_cache,
            client_name,
            group_id,
            running: true,
            client_codec,
        }
    }

    async fn send_messages(
        mut writer: WriteHalf<TcpStream>,
        file_name: String,
        start: u64,
        length: u64,
    ) -> WriteHalf<TcpStream> {
        let buf_size = 128000;
        let mut buf = vec![0; buf_size];
        let mut file = File::open(&file_name).unwrap();
        file.seek(SeekFrom::Start(start)).unwrap();
        let mut left = length as usize;
        // XXX this is sync and dumb, get an async sendfile...
        while left > 0 {
            let buf_len = if left < buf_size { left } else { buf_size };
            match file.read(&mut buf[..buf_len]) {
                Ok(0) => {
                    error!("Failed to read log file.");
                    break;
                }
                Ok(bytes) => {
                    writer.write_all(&buf[..bytes]).await.unwrap(); // XXX no unwrap...
                    left -= bytes;
                }
                Err(error) => {
                    // XXX do better.
                    error!("{}", error);
                }
            }
        }
        writer
    }

    pub async fn message_incoming(&mut self, mut writer: WriteHalf<TcpStream>) {
        let mut message = self.rx.next().await;
        while self.running && message.is_some() {
            let mes = message.unwrap();
            message = None;
            match mes {
                ClientMessage::Over => {
                    self.rx.close();
                }
                ClientMessage::StatusError(code, message) => {
                    send_err!(self, writer, code, message);
                }
                ClientMessage::StatusOk => {
                    send_ok!(self, writer);
                }
                ClientMessage::InternalMessage(message) => {
                    //println!("XXXX internal {}: {}", message.topic, std::str::from_utf8(&message.payload[..]).unwrap());
                }
                ClientMessage::Message(message) => {
                    send_msg!(self, writer, message);
                }
                ClientMessage::MessageBatch(file_name, start, length) => {
                    writer = self
                        .io_pool
                        .spawn_with_handle(MessageCore::send_messages(
                            writer,
                            file_name.clone(),
                            start,
                            length,
                        ))
                        .unwrap()
                        .await;
                }
                ClientMessage::Connect(name, gid) => {
                    self.client_name = name;
                    self.group_id = Some(gid);
                    send_ok!(self, writer);
                }
                ClientMessage::Subscribe { topic, position: _ } => {
                    // XXX Validate connection.
                    for topic in self.broker_manager.expand_topics(topic).await {
                        new_client!(self, 0, topic, false);
                        new_client!(self, 0, "__topic_online".to_string(), true);
                    }
                    send_ok!(self, writer);
                }
                ClientMessage::Unsubscribe { topic: _ } => {
                    send_ok!(self, writer);
                }
                ClientMessage::IncomingStatus(status) => {
                    if status.to_lowercase().eq("close") {
                        info!("Client close request.");
                        self.rx.close();
                    }
                }
                ClientMessage::Commit {
                    topic,
                    partition,
                    commit_offset,
                } => {
                    // XXX should check that the topic/partition are in use by this client.
                    if self.group_id.is_none() {
                        send_err!(
                            self,
                            writer,
                            503,
                            "Client not initialized, closing connection!".to_string()
                        );
                        error!(
                            "Error client tried to set topics with no group id, closing client."
                        );
                        self.running = false;
                        continue;
                    }
                    let group_id = self.group_id.clone().unwrap();
                    let commit_topic =
                        format!("__consumer_offsets-{}-{}-{}", group_id, topic, partition);
                    let tp = TopicPartition {
                        partition: 0,
                        topic: commit_topic.clone(),
                    };
                    let tx = self
                        .broker_tx_cache
                        .entry(tp.clone())
                        .or_insert(self.broker_manager.get_broker_tx(tp.clone()).await.unwrap());
                    let payload = format!(
                        "{{\"group_id\": \"{}\", \"partition\": {}, \"topic\": \"{}\", \"offset\": {}}}",
                        group_id, partition, topic, commit_offset).into_bytes();
                    let message = Message {
                        message_type: MessageType::Message,
                        topic: commit_topic,
                        payload_size: payload.len(),
                        checksum: "".to_string(),
                        sequence: 0,
                        payload,
                    };
                    if let Err(error) = tx.send(BrokerMessage::Message(message)).await {
                        error!("Error sending to broker: {}", error);
                        self.running = false;
                    }
                    send_ok!(self, writer);
                }
                ClientMessage::PublishMessage { message } => {
                    let tp = TopicPartition {
                        partition: 0,
                        topic: message.topic.clone(),
                    };
                    let message_type = message.message_type.clone();
                    if message.payload_size > 0 {
                        let tx = self.broker_tx_cache.entry(tp.clone()).or_insert(
                            self.broker_manager.get_broker_tx(tp.clone()).await.unwrap(),
                        );
                        if let Err(error) = tx.send(BrokerMessage::Message(message)).await {
                            error!("Error sending to broker: {}", error);
                            self.running = false;
                        }
                    }
                    match message_type {
                        MessageType::Message => {
                            send_ok!(self, writer);
                        }
                        MessageType::BatchMessage => {
                            // No feedback during a batch.
                        }
                        MessageType::BatchEnd { count } => {
                            send_ok!(self, writer, count);
                        }
                    }
                }
                ClientMessage::Noop => {
                    // Like the name says...
                }
                ClientMessage::StatusOkCount{ count: _ } => {
                    // Ignore (or maybe abort client), should not happen...
                }
            };
            message = self.rx.next().await;
        }
        info!("Exiting messaging_incoming.");
    }
}
