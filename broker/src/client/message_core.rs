extern crate libc;

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};
use std::os::unix::fs::OpenOptionsExt;
use std::sync::Arc;

use bytes::{BufMut, BytesMut};

use futures::channel::mpsc;
use futures::io::{AsyncWriteExt, WriteHalf};
use futures::sink::SinkExt;
use futures::StreamExt;

use romio::TcpStream;

use super::super::broker::*;
use common::types::*;

use log::{debug, error, info};

pub struct MessageCore {
    broker_tx: mpsc::Sender<ClientMessage>,
    rx: mpsc::Receiver<ClientMessage>,
    broker_manager: Arc<BrokerManager>,
    broker_tx_cache: HashMap<TopicPartition, mpsc::Sender<BrokerMessage>>,
    client_name: String,
    group_id: Option<String>,
    running: bool,
    client_encoder: Box<dyn ProtocolServerEncoder>,
    writer: WriteHalf<TcpStream>,
    out_bytes: BytesMut,
    message_batch: Option<Vec<Message>>,
}

impl MessageCore {
    pub fn new(
        broker_tx: mpsc::Sender<ClientMessage>,
        rx: mpsc::Receiver<ClientMessage>,
        client_name_unique: &str,
        broker_manager: Arc<BrokerManager>,
        encoder_factory: ProtocolServerEncoderFactory,
        writer: WriteHalf<TcpStream>,
    ) -> MessageCore {
        let client_encoder = encoder_factory();
        let broker_tx_cache: HashMap<TopicPartition, mpsc::Sender<BrokerMessage>> = HashMap::new();
        let group_id: Option<String> = None;
        let buf_size = 1_024_000;
        let out_bytes = BytesMut::with_capacity(buf_size);
        MessageCore {
            broker_tx,
            rx,
            broker_manager,
            broker_tx_cache,
            client_name: client_name_unique.to_string(),
            group_id,
            running: true,
            client_encoder,
            writer,
            out_bytes,
            message_batch: None,
        }
    }

    async fn write_buffer(&mut self) {
        if !self.out_bytes.is_empty() {
            if let Err(err) = self.writer.write_all(&self.out_bytes).await {
                error!("Error writing to client, closing: {}", err);
                self.running = false;
            }
            self.out_bytes.truncate(0);
        }
    }

    async fn send_messages(&mut self, file_name: String, start: u64, length: u64) {
        // XXX Using non-blocking but sync file IO, maybe stop doing that as soon as possible...
        // This file should always have the data requested so this may be fine.
        let buf_size = 128_000;
        let mut buf = vec![0; buf_size]; // XXX share a buffer or something other then allocing it each time...
        match OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(&file_name)
        {
            Ok(mut file) => {
                if let Err(err) = file.seek(SeekFrom::Start(start)) {
                    error!("Error seeking log file: {}", err);
                    self.running = false;
                }
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
                            left -= bytes;
                            debug!("Sending {} bytes from log file to client.", bytes);
                            if bytes < buf_size && left > 0 {
                                // Did not read expected bytes and since we are mixing
                                // blocking IO abort.
                                error!("Failed to read expected bytes from log file.");
                                break;
                            }
                            if let Err(err) = self.writer.write_all(&buf[..bytes]).await {
                                error!("Error writing to the client, close connection: {}", err);
                                self.running = false;
                            }
                        }
                        Err(error) => {
                            error!("Error reading from log file: {}", error);
                            self.running = false;
                        }
                    }
                }
            }
            Err(error) => {
                error!("Error opening log file: {}, error: {}", file_name, error);
                self.running = false;
            }
        }
    }

    async fn get_broker_tx(
        &mut self,
        tp: TopicPartition,
    ) -> Result<mpsc::Sender<BrokerMessage>, ()> {
        match self.broker_tx_cache.get(&tp) {
            Some(tx) => Ok(tx.clone()),
            None => match self.broker_manager.get_broker_tx(tp.clone()).await {
                Ok(tx) => {
                    self.broker_tx_cache.insert(tp, tx.clone());
                    Ok(tx)
                }
                Err(_) => {
                    self.running = false;
                    error!(
                        "Failed to get broker tx for {}/{}, will exit client!",
                        tp.topic, tp.partition
                    );
                    Err(())
                }
            },
        }
    }

    async fn new_client(
        &mut self,
        partition: u64,
        topic: String,
        position: TopicPosition,
        sub_type: SubType,
        internal: bool,
    ) {
        let tp = TopicPartition {
            partition,
            topic: topic.clone(),
        };
        if let Ok(mut tx) = self.get_broker_tx(tp).await {
            if let Err(err) = tx
                .send(BrokerMessage::NewClient {
                    client_name: self.client_name.to_string(),
                    group_id: self.group_id.clone().unwrap(),
                    tx: self.broker_tx.clone(),
                    is_internal: internal,
                })
                .await
            {
                error!("Error sending message to broker, close client: {}", err);
                self.running = false;
            }
            if sub_type == SubType::Stream {
                if let Err(err) = tx
                    .send(BrokerMessage::FetchPolicy {
                        client_name: self.client_name.to_string(),
                        sub_type,
                        position,
                    })
                    .await
                {
                    error!("Error sending message to broker, close client: {}", err);
                    self.running = false;
                }
            }
        } else {
            error!("Error tx for broker, closing client!");
            self.running = false;
        }
    }

    async fn check_connected(&mut self) -> bool {
        if self.group_id.is_none() {
            self.send(ServerToClient::StatusError {
                code: 503,
                message: "Client not initialized (connect first), closing connection!".to_string(),
            })
            .await;
            error!("Error client tried to set topics with no group id, closing client.");
            self.running = false;
            false
        } else {
            true
        }
    }

    async fn commit(&mut self, topic: String, partition: u64, commit_offset: u64) {
        // XXX should check that the topic/partition are in use by this client.
        if self.check_connected().await {
            let group_id = self.group_id.clone().unwrap();
            let commit_topic = format!("__consumer_offsets-{}-{}-{}", group_id, topic, partition);
            let tp = TopicPartition {
                partition: 0,
                topic: commit_topic.clone(),
            };
            if let Ok(mut tx) = self.get_broker_tx(tp).await {
                let payload = format!("{{\"offset\":{}}}", commit_offset).into_bytes();
                let message = Message {
                    message_type: MessageType::Message,
                    topic: commit_topic,
                    partition,
                    payload_size: payload.len(),
                    crc: 0,
                    sequence: 0,
                    payload,
                };
                if let Err(error) = tx.send(BrokerMessage::Message { message }).await {
                    error!("Error sending to broker: {}", error);
                    self.running = false;
                }
                self.send(ServerToClient::CommitAck {
                    topic,
                    partition,
                    offset: commit_offset,
                })
                .await;
            }
        }
    }

    async fn fetch(&mut self, topic: String, partition: u64, position: TopicPosition) {
        if self.check_connected().await {
            let tp = TopicPartition { partition, topic };
            if let Ok(mut tx) = self.get_broker_tx(tp).await {
                if let Err(error) = tx
                    .send(BrokerMessage::Fetch {
                        client_name: self.client_name.clone(),
                        position,
                    })
                    .await
                {
                    error!("Error sending fetch to broker: {}", error);
                    self.running = false;
                }
            }
        }
    }

    async fn publish_message(&mut self, message: Message) {
        if self.check_connected().await {
            let tp = TopicPartition {
                partition: 0,
                topic: message.topic.clone(),
            };
            let message_type = message.message_type;
            match message_type {
                MessageType::Message => {
                    if message.payload_size > 0 {
                        if let Ok(mut tx) = self.get_broker_tx(tp).await {
                            if let Err(error) = tx.send(BrokerMessage::Message { message }).await {
                                error!("Error sending to broker: {}", error);
                                self.running = false;
                            }
                        }
                    }
                    self.send(ServerToClient::StatusOk).await;
                }
                MessageType::BatchMessage => {
                    // No feedback during a batch.
                    if let Some(message_batch) = &mut self.message_batch {
                        message_batch.push(message);
                    }
                }
                MessageType::BatchEnd { count } => {
                    let message_batch = std::mem::replace(&mut self.message_batch, None);
                    if let Some(mut message_batch) = message_batch {
                        message_batch.push(message);
                        if let Ok(mut tx) = self.get_broker_tx(tp).await {
                            if let Err(error) = tx
                                .send(BrokerMessage::MessageBatch {
                                    messages: message_batch,
                                })
                                .await
                            {
                                error!("Error sending to broker: {}", error);
                                self.running = false;
                            }
                        }
                    }
                    self.send(ServerToClient::StatusOkCount { count }).await;
                }
            }
        }
    }

    async fn send(&mut self, message: ServerToClient) {
        if let EncodeStatus::BufferToSmall(bytes) =
            self.client_encoder.encode(&mut self.out_bytes, message)
        {
            self.write_buffer().await;
            self.out_bytes.put_slice(&bytes);
        }
        self.write_buffer().await;
    }

    async fn handle_toserver_message(&mut self, message: ClientToServer) {
        match message {
            ClientToServer::Batch => {
                self.message_batch = Some(Vec::with_capacity(100));
            }
            ClientToServer::BatchCount { count } => {
                self.message_batch = Some(Vec::with_capacity(count));
            }
            ClientToServer::Connect {
                client_name,
                group_id,
            } => {
                self.client_name = format!("{}:{}", client_name, self.client_name);
                self.group_id = Some(group_id);
                self.send(ServerToClient::StatusOk).await;
            }
            ClientToServer::Subscribe {
                topic,
                partition,
                position,
                sub_type,
            } => {
                if self.check_connected().await {
                    for topic in self.broker_manager.expand_topics(topic).await {
                        self.new_client(partition, topic, position, sub_type, false)
                            .await;
                        self.new_client(
                            partition,
                            "__topic_online".to_string(),
                            TopicPosition::Latest,
                            SubType::Stream,
                            true,
                        )
                        .await;
                    }
                    self.send(ServerToClient::StatusOk).await;
                }
            }
            ClientToServer::Unsubscribe { .. } => {
                // XXX implement... topic: _ } => {
                self.send(ServerToClient::StatusOk).await;
            }
            ClientToServer::ClientDisconnect => {
                info!("Client close request.");
                self.running = false;
            }
            ClientToServer::Commit {
                topic,
                partition,
                commit_offset,
            } => {
                self.commit(topic, partition, commit_offset).await;
            }
            ClientToServer::PublishMessage { message } => {
                self.publish_message(message).await;
            }
            ClientToServer::Fetch {
                topic,
                partition,
                position,
            } => {
                self.fetch(topic, partition, position).await;
            }
            ClientToServer::PublishBatchStart { .. } => {}
            ClientToServer::StatusOk => {}
            ClientToServer::Noop => {
                // Like the name says...
            }
        };
    }

    async fn handle_toclient_message(&mut self, message: ServerToClient) {
        match message {
            ServerToClient::Over => {
                self.running = false;
            }
            ServerToClient::StatusError { code, message } => {
                self.send(ServerToClient::StatusError { code, message })
                    .await;
            }
            ServerToClient::StatusOk => {
                self.send(ServerToClient::StatusOk).await;
            }
            ServerToClient::InternalMessage { .. } => {}
            ServerToClient::Message { message } => {
                self.send(ServerToClient::Message { message }).await;
            }
            ServerToClient::MessageBatch {
                file_name,
                start,
                length,
            } => {
                self.write_buffer().await; // Flush buffer first.
                self.send_messages(file_name.clone(), start, length).await;
            }

            ServerToClient::MessagesAvailable { topic, partition } => {
                self.send(ServerToClient::MessagesAvailable { topic, partition })
                    .await;
            }
            ServerToClient::Noop => {
                // Like the name says...
            }
            ServerToClient::StatusOkCount { .. } => {
                // Ignore (or maybe abort client), should not happen...
            }
            ServerToClient::CommitAck { .. } => {
                // Ignore (or maybe abort client), should not happen...
            }
        };
    }

    pub async fn message_incoming(&mut self) {
        let mut message = self.rx.next().await;
        while self.running && message.is_some() {
            let mes = message.unwrap();
            message = None;
            match mes {
                ClientMessage::ToServer(m) => {
                    self.handle_toserver_message(m).await;
                }
                ClientMessage::ToClient(m) => {
                    self.handle_toclient_message(m).await;
                }
            }
            if !self.running {
                continue;
            }
            // XXX Still thinking about this, does not seem to matter...
            /*if let Ok(m) = self.rx.try_next() {
                // Next message is already here, go ahead and get it on the output
                // buffer.
                message = m;
            } else {
                // Need to wait so go ahead and send data to client.
                self.write_buffer().await; //, &mut self.out_bytes);
                message = self.rx.next().await;
            }*/
            message = self.rx.next().await;
        }
        self.rx.close();
        info!("Exiting messaging_incoming for {}.", self.client_name);
    }
}
