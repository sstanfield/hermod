// XXX This is producing false positives on async fns with reference params.
// Turn back on when it works...
#![allow(clippy::needless_lifetimes)]

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
        if let Err(err) = tx
            .send(BrokerMessage::NewClient(
                $self.client_name.to_string(),
                $self.group_id.clone().unwrap(),
                $self.broker_tx.clone(),
                $internal,
            ))
            .await
        {
            error!("Error sending message to broker, close client: {}", err);
            $self.running = false;
        }
    };
}

macro_rules! write_buffer {
    ($self:expr) => {
        if $self.out_bytes.len() > 0 {
            if let Err(err) = $self.writer.write_all(&$self.out_bytes).await {
                error!("Error writing to client, closing: {}", err);
                $self.running = false;
            }
            $self.out_bytes.truncate(0);
        }
    };
}

macro_rules! send {
    ($self:expr, $message:expr) => {
        if let EncodeStatus::BufferToSmall(bytes) =
            $self.client_encoder.encode(&mut $self.out_bytes, $message)
        {
            write_buffer!($self);
            $self.out_bytes.put_slice(&bytes);
        }
    };
}

pub struct MessageCore {
    broker_tx: mpsc::Sender<ClientMessage>,
    rx: mpsc::Receiver<ClientMessage>,
    broker_manager: Arc<BrokerManager>,
    broker_tx_cache: HashMap<TopicPartition, mpsc::Sender<BrokerMessage>>,
    client_name: String,
    group_id: Option<String>,
    running: bool,
    client_encoder: Box<dyn ProtocolEncoder>,
    writer: WriteHalf<TcpStream>,
    out_bytes: BytesMut,
}

impl MessageCore {
    pub fn new(
        broker_tx: mpsc::Sender<ClientMessage>,
        rx: mpsc::Receiver<ClientMessage>,
        idx: u64,
        broker_manager: Arc<BrokerManager>,
        encoder_factory: ProtocolEncoderFactory,
        writer: WriteHalf<TcpStream>,
    ) -> MessageCore {
        let client_encoder = encoder_factory();
        let broker_tx_cache: HashMap<TopicPartition, mpsc::Sender<BrokerMessage>> = HashMap::new();
        let client_name = format!("Client_{}", idx);
        let group_id: Option<String> = None;
        let buf_size = 1_024_000;
        let out_bytes = BytesMut::with_capacity(buf_size);
        MessageCore {
            broker_tx,
            rx,
            broker_manager,
            broker_tx_cache,
            client_name,
            group_id,
            running: true,
            client_encoder,
            writer,
            out_bytes,
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

    async fn commit(&mut self, topic: String, partition: u64, commit_offset: u64) {
        // XXX should check that the topic/partition are in use by this client.
        if self.group_id.is_none() {
            send!(
                self,
                ClientMessage::StatusError {
                    code: 503,
                    message: "Client not initialized, closing connection!".to_string(),
                }
            );
            error!("Error client tried to set topics with no group id, closing client.");
            self.running = false;
        } else {
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
                    payload_size: payload.len(),
                    checksum: "".to_string(),
                    sequence: 0,
                    payload,
                };
                if let Err(error) = tx.send(BrokerMessage::Message(message)).await {
                    error!("Error sending to broker: {}", error);
                    self.running = false;
                }
                send!(
                    self,
                    ClientMessage::CommitAck {
                        topic,
                        partition,
                        offset: commit_offset
                    }
                );
            }
        }
    }

    async fn publish_message(&mut self, message: Message) {
        let tp = TopicPartition {
            partition: 0,
            topic: message.topic.clone(),
        };
        let message_type = message.message_type.clone();
        if message.payload_size > 0 {
            if let Ok(mut tx) = self.get_broker_tx(tp).await {
                if let Err(error) = tx.send(BrokerMessage::Message(message)).await {
                    error!("Error sending to broker: {}", error);
                    self.running = false;
                }
            }
        }
        match message_type {
            MessageType::Message => {
                send!(self, ClientMessage::StatusOk);
            }
            MessageType::BatchMessage => {
                // No feedback during a batch.
            }
            MessageType::BatchEnd { count } => {
                send!(self, ClientMessage::StatusOkCount { count });
            }
        }
    }

    async fn handle_message(&mut self, message: ClientMessage) {
        match message {
            ClientMessage::Over => {
                self.running = false;
            }
            ClientMessage::StatusError { code, message } => {
                send!(self, ClientMessage::StatusError { code, message });
            }
            ClientMessage::StatusOk => {
                send!(self, ClientMessage::StatusOk);
            }
            ClientMessage::InternalMessage { .. } => {
                //println!("XXXX internal {}: {}", message.topic, std::str::from_utf8(&message.payload[..]).unwrap());
            }
            ClientMessage::Message { message } => {
                send!(self, ClientMessage::Message { message });
            }
            ClientMessage::MessageBatch {
                file_name,
                start,
                length,
            } => {
                write_buffer!(self); // Flush buffer first.
                self.send_messages(file_name.clone(), start, length).await;
            }
            ClientMessage::Connect {
                client_name,
                group_id,
            } => {
                self.client_name = client_name;
                self.group_id = Some(group_id);
                send!(self, ClientMessage::StatusOk);
            }
            ClientMessage::Subscribe { topic, .. } => {
                // XXX use position: _ } => {
                // XXX Validate connection.
                for topic in self.broker_manager.expand_topics(topic).await {
                    new_client!(self, 0, topic, false);
                    new_client!(self, 0, "__topic_online".to_string(), true);
                }
                send!(self, ClientMessage::StatusOk);
            }
            ClientMessage::Unsubscribe { .. } => {
                // XXX implement... topic: _ } => {
                send!(self, ClientMessage::StatusOk);
            }
            ClientMessage::IncomingStatus { status } => {
                if status.to_lowercase().eq("close") {
                    info!("Client close request.");
                    self.running = false;
                }
            }
            ClientMessage::Commit {
                topic,
                partition,
                commit_offset,
            } => {
                self.commit(topic, partition, commit_offset).await;
            }
            ClientMessage::PublishMessage { message } => {
                self.publish_message(message).await;
            }
            ClientMessage::Noop => {
                // Like the name says...
            }
            ClientMessage::StatusOkCount { .. } => {
                // Ignore (or maybe abort client), should not happen...
            }
            ClientMessage::PublishBatchStart { .. } => {
                // Ignore (or maybe abort client), should not happen...
            }
            ClientMessage::CommitAck { .. } => {
                // Ignore (or maybe abort client), should not happen...
            }
        };
    }

    pub async fn message_incoming(&mut self) {
        let mut message = self.rx.next().await;
        while self.running && message.is_some() {
            let mes = message.unwrap();
            message = None;
            self.handle_message(mes).await;
            if !self.running {
                continue;
            }
            if let Ok(m) = self.rx.try_next() {
                // Next message is already here, go ahead and get it on the output
                // buffer.
                message = m;
            } else {
                // Need to wait so go ahead and send data to client.
                write_buffer!(self); //, &mut self.out_bytes);
                message = self.rx.next().await;
            }
        }
        self.rx.close();
        info!("Exiting messaging_incoming.");
    }
}
