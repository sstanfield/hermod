use std::collections::HashMap;
use std::sync::Arc;
use std::{io, str};

use bytes::BytesMut;
use futures::channel::mpsc;
use futures::executor::ThreadPool;
use futures::io::AsyncReadExt;
use futures::io::AsyncWriteExt;
use futures::sink::SinkExt;
use futures::task::SpawnExt;
use futures::StreamExt;
use serde_json;

use romio::{TcpListener, TcpStream};

use super::broker::*;
use super::common::*;
use super::types::*;

use log::{error, info};

fn zero_val() -> usize {
    0
}

#[derive(Deserialize, Clone, Debug)]
enum BatchType {
    Start,
    End,
    Count,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum PubIncoming {
    Message {
        topic: String,
        payload_size: usize,
        checksum: String,
    },
    Batch {
        batch_type: BatchType,
        #[serde(default = "zero_val")]
        count: usize,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
struct InMessageCodec {
    message: Option<Message>,
    in_batch: bool,
    batch_count: usize,
    expected_batch_count: usize,
}

impl InMessageCodec {
    pub fn new() -> InMessageCodec {
        InMessageCodec {
            message: None,
            in_batch: false,
            batch_count: 0,
            expected_batch_count: 0,
        }
    }

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Message>, io::Error> {
        let mut result: Result<Option<Message>, io::Error> = Ok(None);
        let mut is_batch = false;
        if self.message.is_none() {
            if let Some(brace_offset) = buf[..].iter().position(|b| *b == b'{') {
                if brace_offset > 0 {
                    buf.advance(brace_offset);
                }
            }
            if let Some(message_offset) = last_brace(&buf[..]) {
                if message_offset > 3 {
                    let line = buf.split_to(message_offset + 1);
                    let incoming: PubIncoming = serde_json::from_slice(&line[..])?;
                    match incoming {
                        PubIncoming::Message {
                            topic,
                            payload_size,
                            checksum,
                        } => {
                            let message_type = if self.in_batch {
                                self.batch_count += 1;
                                if self.batch_count == self.expected_batch_count {
                                    self.in_batch = false;
                                    self.batch_count = 0;
                                    let count = self.expected_batch_count;
                                    self.expected_batch_count = 0;
                                    MessageType::BatchEnd { count }
                                } else {
                                    MessageType::BatchMessage
                                }
                            } else {
                                MessageType::Message
                            };
                            let message = Message {
                                message_type,
                                topic,
                                payload_size,
                                checksum,
                                sequence: 0,
                                payload: vec![],
                            };
                            self.message = Some(message);
                        }
                        PubIncoming::Batch { batch_type, count } => {
                            match batch_type {
                                BatchType::Start => {
                                    self.in_batch = true;
                                    result = Ok(None);
                                }
                                BatchType::End => {
                                    self.in_batch = false;
                                    result = Ok(Some(Message {
                                        message_type: MessageType::BatchEnd {
                                            count: self.batch_count,
                                        },
                                        topic: "".to_string(),
                                        payload_size: 0,
                                        checksum: "".to_string(),
                                        sequence: 0,
                                        payload: vec![],
                                    }));
                                    self.batch_count = 0;
                                }
                                BatchType::Count => {
                                    self.in_batch = true;
                                    self.batch_count = 0;
                                    self.expected_batch_count = count;
                                    result = Ok(None);
                                }
                            }
                            is_batch = true;
                        }
                    }
                }
            }
        }
        if !is_batch {
            let mut got_payload = false;
            if let Some(message) = &self.message {
                let mut message = message.clone();
                if buf.len() >= message.payload_size {
                    message.payload = buf[..message.payload_size].to_vec();
                    buf.advance(message.payload_size);
                    got_payload = true;
                    result = Ok(Some(message));
                } else {
                    result = Ok(None);
                }
            } else {
                result = Ok(None);
            }
            if got_payload {
                self.message = None;
            }
        }
        result
    }
}

async fn start_pub(
    mut threadpool: ThreadPool,
    broker_manager: Arc<BrokerManager>,
) -> io::Result<()> {
    let mut listener = TcpListener::bind(&"127.0.0.1:7878".parse().unwrap())?;
    let mut incoming = listener.incoming();

    info!("Pub listening on 127.0.0.1:7878");

    while let Some(stream) = incoming.next().await {
        threadpool
            .spawn(new_pub_client(stream?, broker_manager.clone()))
            .unwrap();
    }
    Ok(())
}

pub async fn start_pub_empty(threadpool: ThreadPool, broker_manager: Arc<BrokerManager>) {
    start_pub(threadpool, broker_manager).await.unwrap();
}

async fn new_pub_client(stream: TcpStream, broker_manager: Arc<BrokerManager>) {
    let buf_size = 64000;
    let mut in_bytes = BytesMut::with_capacity(buf_size);
    in_bytes.resize(buf_size, 0);
    let mut broker_tx_cache: HashMap<TopicPartition, mpsc::Sender<BrokerMessage>> = HashMap::new();
    let addr = stream.peer_addr().unwrap();
    info!("Accepting pub stream from: {}", addr);

    let (mut reader, mut writer) = stream.split();
    let mut codec = InMessageCodec::new();
    let mut cont = true;
    let mut leftover_bytes = 0;
    while cont {
        match reader.read(&mut in_bytes[leftover_bytes..]).await {
            Ok(bytes) => {
                if bytes == 0 {
                    error!("Remote publisher done.");
                    cont = false;
                } else {
                    in_bytes.truncate(leftover_bytes + bytes);
                    leftover_bytes = 0;
                    let mut decoding = true;
                    while decoding {
                        decoding = false;
                        match codec.decode(&mut in_bytes) {
                            Ok(Some(message)) => {
                                let tp = TopicPartition {
                                    partition: 0,
                                    topic: message.topic.clone(),
                                };
                                let tx = broker_tx_cache.entry(tp.clone()).or_insert(
                                    broker_manager.get_broker_tx(tp.clone()).await.unwrap(),
                                );
                                if let Err(error) = tx.send(BrokerMessage::Message(message)).await {
                                    error!("Error sending to broker: {}", error);
                                    cont = false;
                                } else {
                                    decoding = true;
                                }
                            }
                            Ok(None) => {
                                if !in_bytes.is_empty() {
                                    leftover_bytes = in_bytes.len();
                                }
                            }
                            Err(error) => {
                                error!("Decode error: {}", error);
                                cont = false;
                            }
                        }
                    }
                    // Reclaim the entire buffer and copy leftover bytes to front.
                    in_bytes.reserve(buf_size - in_bytes.len());
                    unsafe {
                        in_bytes.set_len(buf_size);
                    }
                }
            }
            Err(error) => {
                error!("Error reading socket: {}", error);
                cont = false;
            }
        }
    }

    info!("Closing pub stream from: {}", addr);
    writer.close();
}
