use std::{io, str};

use bytes::BytesMut;
use futures::channel::mpsc;
use futures::io::{AsyncReadExt, ReadHalf};
use futures::sink::SinkExt;

use romio::TcpStream;

use super::super::common::*;
use super::super::types::*;

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

#[derive(Clone, Deserialize)]
#[serde(untagged)]
enum ClientIncoming {
    Connect {
        client_name: String,
        group_id: String,
        topics: Vec<String>,
    },
    Topics {
        topics: Vec<String>,
    },
    Status {
        status: String,
    },
    Commit {
        topic: String,
        partition: u64,
        commit_offset: u64,
    },
    PublishMessage {
        topic: String,
        payload_size: usize,
        checksum: String,
    },
    Batch {
        batch_type: BatchType,
        #[serde(default = "zero_val")]
        count: usize,
    },
    MessageOut {
        message: Message,
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

    fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<ClientIncoming>> {
        let mut result: io::Result<Option<ClientIncoming>> = Ok(None);
        if self.message.is_none() {
            if let Some((first_brace, message_offset)) = find_brace(&buf[..]) {
                buf.advance(first_brace);
                let line = buf.split_to(message_offset + 1);
                let incoming: ClientIncoming = serde_json::from_slice(&line[..])?;
                match incoming {
                    ClientIncoming::PublishMessage {
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
                    ClientIncoming::Batch { batch_type, count } => match batch_type {
                        BatchType::Start => {
                            self.in_batch = true;
                            result = Ok(None);
                        }
                        BatchType::End => {
                            self.in_batch = false;
                            result = Ok(Some(ClientIncoming::MessageOut {
                                message: Message {
                                    message_type: MessageType::BatchEnd {
                                        count: self.batch_count,
                                    },
                                    topic: "".to_string(),
                                    payload_size: 0,
                                    checksum: "".to_string(),
                                    sequence: 0,
                                    payload: vec![],
                                },
                            }));
                            self.batch_count = 0;
                        }
                        BatchType::Count => {
                            self.in_batch = true;
                            self.batch_count = 0;
                            self.expected_batch_count = count;
                            result = Ok(None);
                        }
                    },
                    ClientIncoming::MessageOut { message: _ } => {
                        result = Ok(None);
                        // XXX Should probably kill client, this is not valid input from remote side.
                    }
                    _ => {
                        // The other variants do not need special treatment.
                        result = Ok(Some(incoming));
                    }
                }
            }
        }
        if let Some(message) = &self.message {
            let mut message = message.clone();
            if buf.len() >= message.payload_size {
                message.payload = buf[..message.payload_size].to_vec();
                buf.advance(message.payload_size);
                self.message = None;
                result = Ok(Some(ClientIncoming::MessageOut { message }));
            } else {
                result = Ok(None);
            }
        }

        result
    }
}

/*fn decode(buf: &mut BytesMut) -> Result<Option<ClientIncoming>, io::Error> {
    let mut result: Result<Option<ClientIncoming>, io::Error> = Ok(None);
    if let Some((first_brace, message_offset)) = find_brace(&buf[..]) {
        buf.advance(first_brace);
        let line = buf.split_to(message_offset + 1);
        let incoming = serde_json::from_slice(&line[..])?;
        result = Ok(Some(incoming));
    }
    result
}*/

async fn send_client_error(
    mut message_incoming_tx: mpsc::Sender<ClientMessage>,
    code: u32,
    message: &str,
) {
    if let Err(_) = message_incoming_tx
        .send(ClientMessage::StatusError(code, message.to_string()))
        .await
    {
        error!("Error sending client status error, {}: {}!", code, message);
    }
}

pub async fn client_incoming(
    mut message_incoming_tx: mpsc::Sender<ClientMessage>,
    mut reader: ReadHalf<TcpStream>,
) {
    let buf_size = 32000;
    let mut in_bytes = BytesMut::with_capacity(buf_size);
    in_bytes.resize(buf_size, 0);
    let mut leftover_bytes = 0;
    let mut cont = true;
    let mut decoder = InMessageCodec::new();
    while cont {
        if leftover_bytes >= in_bytes.len() {
            error!("Error in incoming client message, closing connection");
            cont = false;
            send_client_error(
                message_incoming_tx.clone(),
                502,
                "Invalid data, closing connection!",
            )
            .await;
            continue;
        }
        match reader.read(&mut in_bytes[leftover_bytes..]).await {
            Ok(bytes) => {
                if bytes == 0 {
                    info!("No bytes read, closing client.");
                    cont = false;
                } else {
                    in_bytes.truncate(leftover_bytes + bytes);
                    leftover_bytes = 0;
                    let mut decoding = true;
                    while decoding && cont {
                        decoding = false;
                        match decoder.decode(&mut in_bytes) {
                            Ok(None) => {
                                if !in_bytes.is_empty() {
                                    leftover_bytes = in_bytes.len();
                                }
                            }
                            Ok(Some(ClientIncoming::Status { status })) => {
                                if let Err(_) = message_incoming_tx
                                    .send(ClientMessage::IncomingStatus(status))
                                    .await
                                {
                                    error!("Error sending client status, closing connection!");
                                    cont = false;
                                }
                                decoding = true;
                            }
                            Ok(Some(ClientIncoming::Connect {
                                client_name,
                                group_id,
                                topics,
                            })) => {
                                if let Err(_) = message_incoming_tx
                                    .send(ClientMessage::Connect(client_name, group_id, topics))
                                    .await
                                {
                                    error!("Error sending client connect, closing connection!");
                                    cont = false;
                                } else {
                                    decoding = true;
                                    if let Err(_) =
                                        message_incoming_tx.send(ClientMessage::StatusOk).await
                                    {
                                        error!(
                                            "Error sending client status ok, closing connection!"
                                        );
                                        cont = false;
                                    }
                                }
                            }
                            Ok(Some(ClientIncoming::Topics { topics })) => {
                                if let Err(_) =
                                    message_incoming_tx.send(ClientMessage::Topic(topics)).await
                                {
                                    error!("Error sending client topics, closing connection!");
                                    cont = false;
                                } else {
                                    decoding = true;
                                    if let Err(_) =
                                        message_incoming_tx.send(ClientMessage::StatusOk).await
                                    {
                                        error!(
                                            "Error sending client status ok, closing connection!"
                                        );
                                        cont = false;
                                    }
                                }
                            }
                            Ok(Some(ClientIncoming::Commit {
                                topic,
                                partition,
                                commit_offset,
                            })) => {
                                decoding = true;
                                if let Err(_) = message_incoming_tx
                                    .send(ClientMessage::Commit(topic, partition, commit_offset))
                                    .await
                                {
                                    error!(
                                        "Error sending client commit offset, closing connection!"
                                    );
                                    cont = false;
                                }
                            }
                            Ok(Some(ClientIncoming::MessageOut { message })) => {
                                decoding = true;
                                if let Err(_) = message_incoming_tx
                                    .send(ClientMessage::PublishMessage(message))
                                    .await
                                {
                                    error!("Error sending publishing message, closing connection!");
                                    cont = false;
                                }
                            }
                            Ok(Some(ClientIncoming::PublishMessage {
                                topic: _,
                                payload_size: _,
                                checksum: _,
                            })) => {
                                error!("Leaked an incoming PublishMessage, fix me!");
                                cont = false;
                            }
                            Ok(Some(ClientIncoming::Batch {
                                batch_type: _,
                                count: _,
                            })) => {
                                error!("Leaked an incoming Batch message, fix me!");
                                cont = false;
                            }
                            Err(_) => {
                                error!("Error decoding client message, closing connection");
                                cont = false;
                                send_client_error(
                                    message_incoming_tx.clone(),
                                    501,
                                    "Invalid data, closing connection!",
                                )
                                .await;
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
            Err(_) => {
                error!("Error reading client, closing connection");
                cont = false;
            }
        }
    }
    if let Err(_) = message_incoming_tx.send(ClientMessage::Over).await {
        // ignore, no longer matters...
    }
}
