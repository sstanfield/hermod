use std::{io, str};

use bytes::{BufMut, Bytes, BytesMut};

use super::types::*;
use super::util::*;

use log::{error};

fn zero_val() -> usize {
    0
}

fn zero_val_u32() -> u32 {
    0
}

fn zero_val_u64() -> u64 {
    0
}

fn empty_string() -> String {
    String::new()
}

#[derive(Deserialize, Clone, Debug)]
enum BatchType {
    Start,
    End,
    Count,
}

#[derive(Deserialize, Clone, Debug)]
enum TopicStartIn {
    Earliest,
    Latest,
    Current,
    Offset,
}

/// This enum represents the valid incoming messages from a client.
#[derive(Clone, Deserialize)]
enum ServerIncoming {
    Connect {
        client_name: String,
        group_id: String,
    },
    Subscribe {
        topic: String,
        position: TopicStartIn,
        #[serde(default = "zero_val")]
        offset: usize,
    },
    Unsubscribe {
        topic: String,
    },
    Status {
        status: String,
    },
    Commit {
        topic: String,
        #[serde(default = "zero_val_u64")]
        partition: u64,
        commit_offset: u64,
    },
    Publish {
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

fn move_bytes(buf: &mut BytesMut, bytes: &[u8]) -> EncodeStatus {
    if bytes.len() > buf.remaining_mut() {
        EncodeStatus::BufferToSmall(Bytes::from(bytes))
    } else {
        buf.put_slice(bytes);
        EncodeStatus::Ok
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ServerDecoder {
    message: Option<Message>,
    in_batch: bool,
    batch_count: usize,
    expected_batch_count: usize,
}

impl Default for ServerDecoder {
    fn default() -> Self {
        ServerDecoder {
            message: None,
            in_batch: false,
            batch_count: 0,
            expected_batch_count: 0,
        }
    }
}

impl ServerDecoder {
    pub fn new() -> ServerDecoder {
        ServerDecoder {
            message: None,
            in_batch: false,
            batch_count: 0,
            expected_batch_count: 0,
        }
    }
}

impl ProtocolDecoder for ServerDecoder {
    fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<ClientMessage>> {
        let mut result: io::Result<Option<ClientMessage>> = Ok(None);
        if self.message.is_none() {
            if let Some((first_brace, message_offset)) = find_brace(&buf[..]) {
                buf.advance(first_brace);
                let line = buf.split_to((message_offset - first_brace) + 1);
                //println!("XXXX decoding: {}", String::from_utf8(line.to_vec()).unwrap());
                let incoming: ServerIncoming = serde_json::from_slice(&line[..])?;
                match incoming {
                    ServerIncoming::Publish {
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
                    ServerIncoming::Batch { batch_type, count } => match batch_type {
                        BatchType::Start => {
                            self.in_batch = true;
                            result = Ok(Some(ClientMessage::Noop));
                        }
                        BatchType::End => {
                            self.in_batch = false;
                            result = Ok(Some(ClientMessage::PublishMessage {
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
                            result = Ok(Some(ClientMessage::Noop));
                        }
                    },

                    ServerIncoming::Connect {
                        client_name,
                        group_id,
                    } => {
                        result = Ok(Some(ClientMessage::Connect {
                            client_name,
                            group_id,
                        }));
                    }
                    ServerIncoming::Subscribe {
                        topic,
                        position,
                        offset,
                    } => {
                        let new_pos = match position {
                            TopicStartIn::Earliest => TopicStart::Earliest,
                            TopicStartIn::Latest => TopicStart::Latest,
                            TopicStartIn::Current => TopicStart::Current,
                            TopicStartIn::Offset => TopicStart::Offset { offset },
                        };
                        result = Ok(Some(ClientMessage::Subscribe {
                            topic,
                            position: new_pos,
                        }));
                    }
                    ServerIncoming::Unsubscribe { topic } => {
                        result = Ok(Some(ClientMessage::Unsubscribe { topic }));
                    }
                    ServerIncoming::Status { status } => {
                        result = match status.as_str() {
                            "close" => Ok(Some(ClientMessage::ClientDisconnect)),
                            "ok" => Ok(Some(ClientMessage::StatusOk)),
                            _ => {
                                let mes = format!("Invalid status from client: {}!", status);
                                error!("{}", mes);
                                Err(io::Error::new(io::ErrorKind::Other, mes))
                            }
                        }
                    }
                    ServerIncoming::Commit {
                        topic,
                        partition,
                        commit_offset,
                    } => {
                        result = Ok(Some(ClientMessage::Commit {
                            topic,
                            partition,
                            commit_offset,
                        }));
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
                result = Ok(Some(ClientMessage::PublishMessage { message }));
            } else {
                result = Ok(None);
            }
        }

        result
    }
}

#[derive(Deserialize, Clone, Debug)]
enum StatusType {
    OK,
    Error,
}

#[derive(Deserialize)]
//#[serde(untagged)]
enum ClientIncoming {
    Message {
        topic: String,
        payload_size: usize,
        checksum: String,
        sequence: u64,
    },
    Status {
        status: StatusType,
        #[serde(default = "zero_val_u32")]
        code: u32,
        #[serde(default = "empty_string")]
        message: String,
        #[serde(default = "zero_val")]
        count: usize,
    },
    CommitAck {
        topic: String,
        partition: u64,
        offset: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
struct ClientDecoder {
    message: Option<Message>,
}

impl ClientDecoder {
    pub fn new() -> ClientDecoder {
        ClientDecoder { message: None }
    }
}

impl ProtocolDecoder for ClientDecoder {
    fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<ClientMessage>> {
        let mut result = Ok(None);
        if self.message.is_none() {
            if let Some((first_brace, message_offset)) = find_brace(&buf[..]) {
                buf.advance(first_brace);
                let line = buf.split_to(message_offset + 1);
                //println!("XXXX decoding: {}", String::from_utf8(line.to_vec()).unwrap());
                let incoming: ClientIncoming = serde_json::from_slice(&line[..])?;
                match incoming {
                    ClientIncoming::Message {
                        topic,
                        payload_size,
                        checksum,
                        sequence,
                    } => {
                        let message_type = MessageType::Message;
                        let message = Message {
                            message_type,
                            topic,
                            payload_size,
                            checksum,
                            sequence,
                            payload: vec![],
                        };
                        self.message = Some(message);
                    }
                    ClientIncoming::Status {
                        status,
                        code,
                        message,
                        count,
                    } => {
                        result = match status {
                            StatusType::OK => {
                                if count > 0 {
                                    Ok(Some(ClientMessage::StatusOkCount { count }))
                                } else {
                                    Ok(Some(ClientMessage::StatusOk))
                                }
                            }
                            StatusType::Error => {
                                Ok(Some(ClientMessage::StatusError { code, message }))
                            }
                        }
                    }
                    ClientIncoming::CommitAck {
                        topic,
                        partition,
                        offset,
                    } => {
                        result = Ok(Some(ClientMessage::CommitAck {
                            topic,
                            partition,
                            offset,
                        }))
                    }
                }
            }
        }
        let mut got_payload = false;
        if let Some(message) = &self.message {
            let mut message = message.clone();
            if buf.len() >= message.payload_size {
                message.payload = buf[..message.payload_size].to_vec();
                buf.advance(message.payload_size);
                got_payload = true;
                result = Ok(Some(ClientMessage::Message { message }));
            } else {
                result = Ok(None);
            }
        }
        if got_payload {
            self.message = None;
        }
        result
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Encoder;

impl ProtocolEncoder for Encoder {
    fn encode(&mut self, buf: &mut BytesMut, message: ClientMessage) -> EncodeStatus {
        match message {
            ClientMessage::StatusOk => {
                let v = "{\"Status\":{\"status\":\"OK\"}}";
                let bytes = v.as_bytes();
                move_bytes(buf, bytes)
            }
            ClientMessage::StatusOkCount { count } => {
                let v = format!("{{\"Status\":{{\"status\":\"OK\",\"count\":{}}}}}", count);
                let bytes = v.as_bytes();
                move_bytes(buf, bytes)
            }
            ClientMessage::StatusError { code, message } => {
                let v = format!(
                    "{{\"Status\":{{\"status\":\"ERROR\",\"code\":{},\"message\":\"{}\"}}}}",
                    code, message
                );
                let bytes = v.as_bytes();
                move_bytes(buf, bytes)
            }
            ClientMessage::CommitAck {
                topic,
                partition,
                offset,
            } => {
                let v = format!(
                    "{{\"CommitAck\":{{\"topic\":\"{}\",\"partition\":{},\"offset\":{}}}}}",
                    topic, partition, offset
                );
                let bytes = v.as_bytes();
                move_bytes(buf, bytes)
            }
            ClientMessage::Message { message } => {
                let v = format!("{{\"Message\":{{\"topic\":\"{}\",\"payload_size\":{},\"checksum\":\"{}\",\"sequence\":{}}}}}",
                                   message.topic, message.payload_size, message.checksum, message.sequence);
                let bytes = v.as_bytes();
                if (bytes.len() + message.payload_size) > buf.remaining_mut() {
                    let mut new_bytes = BytesMut::with_capacity(bytes.len() + message.payload_size);
                    new_bytes.put_slice(bytes);
                    new_bytes.put_slice(&message.payload);
                    EncodeStatus::BufferToSmall(new_bytes.freeze())
                } else {
                    buf.put_slice(bytes);
                    buf.put_slice(&message.payload);
                    EncodeStatus::Ok
                }
            }

            // Client encodings.
            ClientMessage::Commit {
                topic,
                partition,
                commit_offset,
            } => {
                let v = format!(
                    "{{\"Commit\": {{\"topic\": \"{}\", \"partition\": {}, \"commit_offset\": {}}}}}",
                    topic, partition, commit_offset
                );
                let bytes = v.as_bytes();
                move_bytes(buf, bytes)
            }
            ClientMessage::Connect {
                client_name,
                group_id,
            } => {
                let v = format!(
                    "{{\"Connect\": {{\"client_name\": \"{}\", \"group_id\": \"{}\"}}}}",
                    client_name, group_id
                );
                let bytes = v.as_bytes();
                move_bytes(buf, bytes)
            }
            ClientMessage::Subscribe {
                topic,
                position: TopicStart::Offset { offset },
            } => {
                let v = format!(
                    "{{\"Subscribe\": {{\"topic\": \"{}\", \"position\": \"Offset\", \"offset\": {}}}}}",
                    topic, offset
                );
                let bytes = v.as_bytes();
                move_bytes(buf, bytes)
            }
            ClientMessage::Subscribe {
                topic,
                position: TopicStart::Earliest,
            } => {
                let v = format!(
                    "{{\"Subscribe\": {{\"topic\": \"{}\", \"position\": \"Earliest\"}}}}",
                    topic
                );
                let bytes = v.as_bytes();
                move_bytes(buf, bytes)
            }
            ClientMessage::Subscribe {
                topic,
                position: TopicStart::Latest,
            } => {
                let v = format!(
                    "{{\"Subscribe\": {{\"topic\": \"{}\", \"position\": \"Latest\"}}}}",
                    topic
                );
                let bytes = v.as_bytes();
                move_bytes(buf, bytes)
            }
            ClientMessage::Subscribe {
                topic,
                position: TopicStart::Current,
            } => {
                let v = format!(
                    "{{\"Subscribe\": {{\"topic\": \"{}\", \"position\": \"Current\"}}}}",
                    topic
                );
                let bytes = v.as_bytes();
                move_bytes(buf, bytes)
            }
            ClientMessage::PublishBatchStart { count } => {
                let v = format!(
                    "{{\"Batch\": {{\"batch_type\": \"Count\", \"count\": {}}}}}",
                    count
                );
                let bytes = v.as_bytes();
                move_bytes(buf, bytes)
            }
            ClientMessage::PublishMessage { message } => {
                let v = format!(
                    "{{\"Publish\": {{\"topic\": \"{}\", \"payload_size\": {}, \"checksum\": \"\"}}}}",
                    message.topic, message.payload_size
                );
                let bytes = v.as_bytes();
                if (bytes.len() + message.payload_size) > buf.remaining_mut() {
                    let mut new_bytes = BytesMut::with_capacity(bytes.len() + message.payload_size);
                    new_bytes.put_slice(bytes);
                    new_bytes.put_slice(&message.payload);
                    EncodeStatus::BufferToSmall(new_bytes.freeze())
                } else {
                    buf.put_slice(bytes);
                    buf.put_slice(&message.payload);
                    EncodeStatus::Ok
                }
            }
            _ => EncodeStatus::Invalid,
        }
    }
}

pub fn decoder_factory() -> Box<dyn ProtocolDecoder> {
    Box::new(ServerDecoder::default())
}

pub fn client_decoder_factory() -> Box<dyn ProtocolDecoder> {
    Box::new(ClientDecoder::new())
}

pub fn encoder_factory() -> Box<dyn ProtocolEncoder> {
    Box::new(Encoder {})
}
