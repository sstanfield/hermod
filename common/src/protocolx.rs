use std::{fmt, io, str};

use bytes::{BufMut, Bytes, BytesMut};

use super::types::*;
use super::util::*;

use log::error;

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

impl TopicStartIn {
    fn to_position(&self, offset: u64) -> TopicPosition {
        match self {
            TopicStartIn::Earliest => TopicPosition::Earliest,
            TopicStartIn::Latest => TopicPosition::Latest,
            TopicStartIn::Current => TopicPosition::Current,
            TopicStartIn::Offset => TopicPosition::Offset { offset },
        }
    }
}

#[derive(Deserialize, Clone, Debug)]
enum SubTypeIn {
    Stream,
    Fetch,
}

impl fmt::Display for SubTypeIn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            SubTypeIn::Stream => write!(f, "Stream"),
            SubTypeIn::Fetch => write!(f, "Fetch"),
        }
    }
}

impl SubTypeIn {
    fn to_subtype(&self) -> SubType {
        match self {
            SubTypeIn::Stream => SubType::Stream,
            SubTypeIn::Fetch => SubType::Fetch,
        }
    }
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
        #[serde(default = "zero_val_u64")]
        partition: u64,
        position: TopicStartIn,
        #[serde(default = "zero_val_u64")]
        offset: u64,
        sub_type: SubTypeIn,
    },
    Unsubscribe {
        topic: String,
        #[serde(default = "zero_val_u64")]
        partition: u64,
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
        #[serde(default = "zero_val_u64")]
        partition: u64,
        payload_size: usize,
        crc: u32,
    },
    Batch {
        batch_type: BatchType,
        #[serde(default = "zero_val")]
        count: usize,
    },
    Fetch {
        topic: String,
        #[serde(default = "zero_val_u64")]
        partition: u64,
        position: TopicStartIn,
        #[serde(default = "zero_val_u64")]
        offset: u64,
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
    message_batch: Option<Vec<Message>>,
}

impl Default for ServerDecoder {
    fn default() -> Self {
        ServerDecoder {
            message: None,
            in_batch: false,
            batch_count: 0,
            expected_batch_count: 0,
            message_batch: None,
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
            message_batch: None,
        }
    }
}

impl ProtocolServerDecoder for ServerDecoder {
    fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<ClientToServer>> {
        let mut result: io::Result<Option<ClientToServer>> = Ok(None);
        if self.message.is_none() {
            if let Some((first_brace, message_offset)) = find_brace(&buf[..]) {
                buf.advance(first_brace);
                let line = buf.split_to((message_offset - first_brace) + 1);
                let incoming: ServerIncoming = serde_json::from_slice(&line[..])?;
                match incoming {
                    ServerIncoming::Publish {
                        topic,
                        partition,
                        payload_size,
                        crc,
                    } => {
                        let message = Message {
                            topic,
                            partition,
                            payload_size,
                            crc,
                            sequence: 0,
                            payload: vec![],
                        };
                        self.message = Some(message);
                    }
                    ServerIncoming::Batch { batch_type, count } => match batch_type {
                        BatchType::Start => {
                            self.in_batch = true;
                            self.message_batch = Some(Vec::with_capacity(100));
                            result = Ok(Some(ClientToServer::Noop));
                        }
                        BatchType::End => {
                            self.in_batch = false;
                            let message_batch = std::mem::replace(&mut self.message_batch, None);
                            if let Some(message_batch) = message_batch {
                                result = Ok(Some(ClientToServer::PublishMessages {
                                    messages: message_batch,
                                }));
                            } else {
                                return Err(io::Error::new(
                                    io::ErrorKind::Other,
                                    "No batch to send!",
                                ));
                            }
                            self.batch_count = 0;
                        }
                        BatchType::Count => {
                            self.in_batch = true;
                            self.batch_count = 0;
                            self.expected_batch_count = count;
                            self.message_batch = Some(Vec::with_capacity(count));
                            result = Ok(Some(ClientToServer::Noop));
                        }
                    },

                    ServerIncoming::Connect {
                        client_name,
                        group_id,
                    } => {
                        result = Ok(Some(ClientToServer::Connect {
                            client_name,
                            group_id,
                        }));
                    }
                    ServerIncoming::Subscribe {
                        topic,
                        partition,
                        position,
                        offset,
                        sub_type,
                    } => {
                        let new_pos = position.to_position(offset);
                        result = Ok(Some(ClientToServer::Subscribe {
                            topic,
                            partition,
                            position: new_pos,
                            sub_type: sub_type.to_subtype(),
                        }));
                    }
                    ServerIncoming::Unsubscribe { topic, partition } => {
                        result = Ok(Some(ClientToServer::Unsubscribe { topic, partition }));
                    }
                    ServerIncoming::Status { status } => {
                        result = match status.as_str() {
                            "close" => Ok(Some(ClientToServer::ClientDisconnect)),
                            "ok" => Ok(Some(ClientToServer::StatusOk)),
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
                        result = Ok(Some(ClientToServer::Commit {
                            topic,
                            partition,
                            commit_offset,
                        }));
                    }
                    ServerIncoming::Fetch {
                        topic,
                        partition,
                        position,
                        offset,
                    } => {
                        let new_pos = position.to_position(offset);
                        result = Ok(Some(ClientToServer::Fetch {
                            topic,
                            partition,
                            position: new_pos,
                        }));
                    }
                }
            }
        }
        let message = std::mem::replace(&mut self.message, None);
        if let Some(mut message) = message {
            if buf.len() >= message.payload_size {
                message.payload = buf[..message.payload_size].to_vec();
                buf.advance(message.payload_size);
                if self.in_batch {
                    if let Some(message_batch) = &mut self.message_batch {
                        message_batch.push(message);
                    }
                    self.batch_count += 1;
                    if self.batch_count == self.expected_batch_count {
                        self.in_batch = false;
                        self.batch_count = 0;
                        self.expected_batch_count = 0;
                        let message_batch = std::mem::replace(&mut self.message_batch, None);
                        if let Some(messages) = message_batch {
                            Ok(Some(ClientToServer::PublishMessages { messages }))
                        } else {
                            Err(io::Error::new(io::ErrorKind::Other, "No batch to send!"))
                        }
                    } else {
                        Ok(Some(ClientToServer::Noop))
                    }
                } else {
                    Ok(Some(ClientToServer::PublishMessage { message }))
                }
            } else {
                self.message = Some(message);
                Ok(None)
            }
        } else {
            result
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ServerEncoder;

impl ProtocolServerEncoder for ServerEncoder {
    fn encode(&mut self, buf: &mut BytesMut, message: ServerToClient) -> EncodeStatus {
        match message {
            ServerToClient::StatusOk => {
                let v = "{\"Status\":{\"status\":\"OK\"}}";
                let bytes = v.as_bytes();
                move_bytes(buf, bytes)
            }
            ServerToClient::StatusOkCount { count } => {
                let v = format!("{{\"Status\":{{\"status\":\"OK\",\"count\":{}}}}}", count);
                let bytes = v.as_bytes();
                move_bytes(buf, bytes)
            }
            ServerToClient::StatusError { code, message } => {
                let v = format!(
                    "{{\"Status\":{{\"status\":\"ERROR\",\"code\":{},\"message\":\"{}\"}}}}",
                    code, message
                );
                let bytes = v.as_bytes();
                move_bytes(buf, bytes)
            }
            ServerToClient::CommitAck {
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
            ServerToClient::Message { message } => {
                let v = format!("{{\"Message\":{{\"topic\":\"{}\",\"payload_size\":{},\"crc\":{},\"sequence\":{}}}}}",
                                   message.topic, message.payload_size, message.crc, message.sequence);
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
            ServerToClient::MessagesAvailable { topic, partition } => {
                let v = format!(
                    "{{\"MessagesAvailable\": {{\"topic\": \"{}\", \"partition\": {}}}}}",
                    topic, partition
                );
                let bytes = v.as_bytes();
                move_bytes(buf, bytes)
            }

            ServerToClient::InternalMessage { .. } => EncodeStatus::Invalid,
            ServerToClient::MessageBatch { .. } => EncodeStatus::Invalid,
            ServerToClient::Over => EncodeStatus::Invalid,
            ServerToClient::Noop => EncodeStatus::Invalid,
        }
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
        #[serde(default = "zero_val_u64")]
        partition: u64,
        payload_size: usize,
        crc: u32,
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
        #[serde(default = "zero_val_u64")]
        partition: u64,
        offset: u64,
    },
    MessagesAvailable {
        topic: String,
        #[serde(default = "zero_val_u64")]
        partition: u64,
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

impl ProtocolClientDecoder for ClientDecoder {
    fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<ServerToClient>> {
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
                        partition,
                        payload_size,
                        crc,
                        sequence,
                    } => {
                        let message = Message {
                            topic,
                            partition,
                            payload_size,
                            crc,
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
                                    Ok(Some(ServerToClient::StatusOkCount { count }))
                                } else {
                                    Ok(Some(ServerToClient::StatusOk))
                                }
                            }
                            StatusType::Error => {
                                Ok(Some(ServerToClient::StatusError { code, message }))
                            }
                        }
                    }
                    ClientIncoming::CommitAck {
                        topic,
                        partition,
                        offset,
                    } => {
                        result = Ok(Some(ServerToClient::CommitAck {
                            topic,
                            partition,
                            offset,
                        }))
                    }
                    ClientIncoming::MessagesAvailable { topic, partition } => {
                        result = Ok(Some(ServerToClient::MessagesAvailable { topic, partition }))
                    }
                }
            }
        }
        let message = std::mem::replace(&mut self.message, None);
        if let Some(mut message) = message {
            if buf.len() >= message.payload_size {
                message.payload = buf[..message.payload_size].to_vec();
                buf.advance(message.payload_size);
                result = Ok(Some(ServerToClient::Message { message }));
            } else {
                self.message = Some(message);
                result = Ok(None);
            }
        }
        result
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ClientEncoder;

impl ProtocolClientEncoder for ClientEncoder {
    fn encode(&mut self, buf: &mut BytesMut, message: ClientToServer) -> EncodeStatus {
        match message {
            ClientToServer::StatusOk => {
                let v = "{\"Status\":{\"status\":\"OK\"}}";
                let bytes = v.as_bytes();
                move_bytes(buf, bytes)
            }
            // Client encodings.
            ClientToServer::Commit {
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
            ClientToServer::Connect {
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
            ClientToServer::Subscribe {
                topic,
                partition,
                position: TopicPosition::Offset { offset },
                sub_type,
            } => {
                let v = format!(
                    "{{\"Subscribe\": {{\"topic\": \"{}\", \"partition\": {}, \"position\": \"Offset\", \"offset\": {}, \"sub_type\":\"{}\"}}}}",
                    topic, partition, offset, sub_type
                );
                let bytes = v.as_bytes();
                move_bytes(buf, bytes)
            }
            ClientToServer::Subscribe {
                topic,
                partition,
                position,
                sub_type,
            } => {
                let v = format!(
                    "{{\"Subscribe\": {{\"topic\": \"{}\", \"partition\": {}, \"position\": \"{}\", \"sub_type\":\"{}\"}}}}",
                    topic, partition, position, sub_type
                );
                let bytes = v.as_bytes();
                move_bytes(buf, bytes)
            }
            ClientToServer::Unsubscribe { topic, partition } => {
                let v = format!(
                    "{{\"Unsubscribe\": {{\"topic\": \"{}\", \"partition\": {}}}}}",
                    topic, partition
                );
                let bytes = v.as_bytes();
                move_bytes(buf, bytes)
            }
            ClientToServer::PublishBatchStart { count } => {
                let v = format!(
                    "{{\"Batch\": {{\"batch_type\": \"Count\", \"count\": {}}}}}",
                    count
                );
                let bytes = v.as_bytes();
                move_bytes(buf, bytes)
            }
            ClientToServer::PublishMessage { message } => {
                let v = format!(
                    "{{\"Publish\": {{\"topic\": \"{}\", \"partition\": {}, \"payload_size\": {}, \"crc\": {}}}}}",
                    message.topic, message.partition, message.payload_size, message.crc
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
            ClientToServer::Fetch {
                topic,
                partition,
                position: TopicPosition::Offset { offset },
            } => {
                let v = format!(
                    "{{\"Fetch\": {{\"topic\": \"{}\", \"partition\": {}, \"position\": \"Offset\", \"offset\": {}}}}}",
                    topic, partition, offset
                );
                let bytes = v.as_bytes();
                move_bytes(buf, bytes)
            }
            ClientToServer::Fetch {
                topic,
                partition,
                position,
            } => {
                let v = format!(
                    "{{\"Fetch\": {{\"topic\": \"{}\", \"partition\": {}, \"position\": \"{}\"}}}}",
                    topic, partition, position
                );
                let bytes = v.as_bytes();
                move_bytes(buf, bytes)
            }
            ClientToServer::ClientDisconnect => EncodeStatus::Invalid,
            ClientToServer::PublishMessages { .. } => EncodeStatus::Invalid,
            ClientToServer::Noop => EncodeStatus::Invalid,
        }
    }
}

pub fn decoder_factory() -> Box<dyn ProtocolServerDecoder> {
    Box::new(ServerDecoder::default())
}

pub fn client_decoder_factory() -> Box<dyn ProtocolClientDecoder> {
    Box::new(ClientDecoder::new())
}

pub fn encoder_factory() -> Box<dyn ProtocolServerEncoder> {
    Box::new(ServerEncoder {})
}

pub fn client_encoder_factory() -> Box<dyn ProtocolClientEncoder> {
    Box::new(ClientEncoder {})
}
