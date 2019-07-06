use bytes::{Bytes, BytesMut};
use std::io;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum MessageType {
    Message,
    BatchMessage,
    BatchEnd { count: usize },
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Message {
    pub message_type: MessageType,
    pub topic: String,
    pub payload_size: usize,
    pub checksum: String,
    pub sequence: u64,
    pub payload: Vec<u8>,
}

#[derive(Clone)]
pub enum TopicStart {
    Earliest,
    Latest,
    Current,
    Offset { offset: usize },
}

#[derive(Clone)]
pub enum ClientMessage {
    StatusOk,
    StatusOkCount {
        count: usize,
    },
    CommitAck {
        topic: String,
        partition: u64,
        offset: u64,
    },
    StatusError(u32, String),
    Message(Message),
    InternalMessage(Message),
    MessageBatch(String, u64, u64),
    Over,
    Connect(String, String),
    IncomingStatus(String),
    Commit {
        topic: String,
        partition: u64,
        commit_offset: u64,
    },
    PublishMessage {
        message: Message,
    },
    Subscribe {
        topic: String,
        position: TopicStart,
    },
    Unsubscribe {
        topic: String,
    },
    Noop,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TopicPartition {
    pub partition: u64,
    pub topic: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum EncodeStatus {
    Ok,
    BufferToSmall(Bytes),
    Invalid,
}

pub trait ProtocolDecoder: Send {
    fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<ClientMessage>>;
}

pub trait ProtocolEncoder: Send {
    fn encode(&mut self, buf: &mut BytesMut, message: ClientMessage) -> EncodeStatus;
}

pub type ProtocolDecoderFactory = fn() -> Box<dyn ProtocolDecoder>;
pub type ProtocolEncoderFactory = fn() -> Box<dyn ProtocolEncoder>;
