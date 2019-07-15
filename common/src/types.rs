use bytes::{Bytes, BytesMut};
use std::{fmt, io};

/// Type of a message.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum MessageType {
    /// Normal message.
    Message,
    /// Message that is part of a batch of messages.
    BatchMessage,
    /// Message that is the last of a batch, count if number of messages.
    BatchEnd { count: usize },
}

/// Message, used within the client and server to contain message data.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Message {
    /// Type of message (ie standalone or batch).
    pub message_type: MessageType,
    /// Topic of the message.
    pub topic: String,
    /// Partition of the message.
    pub partition: u64,
    /// Size of the payload in bytes.
    pub payload_size: usize,
    /// SHA-1 of the message payload.
    pub crc: u32,
    /// Message sequence number.
    pub sequence: u64,
    /// Raw bytes of the payload.
    pub payload: Vec<u8>,
}

/// When a client subscribes this represents then selected start option.
#[derive(Copy, Clone)]
pub enum TopicPosition {
    /// Send client all data for a topic.
    Earliest,
    /// Only send data recieved after subscribing.
    Latest,
    /// Use the recorded offset for the client's group id to send any newer messages.
    Current,
    /// Start sending from offset.
    Offset { offset: u64 },
}

impl fmt::Display for TopicPosition {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            TopicPosition::Earliest => write!(f, "Earliest"),
            TopicPosition::Latest => write!(f, "Latest"),
            TopicPosition::Current => write!(f, "Current"),
            TopicPosition::Offset { offset } => write!(f, "Offset({})", offset),
        }
    }
}

/// When subscribing indicate whether to stream messages as they come in or require the client to fetch them.
#[derive(Copy, Clone, Eq, PartialEq)]
pub enum SubType {
    /// Messages will be sent to client when available.
    Stream,
    /// Messages will be sent to client when the client requests them.
    Fetch,
}

impl fmt::Display for SubType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            SubType::Stream => write!(f, "Stream"),
            SubType::Fetch => write!(f, "Fetch"),
        }
    }
}

/// This is the internal representation of data coming or going from a client.
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
    StatusError {
        code: u32,
        message: String,
    },
    Message {
        message: Message,
    },
    InternalMessage {
        message: Message,
    },
    MessageBatch {
        file_name: String,
        start: u64,
        length: u64,
    },
    Over,
    Connect {
        client_name: String,
        group_id: String,
    },
    ClientDisconnect,
    Commit {
        topic: String,
        partition: u64,
        commit_offset: u64,
    },
    PublishBatchStart {
        count: u32,
    },
    PublishMessage {
        message: Message,
    },
    Subscribe {
        topic: String,
        partition: u64,
        position: TopicPosition,
        sub_type: SubType,
    },
    Unsubscribe {
        topic: String,
        partition: u64,
    },
    Fetch {
        topic: String,
        partition: u64,
        position: TopicPosition,
    },
    MessagesAvailable {
        topic: String,
        partition: u64,
    },
    Noop,
}

/// A topic/partition pair.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TopicPartition {
    pub partition: u64,
    pub topic: String,
}

/// Return type for the encode method.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum EncodeStatus {
    /// Data encoded into byte buffer successfully.
    Ok,
    /// Byte buffer did not have capacity to hold data, encoded data is returned in enum.
    BufferToSmall(Bytes),
    /// The data that encoder was asked to encode is not valid (all ClientMessage types are not sent to client).
    Invalid,
}

/// Trait to implement a decoder for data recieved over network.
pub trait ProtocolDecoder: Send {
    fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<ClientMessage>>;
}

/// Trait to implement encoding data to send to a client.
pub trait ProtocolEncoder: Send {
    fn encode(&mut self, buf: &mut BytesMut, message: ClientMessage) -> EncodeStatus;
}

/// Function prototype for a decoder factory.
pub type ProtocolDecoderFactory = fn() -> Box<dyn ProtocolDecoder>;

/// Function prototype for an encoder factory.
pub type ProtocolEncoderFactory = fn() -> Box<dyn ProtocolEncoder>;
