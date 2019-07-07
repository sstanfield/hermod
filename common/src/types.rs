use bytes::{Bytes, BytesMut};
use std::io;

/// Type of a message.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
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
    // XXX add partition or use TopicPartition.
    /// Size of the payload in bytes.
    pub payload_size: usize,
    /// SHA-1 of the message payload.
    pub checksum: String,
    /// Message sequence number.
    pub sequence: u64,
    /// Raw bytes of the payload.
    pub payload: Vec<u8>,
}

/// When a client subscribes this represents then selected start option.
#[derive(Clone)]
pub enum TopicStart {
    /// Send client all data for a topic.
    Earliest,
    /// Only send data recieved after subscribing.
    Latest,
    /// Use the recorded offset for the client's group id to send any newer messages.
    Current,
    /// Start sending from offset.
    Offset { offset: usize },
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
    IncomingStatus {
        status: String,
    },
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
