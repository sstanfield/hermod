use bytes::{Bytes, BytesMut};
use std::{fmt, io};

/// Message, used within the client and server to contain message data.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Message {
    /// Topic and partition of the message.
    pub tp: TopicPartition,
    /// CRC32 of the message payload.
    pub crc: u32,
    /// Message offset number.
    pub offset: u64,
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

/// This is the internal representation of data coming from a client to server.
#[derive(Clone)]
pub enum ClientToServer {
    StatusOk,
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
    PublishMessages {
        messages: Vec<Message>,
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
    Noop,
}

/// This is the internal representation of data going from a server to a client.
#[derive(Clone)]
pub enum ServerToClient {
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
    InternalMessage {
        message: Message,
    },
    Message {
        message: Message,
    },
    MessageBatch {
        file_name: String,
        start: u64,
        length: u64,
    },
    Over,
    MessagesAvailable {
        topic: String,
        partition: u64,
    },
    Noop,
}

#[derive(Clone)]
pub enum ClientMessage {
    ToClient(ServerToClient),
    ToServer(ClientToServer),
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
    /// The data that encoder was asked to encode is not valid.
    Invalid,
}

/// Trait to implement a decoder for data recieved by server.
pub trait ProtocolServerDecoder: Send {
    fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<ClientToServer>>;
}

/// Trait to implement encoding data to send to a client.
pub trait ProtocolServerEncoder: Send {
    fn encode(&mut self, buf: &mut BytesMut, message: ServerToClient) -> EncodeStatus;
}

/// Trait to implement a decoder for data recieved by server.
pub trait ProtocolClientDecoder: Send {
    fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<ServerToClient>>;
}

/// Trait to implement encoding data to send to a client.
pub trait ProtocolClientEncoder: Send {
    fn encode(&mut self, buf: &mut BytesMut, message: ClientToServer) -> EncodeStatus;
}

/// Function prototype for a server decoder factory.
pub type ProtocolServerDecoderFactory = fn() -> Box<dyn ProtocolServerDecoder>;

/// Function prototype for a server encoder factory.
pub type ProtocolServerEncoderFactory = fn() -> Box<dyn ProtocolServerEncoder>;

/// Function prototype for a client decoder factory.
pub type ProtocolClientDecoderFactory = fn() -> Box<dyn ProtocolClientDecoder>;

/// Function prototype for a client encoder factory.
pub type ProtocolClientEncoderFactory = fn() -> Box<dyn ProtocolClientEncoder>;
