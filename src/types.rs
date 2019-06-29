use futures::channel::mpsc;

#[derive(Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum MessageType {
    Message,
    BatchMessage,
    BatchEnd { count: usize },
}

#[derive(Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
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
    StatusError(u32, String),
    Message(Message),
    InternalMessage(Message),
    MessageBatch(String, u64, u64),
    Over,
    Connect(String, String), //, Vec<String>),
    IncomingStatus(String),
    Commit(String, u64, u64),
    PublishMessage(Message),
    Subscribe { topic: String, position: TopicStart },
    Unsubscribe { topic: String },
}

#[derive(Clone)]
pub enum BrokerMessage {
    Message(Message),
    NewClient(String, String, mpsc::Sender<ClientMessage>, bool),
    CloseClient(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TopicPartition {
    pub partition: u64,
    pub topic: String,
}
