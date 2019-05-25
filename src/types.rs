use futures::channel::mpsc;

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
    pub sequence: usize,
    pub payload: Vec<u8>,
}

#[derive(Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ClientTopics {
    pub topics: Vec<String>,
}

#[derive(Clone)]
pub enum ClientMessage {
    StatusOk,
    StatusError(u32, String),
    Message(Message),
    Over,
    Topic(ClientTopics),
    IncomingStatus(Status),
}

#[derive(Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Status {
    pub status: String,
}

pub enum ClientIncoming {
    Topic(ClientTopics),
    Status(Status),
}

pub enum BrokerMessage {
    Message(Message),
    NewClient(String, ClientTopics, mpsc::Sender<ClientMessage>),
    CloseClient(String),
}
