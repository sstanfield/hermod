use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;
use std::{io, str};

use bytes::BytesMut;
use futures::channel::mpsc;
use futures::executor::ThreadPool;
use futures::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use futures::sink::SinkExt;
use futures::task::SpawnExt;
use futures::StreamExt;

use romio::{TcpListener, TcpStream};

use super::broker::*;
use super::common::*;
use super::types::*;

use log::{error, info};

#[derive(Clone, Deserialize)]
#[serde(untagged)]
pub enum ClientIncoming {
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
}

fn decode(buf: &mut BytesMut) -> Result<Option<ClientIncoming>, io::Error> {
    let mut result: Result<Option<ClientIncoming>, io::Error> = Ok(None);
    if let Some((first_brace, message_offset)) = find_brace(&buf[..]) {
        buf.advance(first_brace);
        let line = buf.split_to(message_offset + 1);
        let incoming = serde_json::from_slice(&line[..])?;
        result = Ok(Some(incoming));
    }
    result
}

async fn start_sub(
    mut threadpool: ThreadPool,
    mut io_pool: ThreadPool,
    broker_manager: Arc<BrokerManager>,
) -> io::Result<()> {
    let mut listener = TcpListener::bind(&"127.0.0.1:7879".parse().unwrap())?;
    let mut incoming = listener.incoming();

    info!("Sub listening on 127.0.0.1:7879");
    let mut connections = 0;

    while let Some(stream) = incoming.next().await {
        threadpool
            .spawn(new_sub_client(
                stream?,
                connections,
                broker_manager.clone(),
                threadpool.clone(),
                io_pool.clone(),
            ))
            .unwrap();
        connections += 1;
    }
    Ok(())
}

pub async fn start_sub_empty(
    threadpool: ThreadPool,
    io_pool: ThreadPool,
    broker_manager: Arc<BrokerManager>,
) {
    start_sub(threadpool, io_pool, broker_manager)
        .await
        .unwrap();
}

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

async fn client_incoming(
    mut message_incoming_tx: mpsc::Sender<ClientMessage>,
    mut reader: ReadHalf<TcpStream>,
) {
    let buf_size = 32000;
    let mut in_bytes = BytesMut::with_capacity(buf_size);
    in_bytes.resize(buf_size, 0);
    let mut leftover_bytes = 0;
    let mut cont = true;
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
                    while decoding {
                        decoding = false;
                        match decode(&mut in_bytes) {
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
                                } else {
                                    decoding = true;
                                }
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
                                        decoding = false;
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
                                        decoding = false;
                                    }
                                }
                            }
                            Ok(Some(ClientIncoming::Commit {
                                topic,
                                partition,
                                commit_offset,
                            })) => {
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

async fn send_messages(
    mut writer: WriteHalf<TcpStream>,
    file_name: String,
    start: u64,
    length: u64,
) -> WriteHalf<TcpStream> {
    let buf_size = 128000;
    let mut buf = vec![0; buf_size];
    let mut file = File::open(&file_name).unwrap();
    file.seek(SeekFrom::Start(start)).unwrap();
    let mut left = length as usize;
    // XXX this is sync and dumb, get an async sendfile...
    while left > 0 {
        let buf_len = if left < buf_size { left } else { buf_size };
        match file.read(&mut buf[..buf_len]) {
            Ok(0) => {
                error!("Failed to read log file.");
                break;
            }
            Ok(bytes) => {
                writer.write_all(&buf[..bytes]).await.unwrap(); // XXX no unwrap...
                left -= bytes;
            }
            Err(error) => {
                // XXX do better.
                error!("{}", error);
            }
        }
    }
    writer
}

async fn message_incoming(
    broker_tx: mpsc::Sender<ClientMessage>,
    mut rx: mpsc::Receiver<ClientMessage>,
    mut writer: WriteHalf<TcpStream>,
    idx: u64,
    broker_manager: Arc<BrokerManager>,
    mut io_pool: ThreadPool,
) {
    let mut broker_tx_cache: HashMap<TopicPartition, mpsc::Sender<BrokerMessage>> = HashMap::new();
    let broker_tx = &broker_tx;
    let mut cont = true;
    let mut message = rx.next().await;
    let mut client_name = format!("Client_{}", idx);
    let mut group_id: Option<String> = None;
    while cont && message.is_some() {
        let mes = message.clone().unwrap();
        let broker_tx = broker_tx.clone();
        match mes {
            ClientMessage::Over => {
                rx.close();
            }
            ClientMessage::StatusError(code, message) => {
                let v = format!(
                    "{{ \"status\": \"ERROR\", \"code\": {}, \"message\": \"{}\" }}",
                    code, message
                );
                if let Err(_) = writer.write_all(v.as_bytes()).await {
                    error!("Error writing to client, closing!");
                    cont = false;
                }
            }
            ClientMessage::StatusOk => {
                let v = "{ \"status\": \"OK\"}";
                if let Err(_) = writer.write_all(v.as_bytes()).await {
                    error!("Error writing to client, closing!");
                    cont = false;
                }
            }
            ClientMessage::InternalMessage(message) => {
                //println!("XXXX internal {}: {}", message.topic, std::str::from_utf8(&message.payload[..]).unwrap());
            }
            ClientMessage::Message(message) => {
                let v = format!("{{ \"topic\": \"{}\", \"payload_size\": {}, \"checksum\": \"{}\", \"sequence\": {} }}",
                                   message.topic, message.payload_size, message.checksum, message.sequence);
                if let Err(_) = writer.write_all(v.as_bytes()).await {
                    error!("Error writing to client, closing!");
                    cont = false;
                }
                if let Err(_) = writer.write_all(&message.payload[..]).await {
                    error!("Error writing to client, closing!");
                    cont = false;
                }
            }
            ClientMessage::MessageBatch(file_name, start, length) => {
                writer = io_pool
                    .spawn_with_handle(send_messages(writer, file_name.clone(), start, length))
                    .unwrap()
                    .await;
            }
            ClientMessage::Connect(name, gid, topics) => {
                client_name = name;
                group_id = Some(gid);
                for topic in topics {
                    for topic in broker_manager.expand_topics(topic).await {
                        let tp = TopicPartition {
                            partition: 0,
                            topic,
                        };
                        let tx = broker_tx_cache
                            .entry(tp.clone())
                            .or_insert(broker_manager.get_broker_tx(tp.clone()).await.unwrap());
                        if let Err(_) = tx
                            .send(BrokerMessage::NewClient(
                                client_name.to_string(),
                                group_id.clone().unwrap(),
                                broker_tx.clone(),
                                false,
                            ))
                            .await
                        {
                            error!("Error sending message to broker, close client.");
                            cont = false;
                        }
                        // XXX refactor!
                        let tp = TopicPartition {
                            partition: 0,
                            topic: "__topic_online".to_string(),
                        };
                        let tx = broker_tx_cache
                            .entry(tp.clone())
                            .or_insert(broker_manager.get_broker_tx(tp.clone()).await.unwrap());
                        if let Err(_) = tx
                            .send(BrokerMessage::NewClient(
                                client_name.to_string(),
                                group_id.clone().unwrap(),
                                broker_tx.clone(),
                                true,
                            ))
                            .await
                        {
                            error!("Error sending message to broker, close client.");
                            cont = false;
                        }
                    }
                }
            }
            ClientMessage::Topic(topics) => {
                if group_id.is_none() {
                    let v = format!(
                        "{{ \"status\": \"ERROR\", \"code\": {}, \"message\": \"{}\" }}",
                        503, "Client initialized, closing connection!"
                    );
                    if let Err(_) = writer.write_all(v.as_bytes()).await {
                        error!("Error writing to client, closing!");
                    }
                    error!("Error client tried to set topics with no group id, close client.");
                    cont = false;
                    continue;
                }
                for topic in topics {
                    let tp = TopicPartition {
                        partition: 0,
                        topic,
                    };
                    let tx = broker_tx_cache
                        .entry(tp.clone())
                        .or_insert(broker_manager.get_broker_tx(tp.clone()).await.unwrap());
                    if let Err(_) = tx
                        .send(BrokerMessage::NewClient(
                            client_name.to_string(),
                            group_id.clone().unwrap(),
                            broker_tx.clone(),
                            false,
                        ))
                        .await
                    {
                        error!("Error sending message to broker, close client.");
                        cont = false;
                    }
                }
            }
            ClientMessage::IncomingStatus(status) => {
                if status.to_lowercase().eq("close") {
                    info!("Client close request.");
                    rx.close();
                }
            }
            ClientMessage::Commit(topic, partition, commit_offset) => {
                // XXX should check that the topic/partition are in use by this client.
                if group_id.is_none() {
                    let v = format!(
                        "{{ \"status\": \"ERROR\", \"code\": {}, \"message\": \"{}\" }}",
                        503, "Client initialized, closing connection!"
                    );
                    if let Err(_) = writer.write_all(v.as_bytes()).await {
                        error!("Error writing to client, closing!");
                    }
                    error!("Error client tried to set topics with no group id, close client.");
                    cont = false;
                    continue;
                }
                let group_id = group_id.clone().unwrap();
                let commit_topic =
                    format!("__consumer_offsets-{}-{}-{}", group_id, topic, partition);
                let tp = TopicPartition {
                    partition: 0,
                    topic: commit_topic.clone(),
                };
                let tx = broker_tx_cache
                    .entry(tp.clone())
                    .or_insert(broker_manager.get_broker_tx(tp.clone()).await.unwrap());
                let payload = format!(
                    "{{\"group_id\": \"{}\", \"partition\": {}, \"topic\": \"{}\", \"offset\": {}}}",
                    group_id, partition, topic, commit_offset
                )
                .into_bytes();
                let message = Message {
                    message_type: MessageType::Message,
                    topic: commit_topic,
                    payload_size: payload.len(),
                    checksum: "".to_string(),
                    sequence: 0,
                    payload,
                };
                if let Err(error) = tx.send(BrokerMessage::Message(message)).await {
                    error!("Error sending to broker: {}", error);
                    cont = false;
                }
            }
        };
        message = rx.next().await;
    }
    info!("Exiting messaging_incoming.");
}

async fn new_sub_client(
    stream: TcpStream,
    idx: u64,
    broker_manager: Arc<BrokerManager>,
    mut threadpool: ThreadPool,
    mut io_pool: ThreadPool,
) {
    let addr = stream.peer_addr().unwrap();
    let (reader, writer) = stream.split();
    info!("Accepting sub stream from: {}", addr);
    let (broker_tx, rx) = mpsc::channel::<ClientMessage>(10);
    // Do this so when message_incoming completes client_incoming is dropped and the connection closes.
    let _client = threadpool
        .spawn_with_handle(client_incoming(broker_tx.clone(), reader))
        .unwrap();
    message_incoming(broker_tx, rx, writer, idx, broker_manager, io_pool).await;

    info!("Closing sub stream from: {}", addr);
}
