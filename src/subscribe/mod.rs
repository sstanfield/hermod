use std::sync::Arc;
use std::{io, str};

use bytes::BytesMut;
use futures::channel::mpsc;
use futures::executor::ThreadPool;
use futures::io::{AsyncReadExt, ReadHalf};
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

pub mod message_core;
pub use crate::message_core::*;

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
    let mut mc = MessageCore::new(broker_tx, rx, idx, broker_manager, io_pool);
    mc.message_incoming(writer).await;

    info!("Closing sub stream from: {}", addr);
}
