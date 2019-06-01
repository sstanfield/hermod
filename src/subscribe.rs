use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;
use std::{io, str};

use bytes::BytesMut;
use futures::channel::mpsc;
use futures::executor::ThreadPool;
use futures::future::FutureExt;
use futures::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use futures::lock::Mutex;
use futures::sink::SinkExt;
use futures::task::SpawnExt;
use futures::StreamExt;

use romio::{TcpListener, TcpStream};

use super::broker::*;
use super::common::*;
use super::types::*;

use log::{error, info};

fn decode(buf: &mut BytesMut) -> Result<Option<ClientIncoming>, io::Error> {
    let mut result: Result<Option<ClientIncoming>, io::Error> = Ok(None);
    if let Some(brace_offset) = buf[..].iter().position(|b| *b == b'{') {
        if brace_offset > 0 {
            buf.advance(brace_offset);
        }
    }
    if let Some(message_offset) = last_brace(&buf[..]) {
        if message_offset > 3 {
            let line = buf.split_to(message_offset + 1);
            let line_str: String;
            match str::from_utf8(&line[..]) {
                Ok(str) => line_str = str.to_string(),
                Err(err) => return Err(io::Error::new(io::ErrorKind::Other, err.description())),
            }
            if line_str.to_lowercase().contains("topics") {
                let topics: ClientTopics = serde_json::from_slice(&line[..])?;
                result = Ok(Some(ClientIncoming::Topic(topics)));
            } else {
                let status: Status = serde_json::from_slice(&line[..])?;
                result = Ok(Some(ClientIncoming::Status(status)));
            }
        }
    }
    result
}

async fn start_sub(
    mut threadpool: ThreadPool,
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
            ))
            .unwrap();
        connections += 1;
    }
    Ok(())
}

pub async fn start_sub_empty(threadpool: ThreadPool, broker_manager: Arc<BrokerManager>) {
    start_sub(threadpool, broker_manager).await.unwrap();
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
                            Ok(Some(ClientIncoming::Status(status))) => {
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
                            Ok(Some(ClientIncoming::Topic(topic))) => {
                                if let Err(_) =
                                    message_incoming_tx.send(ClientMessage::Topic(topic)).await
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
                            Err(_) => {
                                error!("Error decoding client message, closing connection");
                                cont = false;
                                if let Err(_) = message_incoming_tx
                                    .send(ClientMessage::StatusError(
                                        501,
                                        "Invalid data, closing connection!".to_string(),
                                    ))
                                    .await
                                {
                                    error!(
                                        "Error sending client status error, closing connection!"
                                    );
                                }
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

async fn message_incoming(
    broker_tx: mpsc::Sender<ClientMessage>,
    mut rx: mpsc::Receiver<ClientMessage>,
    writer: WriteHalf<TcpStream>,
    idx: u64,
    broker_manager: Arc<BrokerManager>,
) {
    let mut broker_tx_cache: HashMap<TopicPartition, mpsc::Sender<BrokerMessage>> = HashMap::new();
    let broker_tx = &broker_tx;
    let writer: &Mutex<WriteHalf<TcpStream>> = &Mutex::new(writer);
    let mut cont = true;
    let mut message = rx.next().await;
    while cont && message.is_some() {
        let mes = message.clone().unwrap();
        let broker_tx = broker_tx.clone();
        let writer = writer.clone();
        match mes {
            ClientMessage::Over => {
                rx.close();
            }
            ClientMessage::StatusError(code, message) => {
                let v = format!(
                    "{{ \"status\": \"ERROR\", \"code\": {}, \"message\": \"{}\" }}",
                    code, message
                );
                let mut writer = writer.lock().await;
                if let Err(_) = writer.write_all(v.as_bytes()).await {
                    error!("Error writing to client, closing!");
                    cont = false;
                }
            }
            ClientMessage::StatusOk => {
                let v = "{ \"status\": \"OK\"}";
                let mut writer = writer.lock().await;
                if let Err(_) = writer.write_all(v.as_bytes()).await {
                    error!("Error writing to client, closing!");
                    cont = false;
                }
            }
            ClientMessage::Message(message) => {
                let v = format!("{{ \"topic\": \"{}\", \"payload_size\": {}, \"checksum\": \"{}\", \"sequence\": {} }}",
                                   message.topic, message.payload_size, message.checksum, message.sequence); //.clone().as_bytes();
                let mut writer = writer.lock().await;
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
                let buf_size = 128000;
                let mut buf = vec![0; buf_size];
                let mut file = File::open(&file_name).unwrap();
                file.seek(SeekFrom::Start(start)).unwrap();
                let mut left = length as usize;
                let mut writer = writer.lock().await;
                // XXX this is sync and dumb, get an async sendfile...
                while left > 0 {
                    let buf_len = if left < buf_size { left } else { buf_size };
                    match file.read(&mut buf[..buf_len]) {
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
            }
            ClientMessage::Topic(topics) => {
                let client_name = format!("Client_{}", idx);
                for topic in topics.topics {
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
                            broker_tx.clone(),
                        ))
                        .await
                    {
                        error!("Error sending message to broker, close client.");
                        cont = false;
                    }
                }
            }
            ClientMessage::IncomingStatus(status) => {
                if status.status.to_lowercase().eq("close") {
                    info!("Client close request.");
                    rx.close();
                    writer
                        .lock()
                        .then(async move |mut w| {
                            w.close();
                        })
                        .await;
                }
            }
        };
        println!("XXXX awaiting message!");
        message = rx.next().await;
        println!("XXXX got message!");
    }
    info!("Exiting messaging_incoming.");
}

async fn new_sub_client(
    stream: TcpStream,
    idx: u64,
    broker_manager: Arc<BrokerManager>,
    mut threadpool: ThreadPool,
) {
    let addr = stream.peer_addr().unwrap();
    let (reader, writer) = stream.split();
    info!("Accepting sub stream from: {}", addr);
    let (broker_tx, rx) = mpsc::channel::<ClientMessage>(10);
    // Do this so when message_incoming completes client_incoming is dropped and the connection closes.
    let _client = threadpool
        .spawn_with_handle(client_incoming(broker_tx.clone(), reader))
        .unwrap();
    message_incoming(broker_tx, rx, writer, idx, broker_manager).await;

    info!("Closing sub stream from: {}", addr);
}
