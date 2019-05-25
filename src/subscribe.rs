use std::error::Error;
use std::{io, str};

use bytes::BytesMut;
use futures::channel::mpsc;
use futures::executor::ThreadPool;
use futures::future::join;
use futures::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use futures::lock::Mutex;
use futures::sink::SinkExt;
use futures::task::SpawnExt;
use futures::StreamExt;

use romio::{TcpListener, TcpStream};

use super::common::*;
use super::types::*;

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

async fn start_sub(tx: mpsc::Sender<BrokerMessage>, mut threadpool: ThreadPool) -> io::Result<()> {
    let mut listener = TcpListener::bind(&"127.0.0.1:7879".parse().unwrap())?;
    let mut incoming = listener.incoming();

    println!("Sub listening on 127.0.0.1:7879");
    let mut connections = 0;

    while let Some(stream) = incoming.next().await {
        threadpool
            .spawn(new_sub_client(tx.clone(), stream?, connections))
            .unwrap();
        connections += 1;
    }
    Ok(())
}

pub async fn start_sub_empty(tx: mpsc::Sender<BrokerMessage>, threadpool: ThreadPool) {
    start_sub(tx, threadpool).await.unwrap();
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
                    println!("No bytes read, closing client.");
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
                                    //println!("XXXX leftover_bytes: {}", leftover_bytes);
                                }
                            }
                            Ok(Some(ClientIncoming::Status(status))) => {
                                if let Err(_) = message_incoming_tx
                                    .send(ClientMessage::IncomingStatus(status))
                                    .await
                                {
                                    println!("Error sending client status, closing connection!");
                                    cont = false;
                                } else {
                                    decoding = true;
                                }
                            }
                            Ok(Some(ClientIncoming::Topic(topic))) => {
                                if let Err(_) =
                                    message_incoming_tx.send(ClientMessage::Topic(topic)).await
                                {
                                    println!("Error sending client status, closing connection!");
                                    cont = false;
                                } else {
                                    decoding = true;
                                }
                            }
                            Err(_) => {
                                println!("Error decoding client message, closing connection");
                                cont = false;
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
                println!("Error reading client, closing connection");
                cont = false;
            }
        }
    }
    if let Err(_) = message_incoming_tx.send(ClientMessage::Over).await {
        // ignore, no longer matters...
    }
}

async fn message_incoming(
    tx: mpsc::Sender<BrokerMessage>,
    broker_tx: mpsc::Sender<ClientMessage>,
    mut rx: mpsc::Receiver<ClientMessage>,
    writer: WriteHalf<TcpStream>,
    idx: u64,
) {
    let tx = &tx;
    let broker_tx = &broker_tx;
    let writer: &Mutex<WriteHalf<TcpStream>> = &Mutex::new(writer);
    let mut message = rx.next().await;
    let mut cont = true;
    while cont && message.is_some() {
        let mes = message.clone().unwrap();
        let mut tx = tx.clone();
        let broker_tx = broker_tx.clone();
        let writer = writer.clone();
        println!("XXX Got client message!");
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
                    println!("Error writing to client, closing!");
                    cont = false;
                }
            }
            ClientMessage::StatusOk => {
                let v = "{ \"status\": \"OK\"}";
                let mut writer = writer.lock().await;
                if let Err(_) = writer.write_all(v.as_bytes()).await {
                    println!("Error writing to client, closing!");
                    cont = false;
                }
            }
            ClientMessage::Message(message) => {
                let v = format!("{{ \"topic\": \"{}\", \"payload_size\": {}, \"checksum\": \"{}\", \"sequence\": {} }}",
                                   message.topic, message.payload_size, message.checksum, message.sequence); //.clone().as_bytes();
                let mut writer = writer.lock().await;
                if let Err(_) = writer.write_all(v.as_bytes()).await {
                    println!("Error writing to client, closing!");
                    cont = false;
                }
                if let Err(_) = writer.write_all(&message.payload[..]).await {
                    println!("Error writing to client, closing!");
                    cont = false;
                }
            }
            ClientMessage::Topic(topics) => {
                let client_name = format!("Client_{}", idx);
                if let Err(_) = tx
                    .send(BrokerMessage::NewClient(
                        client_name.to_string(),
                        topics,
                        broker_tx.clone(),
                    ))
                    .await
                {
                    println!("Error sending message to broker, close client.");
                    cont = false;
                }
            }
            ClientMessage::IncomingStatus(status) => {
                if status.status.to_lowercase().eq("close") {
                    rx.close();
                }
            }
        };
        message = rx.next().await;
    }
}

async fn new_sub_client(tx: mpsc::Sender<BrokerMessage>, stream: TcpStream, idx: u64) {
    let addr = stream.peer_addr().unwrap();
    let (reader, writer) = stream.split();
    println!("Accepting sub stream from: {}", addr);
    let (broker_tx, rx) = mpsc::channel::<ClientMessage>(10);
    join(
        client_incoming(broker_tx.clone(), reader),
        message_incoming(tx, broker_tx, rx, writer, idx),
    )
    .await;

    println!("Closing sub stream from: {}", addr);
}
