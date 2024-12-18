use std::{io, str};

use bytes::BytesMut;
use futures::channel::mpsc;
use futures::sink::SinkExt;

use tokio::net::tcp::OwnedReadHalf;

use common::types::*;

use log::{debug, error, info};

async fn send_client_error(
    mut message_incoming_tx: mpsc::Sender<ClientMessage>,
    code: u32,
    message: &str,
) {
    if let Err(err) = message_incoming_tx
        .send(ClientMessage::ToClient(ServerToClient::StatusError {
            code,
            message: message.to_string(),
        }))
        .await
    {
        error!(
            "Error sending client status error, {}: {}.  Error: {}",
            code, message, err
        );
    }
}

macro_rules! send {
    ($tx:expr, $message:expr, $cont:expr, $error_msg:expr) => {
        if let Err(err) = $tx.send($message).await {
            error!("{}, error: {}", $error_msg, err);
            $cont = false;
        }
    };
}

pub async fn client_incoming(
    mut message_incoming_tx: mpsc::Sender<ClientMessage>,
    reader: OwnedReadHalf,
    client_decoder_factor: ProtocolServerDecoderFactory,
) {
    let mut decoder = client_decoder_factor();
    //let buf_size = 1_024_000;
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
        if reader.readable().await.is_ok() {
            match reader.try_read(&mut in_bytes[leftover_bytes..]) {
                Ok(bytes) => {
                    if bytes == 0 {
                        info!("No bytes read, closing client.");
                        cont = false;
                    } else {
                        in_bytes.truncate(leftover_bytes + bytes);
                        leftover_bytes = 0;
                        let mut decoding = true;
                        while decoding && cont {
                            decoding = false;
                            match decoder.decode(&mut in_bytes) {
                                Ok(None) => {
                                    if !in_bytes.is_empty() {
                                        leftover_bytes = in_bytes.len();
                                    }
                                }
                                Ok(Some(ClientToServer::Noop)) => decoding = true,
                                Ok(Some(incoming)) => {
                                    send!(
                                        message_incoming_tx,
                                        ClientMessage::ToServer(incoming),
                                        cont,
                                        "Error sending client message, closing connection!"
                                    );
                                    decoding = true;
                                }
                                Err(error) => {
                                    error!(
                                        "Error decoding client message, closing connection: {}",
                                        error
                                    );
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
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(_) => {
                    error!("Error reading client, closing connection");
                    cont = false;
                }
            }
        }
    }
    if let Err(err) = message_incoming_tx
        .send(ClientMessage::ToClient(ServerToClient::Over))
        .await
    {
        debug!("Got error when on client exit, not important: {}", err);
        // ignore, no longer matters...
    }
}
