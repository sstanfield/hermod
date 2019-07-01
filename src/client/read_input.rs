use std::{str};

use bytes::BytesMut;
use futures::channel::mpsc;
use futures::io::{AsyncReadExt, ReadHalf};
use futures::sink::SinkExt;

use romio::TcpStream;

use super::super::types::*;
use super::protocol::*;

use log::{error, info};

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

macro_rules! send {
    ($tx:expr, $message:expr, $cont:expr, $error_msg:expr) => {
        if let Err(_) = $tx.send($message).await {
            error!($error_msg);
            $cont = false;
        }
    };
}

/*macro_rules! send_with_status_ok {
    ($tx:expr, $message:expr, $cont:expr, $error_msg:expr) => {
        if let Err(_) = $tx.send($message).await {
            error!($error_msg);
            $cont = false;
        } else {
            if let Err(_) = $tx.send(ClientMessage::StatusOk).await {
                error!("Error sending client status ok, closing connection!");
                $cont = false;
            }
        }
    };
}*/

pub async fn client_incoming(
    mut message_incoming_tx: mpsc::Sender<ClientMessage>,
    mut reader: ReadHalf<TcpStream>,
    mut decoder: ClientCodec,
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
                    while decoding && cont {
                        decoding = false;
                        match decoder.decode(&mut in_bytes) {
                            Ok(None) => {
                                if !in_bytes.is_empty() {
                                    leftover_bytes = in_bytes.len();
                                }
                            }
                            Ok(Some(incoming)) => {
                                send!(
                                    message_incoming_tx,
                                    incoming,
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
