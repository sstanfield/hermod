#![feature(async_await)]

use std::io;

use bytes::BytesMut;
use futures::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};

#[macro_use]
extern crate serde_derive;

use log::error;
use romio::TcpStream;
use serde_json;

use common::types::*;
use common::util::*;

fn zero_val() -> usize {
    0
}

fn empty_string() -> String {
    String::new()
}

#[derive(Deserialize, Clone, Debug)]
enum StatusType {
    OK,
    Error,
}

#[derive(Deserialize)]
//#[serde(untagged)]
enum ClientIncoming {
    Message {
        topic: String,
        payload_size: usize,
        checksum: String,
        sequence: u64,
    },
    Status {
        status: StatusType,
        #[serde(default = "zero_val")]
        code: usize,
        #[serde(default = "empty_string")]
        message: String,
    },
    CommitAck {
        topic: String,
        partition: u64,
        offset: u64,
    },
}

enum DecodedMessage {
    Message {
        message: Message,
    },
    StatusOk,
    StatusError {
        code: usize,
        message: String,
    },
    CommitAck {
        topic: String,
        partition: u64,
        offset: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
struct InMessageCodec {
    message: Option<Message>,
}

impl InMessageCodec {
    pub fn new() -> InMessageCodec {
        InMessageCodec { message: None }
    }

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<DecodedMessage>, io::Error> {
        let mut result = Ok(None);
        if self.message.is_none() {
            if let Some((first_brace, message_offset)) = find_brace(&buf[..]) {
                buf.advance(first_brace);
                let line = buf.split_to(message_offset + 1);
                //println!("XXXX decoding: {}", String::from_utf8(line.to_vec()).unwrap());
                let incoming: ClientIncoming = serde_json::from_slice(&line[..])?;
                match incoming {
                    ClientIncoming::Message {
                        topic,
                        payload_size,
                        checksum,
                        sequence,
                    } => {
                        let message_type = MessageType::Message;
                        let message = Message {
                            message_type,
                            topic,
                            payload_size,
                            checksum,
                            sequence,
                            payload: vec![],
                        };
                        self.message = Some(message);
                    }
                    ClientIncoming::Status {
                        status,
                        code,
                        message,
                    } => {
                        result = match status {
                            StatusType::OK => Ok(Some(DecodedMessage::StatusOk)),
                            StatusType::Error => {
                                Ok(Some(DecodedMessage::StatusError { code, message }))
                            }
                        }
                    }
                    ClientIncoming::CommitAck {
                        topic,
                        partition,
                        offset,
                    } => {
                        result = Ok(Some(DecodedMessage::CommitAck {
                            topic,
                            partition,
                            offset,
                        }))
                    }
                }
            }
        }
        let mut got_payload = false;
        if let Some(message) = &self.message {
            let mut message = message.clone();
            if buf.len() >= message.payload_size {
                message.payload = buf[..message.payload_size].to_vec();
                buf.advance(message.payload_size);
                got_payload = true;
                result = Ok(Some(DecodedMessage::Message { message }));
            } else {
                result = Ok(None);
            }
        }
        if got_payload {
            self.message = None;
        }
        result
    }
}

pub struct Client {
    host: String,
    port: u32,
    reader: ReadHalf<TcpStream>,
    writer: WriteHalf<TcpStream>,
    codec: InMessageCodec,
    buf_size: usize,
    in_bytes: BytesMut,
    leftover_bytes: usize,
    decoding: bool,
}

impl Client {
    pub async fn connect(host: String, port: u32) -> io::Result<Client> {
        let remote = format!("{}:{}", host, port);
        let stream = TcpStream::connect(&remote.parse().unwrap()).await?;
        let (reader, mut writer) = stream.split();
        writer
            .write_all(
                //                b"{\"client_name\": \"client1\", \"group_id\": \"g1\", \"topics\": [\"top1\"]}",
                b"{\"Connect\": {\"client_name\": \"client1\", \"group_id\": \"g1\"}}",
            )
            .await?;
        writer
            .write_all(
                //                b"{\"client_name\": \"client1\", \"group_id\": \"g1\", \"topics\": [\"top1\"]}",
                b"{\"Subscribe\": {\"topic\": \"top1\", \"position\": \"Current\"}}",
            )
            .await?;
        //writer
        //    .write_all(b"{\"topic\": \"top1\", \"partition\": 0, \"commit_offset\": 0}")
        //    .await?;
        let buf_size = 64000;
        let mut in_bytes = BytesMut::with_capacity(buf_size);
        in_bytes.resize(buf_size, 0);
        let codec = InMessageCodec::new();
        let leftover_bytes = 0;
        Ok(Client {
            host,
            port,
            reader,
            writer,
            codec,
            buf_size,
            in_bytes,
            leftover_bytes,
            decoding: false,
        })
    }

    /*async fn commit(&mut self) -> io::Result<()> {
        Ok(())
    }*/

    pub async fn commit_offset(
        &mut self,
        topic: String,
        partition: u64,
        offset: u64,
    ) -> io::Result<()> {
        let message = format!(
            "{{\"Commit\": {{\"topic\": \"{}\", \"partition\": {}, \"commit_offset\": {}}}}}",
            topic, partition, offset
        );
        println!("XXX committing: {}", message);
        self.writer.write_all(message.as_bytes()).await?;
        Ok(())
    }

    pub async fn next(&mut self) -> io::Result<Message> {
        loop {
            let input = if self.decoding {
                Ok(self.in_bytes.len())
            } else {
                // Reclaim the entire buffer and copy leftover bytes to front.
                self.in_bytes.reserve(self.buf_size - self.in_bytes.len());
                unsafe {
                    self.in_bytes.set_len(self.buf_size);
                }
                self.reader
                    .read(&mut self.in_bytes[self.leftover_bytes..])
                    .await
            };
            match input {
                Ok(bytes) => {
                    //println!("XXXX c1: {}", bytes);
                    if bytes == 0 && !self.decoding {
                        error!("Remote publisher done.");
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            "Remote broker done, closing!",
                        ));
                    } else {
                        self.decoding = false;
                        self.in_bytes.truncate(self.leftover_bytes + bytes);
                        self.leftover_bytes = 0;
                        match self.codec.decode(&mut self.in_bytes) {
                            Ok(Some(DecodedMessage::Message { message })) => {
                                self.decoding = true;
                                return Ok(message);
                            }
                            Ok(Some(DecodedMessage::StatusOk)) => {
                                println!("XXXX got OK status");
                                self.decoding = true;
                            }
                            Ok(Some(DecodedMessage::StatusError { code, message })) => {
                                println!("XXXX got ERROR, code: {}, message: {}", code, message);
                                self.decoding = true;
                            }
                            Ok(Some(DecodedMessage::CommitAck {
                                topic,
                                partition,
                                offset,
                            })) => {
                                println!(
                                    "XXXX got CommitAck, topic: {}, partition: {}, offset: {}",
                                    topic, partition, offset
                                );
                                self.decoding = true;
                            }
                            Ok(None) => {
                                if !self.in_bytes.is_empty() {
                                    self.leftover_bytes = self.in_bytes.len();
                                }
                            }
                            Err(error) => {
                                error!("Decode error: {}", error);
                                return Err(error);
                            }
                        }
                    }
                }
                Err(error) => {
                    error!("Error reading socket: {}", error);
                    return Err(error);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
