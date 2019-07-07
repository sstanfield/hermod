#![feature(async_await)]

use std::io;

use bytes::BytesMut;
use futures::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};

use log::error;
use romio::TcpStream;

use common::protocolx::*;
use common::types::*;

pub struct Client {
    host: String,
    port: u32,
    reader: ReadHalf<TcpStream>,
    writer: WriteHalf<TcpStream>,
    codec: Box<dyn ProtocolDecoder>,
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
        let codec = client_decoder_factory();
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
                            Ok(Some(ClientMessage::Message { message })) => {
                                self.decoding = true;
                                return Ok(message);
                            }
                            Ok(Some(ClientMessage::StatusOk)) => {
                                println!("XXXX got OK status");
                                self.decoding = true;
                            }
                            Ok(Some(ClientMessage::StatusError { code, message })) => {
                                println!("XXXX got ERROR, code: {}, message: {}", code, message);
                                self.decoding = true;
                            }
                            Ok(Some(ClientMessage::CommitAck {
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
                            Ok(Some(_)) => {
                                // Should not happen, not a valid client message...
                                // XXX do something...
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
