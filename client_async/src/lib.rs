#![feature(async_await)]
// XXX This is producing false positives on async fns with reference params.
// Turn back on when it works...
#![allow(clippy::needless_lifetimes)]

use std::io;
use std::net::SocketAddr;

use bytes::{BufMut, BytesMut};
use futures::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};

use crc::{crc32, Hasher32};
use log::error;
use romio::TcpStream;

use common::types::*;

pub const HERMOD_API_VERSION: &str = env!("VERSION_STRING");

macro_rules! write_client {
    ($encoder:expr, $writer:expr, $buf:expr, $mes:expr) => {
        if let EncodeStatus::BufferToSmall(_bytes) = $encoder.encode(&mut $buf, $mes) {
            // XXX cant happen unless buffer is made tiny, return an error.
        }
        $writer.write_all(&$buf).await?;
        $buf.truncate(0);
    };
}

pub struct Client {
    remote: SocketAddr,
    reader: ReadHalf<TcpStream>,
    writer: WriteHalf<TcpStream>,
    codec: Box<dyn ProtocolDecoder>,
    encoder: Box<dyn ProtocolEncoder>,
    buf_size: usize,
    in_bytes: BytesMut,
    out_bytes: BytesMut,
    scratch_bytes: BytesMut,
    leftover_bytes: usize,
    decoding: bool,
    in_publish_batch: bool,
    batch_count: u32,
    client_name: String,
    group_id: String,
    last_offset: u64,
}

impl Client {
    pub async fn connect(
        remote: SocketAddr,
        client_name: String,
        group_id: String,
        decoder_factory: ProtocolDecoderFactory,
        encoder_factory: ProtocolEncoderFactory,
    ) -> io::Result<Client> {
        let codec = decoder_factory();
        let mut encoder = encoder_factory();
        let buf_size = 64000;
        let mut in_bytes = BytesMut::with_capacity(buf_size);
        in_bytes.resize(buf_size, 0);
        let out_bytes = BytesMut::with_capacity(buf_size);
        let mut scratch_bytes = BytesMut::with_capacity(4096);
        let leftover_bytes = 0;
        let stream = TcpStream::connect(&remote).await?;
        let (reader, mut writer) = stream.split();
        write_client!(
            encoder,
            writer,
            scratch_bytes,
            ClientMessage::Connect {
                client_name: client_name.clone(),
                group_id: group_id.clone()
            }
        );
        Ok(Client {
            remote,
            reader,
            writer,
            codec,
            encoder,
            buf_size,
            in_bytes,
            out_bytes,
            scratch_bytes,
            leftover_bytes,
            decoding: false,
            in_publish_batch: false,
            batch_count: 0,
            client_name,
            group_id,
            last_offset: 0,
        })
    }

    pub async fn reconnect(&mut self) -> io::Result<()> {
        self.in_bytes.resize(self.buf_size, 0);
        self.out_bytes.truncate(0);
        self.scratch_bytes.truncate(0);
        self.leftover_bytes = 0;
        let stream = TcpStream::connect(&self.remote).await?;
        let (reader, writer) = stream.split();
        self.reader = reader;
        self.writer = writer;
        write_client!(
            self.encoder,
            self.writer,
            self.scratch_bytes,
            ClientMessage::Connect {
                client_name: self.client_name.clone(),
                group_id: self.group_id.clone()
            }
        );
        Ok(())
    }

    pub async fn subscribe(
        &mut self,
        topic: &str,
        position: TopicPosition,
        sub_type: SubType,
    ) -> io::Result<()> {
        write_client!(
            self.encoder,
            self.writer,
            self.scratch_bytes,
            ClientMessage::Subscribe {
                topic: topic.to_string(),
                partition: 0,
                position,
                sub_type
            }
        );
        Ok(())
    }

    pub async fn fetch(&mut self, topic: &str, position: TopicPosition) -> io::Result<()> {
        println!("XXXXX fetch");
        write_client!(
            self.encoder,
            self.writer,
            self.scratch_bytes,
            ClientMessage::Fetch {
                topic: topic.to_string(),
                partition: 0,
                position,
            }
        );
        Ok(())
    }

    /*async fn commit(&mut self) -> io::Result<()> {
        Ok(())
    }*/

    pub async fn commit_offset(
        &mut self,
        topic: &str,
        partition: u64,
        commit_offset: u64,
    ) -> io::Result<()> {
        write_client!(
            self.encoder,
            self.writer,
            self.scratch_bytes,
            ClientMessage::Commit {
                topic: topic.to_string(),
                partition,
                commit_offset
            }
        );
        Ok(())
    }

    pub async fn start_pub_batch(&mut self) -> io::Result<()> {
        if self.in_publish_batch {
            self.end_pub_batch().await?;
        }
        self.in_publish_batch = true;
        self.batch_count = 0;
        Ok(())
    }

    pub async fn end_pub_batch(&mut self) -> io::Result<()> {
        if self.in_publish_batch && !self.out_bytes.is_empty() && self.batch_count > 0 {
            write_client!(
                self.encoder,
                self.writer,
                self.scratch_bytes,
                ClientMessage::PublishBatchStart {
                    count: self.batch_count
                }
            );
            self.writer.write_all(&self.out_bytes).await?;
            self.out_bytes.truncate(0);
        }
        self.batch_count = 0;
        self.in_publish_batch = false;
        Ok(())
    }

    pub async fn publish(&mut self, topic: &str, partition: u64, payload: &[u8]) -> io::Result<()> {
        let mut digest = crc32::Digest::new(crc32::IEEE);
        digest.write(payload);
        let crc = digest.sum32();
        // XXX check that payload is not to large.
        let packet = ClientMessage::PublishMessage {
            message: Message {
                message_type: MessageType::Message,
                topic: topic.to_string(),
                partition,
                payload_size: payload.len(),
                crc,
                sequence: 0,
                payload: Vec::from(payload),
            },
        };
        if let EncodeStatus::BufferToSmall(bytes) = self.encoder.encode(&mut self.out_bytes, packet)
        {
            if self.in_publish_batch {
                self.end_pub_batch().await?;
                self.start_pub_batch().await?;
                self.out_bytes.put_slice(&bytes);
                self.batch_count += 1;
            } else {
                self.writer.write_all(&bytes).await?;
                self.out_bytes.truncate(0);
            }
        } else if self.in_publish_batch {
            self.batch_count += 1;
        } else {
            self.writer.write_all(&self.out_bytes).await?;
            self.out_bytes.truncate(0);
        }
        Ok(())
    }

    pub async fn next_message(&mut self) -> io::Result<Message> {
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
                                self.last_offset = message.sequence;
                                return Ok(message);
                            }
                            Ok(Some(ClientMessage::StatusOk)) => {
                                println!("XXXX got OK status");
                                self.decoding = true;
                            }
                            Ok(Some(ClientMessage::StatusOkCount { count })) => {
                                println!("XXXX got OK status count: {}", count);
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
                            Ok(Some(ClientMessage::MessagesAvailable { topic, partition })) => {
                                println!(
                                    "XXXX got messages avail, topic: {}, partition: {}",
                                    topic, partition
                                );
                                if let Err(err) = self
                                    .fetch(
                                        &topic,
                                        TopicPosition::Offset {
                                            offset: self.last_offset,
                                        },
                                    )
                                    .await
                                {
                                    error!("Error fetching new messages: {}", err);
                                }
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
