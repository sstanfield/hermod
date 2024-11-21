use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;

use bytes::{BufMut, BytesMut};

use crc::CRC_32_CKSUM;
use log::error;

use common::types::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

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
    reader: OwnedReadHalf,
    writer: OwnedWriteHalf,
    codec: Box<dyn ProtocolClientDecoder>,
    encoder: Box<dyn ProtocolClientEncoder>,
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
    messages: VecDeque<Message>,
    do_fetch: Option<TopicPartition>,
}

impl Client {
    async fn next_client_message(&mut self) -> io::Result<Option<ServerToClient>> {
        let input = if self.decoding && !self.in_bytes.is_empty() {
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
                if bytes == 0 && !self.decoding {
                    error!("Remote publisher done.");
                    Err(io::Error::new(
                        io::ErrorKind::Other,
                        "Remote broker done, closing!",
                    ))
                } else {
                    self.decoding = false;
                    self.in_bytes.truncate(self.leftover_bytes + bytes);
                    self.leftover_bytes = 0;
                    match self.codec.decode(&mut self.in_bytes) {
                        Ok(Some(client_message)) => {
                            self.decoding = true;
                            Ok(Some(client_message))
                        }
                        Ok(None) => {
                            if !self.in_bytes.is_empty() {
                                self.leftover_bytes = self.in_bytes.len();
                            }
                            Ok(None)
                        }
                        Err(error) => {
                            error!("Decode error: {}", error);
                            Err(error)
                        }
                    }
                }
            }
            Err(error) => {
                error!("Error reading socket: {}", error);
                Err(error)
            }
        }
    }

    pub async fn loop_until_status(&mut self, expected_count: Option<usize>) -> io::Result<()> {
        loop {
            if let Some(client_message) = self.next_client_message().await? {
                match client_message {
                    ServerToClient::Message { message } => {
                        self.messages.push_back(message);
                    }
                    ServerToClient::StatusOk => {
                        if expected_count.is_none() {
                            return Ok(());
                        }
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            "Got unexpected StatusOk!",
                        ));
                    }
                    ServerToClient::StatusOkCount { count } => {
                        if let Some(in_count) = expected_count {
                            if count == in_count {
                                return Ok(());
                            } else {
                                let mes = format!("Expected count of {} got {}!", in_count, count);
                                return Err(io::Error::new(io::ErrorKind::Other, mes));
                            }
                        }
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            "Got unexpected StatusOkCount!",
                        ));
                    }
                    ServerToClient::StatusError { code, message } => {
                        let mes = format!("Status ERROR {}: {}!", code, message);
                        return Err(io::Error::new(io::ErrorKind::Other, mes));
                    }
                    ServerToClient::CommitAck {
                        topic,
                        partition,
                        offset,
                    } => {
                        let mes = format!(
                            "Unexpected CommitAck ERROR {}/{}: {}!",
                            topic, partition, offset
                        );
                        return Err(io::Error::new(io::ErrorKind::Other, mes));
                    }
                    ServerToClient::MessagesAvailable { topic, partition } => {
                        self.do_fetch = Some(TopicPartition { topic, partition });
                    }
                    _ => {
                        // Should not happen, not a valid client message...
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            "Got unexpected message!",
                        ));
                    }
                }
            }
        }
    }

    pub async fn connect(
        remote: SocketAddr,
        client_name: String,
        group_id: String,
        decoder_factory: ProtocolClientDecoderFactory,
        encoder_factory: ProtocolClientEncoderFactory,
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
        let (reader, mut writer) = stream.into_split();
        write_client!(
            encoder,
            writer,
            scratch_bytes,
            ClientToServer::Connect {
                client_name: client_name.clone(),
                group_id: group_id.clone()
            }
        );
        let mut client = Client {
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
            messages: VecDeque::with_capacity(100),
            do_fetch: None,
        };
        match client.next_client_message().await? {
            Some(ServerToClient::StatusOk) => Ok(client),
            _ => Err(io::Error::new(
                io::ErrorKind::Other,
                "Did not get OK from server, closing!",
            )),
        }
    }

    pub async fn reconnect(&mut self) -> io::Result<()> {
        self.in_bytes.resize(self.buf_size, 0);
        self.out_bytes.truncate(0);
        self.scratch_bytes.truncate(0);
        self.leftover_bytes = 0;
        let stream = TcpStream::connect(&self.remote).await?;
        let (reader, writer) = stream.into_split();
        self.reader = reader;
        self.writer = writer;
        write_client!(
            self.encoder,
            self.writer,
            self.scratch_bytes,
            ClientToServer::Connect {
                client_name: self.client_name.clone(),
                group_id: self.group_id.clone()
            }
        );
        match self.next_client_message().await? {
            Some(ServerToClient::StatusOk) => Ok(()),
            _ => Err(io::Error::new(
                io::ErrorKind::Other,
                "Did not get OK from server, closing!",
            )),
        }
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
            ClientToServer::Subscribe {
                topic: topic.to_string(),
                partition: 0,
                position,
                sub_type
            }
        );
        self.loop_until_status(None).await?;
        Ok(())
    }

    pub async fn fetch(
        &mut self,
        topic: &str,
        partition: u64,
        position: TopicPosition,
    ) -> io::Result<()> {
        write_client!(
            self.encoder,
            self.writer,
            self.scratch_bytes,
            ClientToServer::Fetch {
                topic: topic.to_string(),
                partition,
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
            ClientToServer::Commit {
                topic: topic.to_string(),
                partition,
                commit_offset
            }
        );
        loop {
            if let Some(client_message) = self.next_client_message().await? {
                match client_message {
                    ServerToClient::Message { message } => {
                        self.decoding = true;
                        self.messages.push_back(message);
                    }
                    ServerToClient::StatusError { code, message } => {
                        self.decoding = true;
                        let mes = format!("Status ERROR {}: {}!", code, message);
                        return Err(io::Error::new(io::ErrorKind::Other, mes));
                    }
                    ServerToClient::CommitAck {
                        topic: t,
                        partition: p,
                        offset: o,
                    } => {
                        self.decoding = true;
                        if topic == t && partition == p && commit_offset == o {
                            return Ok(());
                        }
                        let mes = format!(
                            "CommitAck ERROR {}/{}, {}/{}, {}/{}!",
                            topic, t, partition, p, commit_offset, o
                        );
                        return Err(io::Error::new(io::ErrorKind::Other, mes));
                    }
                    ServerToClient::MessagesAvailable { topic, partition } => {
                        self.do_fetch = Some(TopicPartition { topic, partition });
                        self.decoding = true;
                    }
                    _ => {
                        // Should not happen, not a valid client message...
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            "Got unexpected message!",
                        ));
                    }
                }
            }
        }
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
                ClientToServer::PublishBatchStart {
                    count: self.batch_count
                }
            );
            self.writer.write_all(&self.out_bytes).await?;
            self.out_bytes.truncate(0);
            self.loop_until_status(Some(self.batch_count as usize))
                .await?;
        }
        self.batch_count = 0;
        self.in_publish_batch = false;
        Ok(())
    }

    pub async fn publish(&mut self, topic: &str, partition: u64, payload: &[u8]) -> io::Result<()> {
        let crc = crc::Crc::<u32>::new(&CRC_32_CKSUM);
        let mut digest = crc.digest();
        //let mut digest = crc32::Digest::new(crc32::IEEE);
        digest.update(payload);
        let crc = digest.finalize();
        // XXX check that payload is not to large.
        let packet = ClientToServer::PublishMessage {
            message: Message {
                tp: TopicPartition {
                    topic: topic.to_string(),
                    partition,
                },
                crc,
                offset: 0,
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
                self.loop_until_status(None).await?;
            }
        } else if self.in_publish_batch {
            self.batch_count += 1;
        } else {
            self.writer.write_all(&self.out_bytes).await?;
            self.out_bytes.truncate(0);
            self.loop_until_status(None).await?;
        }
        Ok(())
    }

    pub async fn next_message(&mut self) -> io::Result<Message> {
        let do_fetch = self.do_fetch.clone();
        if let Some(TopicPartition { topic, partition }) = do_fetch {
            if let Err(err) = self
                .fetch(
                    &topic,
                    partition,
                    TopicPosition::Offset {
                        offset: self.last_offset,
                    },
                )
                .await
            {
                error!("Error fetching new messages: {}", err);
            }
            self.do_fetch = None;
        }
        if let Some(message) = self.messages.pop_front() {
            return Ok(message);
        }
        loop {
            if let Some(client_message) = self.next_client_message().await? {
                match client_message {
                    ServerToClient::Message { message } => {
                        self.decoding = true;
                        self.last_offset = message.offset;
                        let crc = crc::Crc::<u32>::new(&CRC_32_CKSUM);
                        let mut digest = crc.digest();
                        //let mut digest = crc32::Digest::new(crc32::IEEE);
                        digest.update(&message.payload);
                        let crc = digest.finalize();
                        return if crc == message.crc {
                            Ok(message)
                        } else {
                            error!("Message has invalid crc, rejecting.");
                            Err(io::Error::new(
                                io::ErrorKind::Other,
                                "Message has invalid crc, rejecting.",
                            ))
                        };
                    }
                    ServerToClient::StatusOk => {
                        self.decoding = true;
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            "Got unexpected StatusOk!",
                        ));
                    }
                    ServerToClient::StatusOkCount { .. } => {
                        self.decoding = true;
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            "Got unexpected StatusOkCount!",
                        ));
                    }
                    ServerToClient::StatusError { code, message } => {
                        self.decoding = true;
                        let mes = format!("Status ERROR {}: {}!", code, message);
                        return Err(io::Error::new(io::ErrorKind::Other, mes));
                    }
                    ServerToClient::CommitAck {
                        topic,
                        partition,
                        offset,
                    } => {
                        self.decoding = true;
                        let mes = format!(
                            "Unexpected CommitAck ERROR {}/{}: {}!",
                            topic, partition, offset
                        );
                        return Err(io::Error::new(io::ErrorKind::Other, mes));
                    }
                    ServerToClient::MessagesAvailable { topic, partition } => {
                        if let Err(err) = self
                            .fetch(
                                &topic,
                                partition,
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
                    _ => {
                        // Should not happen, not a valid client message...
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            "Got unexpected message!",
                        ));
                    }
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
