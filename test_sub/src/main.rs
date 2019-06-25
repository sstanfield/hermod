#![feature(async_await)]

use std::io;

use bytes::BytesMut;
use futures::executor;
use futures::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};

#[macro_use]
extern crate serde_derive;

use serde_json;

use romio::TcpStream;

use log::{error, Level, LevelFilter, Metadata, Record};

struct SimpleLogger;

impl log::Log for SimpleLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Level::Info
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            println!(
                "{}: {} - {}",
                record.level(),
                record.target(),
                record.args()
            );
        }
    }

    fn flush(&self) {}
}

static LOGGER: SimpleLogger = SimpleLogger;

// XXX get this from shared workspace... TODO
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum MessageType {
    Message,
    BatchMessage,
    BatchEnd { count: usize },
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Message {
    pub message_type: MessageType,
    pub topic: String,
    pub payload_size: usize,
    pub checksum: String,
    pub sequence: u64,
    pub payload: Vec<u8>,
}

pub fn find_brace(buf: &[u8]) -> Option<(usize, usize)> {
    let mut opens = 0;
    let mut count = 0;
    let mut first_brace = 0;
    let mut got_open = false;
    for b in buf {
        if *b == b'{' {
            opens += 1;
            if !got_open {
                first_brace = count;
            }
            got_open = true;
        }
        if *b == b'}' {
            opens -= 1;
        }
        if opens == 0 && got_open {
            return Some((first_brace, count));
        };
        count += 1;
    }
    None
}

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
#[serde(untagged)]
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
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
struct InMessageCodec {
    message: Option<Message>,
}

impl InMessageCodec {
    pub fn new() -> InMessageCodec {
        InMessageCodec { message: None }
    }

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Message>, io::Error> {
        let mut result; //: Result<Option<Message>, io::Error> = Ok(None);
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
                        println!("XXXX got status {:?}, {}, {}", status, code, message);
                        self.message = None;
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
                result = Ok(Some(message));
            } else {
                result = Ok(None);
            }
        } else {
            result = Ok(None);
        }
        if got_payload {
            self.message = None;
        }
        result
    }
}

struct Client {
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
    async fn connect(host: String, port: u32) -> io::Result<Client> {
        let remote = format!("{}:{}", host, port);
        let stream = TcpStream::connect(&remote.parse().unwrap()).await?;
        let (reader, mut writer) = stream.split();
        writer
            .write_all(
                b"{\"client_name\": \"client1\", \"group_id\": \"g1\", \"topics\": [\"top1\"]}",
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

    async fn commit(&mut self) -> io::Result<()> {
        Ok(())
    }

    async fn commit_offset(&mut self, topic: String, partition: u64, offset: u64) -> io::Result<()> {
        let message = format!("{{\"topic\": \"{}\", \"partition\": {}, \"commit_offset\": {}}}", topic, partition, offset);
        println!("XXX committing: {}", message);
        self.writer.write_all(message.as_bytes()).await?;
        Ok(())
    }

    async fn next(&mut self) -> io::Result<Message> {
        loop {
            let input = if self.decoding {
                Ok(self.in_bytes.len())
            } else {
                self
                .reader
                .read(&mut self.in_bytes[self.leftover_bytes..])
                .await
            };
            match input
            {
                Ok(bytes) => {
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
                            Ok(Some(message)) => {
                                self.decoding = true;
                                self.leftover_bytes = 0;
                                return Ok(message);
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
                        // Reclaim the entire buffer and copy leftover bytes to front.
                        self.in_bytes.reserve(self.buf_size - self.in_bytes.len());
                        unsafe {
                            self.in_bytes.set_len(self.buf_size);
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

fn main() -> io::Result<()> {
    log::set_logger(&LOGGER)
        .map(|()| log::set_max_level(LevelFilter::Info))
        .unwrap();
    executor::block_on(async {
        let mut last_sequence = 0;
        let mut client = Client::connect("127.0.0.1".to_string(), 7878)
            .await
            .unwrap();
        loop {
            match client.next().await {
                Ok(message) => {
                    last_sequence = message.sequence;
                    println!(
                        "Message loopy: {}",
                        String::from_utf8(message.payload).unwrap()
                    );
                    if message.sequence % 100 == 0 {
                        client.commit_offset("top1".to_string(), 0, message.sequence).await.unwrap();
                    }
                }
                Err(error) => {
                    error!("Client read error: {}", error);
                    return Ok(());
                }
            }
        }
    })
}
