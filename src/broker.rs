use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;

use futures::channel::mpsc;
use futures::executor::ThreadPool;
use futures::lock::Mutex;
use futures::sink::SinkExt;
use futures::task::SpawnExt;
use futures::StreamExt;

use super::types::*;

use log::{error, info};

pub struct BrokerManager {
    brokers: Mutex<(
        HashMap<TopicPartition, mpsc::Sender<BrokerMessage>>,
        ThreadPool,
    )>,
}

impl BrokerManager {
    pub fn new(threadpool: ThreadPool) -> BrokerManager {
        BrokerManager {
            brokers: Mutex::new((HashMap::new(), threadpool)),
        }
    }

    pub async fn get_broker_tx(
        &self,
        tp: TopicPartition,
    ) -> Result<mpsc::Sender<BrokerMessage>, ()> {
        let mut data = self.brokers.lock().await;
        let partition = tp.partition;
        let topic = tp.topic.clone(); // Use for error logging.
        let brokers = &mut data.0;
        if brokers.contains_key(&tp) {
            return Ok(brokers.get(&tp).unwrap().clone());
        }
        let (tx, rx) = mpsc::channel::<BrokerMessage>(10);
        brokers.insert(tp.clone(), tx.clone());
        drop(brokers);
        let threadpool = &mut data.1;
        if let Err(_) = threadpool.spawn(new_message_broker(rx, tp)) {
            error!(
                "Got error spawning task for partion {}, topic {}",
                partition, topic
            );
            Err(())
        } else {
            Ok(tx.clone())
        }
    }
}

struct LogIndex {
    offset: u64,
    time: u64,
    position: u64,
    size: usize,
}

impl LogIndex {
    fn empty() -> LogIndex {
        LogIndex {
            offset: 0,
            time: 0,
            position: 0,
            size: 0,
        }
    }

    fn size() -> usize {
        std::mem::size_of::<LogIndex>()
    }

    unsafe fn as_bytes(&self) -> &[u8] {
        std::slice::from_raw_parts((self as *const LogIndex) as *const u8, LogIndex::size())
    }

    unsafe fn as_bytes_mut(&mut self) -> &mut [u8] {
        std::slice::from_raw_parts_mut((self as *mut LogIndex) as *mut u8, LogIndex::size())
    }
}

struct MessageLog {
    log_file_name: String,
    log_append: File,
    log_read: File,
    idx_append: File,
    idx_read: File,
    log_end: u64,
    idx_end: u64,
    offset: u64,
}

impl MessageLog {
    fn new(tp: &TopicPartition) -> io::Result<MessageLog> {
        let log_file_name = format!("{}.{}.log", tp.topic, tp.partition);
        let mut log_append = OpenOptions::new()
            .read(false)
            .write(true)
            .append(true)
            .create(true)
            .open(&log_file_name)?;
        log_append.seek(SeekFrom::End(0))?;
        let log_end = log_append.seek(SeekFrom::Current(0))?;

        let log_file_idx_name = format!("{}.{}.idx", tp.topic, tp.partition);
        let mut idx_append = OpenOptions::new()
            .read(false)
            .write(true)
            .append(true)
            .create(true)
            .open(&log_file_idx_name)?;
        idx_append.seek(SeekFrom::End(0))?;
        let idx_end = idx_append.seek(SeekFrom::Current(0))?;
        let log_read = File::open(&log_file_name)?;
        let mut idx_read = File::open(&log_file_idx_name)?;

        let mut offset = 0;
        if idx_end > LogIndex::size() as u64 {
            idx_read.seek(SeekFrom::Start(idx_end - LogIndex::size() as u64))?;
            let mut idx = LogIndex::empty();
            unsafe {
                idx_read.read(idx.as_bytes_mut())?;
            }
            offset = idx.offset + 1;
        }

        Ok(MessageLog {
            log_file_name,
            log_append,
            log_read,
            idx_append,
            idx_read,
            log_end,
            idx_end,
            offset,
        })
    }

    fn append(&mut self, message: &Message) -> io::Result<()> {
        let v = format!(
            "{{ \"topic\": \"{}\", \"payload_size\": {}, \"checksum\": \"{}\", \"sequence\": {} }}",
            message.topic, message.payload_size, message.checksum, message.sequence
        );
        self.log_append.write(v.as_bytes())?;
        self.log_append.write(&message.payload)?;
        self.log_append.flush()?;
        let idx = LogIndex {
            offset: message.sequence as u64,
            // XXX set time
            time: 0,
            position: self.log_end,
            size: v.as_bytes().len() + message.payload_size,
        };
        // XXX Verfiy the entire message was written?
        self.log_end += (v.as_bytes().len() + message.payload_size) as u64;
        unsafe {
            self.idx_append.write(idx.as_bytes())?;
        }
        self.idx_append.flush()?;
        self.idx_end += std::mem::size_of::<LogIndex>() as u64;
        self.offset += 1;
        Ok(())
    }
}

async fn new_message_broker(mut rx: mpsc::Receiver<BrokerMessage>, tp: TopicPartition) {
    info!(
        "Broker starting for partition {}, topic {}.",
        tp.partition, tp.topic
    );

    let msg_log = MessageLog::new(&tp);
    if let Err(error) = msg_log {
        error!("Failed to open the message log {}", error);
        return;
    }
    let mut msg_log = msg_log.unwrap();
    let mut client_tx: HashMap<String, mpsc::Sender<ClientMessage>> = HashMap::new();
    let mut message = rx.next().await;
    while message.is_some() {
        let mes = message.clone().unwrap();
        match mes {
            BrokerMessage::Message(mut message) => {
                message.sequence = msg_log.offset;

                if let Err(error) = msg_log.append(&message) {
                    error!("Failed to message log {}", error);
                    return;
                }

                for tx in client_tx.values_mut() {
                    if let Err(_) = tx.send(ClientMessage::Message(message.clone())).await {
                        // XXX Revome bad client.
                    }
                }
            }
            BrokerMessage::NewClient(client_name, mut tx) => {
                if let Err(_) = tx
                    .send(ClientMessage::MessageBatch(
                        msg_log.log_file_name.clone(),
                        0,
                        msg_log.log_end,
                    ))
                    .await
                {
                    // XXX Revome bad client.
                }
                client_tx.insert(client_name.to_string(), tx.clone());
            }
            BrokerMessage::CloseClient(client_name) => {
                client_tx.remove(&client_name);
            }
        };
        message = rx.next().await;
    }
    info!(
        "Broker ending for partition {}, topic {}.",
        tp.partition, tp.topic
    );
}
