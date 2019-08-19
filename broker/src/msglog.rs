use std::collections::HashMap;
use std::fs::create_dir_all;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::sync::Arc;
use std::time::SystemTime;

use futures::lock::Mutex;

use log::{error, info};

use serde_json;

use common::types::*;
use common::util::*;

struct LogIndex {
    pub offset: u64,
    pub time: u128,
    pub position: u64,
    pub size: usize,
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

pub struct MessageChunk {
    pub file_name: String,
    pub start_position: u64,
    pub length: u64,
}

#[derive(Deserialize)]
enum MessageFromLog {
    Message {
        topic: String,
        payload_size: usize,
        crc: u32,
        sequence: u64,
    },
}

#[derive(Debug)]
pub struct MessageLog {
    log_file_name: String,
    log_file_idx_name: String,
    log_append: BufWriter<File>,
    idx_append: BufWriter<File>,
    log_end: u64,
    log_flushed_end: u64,
    offset: u64,
    single_record: bool,
    topic: String,
    partition: u64,
    base_dir: String,
    log_dir: String,
    topic_map: Arc<Mutex<HashMap<TopicPartition, u64>>>,
}

impl MessageLog {
    fn new(
        tp: &TopicPartition,
        single_record: bool,
        base_dir: &str,
        log_dir: &str,
        topic_map: Arc<Mutex<HashMap<TopicPartition, u64>>>,
    ) -> io::Result<MessageLog> {
        let log_file_name = format!("{}/{}.{}.log", log_dir, tp.topic, tp.partition);
        let mut log_append = OpenOptions::new()
            .read(false)
            .write(true)
            .append(true)
            .create(true)
            .open(&log_file_name)?;
        log_append.seek(SeekFrom::End(0))?;
        let log_end = log_append.seek(SeekFrom::Current(0))?;
        let log_flushed_end = log_end;

        let log_file_idx_name = format!("{}/{}.{}.idx", log_dir, tp.topic, tp.partition);
        let mut idx_append = OpenOptions::new()
            .read(false)
            .write(true)
            .append(true)
            .create(true)
            .open(&log_file_idx_name)?;
        idx_append.seek(SeekFrom::End(0))?;
        let idx_end = idx_append.seek(SeekFrom::Current(0))?;
        let mut idx_read = File::open(&log_file_idx_name)?;

        let offset = if idx_end > LogIndex::size() as u64 && !single_record {
            idx_read.seek(SeekFrom::Start(idx_end - LogIndex::size() as u64))?;
            let mut idx = LogIndex::empty();
            unsafe {
                idx_read.read_exact(idx.as_bytes_mut())?;
            }
            idx.offset + 1
        } else {
            0
        };

        Ok(MessageLog {
            log_file_name,
            log_file_idx_name,
            log_append: BufWriter::with_capacity(2_048_000, log_append),
            idx_append: BufWriter::with_capacity(2_048_000, idx_append),
            log_end,
            log_flushed_end,
            offset,
            single_record,
            topic: tp.topic.clone(),
            partition: tp.partition,
            base_dir: base_dir.to_string(),
            log_dir: log_dir.to_string(),
            topic_map,
        })
    }

    fn append_no_flush(&mut self, message: &Message) -> io::Result<()> {
        let v = format!(
            "{{\"Message\":{{\"topic\":\"{}\",\"payload_size\":{},\"crc\":{},\"sequence\":{}}}}}",
            message.topic, message.payload_size, message.crc, message.sequence
        );
        if self.single_record {
            // XXX need to truncate here?  Probably does not matter.
            self.log_append.seek(SeekFrom::Start(0))?;
            self.log_append.get_mut().set_len(0)?;
            self.idx_append.seek(SeekFrom::Start(0))?;
            self.idx_append.get_mut().set_len(0)?;
        }
        self.log_append.write_all(v.as_bytes())?;
        self.log_append.write_all(&message.payload)?;
        let time: u128 = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(n) => n.as_millis(),
            Err(_) => 0,
        };
        let idx = LogIndex {
            offset: message.sequence as u64,
            time,
            position: self.log_end,
            size: v.as_bytes().len() + message.payload_size,
        };
        // XXX Verify the entire message was written?
        self.log_end += (v.as_bytes().len() + message.payload_size) as u64;
        unsafe {
            self.idx_append.write_all(idx.as_bytes())?;
        }
        if !self.single_record {
            self.offset += 1;
        }
        Ok(())
    }

    pub fn append(&mut self, message: &Message) -> io::Result<()> {
        self.append_no_flush(message)?;
        let flush = message.message_type != MessageType::BatchMessage || self.single_record;
        if flush {
            self.log_append.flush()?;
            self.idx_append.flush()?;
            self.log_flushed_end = self.log_end;
        }
        Ok(())
    }

    fn get_index(&self, offset: u64) -> io::Result<LogIndex> {
        let pos = offset * std::mem::size_of::<LogIndex>() as u64;
        let mut idx_read = File::open(&self.log_file_idx_name)?;
        idx_read.seek(SeekFrom::Start(pos))?;
        let mut idx = LogIndex::empty();
        unsafe {
            idx_read.read_exact(idx.as_bytes_mut())?;
        }
        Ok(idx)
    }

    pub fn get_all_message_chunks(&self, start_offset: u64) -> Option<Vec<MessageChunk>> {
        let index = self.get_index(start_offset);

        match index {
            Ok(index) => {
                let mut result = Vec::with_capacity(1);
                result.push(MessageChunk {
                    file_name: self.log_file_name.clone(),
                    start_position: index.position,
                    length: self.log_flushed_end - index.position,
                });
                Some(result)
            }
            Err(error) => {
                info!("Unable to get message chunks: {}.", error);
                None
            }
        }
    }

    pub fn get_message(&mut self, offset: u64) -> io::Result<Message> {
        let idx = self.get_index(offset)?;
        let mut log_read = File::open(&self.log_file_name)?;
        if self.single_record {
            log_read.seek(SeekFrom::Start(0))?;
        } else {
            log_read.seek(SeekFrom::Start(idx.position))?;
        }
        let mut buf = vec![b'\0'; idx.size];
        let mut start = 0;
        let mut cont = true;
        while cont {
            cont = false;
            match log_read.read(&mut buf[start..]) {
                Ok(bytes) => {
                    let bytes = if bytes == 0 { idx.size } else { bytes };
                    if (bytes + start) < idx.size {
                        start = bytes;
                        cont = true;
                    }
                }
                Err(error) => {
                    // XXX do better.
                    error!("{}", error);
                    return Err(error);
                }
            }
        }
        if let Some((first_brace, message_offset)) = find_brace(&buf[..]) {
            let message: MessageFromLog =
                serde_json::from_slice(&buf[first_brace..=message_offset])?;
            match message {
                MessageFromLog::Message {
                    topic,
                    payload_size,
                    crc,
                    sequence,
                } => {
                    let len = buf.len();
                    let payload = buf[len - payload_size..].to_vec();
                    Ok(Message {
                        message_type: MessageType::Message,
                        topic,
                        partition: self.partition,
                        payload_size,
                        crc,
                        sequence,
                        payload,
                    })
                }
            }
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "Failed to find the message from log: {}",
                    self.log_file_name
                ),
            ))
        }
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub async fn get_committed_offset(&self, group_id: &str) -> io::Result<u64> {
        #[derive(Deserialize)]
        struct OffsetRecord {
            offset: u64,
        }

        let commit_topic = format!(
            "__consumer_offsets-{}-{}-{}",
            group_id, self.topic, self.partition
        );
        let tp_offset = TopicPartition {
            topic: commit_topic,
            partition: self.partition,
        };
        let topic_map = self.topic_map.lock().await;
        let index = match topic_map.get(&tp_offset) {
            Some(index) => *index,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Commit offset not available.",
                ));
            }
        };
        let log_dir = format!("{}/{}", self.base_dir, index);
        let mut offset_log = MessageLog::new(
            &tp_offset,
            true,
            &self.base_dir,
            &log_dir,
            self.topic_map.clone(),
        )?;
        let offset_message = offset_log.get_message(0)?;
        let record: OffsetRecord = serde_json::from_slice(&offset_message.payload[..]).unwrap(); // XXX TODO Don't unwrap...
        Ok(record.offset)
    }
}

pub struct MessageLogManager {
    logs: Arc<Mutex<HashMap<TopicPartition, u64>>>,
    base_dir: String,
    max_index: u64,
}

impl MessageLogManager {
    pub fn new(base_dir: &str) -> io::Result<MessageLogManager> {
        let mut logs: HashMap<TopicPartition, u64> = HashMap::new();
        let mut max_index = 0;
        let index_name = format!("{}/topics.index", base_dir);
        let f = match File::open(&index_name) {
            Ok(f) => f,
            Err(_) => File::create(&index_name)?,
        };
        let f = BufReader::new(f);
        for line in f.lines() {
            if let Ok(line) = line {
                let vals: Vec<&str> = line.splitn(3, ':').collect();
                if vals.len() == 3 {
                    let topic = vals[2].to_string();
                    let partition = match vals[1].parse::<u64>() {
                        Ok(i) => i,
                        Err(err) => {
                            let msg =
                                format!("Failed to parse partition from topic index: {}.", err);
                            error!("{}", msg);
                            return Err(io::Error::new(io::ErrorKind::Other, msg));
                        }
                    };
                    let index = match vals[0].parse::<u64>() {
                        Ok(i) => i,
                        Err(err) => {
                            let msg = format!("Failed to parse index from topic index: {}.", err);
                            error!("{}", msg);
                            return Err(io::Error::new(io::ErrorKind::Other, msg));
                        }
                    };
                    let tp = TopicPartition { topic, partition };
                    if index > max_index {
                        max_index = index;
                    }
                    logs.insert(tp, index);
                } else {
                    let msg = format!("Invalid line in topic index: {}.", line);
                    error!("{}", msg);
                    return Err(io::Error::new(io::ErrorKind::Other, msg));
                }
            } else {
                break;
            }
        }
        Ok(MessageLogManager {
            logs: Arc::new(Mutex::new(logs)),
            base_dir: base_dir.to_string(),
            max_index,
        })
    }

    pub async fn get_message_log(
        &mut self,
        tp: &TopicPartition,
        single_record: bool,
    ) -> io::Result<MessageLog> {
        let mut logs = self.logs.lock().await;
        let index = match logs.get(tp) {
            Some(index) => *index,
            None => {
                self.max_index += 1;
                let index_name = format!("{}/topics.index", &self.base_dir);
                let mut append = OpenOptions::new()
                    .read(false)
                    .write(true)
                    .append(true)
                    .create(true)
                    .open(&index_name)?;
                append.seek(SeekFrom::End(0))?;
                let d = format!("{}:{}:{}\n", self.max_index, tp.partition, tp.topic);
                append.write_all(d.as_bytes())?;
                logs.insert(tp.clone(), self.max_index);
                self.max_index
            }
        };
        let msg_dir = format!("{}/{}", self.base_dir, index);
        if let Err(err) = create_dir_all(msg_dir.clone()) {
            error!("Unable to create log directory: {}- {}", msg_dir, err);
            return Err(err);
        }
        let msg_log = MessageLog::new(
            tp,
            single_record,
            &self.base_dir,
            &msg_dir,
            self.logs.clone(),
        )?;
        Ok(msg_log)
    }
}
