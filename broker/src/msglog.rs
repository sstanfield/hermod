use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::time::SystemTime;

use log::error;

use serde_json;

use common::types::*;
use common::util::*;

pub struct LogIndex {
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
    log_append: File,
    idx_append: File,
    log_end: u64,
    idx_end: u64,
    offset: u64,
    single_record: bool,
    partition: u64,
}

impl MessageLog {
    pub fn new(tp: &TopicPartition, single_record: bool, log_dir: &str) -> io::Result<MessageLog> {
        let log_file_name = format!("{}/{}.{}.log", log_dir, tp.topic, tp.partition);
        let mut log_append = OpenOptions::new()
            .read(false)
            .write(true)
            .append(true)
            .create(true)
            .open(&log_file_name)?;
        log_append.seek(SeekFrom::End(0))?;
        let log_end = log_append.seek(SeekFrom::Current(0))?;

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
            log_append,
            idx_append,
            log_end,
            idx_end,
            offset,
            single_record,
            partition: tp.partition,
        })
    }

    pub fn append(&mut self, message: &Message) -> io::Result<()> {
        let v = format!(
            "{{\"Message\":{{\"topic\":\"{}\",\"payload_size\":{},\"crc\":{},\"sequence\":{}}}}}",
            message.topic, message.payload_size, message.crc, message.sequence
        );
        if self.single_record {
            // XXX need to truncate here?  Probably does not matter.
            self.log_append.seek(SeekFrom::Start(0))?;
            self.log_append.set_len(0)?;
            self.idx_append.seek(SeekFrom::Start(0))?;
            self.idx_append.set_len(0)?;
        }
        self.log_append.write_all(v.as_bytes())?;
        self.log_append.write_all(&message.payload)?;
        self.log_append.flush()?;
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
        self.idx_append.flush()?;
        self.idx_end += std::mem::size_of::<LogIndex>() as u64;
        if !self.single_record {
            self.offset += 1;
        }
        Ok(())
    }

    pub fn get_index(&self, offset: u64) -> io::Result<LogIndex> {
        let pos = offset * std::mem::size_of::<LogIndex>() as u64;
        let mut idx_read = File::open(&self.log_file_idx_name)?;
        idx_read.seek(SeekFrom::Start(pos))?;
        let mut idx = LogIndex::empty();
        unsafe {
            idx_read.read_exact(idx.as_bytes_mut())?;
        }
        Ok(idx)
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
    pub fn log_file_name(&self) -> String {
        self.log_file_name.clone()
    }
    pub fn log_file_end(&self) -> u64 {
        self.log_end
    }
}
