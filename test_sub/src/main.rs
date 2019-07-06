#![feature(async_await)]

use std::io;
use futures::executor;
use log::{error, Level, LevelFilter, Metadata, Record};
use client_async::*;

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

fn main() -> io::Result<()> {
    log::set_logger(&LOGGER)
        .map(|()| log::set_max_level(LevelFilter::Info))
        .unwrap();
    executor::block_on(async {
        let mut client = Client::connect("127.0.0.1".to_string(), 7878)
            .await
            .unwrap();
        loop {
            match client.next().await {
                Ok(message) => {
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
