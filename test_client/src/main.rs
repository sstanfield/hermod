#![feature(async_await)]

use client_async::*;
use futures::executor;
use log::{error, Level, LevelFilter, Metadata, Record};
use std::io;

use common::protocolx::*;
use common::types::*;

use test_client::*;

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

    if let Ok(config) = get_config() {
        println!(
            "Name: {}, group: {}, topic: {}, client: {}",
            config.name, config.group, config.topic, config.is_client
        );

        executor::block_on(async {
            let mut client = Client::connect(
                "127.0.0.1".to_string(),
                7878,
                config.name,
                config.group,
                client_decoder_factory,
                encoder_factory,
            )
            .await
            .unwrap();
            if config.is_client {
                client
                    .subscribe(&config.topic, TopicPosition::Current, SubType::Fetch)
                    .await?;
                client.fetch(&config.topic, TopicPosition::Current).await?;
                loop {
                    match client.next_message().await {
                        Ok(message) => {
                            println!(
                                "Message loopy: {}",
                                String::from_utf8(message.payload).unwrap()
                            );
                            if message.sequence % 100 == 0 {
                                client
                                    .commit_offset("top1", 0, message.sequence)
                                    .await
                                    .unwrap();
                            }
                        }
                        Err(error) => {
                            error!("Client read error: {}", error);
                            return Ok(());
                        }
                    }
                }
            } else {
                client.start_pub_batch().await?;
                for n in 0..10000 {
                    if n > 0 && n % 1000 == 0 {
                        client.end_pub_batch().await?;
                        client.start_pub_batch().await?;
                    }
                    let payload = format!("{}-{}\n", "sls", n);
                    println!("XXX pub: {}", payload);
                    client.publish(&config.topic, 0, payload.as_bytes()).await?;
                }
                client.end_pub_batch().await?;
                loop {
                    match client.next_message().await {
                        Ok(message) => {
                            println!(
                                "Message loopy: {}",
                                String::from_utf8(message.payload).unwrap()
                            );
                            if message.sequence % 100 == 0 {
                                client
                                    .commit_offset(&config.topic, 0, message.sequence)
                                    .await
                                    .unwrap();
                            }
                        }
                        Err(error) => {
                            error!("Client read error: {}", error);
                            return Ok(());
                        }
                    }
                }
                //           Ok(())
            }
        })
    } else {
        Ok(())
    }
}
