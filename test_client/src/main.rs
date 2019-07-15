#![feature(async_await)]

#[macro_use]
extern crate clap;

use client_async::*;
use futures::executor;
use log::{error, Level, LevelFilter, Metadata, Record};
use std::io;

use common::protocolx::*;
use common::types::*;

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

    let matches = clap_app!(test_client =>
        (version: "0.1")
        (author: "Steven Stanfield<stanfield@scarecrowtech.com>")
        (about: "Test client for Hermod message system.")
        (@arg config: -t --topic +takes_value "Sets a topic")
        (@arg name: -n --name +takes_value "Sets client name")
        (@arg group: -g --group +takes_value "Sets a group id")
        (@arg client: -c --client "Run as client")
        (@arg server: -s --server "Run as server")
    )
    .get_matches();
    let topic = matches.value_of("topic").unwrap_or("top1");
    let name = matches.value_of("name").unwrap_or("test_client");
    let group = matches.value_of("group").unwrap_or("g1");
    let is_client = matches.occurrences_of("server") == 0;
    println!(
        "Name: {}, group: {}, topic: {}, client: {}",
        name, group, topic, is_client
    );

    executor::block_on(async {
        let mut client = Client::connect(
            "127.0.0.1".to_string(),
            7878,
            name.to_string(),
            group.to_string(),
            client_decoder_factory,
            encoder_factory,
        )
        .await
        .unwrap();
        if is_client {
            client
                .subscribe(&topic, TopicPosition::Current, SubType::Fetch)
                .await?;
            client.fetch(&topic, TopicPosition::Current).await?;
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
                client.publish(&topic, 0, payload.as_bytes()).await?;
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
                                .commit_offset(&topic, 0, message.sequence)
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
}
