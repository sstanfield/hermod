use client_async::*;
use log::{error, info, Level, LevelFilter, Metadata, Record};
use std::io;
use std::time::SystemTime;

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

async fn run_consumer(client: &mut Client, config: &Config) -> io::Result<()> {
    let sub_type = if config.fetch {
        SubType::Fetch
    } else {
        SubType::Stream
    };
    client
        .subscribe(&config.topic, config.position, sub_type)
        .await?;
    client.fetch(&config.topic, 0, config.position).await?;
    let mut i = 0;
    let mut start_time: Option<u128> = None;
    loop {
        match client.next_message().await {
            Ok(message) => {
                if start_time.is_none() {
                    start_time = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
                        Ok(n) => Some(n.as_millis()),
                        Err(_) => None,
                    };
                }
                /*println!(
                    "Message loopy: {}",
                    String::from_utf8(message.payload).unwrap()
                );*/
                if message.offset % 100 == 0 {
                    client
                        .commit_offset(&config.topic, 0, message.offset)
                        .await
                        .unwrap();
                }
            }
            Err(error) => {
                error!("Client read error: {}", error);
                return Err(error);
            }
        }
        i += 1;
        if config.count > 0 && i >= config.count {
            if let Some(start_time) = start_time {
                let end_time: u128 = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)
                {
                    Ok(n) => n.as_millis(),
                    Err(_) => 0,
                };
                let run_time = (end_time - start_time) as f64 / 1000.0;
                info!(
                    "##########CLIENT {} Ran for {} seconds, {} messages/second.#######",
                    config.name,
                    run_time,
                    config.count as f64 / run_time
                );
            }
            info!(
                "########### Consumer {} ending due to reaching count {}.##########",
                config.name, config.count
            );
            return Ok(());
        }
    }
}

async fn run_publisher(client: &mut Client, config: &Config) -> io::Result<()> {
    let start_time: u128 = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => n.as_millis(),
        Err(_) => 0,
    };
    if config.batch {
        client.start_pub_batch().await?;
    }
    for n in 0..config.count {
        if config.batch && n > 0 && n % 1000 == 0 {
            client.end_pub_batch().await?;
            client.start_pub_batch().await?;
        }
        let payload = format!("{}-{}\n", config.base_message, n);
        //println!("XXX pub: {}", payload);
        if let Err(err) = client.publish(&config.topic, 0, payload.as_bytes()).await {
            error!("Error publishing: {}.", err);
            return Err(err);
        }
    }
    if config.batch {
        client.end_pub_batch().await?;
    }
    let end_time: u128 = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => n.as_millis(),
        Err(_) => 0,
    };
    let run_time = (end_time - start_time) as f64 / 1000.0;
    info!(
        "##########PUBLISHER {} Ran for {} seconds, {} messages/second.#######",
        config.name,
        run_time,
        config.count as f64 / run_time
    );
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> io::Result<()> {
    log::set_logger(&LOGGER)
        .map(|()| log::set_max_level(LevelFilter::Info))
        .unwrap();

    if let Some(config) = get_config() {
        println!(
            "Name: {}, group: {}, topic: {}, client: {}",
            config.name, config.group, config.topic, config.is_client
        );

        let mut client = Client::connect(
            config.remote,
            config.name.clone(),
            config.group.clone(),
            client_decoder_factory,
            client_encoder_factory,
        )
        .await
        .unwrap();
        if config.is_client {
            run_consumer(&mut client, &config).await
        } else {
            run_publisher(&mut client, &config).await
        }
    } else {
        Ok(())
    }
}
