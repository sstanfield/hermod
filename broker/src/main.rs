#![feature(async_await)]

extern crate ctrlc;
use std::io;

use futures::channel::mpsc;
use futures::executor::LocalPool;
use futures::executor::ThreadPoolBuilder;
use futures::task::SpawnExt;
use futures::StreamExt;
//use futures::future::join;

use log::{error, info, Level, LevelFilter, Metadata, Record};
use std::clone::Clone;
use std::sync::Arc;

use common::protocolx::*;
use hermod::*;

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
        let mut threadpool = ThreadPoolBuilder::new()
            .name_prefix("hermod Pool")
            .create()?;
        let client_manager = Arc::new(ClientManager::new());
        let broker_manager = Arc::new(BrokerManager::new(threadpool.clone(), &config.log_dir));

        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<u32>(1);
        let ctrlc_client_manager = client_manager.clone();
        let ctrlc_broker_manager = broker_manager.clone();
        ctrlc::set_handler(move || {
            let shutdown_tx = &mut shutdown_tx.clone();
            info!("Got termination signal, shutting down.");
            let fut = ctrlc_broker_manager.shutdown(10_000);
            let mut lp = LocalPool::new();
            lp.run_until(fut);
            let fut = ctrlc_client_manager.is_shutdown(5_000);
            lp.run_until(fut);
            if let Err(err) = shutdown_tx.try_send(1) {
                error!("Failed to send shutdown message, {}.", err);
            }
        })
        .expect("Error setting Ctrl-C handler");

        /*threadpool.run(join(
            start_pub_empty(threadpool.clone(), broker_manager.clone()),
            start_sub_empty(threadpool.clone(), io_pool.clone(), broker_manager.clone()),
        ));*/
        threadpool
            .spawn(start_client(
                threadpool.clone(),
                client_manager,
                broker_manager,
                decoder_factory,
                encoder_factory,
                config.bind,
            ))
            .map_err(|e| {
                error!("Error spawning client: {}.", e);
                io::Error::new(
                    io::ErrorKind::Other,
                    "Failed to spawn client in main.".to_string(),
                )
            })?;
        threadpool.run(shutdown_rx.next());
    }
    Ok(())
}
