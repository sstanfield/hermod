use std::io;

use futures::channel::mpsc;

use futures::StreamExt;
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

#[tokio::main(flavor = "multi_thread")]
async fn main() -> io::Result<()> {
    log::set_logger(&LOGGER)
        .map(|()| log::set_max_level(LevelFilter::Info))
        .unwrap();

    if let Some(config) = get_config() {
        let client_manager = Arc::new(ClientManager::new());
        let broker_manager = Arc::new(BrokerManager::new(&config.log_dir)?);

        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<u32>(1);
        let ctrlc_client_manager = client_manager.clone();
        let ctrlc_broker_manager = broker_manager.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.unwrap();
            let shutdown_tx = &mut shutdown_tx.clone();
            info!("Got termination signal, shutting down.");
            let ctrlc_broker_manager = ctrlc_broker_manager.clone();
            let ctrlc_client_manager = ctrlc_client_manager.clone();
            tokio::task::spawn(async move { ctrlc_broker_manager.shutdown(10_000).await });
            tokio::task::spawn(async move { ctrlc_client_manager.is_shutdown(5_000).await });
            if let Err(err) = shutdown_tx.try_send(1) {
                error!("Failed to send shutdown message, {}.", err);
            }
        });

        tokio::task::spawn(start_client(
            client_manager,
            broker_manager,
            decoder_factory,
            encoder_factory,
            config.bind,
        ));
        shutdown_rx.next().await;
    }
    Ok(())
}
