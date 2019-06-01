#![feature(async_await)]

use std::io;

use futures::executor::ThreadPoolBuilder;
use futures::future::join;

use std::clone::Clone;
use std::sync::Arc;

use hermod::*;

use log::{Level, LevelFilter, Metadata, Record};

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

    let mut threadpool = ThreadPoolBuilder::new()
        .name_prefix("hermod Pool")
        .create()?;
    let io_pool = ThreadPoolBuilder::new().name_prefix("hermod IO").create()?;

    let broker_manager = Arc::new(BrokerManager::new(threadpool.clone()));

    threadpool.run(join(
        start_pub_empty(threadpool.clone(), broker_manager.clone()),
        start_sub_empty(threadpool.clone(), io_pool.clone(), broker_manager.clone()),
    ));
    Ok(())
}
