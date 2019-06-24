#![feature(async_await)]

use std::io;

use futures::executor;
use futures::io::{AsyncReadExt, AllowStdIo, AsyncWriteExt};

use romio::TcpStream;

fn main() -> io::Result<()> {
    executor::block_on(async {
        let mut stream = TcpStream::connect(&"127.0.0.1:7878".parse().unwrap()).await?;
        let mut stdout = AllowStdIo::new(io::stdout());
        for n in 1..10001 {
            let payload = format!("{}-{}\n", "sls", n);
            let mes = format!("{{\"topic\": \"top1\", \"payload_size\": {}, \"checksum\": \"\"}}{}", payload.len(), payload);
            stream.write_all(mes.as_bytes()).await?;
        }
        stream.copy_into(&mut stdout).await?;
        Ok(())
    })
}