#![feature(async_await)]

use std::io;

use futures::executor;
use futures::io::{AsyncReadExt, AllowStdIo, AsyncWriteExt};

use romio::TcpStream;

fn main() -> io::Result<()> {
    executor::block_on(async {
        let mut stream = TcpStream::connect(&"127.0.0.1:7878".parse().unwrap()).await?;
        let mut stdout = AllowStdIo::new(io::stdout());
        for n in 0..10000 {
            if n > 0 && n % 1000 == 0 {
                let mes = format!("{{\"batch_type\": \"End\"}}");
                stream.write_all(mes.as_bytes()).await?;
            }
            if n % 1000 == 0 {
                //let mes = format!("{{\"batch_type\": \"Count\", \"count\": 100}}");
                let mes = format!("{{\"batch_type\": \"Start\"}}");
                stream.write_all(mes.as_bytes()).await?;
            }
            let payload = format!("{}-{}\n", "sls", n);
            let mes = format!("{{\"topic\": \"top1\", \"payload_size\": {}, \"checksum\": \"\"}}{}", payload.len(), payload);
            stream.write_all(mes.as_bytes()).await?;
            //println!("XXXX n: {}", n);
        }
        /*for n in 0..100 {
            if n > 0 && n % 10 == 0 {
                let mes = format!("{{\"batch_type\": \"End\"}}");
                stream.write_all(mes.as_bytes()).await?;
            }
            if n % 10 == 0 {
                //let mes = format!("{{\"batch_type\": \"Count\", \"count\": 100}}");
                let mes = format!("{{\"batch_type\": \"Start\"}}");
                stream.write_all(mes.as_bytes()).await?;
            }
            let payload = format!("{}-{}\n", "sls2", n);
            let mes = format!("{{\"topic\": \"top1\", \"payload_size\": {}, \"checksum\": \"\"}}{}", payload.len(), payload);
            stream.write_all(mes.as_bytes()).await?;
            //println!("XXXX n: {}", n);
        }*/
        //stream.flush();
        println!("XXXX pre stdout");
        stream.copy_into(&mut stdout).await?;
        println!("XXXX post stdout");
        Ok(())
    })
}