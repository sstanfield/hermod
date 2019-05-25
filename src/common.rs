//use futures::channel::mpsc;
//use futures::sink::SinkExt;

/*pub fn send_wait<T>(tx: &mut mpsc::Sender<T>, item: T) -> Result<(), &str> {
    match tx.try_send(item) {
        Ok(_) => { Ok(()) },
        Err(in_item) => {
            let item = in_item.into_inner();
            await!(tx.clone().send(item));
            /*match await!(tx.clone().send(item)) {
                Ok(_tx) => Ok(()),
                Err(_) => Err("Sending data failed, aborting!"),
            }*/
            Ok(())
        }
    }
}*/

/*pub fn send_writer<C: Sync>(writer: &mut C, item: C) -> Result<(), &str> {
    match writer.try_send(item) {
        Ok(_) => { Ok(()) },
        Err(_in_item) => {
            Err("Tcp sink not ready for data, closing!")
        },
    }
}*/

pub fn last_brace(buf: &[u8]) -> Option<usize> {
    let mut opens = 0;
    let mut count = 0;
    for b in buf {
        if *b == b'{' {
            opens += 1;
        }
        if *b == b'}' {
            opens -= 1;
        }
        if opens == 0 {
            return Some(count);
        };
        count += 1;
    }
    None
}
