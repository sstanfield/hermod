use futures::channel::mpsc;
//use futures::executor::ThreadPool;
//use futures::future;
use futures::future::FutureExt;
use futures::lock::Mutex;
use futures::sink::SinkExt;
//use futures::task::SpawnExt;
use futures::StreamExt;
use std::collections::HashMap;
//use std::thread;
//use std::thread::JoinHandle;

use super::types::*;

/*#[derive(Clone)]
struct Store {
    tx: std::sync::mpsc::Sender<BrokerMessage>,
    //TTX: thread::LocalKey,
    //thread_id: JoinHandle<()>,
}

impl Store {
    pub fn new() -> Store {
        let (tx, rx) = std::sync::mpsc::channel::<BrokerMessage>();

        // each thread starts out with the initial value of 1
        thread::spawn(move || {
            let topic_map: HashMap<String, Vec<String>> = HashMap::new();
            let client_tx: HashMap<String, mpsc::Sender<ClientMessage>> = HashMap::new();
            let sequences: HashMap<String, usize> = HashMap::new();
            for mes in rx {
                println!("XXX store message!");
            }
        });
        Store { tx }
    }

    pub fn get_val(&mut self) -> String {
        //let tx = &self.tx;//.clone();
        thread_local!(static TTX: RefCell<Option<std::sync::mpsc::Sender<BrokerMessage>>> = RefCell::new(Option::None));
        /*TTX.with(|tx| {
            tx.send(BrokerMessage::CloseClient("XXX".to_string())).unwrap();
        });*/
        self.tx
            .send(BrokerMessage::CloseClient("XXX".to_string()))
            .unwrap();
        "XXX".to_string()
    }
}*/

pub async fn new_message_broker_concurrent(rx: mpsc::Receiver<BrokerMessage>) {
    //let mut topic_map: HashMap<String, Vec<String>> = HashMap::new();
    let topic_map: &Mutex<HashMap<String, Vec<String>>> = &Mutex::new(HashMap::new());
    let client_tx: &Mutex<HashMap<String, mpsc::Sender<ClientMessage>>> =
        &Mutex::new(HashMap::new());
    let sequences: &Mutex<HashMap<String, usize>> = &Mutex::new(HashMap::new());
    //let store = &Store::new();
    let fut = rx.for_each_concurrent(None, async move |mes| {
        //let topic_map = topic_map.clone();
        let client_tx = client_tx.clone();
        let sequences = sequences.clone();
        //let mut store = store.clone();
        //store.get_val();
        println!("XXX Got message!");
        match mes {
            BrokerMessage::Message(mut message) => {
                // insert a key only if it doesn't already exist
                {
                    let mut sequences = sequences.lock().await;
                    let seq = sequences.entry(message.topic.to_string()).or_insert(0);
                    message.sequence = *seq;
                    *seq += 1;
                }
                let mut topic_map = topic_map.lock().await;
                if let Some(clients) = topic_map.get_mut(&message.topic) {
                    for client in clients {
                        let mut client_tx = client_tx.lock().await; //.map(|mut client_tx| {
                        if let Some(tx) = client_tx.get_mut(client) {
                            if let Err(_) = tx.send(ClientMessage::Message(message.clone())).await {
                                // XXX Revome bad client.
                            }
                        }
                    }
                }
            }
            BrokerMessage::NewClient(client_name, topics, tx) => {
                let fut = client_tx.lock().map(|mut txmap| {
                    txmap.insert(client_name.to_string(), tx.clone());
                });
                fut.await;
                for topic in &topics.topics {
                    // insert a key only if it doesn't already exist
                    let fut = topic_map.lock().map(|mut topic_map| {
                        topic_map
                            .entry(topic.to_string())
                            .or_insert(Vec::with_capacity(20));
                        if let Some(clients) = topic_map.get_mut(topic) {
                            clients.push(client_name.to_string());
                        }
                    });
                    fut.await;
                }
            }
            BrokerMessage::CloseClient(_client_name) => {}
        };
        //future::ready(())
        ()
    });
    fut.await;
}

/*pub async fn new_message_broker_broke(
    rx: mpsc::Receiver<BrokerMessage>,
    mut threadpool: ThreadPool,
) {
    //let mut topic_map: HashMap<String, Vec<String>> = HashMap::new();
    let mut topic_map: HashMap<String, Vec<String>> = HashMap::new();
    let mut client_tx: HashMap<String, mpsc::Sender<ClientMessage>> = HashMap::new();
    let mut sequences: HashMap<String, usize> = HashMap::new();
    let fut = rx.for_each(move |mes| {
        println!("XXX Got message!");
        match mes {
            BrokerMessage::Message(mut message) => {
                // insert a key only if it doesn't already exist
                {
                    let seq = sequences.entry(message.topic.to_string()).or_insert(0);
                    message.sequence = *seq;
                    *seq += 1;
                }
                if let Some(clients) = topic_map.get_mut(&message.topic) {
                    for client in clients {
                        if let Some(tx) = client_tx.get_mut(client) {
                            // XXX TODO, spawn this on the executor?
                            //threadpool.spawn(tx.send(ClientMessage::Message(message.clone())).map(|_| {()}));
                            tx.send(ClientMessage::Message(message.clone())); //.map_err(|_e| {
                                                                              // XXX Remove bad client.
                                                                              //});
                        }
                    }
                }
            }
            BrokerMessage::NewClient(client_name, topics, tx) => {
                client_tx.insert(client_name.to_string(), tx.clone());
                for topic in &topics.topics {
                    // insert a key only if it doesn't already exist
                    topic_map
                        .entry(topic.to_string())
                        .or_insert(Vec::with_capacity(20));
                    if let Some(clients) = topic_map.get_mut(topic) {
                        clients.push(client_name.to_string());
                    }
                }
            }
            BrokerMessage::CloseClient(_client_name) => {}
        };
        future::ready(())
    });
    fut.await;
}*/

/*pub async fn new_message_broker(rx: mpsc::Receiver<BrokerMessage>) {
    let mut topic_map: &Arc<HashMap<String, Vec<String>>> = &Arc::new(HashMap::new());
    let mut client_tx: &Arc<HashMap<String, mpsc::Sender<ClientMessage>>> = &Arc::new(HashMap::new());
    let mut sequences: &Arc<HashMap<String, usize>> = &Arc::new(HashMap::new());
    let fut = rx.for_each(async move |mes| {
        let mut topic_map = topic_map.clone();
        let mut client_tx = client_tx.clone();
        let mut sequences = sequences.clone();
        println!("XXX Got message!");
        match mes {
            BrokerMessage::Message(mut message) => {
                // insert a key only if it doesn't already exist
                let seq = sequences.entry(message.topic.to_string()).or_insert(0);
                message.sequence = *seq;
                *seq += 1;
                if let Some(clients) = topic_map.get_mut(&message.topic) {
                    for client in clients {
                        if let Some(tx) = client_tx.get_mut(client) {
                            if let Err(error) = tx.try_send(ClientMessage::Message(message.clone()))
                            {
                                if error.is_disconnected() {
                                // XXX Revome bad client.
                                    println!("XXXX is disconnected");
                                }
                                if error.is_full() {
                                    println!("XXXX is full");
                                }
                            }
                        }
                    }
                }
            }
            BrokerMessage::NewClient(client_name, topics, tx) => {
                client_tx.insert(client_name.to_string(), tx.clone());
                for topic in &topics.topics {
                    // insert a key only if it doesn't already exist
                    topic_map
                        .entry(topic.to_string())
                        .or_insert(Vec::with_capacity(20));
                    if let Some(clients) = topic_map.get_mut(topic) {
                        clients.push(client_name.to_string());
                    }
                }
            }
            BrokerMessage::CloseClient(_client_name) => {}
        };
        //future::ready(())
        ()
    });
    await!(fut);
}*/
