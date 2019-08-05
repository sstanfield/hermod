use std::env;
use std::ffi::OsString;
use std::net::SocketAddr;

pub struct Config {
    pub name: String,
    pub group: String,
    pub topic: String,
    pub is_client: bool,
    pub remote: SocketAddr,
    pub count: usize,
    pub base_message: String,
}

const VERSION_STRING: &str = env!("VERSION_STRING");

const HELP: &str = r#"test_client - Hermod test client
Run a test client, either publisher or subscriber.

USAGE:
    test_client [FLAGS] [OPTIONS]

FLAGS:
    -p, --publish  Publish messages to broker
    -c, --consume  Subscribe to topic
    -v, --version  Print the version, platform and revision of client then exit

OPTIONS:
    -r, --remote <hermod server>  Sets the remote hermod server (server:port).
    -n, --name <client_name>      Sets the name the client will present to the server.
    -g, --group <group_id>        Sets the consumer group name for the client.
        --count <count>           Sets the number of messages to consume or publish.
        --base_message <str>      Sets the message to append index to for publishing.
    -t, --topic <topic>           Sets the topic to use."#;

fn help(_name: &str) {
    println!("{}", HELP);
}

fn version() {
    println!("{}", VERSION_STRING);
}

fn get_arg(exe_name: &str, args: &mut Vec<OsString>) -> Result<String, ()> {
    if let Some(argument) = args.pop() {
        if let Ok(arg) = argument.into_string() {
            return Ok(arg);
        }
    }
    help(exe_name);
    Err(())
}

pub fn get_config() -> Result<Config, ()> {
    let mut name = "test_client".to_string();
    let mut group = "g1".to_string();
    let mut topic = "top1".to_string();
    let mut count = 0;
    let mut base_message = "SOME MESSAGE".to_string();
    let mut remote: SocketAddr = "127.0.0.1:7878".parse().unwrap();

    if let Some(name_os) = env::var_os("HERMOD_CLIENT_REMOTE") {
        if let Ok(r) = name_os.into_string() {
            remote = r.parse().expect("Invalid socket address for server");
        }
    }
    if let Some(name_os) = env::var_os("HERMOD_CLIENT_NAME") {
        if let Ok(n) = name_os.into_string() {
            name = n;
        }
    }
    if let Some(group_os) = env::var_os("HERMOD_CLIENT_GROUP") {
        if let Ok(g) = group_os.into_string() {
            group = g;
        }
    }
    if let Some(topic_os) = env::var_os("HERMOD_CLIENT_TOPIC") {
        if let Ok(t) = topic_os.into_string() {
            topic = t;
        }
    }
    let mut is_client = env::var_os("HERMOD_CLIENT_SERVER").is_none();

    let mut args: Vec<OsString> = env::args_os().collect();
    args.reverse();
    let exe_name = get_arg("unknown", &mut args)?; // Pop off the executable name.
    while !args.is_empty() {
        if let Some(argument) = args.pop() {
            if let Ok(arg) = argument.into_string() {
                match &arg[..] {
                    "-r" | "--remote" => match get_arg(&exe_name, &mut args)?.parse() {
                        Ok(r) => remote = r,
                        Err(err) => {
                            println!("Invalid server socket, {}!", err);
                            help(&exe_name);
                            return Err(());
                        }
                    },
                    "-n" | "--name" => {
                        name = get_arg(&exe_name, &mut args)?;
                    }
                    "-g" | "--group" => {
                        group = get_arg(&exe_name, &mut args)?;
                    }
                    "--count" => {
                        count = get_arg(&exe_name, &mut args)?
                            .parse::<usize>()
                            .map_err(|_| ())?;
                    }
                    "--base_message" => {
                        base_message = get_arg(&exe_name, &mut args)?;
                    }
                    "-c" | "--consume" => {
                        is_client = true;
                    }
                    "-p" | "--publish" => {
                        is_client = false;
                    }
                    "-t" | "--topic" => {
                        topic = get_arg(&exe_name, &mut args)?;
                    }
                    "-v" | "--version" => {
                        version();
                        return Err(());
                    }
                    "-h" | "--help" => {
                        help(&exe_name);
                        return Err(());
                    }
                    _ => {
                        help(&exe_name);
                        return Err(());
                    }
                }
            } else {
                help(&exe_name);
                return Err(());
            }
        }
    }
    Ok(Config {
        name,
        group,
        topic,
        is_client,
        remote,
        count,
        base_message,
    })
}
