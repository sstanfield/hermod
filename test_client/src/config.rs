use std::env;
use std::ffi::OsString;

pub struct Config {
    pub name: String,
    pub group: String,
    pub topic: String,
    pub is_client: bool,
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
    -n, --name <client_name>  Sets the name the client will present to the server.
    -g, --group <group_id>    Sets the consumer group name for the client.
    -t, --topic <topic>       Sets the topic to use."#;

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
                    "-n" | "--name" => {
                        name = get_arg(&exe_name, &mut args)?;
                    }
                    "-g" | "--group" => {
                        group = get_arg(&exe_name, &mut args)?;
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
    })
}
