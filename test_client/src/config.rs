use std::env;
use std::ffi::OsString;

pub struct Config {
    pub name: String,
    pub group: String,
    pub topic: String,
    pub is_client: bool,
}

fn help(name: &str) {
    println!("Usage: {} [OPTION]...", name);
    println!("Run a test client.");
    println!("  -n, --name [CLIENT NAME]  Sets the name the client will present to the server.");
    println!("  -g, --group [GROUP NAME]  Sets the consumer group name for the client.");
    println!("  -c, --client              Run as a client.");
    println!("  -s, --server              Run as a server.");
    println!("  -t, --topic [TOPIC]       Sets the topic to use.");
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
                    "-c" | "--client" => {
                        is_client = true;
                    }
                    "-s" | "--server" => {
                        is_client = false;
                    }
                    "-t" | "--topic" => {
                        topic = get_arg(&exe_name, &mut args)?;
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
