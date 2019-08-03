use std::env;
use std::ffi::OsString;
use std::fs::create_dir_all;
use std::net::SocketAddr;

pub struct Config {
    pub bind: SocketAddr,
    pub log_dir: String,
}

const VERSION_STRING: &str = env!("VERSION_STRING");

const HELP: &str = r#"hermod - Hermod server
Run a Hermod server.

USAGE:
    hermod [FLAGS] [OPTIONS]

FLAGS:
    -v, --version  Print the version, platform and revision of server then exit.
    -h, --help     Print help (this) and exit.

OPTIONS:
    -l, --logdir, HERMOD_LOGDIR <log file directory>  Sets the directory that will contain log files.
    -b, --bind, HERMOD_BIND <bind address>            Sets the hermod server ip and port (ip:port)."#;

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
    let mut bind: SocketAddr = "127.0.0.1:7878".parse().unwrap();
    let mut log_dir = "logs".to_string();

    if let Some(name_os) = env::var_os("HERMOD_BIND") {
        if let Ok(b) = name_os.into_string() {
            bind = b.parse().expect("Invalid socket address for server");
        }
    }

    if let Some(name_os) = env::var_os("HERMOD_LOGDIR") {
        if let Ok(l) = name_os.into_string() {
            log_dir = l;
        }
    }

    let mut args: Vec<OsString> = env::args_os().collect();
    args.reverse();
    let exe_name = get_arg("unknown", &mut args)?; // Pop off the executable name.
    while !args.is_empty() {
        if let Some(argument) = args.pop() {
            if let Ok(arg) = argument.into_string() {
                match &arg[..] {
                    "-b" | "--bind" => match get_arg(&exe_name, &mut args)?.parse() {
                        Ok(r) => bind = r,
                        Err(err) => {
                            println!("Invalid server socket, {}!", err);
                            help(&exe_name);
                            return Err(());
                        }
                    },
                    "-l" | "--logdir" => {
                        log_dir = get_arg(&exe_name, &mut args)?;
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
    if log_dir.ends_with('/') {
        log_dir = log_dir[..log_dir.len()].to_string();
    }
    if let Err(err) = create_dir_all(log_dir.clone()) {
        println!("Unable to create log directory: {}- {}", log_dir, err);
        return Err(());
    }
    Ok(Config { bind, log_dir })
}
