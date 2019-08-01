use std::env;
use std::ffi::OsString;
use std::net::SocketAddr;

pub struct Config {
    pub bind: SocketAddr,
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
    -b, --bind <bind address>  Sets the hermod server ip and port (ip:port)."#;

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

    if let Some(name_os) = env::var_os("HERMOD_ADDRESS") {
        if let Ok(b) = name_os.into_string() {
            bind = b.parse().expect("Invalid socket address for server");
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
    Ok(Config { bind })
}
