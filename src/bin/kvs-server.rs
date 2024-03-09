#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

use slog::Drain;

use clap::{Parser, Subcommand};
use kvs::{kvs_server, KvStore};
use slog_term::Decorator;
use std::{env, process};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    addr: String,
    #[arg(short, long)]
    engine: String,
}

fn main() {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();

    let _log = slog::Logger::root(drain, o!());

    let args = Args::parse();

    if args.addr.len() == 0 || args.engine.len() == 0 {
        process::exit(1);
    }

    let kvs_server = KvsServer::new(args.addr);
    kvs_server.listen_forever();
    process::exit(0);
}

fn setup_logger() {}

use std::{
    io::Read,
    net::{TcpListener, TcpStream},
};

pub struct KvsServer {
    tcp_listener: TcpListener,
}

impl KvsServer {
    pub fn new(ip_addr: String) -> KvsServer {
        let tcp_listener = TcpListener::bind(ip_addr).unwrap();

        KvsServer { tcp_listener }
    }

    pub fn listen_forever(&self) -> Result<(), std::io::Error> {
        for stream in self.tcp_listener.incoming() {
            match stream {
                Ok(mut stream) => {
                    handle_connection(&mut stream)?;
                }
                Err(_) => {}
            }
        }

        Ok(())
    }
}

fn handle_connection(stream: &mut TcpStream) -> Result<(), std::io::Error> {
    let mut buffer: Vec<u8> = Vec::new();

    stream.read_to_end(&mut buffer)?;
    // Deserialize this to a command, we will need to implement a shared library for the commands
    // as well as a method that can Deserialize from bytes.
    Ok(())
}
