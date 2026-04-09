#![allow(unused_imports, unused_macros, dead_code)]
#![allow(semicolon_in_expressions_from_macros)]
#![allow(mismatched_lifetime_syntaxes)]

use std::cell::RefCell;
use std::error::Error;
use std::io;
use std::io::{Error as IOError, ErrorKind, Read, Result, Write};
use std::net::SocketAddr;
use std::net::{TcpListener, TcpStream};
use std::rc::Rc;
use std::sync::{Arc, RwLock};
use std::thread;

use clap::App;
use clap::Arg;
use env_logger;
use futures::{future, Future};
use log::info;
use tokio::prelude::*;
use tokio_proto::TcpServer;
use tokio_service::{NewService, Service};

use crate::error::MyResult;
use crate::http::server::{create_app_state_with_endpoint, HttpConfig, start_http_server};
use crate::options::Options;
use crate::parser::parse;
use crate::proto::Proto;
use crate::request::Request;
use crate::response::Response;
use crate::store::Store;
use crate::thread_pool::ThreadPool;
use crate::utils::to_str;

#[macro_use]
mod utils;
#[macro_use]
mod error;
#[macro_use]
mod request;
mod response;
#[macro_use]
mod parser_util;
mod config;
mod data_manager;
mod manifest;
mod memtable;
mod memtable_list;
mod merger;
mod options;
mod parser;
mod proto;
mod slice;
mod sstable_builder;
mod sstable_reader;
mod store;
mod test_utils;
mod thread_pool;
mod types;
mod wal;
mod http;

pub struct Server {
    store: Arc<Store>,
}

impl Server {
    fn new(store: Arc<Store>) -> Self {
        Server { store }
    }
}

impl Service for Server {
    type Request = Request;
    type Response = Response;
    type Error = io::Error;
    type Future = Box<Future<Item = Response, Error = io::Error>>;

    fn call(&self, req: Self::Request) -> Self::Future {
        Box::new(future::done(match self.store.apply(req) {
            Ok(response) => Ok(response),
            Err(e) => Ok(Response::ServerError(e.msg)),
        }))
    }
}

pub fn serve<T>(addr: SocketAddr, new_service: T)
where
    T: NewService<Request = Request, Response = Response, Error = io::Error>
        + Send
        + Sync
        + 'static,
{
    TcpServer::new(Proto, addr).serve(new_service);
}

fn main() -> MyResult<()> {
    env_logger::init();

    let matches = App::new("MirDB")
        .version("0.0.1")
        .author("yetone <yetoneful@gmail.com>")
        .about("A KV DB")
        .arg(
            Arg::with_name("config")
                .short("c")
                .long("config")
                .value_name("FILE")
                .help("Sets a custom config file")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("http-port")
                .long("http-port")
                .value_name("PORT")
                .help("HTTP server port (default: 8080)")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("no-http")
                .long("no-http")
                .help("Disable HTTP server")
                .takes_value(false),
        )
        .get_matches();

    let conf_path = matches.value_of("config").unwrap_or("default.conf");
    let conf = config::from_path(conf_path)?;

    let addr: SocketAddr = conf.addr.parse().unwrap();
    let opt = conf.to_options()?;

    let store = Store::new(opt.clone())?;
    let store = Arc::new(store);

    println!(
        "{}",
        r#"
  __  __ _     ___  ___
 |  \/  (_)_ _|   \| _ )
 | |\/| | | '_| |) | _ \
 |_|  |_|_|_| |___/|___/

Welcome to MirDB!
"#
        .trim_matches('\n')
    );

    // Start HTTP server if enabled
    let enable_http = !matches.is_present("no-http");
    if enable_http {
        let http_port: u16 = matches
            .value_of("http-port")
            .unwrap_or("8080")
            .parse()
            .unwrap_or(8080);

        // Extract host and port from Memcached address for status display
        let memcached_addr = conf.addr.clone();

        // Parse endpoint info
        let (host, port) = if let Some(idx) = memcached_addr.rfind(':') {
            let h = memcached_addr[..idx].to_string();
            let p: u16 = memcached_addr[idx + 1..].parse().unwrap_or(12333);
            (h, p)
        } else {
            ("0.0.0.0".to_string(), 12333)
        };

        // Create shared state with Memcached endpoint info
        let app_state = create_app_state_with_endpoint(host, port);

        // Configure HTTP server
        let http_addr: SocketAddr = format!("0.0.0.0:{}", http_port).parse().unwrap();
        let http_config = HttpConfig::new(http_addr, memcached_addr);

        // Start HTTP server in a separate thread
        thread::spawn(move || {
            info!("Starting HTTP server on port {}", http_port);
            let http_server = start_http_server(http_config, app_state);
            tokio::run(http_server);
        });

        println!("HTTP server: http://0.0.0.0:{}", http_port);
    }

    println!("Memcached protocol: {}", conf.addr);
    println!();

    // Start Memcached protocol server (blocks)
    serve(addr, move || Ok(Server::new(store.clone())));

    Ok(())
}
