#![allow(unused_imports, unused_macros, dead_code)]
#![allow(semicolon_in_expressions_from_macros)]

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
mod http_server;
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
#[cfg(test)]
mod http_server_tests;
#[cfg(test)]
mod memtable_status_tests;

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
        .get_matches();

    let conf_path = matches.value_of("config").unwrap_or("default.conf");
    let conf = config::from_path(conf_path)?;

    // Validate configuration (includes port conflict detection)
    conf.validate()?;

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

    // Initialize uptime tracking
    http_server::init_start_time();

    // Start HTTP server if enabled
    if conf.http_enabled() {
        let http_addr: SocketAddr = conf.http_addr().unwrap().parse().unwrap();
        let http_store = store.clone();

        info!("Starting HTTP server on {}", http_addr);
        info!("Dashboard available at http://{}/dashboard", http_addr);

        // Spawn HTTP server in a separate thread with its own Tokio runtime
        thread::spawn(move || {
            let http_server = http_server::HttpServer::new(http_addr, http_store);
            tokio::run(http_server.run());
        });
    }

    info!("Starting Memcached TCP server on {}", addr);
    serve(addr, move || Ok(Server::new(store.clone())));

    Ok(())
}
