//! MirDB Server Binary
//!
//! This binary starts the MirDB server with Memcached protocol support.

#![allow(unused_imports, unused_macros, dead_code)]

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use futures::SinkExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;
use futures::StreamExt;

use crate::error::MyResult;
use crate::options::Options;
use crate::proto::ServerCodec;
use crate::request::Request;
use crate::response::Response;
use crate::store::Store;

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
pub mod web;

#[derive(Parser, Debug)]
#[command(name = "MirDB")]
#[command(version = "0.0.1")]
#[command(author = "yetone <yetoneful@gmail.com>")]
#[command(about = "A KV DB with embedded web server")]
struct Args {
    /// Sets a custom config file
    #[arg(short, long, value_name = "FILE", default_value = "default.conf")]
    config: String,
}

async fn handle_memcached_connection(socket: TcpStream, store: Arc<Store>) {
    let mut framed = Framed::new(socket, ServerCodec);

    while let Some(result) = framed.next().await {
        match result {
            Ok(request) => {
                let response = match store.apply(request) {
                    Ok(resp) => resp,
                    Err(e) => Response::ServerError(e.msg),
                };
                if let Err(e) = framed.send(response).await {
                    log::error!("Error sending response: {}", e);
                    break;
                }
            }
            Err(e) => {
                log::error!("Error reading request: {}", e);
                break;
            }
        }
    }
}

async fn run_memcached_server(addr: SocketAddr, store: Arc<Store>) -> io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    log::info!("Memcached server listening on {}", addr);

    loop {
        let (socket, peer_addr) = listener.accept().await?;
        log::debug!("Accepted connection from {}", peer_addr);
        let store = store.clone();
        tokio::spawn(async move {
            handle_memcached_connection(socket, store).await;
        });
    }
}

#[tokio::main]
async fn main() -> MyResult<()> {
    env_logger::init();

    let args = Args::parse();
    let conf = config::from_path(&args.config)?;

    let memcached_addr: SocketAddr = conf.addr.parse().unwrap();
    let web_port = conf.web_port.unwrap_or(8080);
    let web_addr: SocketAddr = format!("0.0.0.0:{}", web_port).parse().unwrap();

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

    // Check for port conflict
    if memcached_addr.port() == web_port {
        return Err(crate::error::Status::new(
            crate::error::StatusCode::ConfigError,
            &format!(
                "HTTP port {} conflicts with Memcached port {}. Please use different ports.",
                web_port, memcached_addr.port()
            ),
        ));
    }

    log::info!("Starting Memcached server on {}", memcached_addr);
    log::info!("Starting HTTP server on {}", web_addr);

    // Start both servers concurrently
    let memcached_store = store.clone();
    let web_store = store.clone();
    let web_config = conf.clone();

    tokio::select! {
        result = run_memcached_server(memcached_addr, memcached_store) => {
            if let Err(e) = result {
                log::error!("Memcached server error: {}", e);
            }
        }
        result = web::server::start_web_server(web_addr, web_store, web_config) => {
            if let Err(e) = result {
                log::error!("Web server error: {}", e);
            }
        }
    }

    Ok(())
}
