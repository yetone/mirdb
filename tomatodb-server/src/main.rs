#![feature(trace_macros, uniform_paths, box_syntax)]
#![allow(unused_imports, unused_macros, dead_code)]

use std::cell::RefCell;
use std::error::Error;
use std::io::{Error as IOError, ErrorKind, Read, Result, Write};
use std::io;
use std::net::{TcpListener, TcpStream};
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::{Arc, RwLock};

use futures::{Future, future};
use tokio::prelude::*;
use tokio_proto::TcpServer;
use tokio_service::{NewService, Service};

pub use proto::Proto;

use crate::parser::parse;
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
mod parser;
mod store;
mod thread_pool;
mod data_manager;
mod memtable;
mod proto;
mod memtable_list;
mod types;
mod options;
mod sstable_builder;
mod sstable_reader;
mod manifest;
mod wal;
mod test_utils;
mod merger;

use crate::options::Options;
use crate::error::MyResult;

pub struct Server {
    store: Arc<Store>,
}

impl Server {
    fn new(store: Arc<Store>) -> Self {
        Server {
            store
        }
    }
}

impl Service for Server {
    type Request = Request;
    type Response = Response;
    type Error = io::Error;
    type Future = Box<Future<Item = Response, Error = io::Error>>;

    fn call(&self, req: Self::Request) -> Self::Future {
        box future::done(match self.store.apply(req) {
            Ok(response) => Ok(response),
            Err(e) => Ok(Response::ServerError(e.msg)),
        })
    }
}

pub fn serve<T>(addr: SocketAddr, new_service: T)
    where T: NewService<Request = Request, Response = Response, Error = io::Error> + Send + Sync + 'static,
{
    TcpServer::new(Proto, addr).serve(new_service);
}

fn main() -> MyResult<()> {
    let addr = "0.0.0.0:12333".parse().unwrap();

    let opt = Options::default();

    let store = Store::new(opt.clone())?;
    let store = Arc::new(store);

    serve(addr, move || {
        Ok(Server::new(store.clone()))
    });

    Ok(())
}
