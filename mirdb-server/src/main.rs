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
use crate::request::RequestConf;
use crate::response::Response;
use crate::store::Store;
use crate::thread_pool::ThreadPool;
use crate::utils::to_str;

#[macro_use]
mod error;
#[macro_use]
mod request;
mod response;
#[macro_use]
mod parser_util;
mod parser;
mod store;
mod utils;
mod thread_pool;
mod data_manager;
mod memtable;
mod proto;

pub struct Server {
    store: Arc<RwLock<Store>>,
}

impl Server {
    fn new(store: Arc<RwLock<Store>>) -> Self {
        Server {
            store
        }
    }
}

impl Service for Server {
    type Request = RequestConf;
    type Response = Response;
    type Error = io::Error;
    type Future = Box<Future<Item = Response, Error = io::Error>>;

    fn call(&self, req: Self::Request) -> Self::Future {
        match req.request {
            Request::Getter{ .. } => {
                let store = match self.store.read() {
                    Ok(guard) => guard,
                    Err(poisoned) => poisoned.into_inner()
                };

                box future::done(match store.apply(req.request) {
                    Ok(response) => Ok(response),
                    Err(e) => Ok(Response::ServerError(e.msg)),
                })
            }
            _ => {
                let mut store = match self.store.write() {
                    Ok(guard) => guard,
                    Err(poisoned) => poisoned.into_inner()
                };

                box future::done(match store.apply_mut(req.request) {
                    Ok(response) => Ok(response),
                    Err(e) => Ok(Response::ServerError(e.msg)),
                })
            }
        }
    }
}

pub fn serve<T>(addr: SocketAddr, new_service: T)
    where T: NewService<Request = RequestConf, Response = Response, Error = io::Error> + Send + Sync + 'static,
{
    TcpServer::new(Proto, addr).serve(new_service);
}

fn main() {
    let addr = "127.0.0.1:12333".parse().unwrap();
    let store = Arc::new(RwLock::new(Store::new()));

    serve(addr, move || {
        Ok(Server::new(store.clone()))
    });
}
