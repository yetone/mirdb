#![feature(trace_macros, uniform_paths, box_syntax)]
#![allow(unused_imports, unused_macros, dead_code)]

use std::error::Error;
use std::io::{Read, Write, Result, Error as IOError, ErrorKind};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, RwLock};

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
#[macro_use]
mod error;

use crate::parser::parse;
use crate::store::Store;
use crate::utils::to_str;
use crate::thread_pool::ThreadPool;
use crate::error::MyResult;
use crate::request::Request;

fn main() -> MyResult<()> {
    let listener = TcpListener::bind("127.0.0.1:12333").unwrap();

    let store = Arc::new(RwLock::new(Store::new()));

    let tp = ThreadPool::new(16);

    for stream in listener.incoming() {
        let stream = stream.unwrap();

        let store = store.clone();

        tp.execute(|| {
            match handle_connection(stream, store) {
                Err(e) => println!("{:?}", e),
                _ => ()
            }
        });
    }

    Ok(())
}

fn handle_connection(mut stream: TcpStream, store: Arc<RwLock<Store>>) -> MyResult<()> {
    let mut buffer = [0; 512];

    let mut data: Vec<u8> = Vec::with_capacity(buffer.len());

    loop {
        let size = stream.read(&mut buffer)?;

        if size == 0 {
            println!("disconnected");
            break;
        }

        data.extend_from_slice(&buffer[..size]);

        let (remain, cfg) = parse(&data);

        match cfg.request {
            Request::Incomplete => continue,
            Request::Getter{ .. } => {
                let store = match store.read() {
                    Ok(guard) => guard,
                    Err(poisoned) => poisoned.into_inner()
                };

                match store.apply(cfg.request) {
                    Some(response) => response.write(&mut stream)?,
                    None => continue,
                };
            }
            _ => {
                let mut store = match store.write() {
                    Ok(guard) => guard,
                    Err(poisoned) => poisoned.into_inner()
                };

                match store.apply_mut(cfg.request) {
                    Some(response) => response.write(&mut stream)?,
                    None => continue,
                };
            }
        };

        data = remain.to_vec();
        if data.capacity() < buffer.len() {
            data.reserve(buffer.len() - data.capacity());
        }
    }

    Ok(())
}
