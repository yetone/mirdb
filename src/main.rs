#![feature(trace_macros, uniform_paths, box_syntax)]
#![allow(unused_imports, unused_macros, dead_code)]

use std::error::Error;
use std::io::{Read, Write, Result, Error as IOError, ErrorKind};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, RwLock};

#[macro_use]
mod parser;
mod store;
mod utils;
mod thread_pool;

use crate::parser::{parse, Command};
use crate::store::Store;
use crate::utils::to_str;
use crate::thread_pool::ThreadPool;

fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:12333").unwrap();

    let store = Arc::new(RwLock::new(Store::new()));

    let tp = ThreadPool::new(16);

    for stream in listener.incoming() {
        let stream = stream.unwrap();

        let store = store.clone();

        tp.execute(|| {
            match handle_connection(stream, store) {
                Err(e) => println!("{}", e),
                _ => ()
            }
        });
    }

    Ok(())
}

fn handle_connection(mut stream: TcpStream, store: Arc<RwLock<Store>>) -> Result<()> {
    let mut buffer = [0; 512];

    let mut data: Vec<u8> = Vec::with_capacity(buffer.len());

    loop {
        let size = stream.read(&mut buffer)?;
        if size == 0 {
            println!("disconnected");
            break;
        }

        data.extend_from_slice(&buffer[0..size]);

        let cfg = parse(&data);

        match cfg.command {
            Command::Incomplete => continue,
            Command::Getter{ .. } => {
                let store = store.read().map_err(|_| IOError::new(ErrorKind::Other, "cannot get store"))?;
                match store.apply(cfg.command) {
                    Some(response) => response.write(&mut stream)?,
                    None => continue,
                };
            }
            _ => {
                let mut store = store.write().map_err(|_| IOError::new(ErrorKind::Other, "cannot get store"))?;

                match store.apply_mut(cfg.command) {
                    Some(response) => response.write(&mut stream)?,
                    None => continue,
                };
            }
        };


        data.clear();
    }

    Ok(())
}
