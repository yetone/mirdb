#![feature(trace_macros, uniform_paths, box_syntax)]
#![allow(unused_imports, unused_macros, dead_code)]

use std::io::{Read, Write, Result};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

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

    let store = Arc::new(Mutex::new(Store::new()));

    let tp = ThreadPool::new(16);

    for stream in listener.incoming() {
        let stream = stream.unwrap();

        let store = store.clone();

        tp.execute(|| {
            handle_connection(stream, store).unwrap();
        });
    }

    Ok(())
}

fn handle_connection(mut stream: TcpStream, store: Arc<Mutex<Store>>) -> Result<()> {
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
            Command::Error(e) => {
                stream.write(format!("{}\r\n", e).as_bytes())?;
            }
            Command::Incomplete => {
                continue;
            }
            Command::Getter {
                key
            } => {
                let store = store.lock().unwrap();
                match store.get(key) {
                    Some(p) => {
                        stream.write(format!("VALUE {} {} {}\r\n", to_str(key), p.flags, p.bytes).as_bytes())?;
                        stream.write(&p.data[..])?;
                        stream.write(b"\r\n")?;
                    }
                    _ => {}
                }
            }
            Command::Setter {
                setter, key, flags, ttl, bytes, payload
            } => {
                let mut store = store.lock().unwrap();
                let res = store.set(setter, key, flags, ttl, bytes, payload);
                match res {
                    Err(e) => {
                        stream.write(format!("{}", e).as_bytes())?;
                        stream.write(b"\r\n")?;
                    },
                    _ => {
                        if !cfg.noreply {
                            stream.write(b"STORED\r\n")?;
                        }
                    }
                }
            }
        };

        data.clear();
    }

    Ok(())
}
