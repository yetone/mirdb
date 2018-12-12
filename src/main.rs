use std::io::{Read, Write, Result};
use std::net::{TcpListener, TcpStream};

mod parser;
mod store;
mod utils;

use crate::parser::{parse, Command};
use crate::store::Store;
use crate::utils::to_str;

fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:12333").unwrap();

    let mut store = Store::new();

    for stream in listener.incoming() {
        let stream = stream.unwrap();

        handle_connection(stream, &mut store)?;
    }

    Ok(())
}

fn handle_connection(mut stream: TcpStream, store: &mut Store) -> Result<()> {
    let mut buffer = [0; 512];

    let mut data: Vec<u8> = Vec::with_capacity(buffer.len());

    loop {
        let size = stream.read(&mut buffer)?;
        if size == 0 {
            println!("disconnected");
            break;
        }

        data.extend_from_slice(&buffer[0..size]);

        match parse(&data) {
            Command::Error(e) => {
                stream.write(format!("{}\r\n", e).as_bytes())?;
            }
            Command::Incomplete => {
                continue;
            }
            Command::Getter {
                key
            } => {
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
                key, flags, ttl, bytes, noreply, payload
            } => {
                let res = store.set(key, flags, ttl, bytes, noreply, payload);
                match res {
                    Err(e) => {
                        stream.write(format!("{}", e).as_bytes())?;
                        stream.write(b"\r\n")?;
                    },
                    _ => {
                        if !noreply {
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
