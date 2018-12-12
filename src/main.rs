use std::io::{Read, Write, Result};
use std::net::{TcpListener, TcpStream};

mod parser;
mod store;

use crate::parser::{parse, Command};
use crate::store::Store;

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
            Command::Error => {
                stream.write(b"<error>\r\n")?;
            }
            Command::Incomplete => {
                continue;
            }
            Command::Getter {
                key
            } => {
                match store.get(key) {
                    Some(v) => {
                        stream.write(v)?;
                        stream.write(b"\r\n")?;
                    }
                    _ => {}
                }
            }
            Command::Setter {
                key, flags, ttl, bytes, noreply, payload
            } => {
                store.set(key, flags, ttl, bytes, noreply, payload);
                if !noreply {
                    stream.write(b"<ok>\r\n")?;
                }
            }
        };

        data.clear();
    }

    Ok(())
}
