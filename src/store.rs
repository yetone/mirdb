use std::time::{SystemTime, UNIX_EPOCH};
use std::io::{Write, Result};
use std::collections::HashMap;
use std::error::Error;
use std::convert::From;

use crate::parser::command::{SetterType, GetterType, Command};
use crate::utils::to_str;

pub type StoreKey = Vec<u8>;

#[derive(Debug, PartialEq)]
pub struct StorePayload {
    pub(crate) data: Vec<u8>,
    pub(crate) flags: u32,
    ttl: u32,
    pub(crate) bytes: usize,
    created_at: u64,
}

#[derive(Debug, PartialEq)]
pub struct GetRespItem<'a> {
    key: &'a [u8],
    data: Vec<u8>,
    flags: u32,
    bytes: usize,
}

pub struct Store {
    data: HashMap<StoreKey, StorePayload>
}

#[derive(Debug, PartialEq)]
pub enum Response<'a> {
    Stored,
    NotStored,
    Exists,
    NotFound,
    Get(Vec<GetRespItem<'a>>),
    Gets(Vec<GetRespItem<'a>>),
    Deleted,
    Touched,
    Ok,
    Busy(&'a [u8]),
    Badclass(&'a [u8]),
    Nospare(&'a [u8]),
    Notfull(&'a [u8]),
    Unsafe(&'a [u8]),
    Same(&'a [u8]),
    Error,
    ClientError(&'a str),
    ServerError(&'a str),
}

impl<'a> Response<'a> {
    pub fn write(&self, writer: &mut Write) -> Result<()> {
        match self {
            Response::Stored => {
                writer.write(b"STORED\r\n")?;
            }
            Response::NotStored => {
                writer.write(b"NOT_STORED\r\n")?;
            }
            Response::Exists => {
                writer.write(b"EXISTS\r\n")?;
            }
            Response::NotFound => {
                writer.write(b"NOT_FOUND\r\n")?;
            }
            Response::Get(v) => {
                for GetRespItem{ key, data, flags, bytes } in v {
                    writer.write(format!(
                        "VALUE {} {} {}\r\n",
                        to_str(key), flags, bytes
                    ).as_bytes())?;
                    writer.write(&data[..])?;
                    writer.write(b"\r\n")?;
                }
                writer.write(b"END\r\n")?;
            }
            Response::Deleted => {
                writer.write(b"DELETED\r\n")?;
            }
            Response::Touched => {
                writer.write(b"TOUCHED\r\n")?;
            }
            Response::Ok => {
                writer.write(b"OK\r\n")?;
            }
            Response::Error => {
                writer.write(b"ERROR\r\n")?;
            }
            Response::ClientError(e) => {
                writer.write(format!("CLIENT_ERROR {}\r\n", e).as_bytes())?;
            }
            Response::ServerError(e) => {
                writer.write(format!("SERVER_ERROR {}\r\n", e).as_bytes())?;
            }
            _ => {
                unimplemented!();
            }
        }
        Ok(())
    }
}

impl Store {
    pub fn new() -> Self {
        Store {
            data: HashMap::default()
        }
    }

    pub fn apply<'a>(&mut self, command: Command<'a>) -> Option<Response<'a>> {
        match command {
            Command::Getter{ getter, keys } => {
                let mut v = Vec::with_capacity(keys.len());
                for key in keys {
                    match self.data.get(key) {
                        Some(p) => {
                            if p.created_at + p.ttl as u64 > SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() {
                                v.push(GetRespItem {
                                    key,
                                    data: p.data.clone(),
                                    flags: p.flags,
                                    bytes: p.bytes,
                                });
                            }
                        }
                        _ => ()
                    }
                }
                Some(match getter {
                    GetterType::Get => Response::Get(v),
                    GetterType::Gets => Response::Gets(v),
                })
            }
            Command::Setter{ setter, key, flags, ttl, bytes, payload } => {
                if payload.len() > bytes {
                    return Some(Response::ClientError("bad data chunk"));
                }
                let data = payload[..bytes as usize].to_vec();
                let created_at = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                let sp = StorePayload {
                    flags, ttl, bytes,
                    data, created_at,
                };
                let key = key.to_vec();
                match setter {
                    SetterType::Set => {
                        self.data.insert(key, sp);
                    }
                    SetterType::Add => {
                        // Cannot use self.data.entry(key).or_insert(sp);
                        // because of the NOT_STORED response
                        match self.data.get_mut(&key) {
                            Some(_) => {
                                return Some(Response::NotStored);
                            }
                            None => {
                                self.data.insert(key, sp);
                            }
                        }
                    }
                    SetterType::Replace => {
                        // Cannot use self.data.entry(key).and_modify(|e| *e = sp);
                        // because of the NOT_STORED response
                        match self.data.get_mut(&key) {
                            Some(_) => {
                                self.data.insert(key, sp);
                            }
                            None => {
                                return Some(Response::NotStored);
                            }
                        }
                    }
                    SetterType::Append => {
                        match self.data.get_mut(&key) {
                            Some(v) => {
                                v.data.extend(sp.data);
                                v.ttl = sp.ttl;
                                v.created_at = sp.created_at;
                                v.bytes += sp.bytes;
                                v.flags = sp.flags;
                            }
                            None => {
                                return Some(Response::NotStored);
                            }
                        }
                    }
                    SetterType::Prepend => {
                        match self.data.get_mut(&key) {
                            Some(v) => {
                                let mut tmp: Vec<_> = sp.data.to_owned();
                                tmp.extend(&v.data);
                                v.data = tmp;
                                v.ttl = sp.ttl;
                                v.created_at = sp.created_at;
                                v.bytes += sp.bytes;
                                v.flags = sp.flags;
                            }
                            None => {
                                return Some(Response::NotStored);
                            }
                        }
                    }
                }
                Some(Response::Stored)
            }
            Command::Error(_) => {
                Some(Response::Error)
            }
            Command::Incomplete => {
                None
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_get_none() {
        let mut store = Store::new();
        let r = store.apply(Command::Getter{ getter: GetterType::Get, keys: vec![b"a"] });
        assert_eq!(Some(Response::Get(vec![])), r);
    }

    #[test]
    fn test_get_some() {
        let mut store = Store::new();
        let key = b"a";
        let payload = b"abc";
        let r = store.apply(Command::Setter{
            setter: SetterType::Set,
            key,
            flags: 1,
            ttl: 10,
            bytes: payload.len(),
            payload
        });
        assert!(r.is_some(), "stored");
        let r = store.apply(Command::Getter{ getter: GetterType::Get, keys: vec![key] });
        assert_eq!(Some(Response::Get(vec!(GetRespItem {
            key,
            data: payload.to_vec(),
            flags: 1,
            bytes: payload.len(),
        }))), r);
    }

    #[test]
    fn test_set() {
        let mut store = Store::new();
        let key = b"a";
        let payload = b"abc";
        let r = store.apply(Command::Setter{
            setter: SetterType::Set,
            key,
            flags: 1,
            ttl: 10,
            bytes: payload.len(),
            payload
        });
        assert_eq!(Some(Response::Stored), r);
    }

    #[test]
    fn test_set_err() {
        let mut store = Store::new();
        let key = b"a";
        let payload = b"abc";
        let r = store.apply(Command::Setter{
            setter: SetterType::Set,
            key,
            flags: 1,
            ttl: 10,
            bytes: payload.len() - 1,
            payload
        });
        assert_eq!(Some(Response::ClientError("bad data chunk")), r);
    }
}
