use std::time::{SystemTime, UNIX_EPOCH};
use std::io::{Write, Result};
use std::collections::HashMap;
use std::error::Error;
use std::convert::From;

use skip_list::SkipList;

use crate::request::{SetterType, GetterType, Request};
use crate::utils::to_str;
use crate::data_manager::DataManager;
use crate::response::Response;
use crate::response::GetRespItem;

pub type StoreKey = Vec<u8>;

#[derive(Debug, PartialEq, Clone)]
pub struct StorePayload {
    pub(crate) data: Vec<u8>,
    pub(crate) flags: u32,
    ttl: u32,
    pub(crate) bytes: usize,
    created_at: u64,
}

pub struct Store {
    data: DataManager<StoreKey, StorePayload>
}

impl Store {
    pub fn new() -> Self {
        Store {
            data: DataManager::new(10, 3)
        }
    }

    pub fn apply<'a>(&self, request: Request<'a>) -> Option<Response<'a>> {
        match request {
            Request::Getter{ getter, keys } => {
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
            Request::Error(_) => {
                Some(Response::Error)
            }
            _ => None
        }
    }

    pub fn apply_mut<'a>(&mut self, request: Request<'a>) -> Option<Response<'a>> {
        match request {
            Request::Setter{ setter, key, flags, ttl, bytes, payload } => {
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
                        match self.data.get(&key) {
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
                        match self.data.get(&key) {
                            Some(_) => {
                                self.data.insert(key, sp);
                            }
                            None => {
                                return Some(Response::NotStored);
                            }
                        }
                    }
                    SetterType::Append => {
                        match self.data.get(&key) {
                            Some(v) => {
                                let mut c = v.clone();
                                c.data.extend(sp.data);
                                c.ttl = sp.ttl;
                                c.created_at = sp.created_at;
                                c.bytes += sp.bytes;
                                c.flags = sp.flags;
                                self.data.insert(key, c);
                            }
                            None => {
                                return Some(Response::NotStored);
                            }
                        }
                    }
                    SetterType::Prepend => {
                        match self.data.get(&key) {
                            Some(v) => {
                                let mut tmp: Vec<_> = sp.data.to_owned();
                                let mut c = v.clone();
                                tmp.extend(&v.data);
                                c.data = tmp;
                                c.ttl = sp.ttl;
                                c.created_at = sp.created_at;
                                c.bytes += sp.bytes;
                                c.flags = sp.flags;
                                self.data.insert(key, c);
                            }
                            None => {
                                return Some(Response::NotStored);
                            }
                        }
                    }
                }
                Some(Response::Stored)
            }
            Request::Deleter{ key } => {
                match self.data.remove(key) {
                    Some(_) => Some(Response::Deleted),
                    None => Some(Response::NotFound),
                }
            }
            Request::Error(_) => {
                Some(Response::Error)
            }
            _ => None
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_get_none() {
        let store = Store::new();
        let r = store.apply(Request::Getter{ getter: GetterType::Get, keys: vec!["a".as_bytes()] });
        assert_eq!(Some(Response::Get(vec![])), r);
    }

    #[test]
    fn test_get_some() {
        let mut store = Store::new();
        let key = "a".as_bytes();
        let payload = "abc".as_bytes();
        let r = store.apply_mut(Request::Setter{
            setter: SetterType::Set,
            key,
            flags: 1,
            ttl: 10,
            bytes: payload.len(),
            payload
        });
        assert!(r.is_some(), "stored");
        let r = store.apply(Request::Getter{ getter: GetterType::Get, keys: vec![key] });
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
        let r = store.apply_mut(Request::Setter{
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
        let r = store.apply_mut(Request::Setter{
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
