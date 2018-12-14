use std::time::{SystemTime, UNIX_EPOCH};
use std::collections::HashMap;
use std::error::Error;
use std::convert::From;

use crate::parser::command::SetterType;

pub type StoreKey = Vec<u8>;

#[allow(dead_code)]
pub struct StorePayload {
    pub(crate) data: Vec<u8>,
    pub(crate) flags: u32,
    ttl: u32,
    pub(crate) bytes: usize,
    created_at: u64,
}

pub struct Store {
    data: HashMap<StoreKey, StorePayload>
}

impl Store {
    pub fn new() -> Self {
        Store {
            data: HashMap::default()
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<&StorePayload> {
        match self.data.get(key) {
            Some(p) => {
                if p.created_at + p.ttl as u64 <= SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() {
                    return None;
                }
                Some(p)
            },
            _ => None
        }
    }

    pub fn set(&mut self, setter: SetterType, key: &[u8], flags: u32, ttl: u32, bytes: usize, payload: &[u8]) -> Result<(), Box<dyn Error>> {
        if payload.len() > bytes {
            return Err(From::from("CLIENT_ERROR bad data chunk"));
        }
        let data = payload[..bytes as usize].to_vec();
        let created_at = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let sp = StorePayload {
            flags, ttl, bytes,
            data, created_at,
        };
        let key = key.to_vec();
        match setter {
            SetterType::Set => {self.data.insert(key, sp);}
            SetterType::Add => {
                // cannot use self.data.entry(key).or_insert(sp);
                // because of response NOT_STORED
                match self.data.get_mut(&key) {
                    Some(_) => {
                        return Err(From::from("NOT_STORED"));
                    }
                    None => {
                        self.data.insert(key, sp);
                    }
                }
            }
            SetterType::Replace => {
                // cannot use self.data.entry(key).and_modify(|e| *e = sp);
                // because of response NOT_STORED
                match self.data.get_mut(&key) {
                    Some(_) => {
                        self.data.insert(key, sp);
                    }
                    None => {
                        return Err(From::from("NOT_STORED"));
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
                        return Err(From::from("NOT_STORED"));
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
                        return Err(From::from("NOT_STORED"));
                    }
                }
            }
        }
        Ok(())
    }
}
