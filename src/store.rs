use std::time::{SystemTime, UNIX_EPOCH};
use std::collections::HashMap;
use std::error::Error;
use std::convert::From;

use crate::parser::command::SetterType;

pub type StoreKey = Vec<u8>;

#[allow(dead_code)]
pub struct StorePayload {
    pub data: Vec<u8>,
    pub flags: u32,
    ttl: u32,
    pub bytes: usize,
    noreply: bool,
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

    pub fn set(&mut self, setter: SetterType, key: &[u8], flags: u32, ttl: u32, bytes: usize, noreply: bool, payload: &[u8]) -> Result<(), Box<dyn Error>> {
        if payload.len() > bytes {
            return Err(From::from("CLIENT_ERROR bad data chunk"));
        }
        let data = payload[..bytes as usize].to_vec();
        let created_at = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let sp = StorePayload {
            flags, ttl, bytes, noreply,
            data, created_at,
        };
        match setter {
            SetterType::Set => {self.data.insert(key.to_vec(), sp);},
            SetterType::Add => {self.data.entry(key.to_vec()).or_insert(sp);},
            _ => ()
        }
        Ok(())
    }
}
