use std::time::{SystemTime, UNIX_EPOCH};
use std::collections::HashMap;

pub type StoreKey = Vec<u8>;

#[allow(dead_code)]
pub struct StorePayload {
    data: Vec<u8>,
    flags: u32,
    ttl: u32,
    bytes: u32,
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

    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        match self.data.get(key) {
            Some(p) => {
                if p.created_at + p.ttl as u64 <= SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() {
                    return None;
                }
                Some(&p.data[..])
            },
            _ => None
        }
    }

    pub fn set(&mut self, key: &[u8], flags: u32, ttl: u32, bytes: u32, noreply: bool, payload: &[u8]) {
        self.data.insert(key.to_vec(), StorePayload {
            flags, ttl, bytes, noreply,
            data: payload[..bytes as usize].to_vec(),
            created_at: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
        });
    }
}
