use std::collections::HashMap;

pub struct Store {
    data: HashMap<Vec<u8>, Vec<u8>>
}

impl Store {
    pub fn new() -> Self {
        Store {
            data: HashMap::default()
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.data.get(key).map(|x| &x[..])
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) {
        self.data.insert(key.to_vec(), value.to_vec());
    }
}
