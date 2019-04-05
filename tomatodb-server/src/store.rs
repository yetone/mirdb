use std::collections::HashMap;
use std::convert::From;
use std::error::Error;
use std::fs::create_dir_all;
use std::io::{Result, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use skip_list::SkipList;

use crate::data_manager::DataManager;
use crate::error::{MyResult, StatusCode};
use crate::options::Options;
use crate::request::{GetterType, Request, SetterType};
use crate::response::GetRespItem;
use crate::response::Response;

pub type StoreKey = Vec<u8>;

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct StorePayload {
    pub(crate) data: Vec<u8>,
    pub(crate) flags: u32,
    ttl: u32,
    pub(crate) bytes: usize,
    created_at: u64,
}

pub struct Store {
    opt: Options,
    data: DataManager<StoreKey, StorePayload>,
}

pub fn is_expire(p: &StorePayload) -> bool {
    p.created_at + p.ttl as u64 <= SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}

impl Store {
    pub fn new(opt: Options) -> MyResult<Self> {
        let path = Path::new(&opt.work_dir);
        if !path.exists() {
            create_dir_all(path)?;
        } else if !path.is_dir() {
            return err!(StatusCode::IOError, "work dir is not a dir");
        }
        Ok(Store {
            data: DataManager::new(opt.clone())?,
            opt,
        })
    }

    pub fn apply(&self, request: Request) -> MyResult<Response> {
        match request {
            Request::Getter{ getter, keys } => {
                let mut v = Vec::with_capacity(keys.len());
                for key in keys {
                    if let Some(p) = self.data.get(&key)? {
                        if !is_expire(&p) {
                            v.push(GetRespItem {
                                key,
                                data: p.data,
                                flags: p.flags,
                                bytes: p.bytes,
                            });
                        }
                    }
                }
                Ok(match getter {
                    GetterType::Get => Response::Get(v),
                    GetterType::Gets => Response::Gets(v),
                })
            }
            _ => err!(StatusCode::NotSupport, "not support")
        }
    }

    pub fn apply_mut(&mut self, request: Request) -> MyResult<Response> {
        match request {
            Request::Setter{ setter, key, flags, ttl, bytes, payload, .. } => {
                if payload.len() > bytes {
                    return Ok(Response::ClientError("bad data chunk".to_owned()));
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
                        self.data.insert(key, sp)?;
                    }
                    SetterType::Add => {
                        // Cannot use self.data.entry(key).or_insert(sp);
                        // because of the NOT_STORED response
                        if let None = self.data.get(&key)? {
                            self.data.insert(key, sp)?;
                        } else {
                            return Ok(Response::NotStored);
                        }
                    }
                    SetterType::Replace => {
                        // Cannot use self.data.entry(key).and_modify(|e| *e = sp);
                        // because of the NOT_STORED response
                        if let None = self.data.get(&key)? {
                            return Ok(Response::NotStored);
                        } else {
                            self.data.insert(key, sp)?;
                        }
                    }
                    SetterType::Append => {
                        if let Some(v) = self.data.get(&key)? {
                            let mut c = v.clone();
                            c.data.extend(sp.data);
                            c.ttl = sp.ttl;
                            c.created_at = sp.created_at;
                            c.bytes += sp.bytes;
                            c.flags = sp.flags;
                            self.data.insert(key, c)?;
                        } else {
                            return Ok(Response::NotStored);
                        }

                    }
                    SetterType::Prepend => {
                        if let Some(v) = self.data.get(&key)? {
                            let mut tmp: Vec<_> = sp.data.to_owned();
                            let mut c = v.clone();
                            tmp.extend(&v.data);
                            c.data = tmp;
                            c.ttl = sp.ttl;
                            c.created_at = sp.created_at;
                            c.bytes += sp.bytes;
                            c.flags = sp.flags;
                            self.data.insert(key, c)?;
                        } else {
                            return Ok(Response::NotStored);
                        }
                    }
                }
                Ok(Response::Stored)
            }
            Request::Deleter{ key, .. } => {
                match self.data.remove(&key)? {
                    Some(_) => Ok(Response::Deleted),
                    None => Ok(Response::NotFound),
                }
            }
            _ => err!(StatusCode::NotSupport, "not support")
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;
    use std::fs::create_dir_all;
    use std::fs::remove_dir_all;
    use std::path::Path;

    use rand::{Rng, thread_rng};
    use rand::distributions::Alphanumeric;

    use crate::utils::to_str;
    use crate::test_utils::get_test_opt;

    use super::*;

    #[test]
    fn test_get_none() {
        let opt = get_test_opt();
        let store = Store::new(opt).unwrap();
        let r = store.apply(Request::Getter { getter: GetterType::Get, keys: vec![b"a".to_vec()] });
        assert_eq!(Ok(Response::Get(vec![])), r);
    }

    #[test]
    fn test_get_some() {
        let opt = get_test_opt();
        let mut store = Store::new(opt).unwrap();
        let key = b"a".to_vec();
        let payload = b"abc".to_vec();
        let r = store.apply_mut(Request::Setter {
            setter: SetterType::Set,
            key: key.clone(),
            flags: 1,
            ttl: 10,
            payload: payload.clone(),
            bytes: payload.len(),
            no_reply: false,
        });
        assert!(r.is_ok(), "stored");
        let r = store.apply(Request::Getter { getter: GetterType::Get, keys: vec![key.clone()] });
        let bytes = payload.len();
        assert_eq!(Ok(Response::Get(vec!(GetRespItem {
            key,
            data: payload.to_vec(),
            flags: 1,
            bytes,
        }))), r);
    }

    #[test]
    fn test_set() {
        let opt = get_test_opt();
        let mut store = Store::new(opt).unwrap();
        let mut map = HashMap::new();
        map.insert(b"a".to_vec(), b"abc".to_vec());
        map.insert(b"b".to_vec(), b"bbc".to_vec());
        map.insert(b"c".to_vec(), b"cbc".to_vec());
        for (key, payload) in map.iter() {
            let r = store.apply_mut(Request::Setter {
                setter: SetterType::Set,
                key: key.clone(),
                flags: 1,
                ttl: 100,
                bytes: payload.len(),
                payload: payload.clone(),
                no_reply: false,
            });
            assert_eq!(Ok(Response::Stored), r);
        }
        for (key, payload) in map.iter() {
            let r = store.apply(Request::Getter { getter: GetterType::Get, keys: vec![key.clone()] });
            let bytes = payload.len();
            println!("get key: {}", to_str(key));
            assert_eq!(Ok(Response::Get(vec!(GetRespItem {
                key: key.clone(),
                data: payload.to_vec(),
                flags: 1,
                bytes,
            }))), r);
        }
        let mut deleted = HashSet::new();
        for (key, _payload) in map.iter() {
            let r = store.apply_mut(Request::Deleter { key: key.clone(), no_reply: false });
            assert_eq!(Ok(Response::Deleted), r);
            println!("delete key: {}", to_str(key));
            deleted.insert(key.clone());
            for (key, payload) in map.iter() {
                let r = store.apply(Request::Getter { getter: GetterType::Get, keys: vec![key.clone()] });
                println!("get key: {}", to_str(key));
                if deleted.contains(key) {
                    println!("empty");
                    assert_eq!(Ok(Response::Get(vec![])), r);
                } else {
                    println!("not empty");
                    let bytes = payload.len();
                    assert_eq!(Ok(Response::Get(vec!(GetRespItem {
                        key: key.clone(),
                        data: payload.to_vec(),
                        flags: 1,
                        bytes,
                    }))), r);
                }
            }
        }
    }

    #[test]
    fn test_set_err() {
        let opt = get_test_opt();
        let mut store = Store::new(opt).unwrap();
        let key = b"a".to_vec();
        let payload = b"abc".to_vec();
        let r = store.apply_mut(Request::Setter {
            setter: SetterType::Set,
            key,
            flags: 1,
            ttl: 10,
            bytes: payload.len() - 1,
            payload,
            no_reply: false,
        });
        assert_eq!(Ok(Response::ClientError("bad data chunk".to_owned())), r);
    }
}
