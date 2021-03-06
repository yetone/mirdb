use std::collections::HashMap;
use std::convert::From;
use std::error::Error;
use std::fs::create_dir_all;
use std::io::{Result, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use skip_list::SkipList;

use crate::data_manager::DataManager;
use crate::error::{MyResult, StatusCode};
use crate::options::Options;
use crate::request::{GetterType, Request, SetterType};
use crate::response::GetRespItem;
use crate::response::Response;
use crate::slice::Slice;

pub type StoreKey = Slice;

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct StorePayload {
    pub(crate) data: Slice,
    pub(crate) flags: u32,
    ttl: u32,
    pub(crate) bytes: usize,
    created_at: u64,
}

impl StorePayload {
    pub fn new(data: Slice, flags: u32, ttl: u32, bytes: usize, created_at: u64) -> Self {
        Self {
            data,
            flags,
            ttl,
            bytes,
            created_at,
        }
    }

    pub fn is_expired(&self) -> bool {
        if self.ttl == 0 {
            return false;
        }
        self.created_at + u64::from(self.ttl)
            <= SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
    }
}

pub struct Store {
    opt: Options,
    data: Arc<DataManager>,
}

impl Store {
    pub fn new(opt: Options) -> MyResult<Self> {
        let path = Path::new(&opt.work_dir);
        if !path.exists() {
            create_dir_all(path)?;
        } else if !path.is_dir() {
            return err!(StatusCode::IOError, "work dir is not a dir");
        }
        let dm = DataManager::new(opt.clone())?;
        #[cfg(not(test))]
        {
            DataManager::background_thread(dm.clone());
        }
        Ok(Store { data: dm, opt })
    }

    pub fn apply(&self, request: Request) -> MyResult<Response> {
        match request {
            Request::Getter { getter, keys } => {
                let mut v = Vec::with_capacity(keys.len());
                for key in keys {
                    if let Some(p) = self.data.get(&key)? {
                        if !p.is_expired() {
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
            Request::Setter {
                setter,
                key,
                flags,
                ttl,
                bytes,
                payload,
                ..
            } => {
                if payload.len() > bytes {
                    return Ok(Response::ClientError("bad data chunk".to_owned()));
                }
                let data = Slice::from(&payload[..bytes as usize]);
                let created_at = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                let sp = StorePayload {
                    flags,
                    ttl,
                    bytes,
                    data,
                    created_at,
                };
                match setter {
                    SetterType::Set => {
                        self.data.insert(key, sp)?;
                    }
                    SetterType::Add => {
                        // Cannot use self.data.entry(key).or_insert(sp);
                        // because of the NOT_STORED response
                        if self.data.get(&key)?.is_none() {
                            self.data.insert(key, sp)?;
                        } else {
                            return Ok(Response::NotStored);
                        }
                    }
                    SetterType::Replace => {
                        // Cannot use self.data.entry(key).and_modify(|e| *e = sp);
                        // because of the NOT_STORED response
                        if self.data.get(&key)?.is_none() {
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
                            let mut tmp: Slice = sp.data.to_owned();
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
            Request::Deleter { key, .. } => match self.data.remove(&key)? {
                Some(_) => Ok(Response::Deleted),
                None => Ok(Response::NotFound),
            },
            Request::Info => Ok(Response::Info(self.data.info())),
            Request::Error => Ok(Response::Error),
            Request::MajorCompaction => {
                self.data.major_compaction()?;
                Ok(Response::Ok)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;
    use std::fs::create_dir_all;
    use std::fs::remove_dir_all;
    use std::path::Path;

    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};

    use crate::test_utils::get_test_opt;
    use crate::utils::to_str;

    use super::*;

    #[test]
    fn test_get_none() {
        let opt = get_test_opt();
        let store = Store::new(opt).unwrap();
        let r = store.apply(Request::Getter {
            getter: GetterType::Get,
            keys: vec![Slice::from("a")],
        });
        assert_eq!(Ok(Response::Get(vec![])), r);
    }

    #[test]
    fn test_get_some() {
        let opt = get_test_opt();
        let store = Store::new(opt).unwrap();
        let key = Slice::from("a");
        let payload = Slice::from("abc");
        let r = store.apply(Request::Setter {
            setter: SetterType::Set,
            key: key.clone(),
            flags: 1,
            ttl: 10,
            payload: payload.clone(),
            bytes: payload.len(),
            no_reply: false,
        });
        assert!(r.is_ok(), "stored");
        let r = store.apply(Request::Getter {
            getter: GetterType::Get,
            keys: vec![key.clone()],
        });
        let bytes = payload.len();
        assert_eq!(
            Ok(Response::Get(vec!(GetRespItem {
                key,
                data: payload,
                flags: 1,
                bytes,
            }))),
            r
        );
    }

    #[test]
    fn test_set() {
        let opt = get_test_opt();
        let store = Store::new(opt).unwrap();
        let mut map = HashMap::new();
        map.insert(b"a".to_vec(), b"abc".to_vec());
        map.insert(b"b".to_vec(), b"bbc".to_vec());
        map.insert(b"c".to_vec(), b"cbc".to_vec());
        for (key, payload) in map.iter() {
            let key = Slice::from(key.clone());
            let payload = Slice::from(payload.clone());
            let r = store.apply(Request::Setter {
                setter: SetterType::Set,
                key,
                flags: 1,
                ttl: 100,
                bytes: payload.len(),
                payload,
                no_reply: false,
            });
            assert_eq!(Ok(Response::Stored), r);
        }
        for (key, payload) in map.iter() {
            let key = Slice::from(key.clone());
            let payload = Slice::from(payload.clone());
            let r = store.apply(Request::Getter {
                getter: GetterType::Get,
                keys: vec![key.clone()],
            });
            let bytes = payload.len();
            println!("get key: {}", to_str(&key));
            assert_eq!(
                Ok(Response::Get(vec!(GetRespItem {
                    key,
                    data: payload,
                    flags: 1,
                    bytes,
                }))),
                r
            );
        }
        let mut deleted = HashSet::new();
        for (key, _payload) in map.iter() {
            let key = Slice::from(key.clone());
            let r = store.apply(Request::Deleter {
                key: key.clone(),
                no_reply: false,
            });
            assert_eq!(Ok(Response::Deleted), r);
            println!("delete key: {}", to_str(&key));
            deleted.insert(key.clone());
            for (key, payload) in map.iter() {
                let key = Slice::from(key.clone());
                let payload = Slice::from(payload.clone());
                let r = store.apply(Request::Getter {
                    getter: GetterType::Get,
                    keys: vec![key.clone()],
                });
                println!("get key: {}", to_str(&key));
                if deleted.contains(&key) {
                    println!("empty");
                    assert_eq!(Ok(Response::Get(vec![])), r);
                } else {
                    println!("not empty");
                    let bytes = payload.len();
                    assert_eq!(
                        Ok(Response::Get(vec!(GetRespItem {
                            key,
                            data: payload,
                            flags: 1,
                            bytes,
                        }))),
                        r
                    );
                }
            }
        }
    }

    #[test]
    fn test_set_err() {
        let opt = get_test_opt();
        let store = Store::new(opt).unwrap();
        let key = Slice::from("a");
        let payload = Slice::from("abc");
        let r = store.apply(Request::Setter {
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
