use std::borrow::Borrow;
use std::str;
use std::sync::RwLock;
use std::sync::RwLockReadGuard;
use std::sync::RwLockWriteGuard;

pub fn to_str<T: Borrow<[u8]>>(cs: &T) -> &str {
    str::from_utf8(cs.borrow()).expect("not a valid utf8")
}

pub fn make_file_name(num: usize, ext: &str) -> String {
    format!("{:08}.{}", num, ext)
}

pub fn read_lock<T>(l: &RwLock<T>) -> RwLockReadGuard<T> {
    match l.read() {
        Ok(v) => v,
        Err(poised) => poised.into_inner(),
    }
}

pub fn write_lock<T>(l: &RwLock<T>) -> RwLockWriteGuard<T> {
    match l.write() {
        Ok(v) => v,
        Err(poised) => poised.into_inner(),
    }
}
