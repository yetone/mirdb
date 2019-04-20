use crate::slice::Slice;

pub type Key = Slice;
pub type Payload = Slice;

#[derive(Debug, Clone, PartialEq)]
pub enum SetterType {
    Set,
    Add,
    Replace,
    Append,
    Prepend,
}

#[derive(Debug, Clone, PartialEq)]
pub enum GetterType {
    Get,
    Gets,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Request {
    Getter {
        getter: GetterType,
        keys: Vec<Key>,
    },
    Setter {
        setter: SetterType,
        key: Key,
        flags: u32,
        ttl: u32,
        bytes: usize,
        payload: Payload,
        no_reply: bool,
    },
    Deleter {
        key: Key,
        no_reply: bool,
    },
    Info,
    Error,
    MajorCompaction,
}
