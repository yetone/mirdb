pub type Key = Vec<u8>;
pub type Payload = Vec<u8>;

#[derive(Debug, PartialEq)]
pub enum SetterType {
    Set, Add, Replace, Append, Prepend
}

#[derive(Debug, PartialEq)]
pub enum GetterType {
    Get, Gets
}

#[derive(Debug, PartialEq)]
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
    },
    Deleter {
        key: Key,
    },
    Error(String),
}

#[derive(Debug, PartialEq)]
pub struct RequestConf {
    pub request: Request,
    pub noreply: bool,
}

#[macro_export]
macro_rules! cc {
    ($r:expr, $n:expr) => {{
        use $crate::request::RequestConf;
        RequestConf {
            request: $r,
            noreply: $n,
        }
    }};
    ($c:expr) => {{
        cc!($c, false)
    }}
}
