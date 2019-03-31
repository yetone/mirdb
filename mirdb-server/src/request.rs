pub type Key<'a> = &'a [u8];
pub type Payload<'a> = &'a [u8];

#[derive(Debug, PartialEq)]
pub enum SetterType {
    Set, Add, Replace, Append, Prepend
}

#[derive(Debug, PartialEq)]
pub enum GetterType {
    Get, Gets
}

#[derive(Debug, PartialEq)]
pub enum Request<'a> {
    Getter {
        getter: GetterType,
        keys: Vec<Key<'a>>,
    },
    Setter {
        setter: SetterType,
        key: Key<'a>,
        flags: u32,
        ttl: u32,
        bytes: usize,
        payload: Payload<'a>,
    },
    Deleter {
        key: Key<'a>,
    },
    Error(&'a str),
    Incomplete,
}

#[derive(Debug, PartialEq)]
pub struct RequestConf<'a> {
    pub request: Request<'a>,
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
