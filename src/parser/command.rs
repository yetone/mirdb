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
pub enum Command<'a> {
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
    Error(&'a str),
    Incomplete,
}

#[derive(Debug, PartialEq)]
pub struct CommandConf<'a> {
    pub command: Command<'a>,
    pub noreply: bool,
}

#[macro_export]
macro_rules! cc {
    ($c:expr, $n:expr) => {{
        use $crate::parser::command::CommandConf;
        CommandConf {
            command: $c,
            noreply: $n,
        }
    }};
    ($c:expr) => {{
        use $crate::parser::command::CommandConf;
        CommandConf {
            command: $c,
            noreply: false,
        }
    }}
}
