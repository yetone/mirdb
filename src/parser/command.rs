pub type Key<'a> = &'a [u8];
pub type Value<'a> = &'a [u8];

#[derive(Debug, PartialEq)]
pub enum Command<'a> {
    Getter {
        key: Key<'a>,
    },
    Setter {
        key: Key<'a>,
        value: Value<'a>,
    },
    Error,
    Incomplete,
}
