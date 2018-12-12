pub type Key<'a> = &'a [u8];
pub type Payload<'a> = &'a [u8];

#[derive(Debug, PartialEq)]
pub enum Command<'a> {
    Getter {
        key: Key<'a>,
    },
    Setter {
        key: Key<'a>,
        flags: u32,
        ttl: u32,
        bytes: u32,
        noreply: bool,
        payload: Payload<'a>,
    },
    Error,
    Incomplete,
}
