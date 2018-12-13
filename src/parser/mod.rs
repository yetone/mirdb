pub mod command;
#[macro_use]
pub mod macros;

use self::macros::*;
pub use self::command::Command;

macro_rules! try_cmd {
    ($e:expr, $err:expr) => {{
        use $crate::parser::command::Command;
        match $e {
            Ok(v) => v,
            _ => return Command::Error($err),
        }
    }}
}

gen_parser!(key_parser<&[u8], &[u8]>, is_not!(b" \t\r\n\0"));

gen_parser!(getter<Command>,
      chain!(
          tag!(b"get") >>
              tag!(b" ") >>
              key: key_parser >>
              tag!(b"\r\n") >>
              ({
                  Command::Getter{ key }
              })
      )
);

fn u32_parser(i: &[u8]) -> IRResult<u32> {
    digit::<u32>(i)
}

fn usize_parser(i: &[u8]) -> IRResult<usize> {
    digit::<usize>(i)
}

gen_parser!(setter<Command>,
      chain!(
          tag!(b"set") >>
              tag!(b" ") >>
              key: key_parser >>
              tag!(b" ") >>
              flags: u32_parser >>
              tag!(b" ") >>
              ttl: u32_parser >>
              tag!(b" ") >>
              bytes: usize_parser >>
              opt!(tag!(b" ")) >>
              noreply: opt!(tag!(b"noreply")) >>
              tag!(b"\r\n") >>
              payload: take!(bytes) >>
              tag!(b"\r\n") >>
              (
                  Command::Setter{
                      key,
                      flags,
                      ttl,
                      bytes,
                      noreply: if let Some(_) = noreply { true } else { false },
                      payload: payload
                  }
              )
      )
);

gen_parser!(_parse<Command>, alt!(
    getter | setter
));

pub fn parse<'a>(i: &'a [u8]) -> Command<'a> {
    match _parse(i) {
        IRResult::Ok((_, o)) => o,
        IRResult::Incomplete(_) => Command::Incomplete,
        IRResult::Err(_) => Command::Error("ERROR")
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        use super::parse;
        use super::command::Command;
        assert_eq!(parse(b"get abc\r\n"), Command::Getter {
            key: b"abc",
        });
        assert_eq!(parse(b"set abc 1 0 7\r\n\"a b c\"\r\n"), Command::Setter {
            key: b"abc",
            flags: 1,
            ttl: 0,
            bytes: 7,
            noreply: false,
            payload: b"\"a b c\"",
        });
        assert_eq!(parse(b"set abc 1 0 7 noreply\r\n\"a b c\"\r\n"), Command::Setter {
            key: b"abc",
            flags: 1,
            ttl: 0,
            bytes: 7,
            noreply: true,
            payload: b"\"a b c\"",
        });
    }
}
