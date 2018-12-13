#[macro_use]
pub mod command;
#[macro_use]
pub mod macros;

use self::macros::*;
pub use self::command::{Command, CommandConf, SetterType};

gen_parser!(key_parser<&[u8], &[u8]>, is_not!(b" \t\r\n\0"));

gen_parser!(setter_name_parser<&[u8]>,
            alt!(
                tag!(b"set") |
                tag!(b"add") |
                tag!(b"replace") |
                tag!(b"append") |
                tag!(b"prepend")
            )
);

gen_parser!(getter<CommandConf>,
      chain!(
          tag!(b"get") >>
          tag!(b" ") >>
          key: key_parser >>
          tag!(b"\r\n") >>
          (
              cc!(Command::Getter{ key })
          )
      )
);

fn u32_parser(i: &[u8]) -> IRResult<u32> {
    digit::<u32>(i)
}

fn usize_parser(i: &[u8]) -> IRResult<usize> {
    digit::<usize>(i)
}

fn unwrap_noreply(x: Option<&[u8]>) -> bool {
    if let Some(_) = x { true } else { false }
}

fn to_setter_type(x: &[u8]) -> SetterType {
    match x {
        b"set" => SetterType::Set,
        b"add" => SetterType::Add,
        b"replace" => SetterType::Replace,
        b"append" => SetterType::Append,
        b"prepend" => SetterType::Prepend,
        _ => panic!(format!("unknown setter {:?}", x))
    }
}

gen_parser!(setter<CommandConf>,
      chain!(
          setter: setter_name_parser >>
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
              cc!(
                  Command::Setter {
                      setter: to_setter_type(setter),
                      key,
                      flags,
                      ttl,
                      bytes,
                      payload
                  },
                  unwrap_noreply(noreply)
              )
          )
      )
);

gen_parser!(_parse<CommandConf>, alt!(
    getter | setter
));

pub fn parse<'a>(i: &'a [u8]) -> CommandConf<'a> {
    match _parse(i) {
        IRResult::Ok((_, o)) => o,
        IRResult::Incomplete(_) => cc!(Command::Incomplete),
        IRResult::Err(_) => cc!(Command::Error("ERROR"))
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        use super::parse;
        use super::command::{Command, SetterType};
        assert_eq!(parse(b"get abc\r\n"), cc!(Command::Getter {
            key: b"abc",
        }));
        assert_eq!(parse(b"set abc 1 0 7\r\n\"a b c\"\r\n"), cc!(Command::Setter {
            setter: SetterType::Set,
            key: b"abc",
            flags: 1,
            ttl: 0,
            bytes: 7,
            payload: b"\"a b c\"",
        }));
        assert_eq!(parse(b"set abc 1 0 7 noreply\r\n\"a b c\"\r\n"), cc!(Command::Setter {
            setter: SetterType::Set,
            key: b"abc",
            flags: 1,
            ttl: 0,
            bytes: 7,
            payload: b"\"a b c\"",
        }, true));
    }
}
