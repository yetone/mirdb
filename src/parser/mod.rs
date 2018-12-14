#[macro_use]
pub mod command;
#[macro_use]
pub mod macros;

use self::macros::*;
pub use self::command::{Command, CommandConf, SetterType, GetterType};

gen_parser!(key_parser<&[u8], &[u8]>, is_not!(b" \t\r\n\0"));

gen_parser!(getter_name_parser<&[u8]>,
            alt!(
                tag!(b"gets") |
                tag!(b"get")
            )
);

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
          getter: getter_name_parser >>
          space >>
          keys: split!(space, key_parser) >>
          tag!(b"\r\n") >>
          (
              cc!(Command::Getter{
                  getter: to_getter_type(getter),
                  keys,
              })
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

fn to_getter_type(x: &[u8]) -> GetterType {
    match x {
        b"get" => GetterType::Get,
        b"gets" => GetterType::Gets,
        _ => panic!(format!("unknown getter {:?}", x))
    }
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
          space >>
          key: key_parser >>
          space >>
          flags: u32_parser >>
          space >>
          ttl: u32_parser >>
          space >>
          bytes: usize_parser >>
          opt!(space) >>
          noreply: opt!(tag!(b"noreply")) >>
          tag!(b"\r\n") >>
          payload: take_at_least!(bytes, b"\r\n") >>
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
        IRResult::Err(e) => cc!(Command::Error(if e != "" { e } else { "ERROR" }))
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        use super::parse;
        use super::command::{Command, SetterType, GetterType};
        assert_eq!(parse(b"get abc\r\n"), cc!(Command::Getter {
            getter: GetterType::Get,
            keys: vec![b"abc"],
        }));
        assert_eq!(parse(b"gets abc\r\n"), cc!(Command::Getter {
            getter: GetterType::Gets,
            keys: vec![b"abc"],
        }));
        assert_eq!(parse(b"get  abc\r\n"), cc!(Command::Getter {
            getter: GetterType::Get,
            keys: vec![b"abc"],
        }));
        assert_eq!(parse(b"get abc def\r\n"), cc!(Command::Getter {
            getter: GetterType::Get,
            keys: vec![b"abc", b"def"],
        }));
        assert_eq!(parse(b"get    abc  def   ghi\r\n"), cc!(Command::Getter {
            getter: GetterType::Get,
            keys: vec![b"abc", b"def", b"ghi"],
        }));
        assert_eq!(parse(b"gets    abc  def   ghi\r\n"), cc!(Command::Getter {
            getter: GetterType::Gets,
            keys: vec![b"abc", b"def", b"ghi"],
        }));
        assert_eq!(parse(b"set abc 1 0 7\r\n"), cc!(Command::Incomplete));
        assert_eq!(parse(b"set abc   1 0 7\r\n"), cc!(Command::Incomplete));
        assert_eq!(parse(b"set abc 1 0 7\r\n\"a b c\"\r\n"), cc!(Command::Setter {
            setter: SetterType::Set,
            key: b"abc",
            flags: 1,
            ttl: 0,
            bytes: 7,
            payload: b"\"a b c\"",
        }));
        assert_eq!(parse(b"set    abc    1 0 7\r\n\"a b c\"\r\n"), cc!(Command::Setter {
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
        assert_eq!(parse(b"set abc 1 0 6\r\nabcd\r\n\r\n"), cc!(Command::Setter {
            setter: SetterType::Set,
            key: b"abc",
            flags: 1,
            ttl: 0,
            bytes: 6,
            payload: b"abcd\r\n",
        }));
        assert_eq!(parse(b"add abc 1 0 6\r\nabcd\r\n\r\n"), cc!(Command::Setter {
            setter: SetterType::Add,
            key: b"abc",
            flags: 1,
            ttl: 0,
            bytes: 6,
            payload: b"abcd\r\n",
        }));
    }
}
