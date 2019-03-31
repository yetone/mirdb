use crate::request::GetterType;
use crate::request::SetterType;
use crate::request::{Request, RequestConf};
use crate::parser_util::macros::{IRResult, space, digit};

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

gen_parser!(getter<RequestConf>,
      chain!(
          getter: getter_name_parser >>
          space >>
          keys: split!(space, key_parser) >>
          tag!(b"\r\n") >>
          (
              cc!(Request::Getter{
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

gen_parser!(setter<RequestConf>,
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
                  Request::Setter {
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

gen_parser!(deleter<RequestConf>,
            chain!(
                tag!(b"delete") >>
                space >>
                key: key_parser >>
                opt!(space) >>
                noreply: opt!(tag!(b"noreply")) >>
                tag!(b"\r\n") >>
                (
                    cc!(
                        Request::Deleter {
                            key
                        },
                        unwrap_noreply(noreply)
                    )
                )
            )
);

gen_parser!(_parse<RequestConf>, alt!(
    getter | setter | deleter
));

pub fn parse(i: &[u8]) -> (&[u8], RequestConf) {
    match _parse(i) {
        IRResult::Ok(r) => r,
        IRResult::Incomplete(_) => (b"", cc!(Request::Incomplete)),
        IRResult::Err(e) => (b"", cc!(Request::Error(if e != "" { e } else { "ERROR" })))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::request::{Request, SetterType, GetterType};

    #[test]
    fn test() {
        assert_eq!(parse(b"get abc\r\nget"), ("get".as_bytes(), cc!(Request::Getter {
            getter: GetterType::Get,
            keys: vec![b"abc"],
        })));
        assert_eq!(parse(b"get abc\r\n"), ("".as_bytes(), cc!(Request::Getter {
            getter: GetterType::Get,
            keys: vec![b"abc"],
        })));
        assert_eq!(parse(b"gets abc\r\n"), ("".as_bytes(), cc!(Request::Getter {
            getter: GetterType::Gets,
            keys: vec![b"abc"],
        })));
        assert_eq!(parse(b"get  abc\r\n"), ("".as_bytes(), cc!(Request::Getter {
            getter: GetterType::Get,
            keys: vec![b"abc"],
        })));
        assert_eq!(parse(b"get abc def\r\n"), ("".as_bytes(), cc!(Request::Getter {
            getter: GetterType::Get,
            keys: vec![b"abc", b"def"],
        })));
        assert_eq!(parse(b"get    abc  def   ghi\r\n"), ("".as_bytes(), cc!(Request::Getter {
            getter: GetterType::Get,
            keys: vec![b"abc", b"def", b"ghi"],
        })));
        assert_eq!(parse(b"gets    abc  def   ghi\r\n"), ("".as_bytes(), cc!(Request::Getter {
            getter: GetterType::Gets,
            keys: vec![b"abc", b"def", b"ghi"],
        })));
        assert_eq!(parse(b"set abc 1 0 7\r\n"), ("".as_bytes(), cc!(Request::Incomplete)));
        assert_eq!(parse(b"set abc   1 0 7\r\n"), ("".as_bytes(), cc!(Request::Incomplete)));
        assert_eq!(parse(b"set abc 1 0 7\r\n\"a b c\"\r\n"), ("".as_bytes(), cc!(Request::Setter {
            setter: SetterType::Set,
            key: b"abc",
            flags: 1,
            ttl: 0,
            bytes: 7,
            payload: b"\"a b c\"",
        })));
        assert_eq!(parse(b"set    abc    1 0 7\r\n\"a b c\"\r\n"), ("".as_bytes(), cc!(Request::Setter {
            setter: SetterType::Set,
            key: b"abc",
            flags: 1,
            ttl: 0,
            bytes: 7,
            payload: b"\"a b c\"",
        })));
        assert_eq!(parse(b"set abc 1 0 7 noreply\r\n\"a b c\"\r\n"), ("".as_bytes(), cc!(Request::Setter {
            setter: SetterType::Set,
            key: b"abc",
            flags: 1,
            ttl: 0,
            bytes: 7,
            payload: b"\"a b c\"",
        }, true)));
        assert_eq!(parse(b"set abc 1 0 6\r\nabcd\r\n\r\n"), ("".as_bytes(), cc!(Request::Setter {
            setter: SetterType::Set,
            key: b"abc",
            flags: 1,
            ttl: 0,
            bytes: 6,
            payload: b"abcd\r\n",
        })));
        assert_eq!(parse(b"add abc 1 0 6\r\nabcd\r\n\r\n"), ("".as_bytes(), cc!(Request::Setter {
            setter: SetterType::Add,
            key: b"abc",
            flags: 1,
            ttl: 0,
            bytes: 6,
            payload: b"abcd\r\n",
        })));
        assert_eq!(parse(b"delete abc\r\n"), ("".as_bytes(), cc!(Request::Deleter {
            key: b"abc"
        })));
        assert_eq!(parse(b"delete abc noreply\r\n"), ("".as_bytes(), cc!(Request::Deleter {
            key: b"abc"
        }, true)));
    }
}
