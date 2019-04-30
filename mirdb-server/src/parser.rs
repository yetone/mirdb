use crate::parser_util::macros::{digit, space, u32_parser, usize_parser, IRResult};
use crate::request::GetterType;
use crate::request::Request;
use crate::request::SetterType;
use crate::slice::Slice;

gen_parser!(key_parser<&[u8], &[u8]>, is_not!(b" \t\r\n\0"));

gen_parser!(
    getter_name_parser<&[u8]>,
    alt!(tag!(b"gets") | tag!(b"get"))
);

gen_parser!(
    setter_name_parser<&[u8]>,
    alt!(tag!(b"set") | tag!(b"add") | tag!(b"replace") | tag!(b"append") | tag!(b"prepend"))
);

gen_parser!(
    getter<Request>,
    chain!(
        getter: getter_name_parser
            >> space
            >> keys: split!(space, key_parser)
            >> tag!(b"\r\n")
            >> (Request::Getter {
                getter: to_getter_type(getter),
                keys: keys.into_iter().map(Slice::from).collect(),
            })
    )
);

fn unwrap_no_reply(x: Option<&[u8]>) -> bool {
    x.is_some()
}

fn to_getter_type(x: &[u8]) -> GetterType {
    match x {
        b"get" => GetterType::Get,
        b"gets" => GetterType::Gets,
        _ => panic!(format!("unknown getter {:?}", x)),
    }
}

fn to_setter_type(x: &[u8]) -> SetterType {
    match x {
        b"set" => SetterType::Set,
        b"add" => SetterType::Add,
        b"replace" => SetterType::Replace,
        b"append" => SetterType::Append,
        b"prepend" => SetterType::Prepend,
        _ => panic!(format!("unknown setter {:?}", x)),
    }
}

gen_parser!(
    setter<Request>,
    chain!(
        setter: setter_name_parser
            >> space
            >> key: key_parser
            >> space
            >> flags: u32_parser
            >> space
            >> ttl: u32_parser
            >> space
            >> bytes: usize_parser
            >> opt!(space)
            >> no_reply: opt!(tag!(b"noreply"))
            >> tag!(b"\r\n")
            >> payload: take_at_least!(bytes, b"\r\n")
            >> tag!(b"\r\n")
            >> (Request::Setter {
                setter: to_setter_type(setter),
                key: Slice::from(key),
                flags,
                ttl,
                bytes,
                payload: Slice::from(payload),
                no_reply: unwrap_no_reply(no_reply),
            })
    )
);

gen_parser!(
    deleter<Request>,
    chain!(
        tag!(b"delete")
            >> space
            >> key: key_parser
            >> opt!(space)
            >> no_reply: opt!(tag!(b"noreply"))
            >> tag!(b"\r\n")
            >> (Request::Deleter {
                key: Slice::from(key),
                no_reply: unwrap_no_reply(no_reply),
            })
    )
);

gen_parser!(
    info<Request>,
    chain!(tag!(b"info") >> tag!(b"\r\n") >> (Request::Info))
);

gen_parser!(
    major_compaction<Request>,
    chain!(tag!(b"major_compaction") >> tag!(b"\r\n") >> (Request::MajorCompaction))
);

gen_parser!(
    parse<Request>,
    alt!(getter | setter | deleter | info | major_compaction)
);

#[cfg(test)]
mod test {
    use crate::request::{GetterType, Request, SetterType};

    use super::*;

    #[test]
    fn test() {
        assert_eq!(
            parse(b"get abc\r\nget"),
            IRResult::Ok((
                "get".as_bytes(),
                Request::Getter {
                    getter: GetterType::Get,
                    keys: vec![Slice::from("abc")],
                }
            ))
        );
        assert_eq!(
            parse(b"get abc\r\n"),
            IRResult::Ok((
                "".as_bytes(),
                Request::Getter {
                    getter: GetterType::Get,
                    keys: vec![Slice::from("abc")],
                }
            ))
        );
        assert_eq!(
            parse(b"gets abc\r\n"),
            IRResult::Ok((
                "".as_bytes(),
                Request::Getter {
                    getter: GetterType::Gets,
                    keys: vec![Slice::from("abc")],
                }
            ))
        );
        assert_eq!(
            parse(b"get  abc\r\n"),
            IRResult::Ok((
                "".as_bytes(),
                Request::Getter {
                    getter: GetterType::Get,
                    keys: vec![Slice::from("abc")],
                }
            ))
        );
        assert_eq!(
            parse(b"get abc def\r\n"),
            IRResult::Ok((
                "".as_bytes(),
                Request::Getter {
                    getter: GetterType::Get,
                    keys: vec![Slice::from("abc"), Slice::from("def")],
                }
            ))
        );
        assert_eq!(
            parse(b"get    abc  def   ghi\r\n"),
            IRResult::Ok((
                "".as_bytes(),
                Request::Getter {
                    getter: GetterType::Get,
                    keys: vec![Slice::from("abc"), Slice::from("def"), Slice::from("ghi")],
                }
            ))
        );
        assert_eq!(
            parse(b"gets    abc  def   ghi\r\n"),
            IRResult::Ok((
                "".as_bytes(),
                Request::Getter {
                    getter: GetterType::Gets,
                    keys: vec![Slice::from("abc"), Slice::from("def"), Slice::from("ghi")],
                }
            ))
        );
        assert_eq!(parse(b"set abc 1 0 7\r\n"), IRResult::Incomplete(7));
        assert_eq!(parse(b"set abc   1 0 7\r\n"), IRResult::Incomplete(7));
        assert_eq!(parse(b"set abc   1 0 7\r\na"), IRResult::Incomplete(6));
        assert_eq!(
            parse(b"set abc 1 0 7\r\n\"a b c\"\r\n"),
            IRResult::Ok((
                "".as_bytes(),
                Request::Setter {
                    setter: SetterType::Set,
                    key: Slice::from("abc"),
                    flags: 1,
                    ttl: 0,
                    bytes: 7,
                    payload: Slice::from("\"a b c\""),
                    no_reply: false,
                }
            ))
        );
        assert_eq!(
            parse(b"set    abc    1 0 7\r\n\"a b c\"\r\n"),
            IRResult::Ok((
                "".as_bytes(),
                Request::Setter {
                    setter: SetterType::Set,
                    key: Slice::from("abc"),
                    flags: 1,
                    ttl: 0,
                    bytes: 7,
                    payload: Slice::from("\"a b c\""),
                    no_reply: false,
                }
            ))
        );
        assert_eq!(
            parse(b"set abc 1 0 7 noreply\r\n\"a b c\"\r\n"),
            IRResult::Ok((
                "".as_bytes(),
                Request::Setter {
                    setter: SetterType::Set,
                    key: Slice::from("abc"),
                    flags: 1,
                    ttl: 0,
                    bytes: 7,
                    payload: Slice::from("\"a b c\""),
                    no_reply: true,
                }
            ))
        );
        assert_eq!(
            parse(b"set abc 1 0 6\r\nabcd\r\n\r\n"),
            IRResult::Ok((
                "".as_bytes(),
                Request::Setter {
                    setter: SetterType::Set,
                    key: Slice::from("abc"),
                    flags: 1,
                    ttl: 0,
                    bytes: 6,
                    payload: Slice::from("abcd\r\n"),
                    no_reply: false,
                }
            ))
        );
        assert_eq!(
            parse(b"add abc 1 0 6\r\nabcd\r\n\r\n"),
            IRResult::Ok((
                "".as_bytes(),
                Request::Setter {
                    setter: SetterType::Add,
                    key: Slice::from("abc"),
                    flags: 1,
                    ttl: 0,
                    bytes: 6,
                    payload: Slice::from("abcd\r\n"),
                    no_reply: false,
                }
            ))
        );
        assert_eq!(
            parse(b"delete abc\r\n"),
            IRResult::Ok((
                "".as_bytes(),
                Request::Deleter {
                    key: Slice::from("abc"),
                    no_reply: false,
                }
            ))
        );
        assert_eq!(
            parse(b"delete abc noreply\r\n"),
            IRResult::Ok((
                "".as_bytes(),
                Request::Deleter {
                    key: Slice::from("abc"),
                    no_reply: true,
                }
            ))
        );
    }
}
