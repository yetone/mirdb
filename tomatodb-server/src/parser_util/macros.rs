#![allow(dead_code)]

use std::str::FromStr;
use std::str::from_utf8;
use std::fmt::Debug;

#[derive(Debug, PartialEq)]
pub enum IRResult<'a, 'b, T> {
    Ok((&'a [u8], T)),
    Err(&'b str),
    Incomplete(usize),
}

impl<'a, 'b, T: Debug> IRResult<'a, 'b, T> {
    fn unwrap(self) -> (&'a [u8], T) {
        if let IRResult::Ok(v) = self {
            return v;
        }
        panic!("not ok");
    }
}

#[derive(Debug, PartialEq)]
pub enum CompareResult {
    Ok,
    Err,
    Incomplete,
}

pub fn compare(i: &[u8], t: &[u8]) -> CompareResult {
    let pos = i.iter().zip(t.iter()).position(|(a, b)| a != b);
    match pos {
        Some(_) => CompareResult::Err,
        None => {
            if i.len() >= t.len() {
                CompareResult::Ok
            } else {
                CompareResult::Incomplete
            }
        }
    }
}

pub fn take_split(i: &[u8], count: usize) -> (&[u8], &[u8]) {
    let r = i.split_at(count);
    (r.1, r.0)
}

#[macro_export]
macro_rules! is_not {
    ($i:expr, $e:expr) => ({
        use $crate::parser_util::macros::{compare, take_split, CompareResult, IRResult};
        let pos = $i.iter().position(|x| $e.contains(x));
        match pos {
            None => IRResult::Ok(take_split($i, $i.len())),
            Some(v) if v > 0 => IRResult::Ok(take_split($i, v)),
            _ => IRResult::Err("")
        }
    })
}

#[macro_export]
macro_rules! tag {
    ($i:expr, $tag:expr) => ({
        use ::std::str::from_utf8;
        use $crate::parser_util::macros::{compare, take_split, CompareResult, IRResult};

        match compare($i, $tag) {
            CompareResult::Ok => {
                IRResult::Ok(take_split($i, $tag.len()))
            }
            CompareResult::Err => {
                match from_utf8($i) {
                    Ok(v) => IRResult::Err(v),
                    Err(_) => IRResult::Err("not a valid utf8")
                }
            }
            CompareResult::Incomplete => {
                IRResult::Incomplete($tag.len() - $i.len())
            }
        }
    })
}

#[macro_export]
macro_rules! take {
    ($i:expr, $size:expr) => ({
        use $crate::parser_util::macros::{take_split, IRResult};
        let l = $i.len();
        if l >= $size {
            IRResult::Ok(take_split($i, $size))
        } else {
            IRResult::Incomplete($size - l)
        }
    })
}

#[macro_export]
macro_rules! take_at_least {
    ($i:expr, $size:expr, $tag:expr) => ({
        use $crate::parser_util::macros::{take_split, IRResult};

        (|| {
            let l = $i.len();
            let tl = $tag.len();
            if l >= $size {
                let ni = &$i[$size..];
                let nl = ni.len();
                if nl > 0 && tl > 0 && nl >= tl {
                    let mut i = None;
                    let mut ii = 0;
                    while ii < nl - tl + 1 {
                        if &ni[ii..ii + tl] == $tag {
                            i = Some(ii);
                            break;
                        }
                        ii += 1;
                    }
                    if let Some(i) = i {
                        return IRResult::Ok(take_split($i, i + $size));
                    }
                }
                IRResult::Ok(take_split($i, l))
            } else {
                IRResult::Incomplete($size - l)
            }
        })()
    })
}

#[macro_export]
macro_rules! split {
    ($i:expr, $sep:ident, $fn:ident) => {{
        (|| {
            let mut res = vec![];
            let mut input = &$i[..];
            let mut i = 0;
            loop {
                if i % 2 == 0 {
                    match call!(input, $fn) {
                        IRResult::Incomplete(i) => return IRResult::Incomplete(i),
                        IRResult::Err(_) => break,
                        IRResult::Ok((i, o)) => {
                            input = i;
                            res.push(o);
                        }
                    };
                } else {
                    match call!(input, $sep) {
                        IRResult::Incomplete(i) => return IRResult::Incomplete(i),
                        IRResult::Err(_) => break,
                        IRResult::Ok((i, _)) => {
                            input = i
                        }
                    };
                }
                i += 1;
            }
            if res.len() > 0 {
                IRResult::Ok((input, res))
            } else {
                IRResult::Err("")
            }
        })()
    }}
}

#[macro_export]
macro_rules! call (
    ($i:expr, $fun:expr) => ($fun($i));
    ($i:expr, $fun:expr, $($args:expr),*) => ($fun($i, $($args),*));
);

#[macro_export]
macro_rules! chain {
    (@inner $i:expr, ($($rest:expr),*)) => ({
        use $crate::parser_util::macros::IRResult;
        IRResult::Ok(($i, ($($rest),*)))
    });
    (@inner $i:expr, $field:ident : $submac:ident!($($args:tt)*)) => (
        chain!(@inner $i, $mac!($($args)*))
    );
    (@inner $i:expr, $e:ident >> $($rest:tt)*) => (
        chain!(@inner $i, call!($e) >> $($rest)*);
    );
    (@inner $i:expr, $mac:ident!($($args:tt)*) >> $($rest:tt)*) => ({
        use $crate::parser_util::macros::IRResult;
        match $mac!($i, $($args)*) {
            IRResult::Err(e) => IRResult::Err(e),
            IRResult::Incomplete(n) => IRResult::Incomplete(n),
            IRResult::Ok((i, _)) => {
                chain!(@inner i, $($rest)*)
            }
        }
    });
    (@inner $i:expr, $field:ident : $e:ident >> $($rest:tt)*) => (
        chain!(@inner $i, $field: call!($e) >> $($rest)*);
    );
    (@inner $i:expr, $field:ident : $mac:ident!($($args:tt)*) >> $($rest:tt)*) => ({
        use $crate::parser_util::macros::IRResult;
        match $mac!($i, $($args)*) {
            IRResult::Err(e) => IRResult::Err(e),
            IRResult::Incomplete(n) => IRResult::Incomplete(n),
            IRResult::Ok((i, o)) => {
                let $field = o;
                chain!(@inner i, $($rest)*)
            }
        }
    });
    (@inner $i:expr, $e:ident >> ($($rest:tt)*)) => (
        chain!(@inner $i, call!($e) >> ($($rest)*));
    );
    (@inner $i:expr, $mac:ident!($($args:tt)*) >> ($($rest:tt)*)) => ({
        use $crate::parser_util::macros::IRResult;

        match $mac!($i, $($args)*) {
            IRResult::Err(e) => IRResult::Err(e),
            IRResult::Incomplete(n) => IRResult::Incomplete(n),
            IRResult::Ok((i, _)) => {
                chain!(@fin i, $($rest)*)
            },
        }
    });
    (@inner $i:expr, $field:ident : $e:ident >> ( $($rest:tt)* )) => (
        chain!(@inner $i, $field: call!($e) >> ( $($rest)* ) );
    );
    (@inner $i:expr, $field:ident : $mac:ident!( $($args:tt)* ) >> ( $($rest:tt)* )) => ({
        use $crate::parser_util::macros::IRResult;

        match $mac!($i, $($args)*) {
            IRResult::Err(e) => IRResult::Err(e),
            IRResult::Incomplete(n) => IRResult::Incomplete(n),
            IRResult::Ok((i, o)) => {
                let $field = o;
                chain!(@fin i, $($rest)*)
            },
        }
    });
    (@fin $i:expr, ($o:expr)) => ({
        use $crate::parser_util::macros::IRResult;
        IRResult::Ok(($i, $o))
    });
    (@fin $i:expr, ($($rest:tt)*)) => ({
        use $crate::parser_util::macros::IRResult;
        IRResult::Ok(($i, ($($rest)*)))
    });
    ($i:expr, $($rest:tt)*) => (
        chain!(@inner $i, $($rest)*)
    );
    ($e:ident! >> $($rest:tt)* ) => (
        chain!(call!($e) >> $($rest)*);
    );
}

#[macro_export]
macro_rules! gen_parser {
    ($name:ident<$ot:ty>, $mac:ident!($($args:tt)*)) => {
        gen_parser!($name<&[u8], $ot>, $mac!($($args)*));
    };
    ($name:ident<$it:ty, $ot:ty>, $mac:ident!($($args:tt)*)) => {
        pub fn $name(i: $it) -> IRResult<$ot> {
            $mac!(i, $($args)*)
        }
    }
}

#[macro_export]
macro_rules! opt {
    ($i:expr, $fn:ident) => {
        opt!($i, $fn())
    };
    ($i:expr, $fn:ident($($args:tt)*)) => {
        {
            use $crate::parser_util::macros::IRResult;
            match call!($i, $fn, $($args)*) {
                IRResult::Ok((i, o)) => IRResult::Ok((i, Some(o))),
                IRResult::Err(_) => IRResult::Ok(($i, None)),
                IRResult::Incomplete(i) => IRResult::Incomplete(i)
            }
        }
    };
    ($i:expr, $mac:ident!($($args:tt)*)) => {
        {
            use $crate::parser_util::macros::IRResult;
            match $mac!($i, $($args)*) {
                IRResult::Ok((i, o)) => IRResult::Ok((i, Some(o))),
                IRResult::Err(_) => IRResult::Ok(($i, None)),
                IRResult::Incomplete(i) => IRResult::Incomplete(i)
            }
        }
    }
}

#[macro_export]
macro_rules! alt {
    (@inner $i:expr, $e:path | $($rest:tt)*) => {
        alt!(@inner $i, call!($e) | $($rest)*)
    };
    (@inner $i:expr, $mac:ident!($($args:tt)*) | $($rest:tt)*) => {
        {
            use $crate::parser_util::macros::IRResult;
            match $mac!($i, $($args)*) {
                IRResult::Ok((i, o)) => IRResult::Ok((i, o)),
                IRResult::Incomplete(v) => IRResult::Incomplete(v),
                IRResult::Err(_) => alt!(@inner $i, $($rest)*)
            }
        }
    };
    (@inner $i:expr, @fin) => {{
        use $crate::parser_util::macros::IRResult;
        IRResult::Err("")
    }};
    ($i:expr, $($rest:tt)*) => {
        alt!(@inner $i, $($rest)* | @fin)
    }
}

#[inline]
pub fn is_alphabetic(chr: u8) -> bool {
    (chr >= 0x41 && chr <= 0x5A) || (chr >= 0x61 && chr <= 0x7A)
}

#[inline]
pub fn is_digit(chr: u8) -> bool {
    chr >= 0x30 && chr <= 0x39
}

#[inline]
pub fn is_space(chr: u8) -> bool {
    chr == 0x20
}

pub fn alpha(i: &[u8]) -> IRResult<&[u8]> {
    if i.len() == 0 {
        return IRResult::Err("");
    }
    let position = i.iter().position(|x| !is_alphabetic(*x));
    match position {
        None => IRResult::Ok(take_split(i, i.len())),
        Some(v) if v > 0 => IRResult::Ok(take_split(i, v)),
        _ => IRResult::Err(""),
    }
}

pub fn digit<T: FromStr>(i: &[u8]) -> IRResult<T> {
    if i.len() == 0 {
        return IRResult::Err("");
    }
    let position = i.iter().position(|x| !is_digit(*x));
    let (i, o) = match position {
        None => {
            take_split(i, i.len())
        },
        Some(v) if v > 0 => {
            take_split(i, v)
        },
        _ => return IRResult::Err("")
    };
    let s = match from_utf8(o) {
        Ok(v) => v,
        Err(_) => return IRResult::Err("not a valid utf8 input")
    };
    match FromStr::from_str(s) {
        Ok(v) => IRResult::Ok((i, v)),
        Err(_e) => IRResult::Err(""),
    }
}

pub fn space(i: &[u8]) -> IRResult<&[u8]> {
    if i.len() == 0 {
        return IRResult::Err("");
    }
    let position = i.iter().position(|x| !is_space(*x));
    match position {
        None => IRResult::Ok(take_split(i, i.len())),
        Some(v) if v > 0 => IRResult::Ok(take_split(i, v)),
        _ => IRResult::Err(""),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_is_not() {
        let r = is_not!(
            b"hello world",
            b" "
        ).unwrap();
        assert_eq!((" world".as_bytes(), "hello".as_bytes()), r);
    }
    #[test]
    fn test_tag() {
        let r = tag!(
            b"hello world",
            b"hello"
        ).unwrap();
        assert_eq!((" world".as_bytes(), "hello".as_bytes()), r);
        let r = tag!(
            b" world",
            b" "
        ).unwrap();
        assert_eq!(("world".as_bytes(), " ".as_bytes()), r);
    }
    #[test]
    fn test_take() {
        let r = take!(
            b"hello world",
            7
        ).unwrap();
        assert_eq!(("orld".as_bytes(), "hello w".as_bytes()), r);
    }
    #[test]
    fn test_take_incomplete() {
        let r = take!(
            b"hello world",
            12
        );
        assert_eq!(IRResult::Incomplete(1), r);
    }
    #[test]
    fn test_at_least() {
        let r = take_at_least!(
            b"hello world",
            3,
            b""
        ).unwrap();
        assert_eq!(("".as_bytes(), "hello world".as_bytes()), r);
        let r = take_at_least!(
            b"hello world",
            3,
            b"we"
        ).unwrap();
        assert_eq!(("".as_bytes(), "hello world".as_bytes()), r);
        let r = take_at_least!(
            b"hello world",
            3,
            b"wo"
        ).unwrap();
        assert_eq!(("world".as_bytes(), "hello ".as_bytes()), r);
        let r = take_at_least!(
            b"hello world! hello world",
            7,
            b"wo"
        ).unwrap();
        assert_eq!(("world".as_bytes(), "hello world! hello ".as_bytes()), r);
    }
    #[test]
    fn test_alpha() {
        let r = alpha(
            b"hello world"
        ).unwrap();
        assert_eq!((" world".as_bytes(), "hello".as_bytes()), r);
        let r = alpha(
            b" hello world"
        );
        assert_eq!(IRResult::Err(""), r);
        let r = alpha(
            b""
        );
        assert_eq!(IRResult::Err(""), r);
    }
    #[test]
    fn test_digit() {
        let r = digit::<i32>(
            b"123"
        ).unwrap();
        assert_eq!(("".as_bytes(), 123i32), r);
        let r = digit::<usize>(
            b"123x"
        ).unwrap();
        assert_eq!(("x".as_bytes(), 123usize), r);
        let r = digit::<usize>(
            b"x123"
        );
        assert_eq!(IRResult::Err(""), r);
        let r = digit::<usize>(
            b""
        );
        assert_eq!(IRResult::Err(""), r);
    }
    #[test]
    fn test_space() {
        let r = space(
            b" "
        ).unwrap();
        assert_eq!(("".as_bytes(), " ".as_bytes()), r);
        let r = space(
            b"   "
        ).unwrap();
        assert_eq!(("".as_bytes(), "   ".as_bytes()), r);
        let r = space(
            b"   x"
        ).unwrap();
        assert_eq!(("x".as_bytes(), "   ".as_bytes()), r);
        let r = space(
            b"x "
        );
        assert_eq!(IRResult::Err(""), r);
        let r = space(
            b""
        );
        assert_eq!(IRResult::Err(""), r);
    }
    #[test]
    fn test_chain() {
        let r = chain!(
            b"hello world",
            hello: alpha >>
                tag!(b" ") >>
                world: alpha >>
            (hello, world)
        ).unwrap().1;
        assert_eq!(("hello".as_bytes(), "world".as_bytes()), r);
    }
    #[test]
    fn test_chain_incomplete() {
        let r = chain!(
            b"hello world",
            hello: alpha >>
                tag!(b" ") >>
                world: alpha >>
                tag!(b" ") >>
                (hello, world)
        );
        assert_eq!(IRResult::Incomplete(1), r);
    }
    #[test]
    fn test_gen_parser() {
        gen_parser!(test<(&[u8], &[u8])>,
               chain!(
                   hello: alpha >>
                       tag!(b" ") >>
                       world: alpha >>
                       (hello, world)
               )
        );
        assert_eq!(IRResult::Ok(("".as_bytes(), ("hello".as_bytes(), "world".as_bytes()))), test(b"hello world"));
        gen_parser!(getter_name<&[u8]>,
                    alt!(
                        tag!(b"gets") |
                        tag!(b"get")
                    )
        );
        assert_eq!(IRResult::Ok((" hello world".as_bytes(), "get".as_bytes())), getter_name(b"get hello world"));
        assert_eq!(IRResult::Ok((" hello world".as_bytes(), "gets".as_bytes())), getter_name(b"gets hello world"));
    }
    #[test]
    fn test_opt() {
        let r = opt!(
            b"hello world",
            tag!(b"hello")
        );
        assert_eq!(IRResult::Ok((" world".as_bytes(), Some("hello".as_bytes()))), r);
        let r = opt!(
            b"hello world",
            tag!(b"xixi")
        );
        assert_eq!(IRResult::Ok(("hello world".as_bytes(), None)), r);
        let r = opt!(
            b"hello",
            tag!(b"hello world")
        );
        assert_eq!(IRResult::Incomplete(6), r);
    }
    #[test]
    fn test_alt() {
        fn custom_parser(_i: &[u8]) -> IRResult<&[u8]> {
            IRResult::Err("")
        }

        let r = alt!(
            b"hello world",
            tag!(b"xixi") |
            tag!(b"hello") |
            tag!(b"h") |
            custom_parser
        );
        assert_eq!(IRResult::Ok((" world".as_bytes(), "hello".as_bytes())), r);

        let r = alt!(
            b"xixi hello world",
            tag!(b"xixi") |
            tag!(b"hello") |
            tag!(b"h") |
            custom_parser
        );
        assert_eq!(IRResult::Ok((" hello world".as_bytes(), "xixi".as_bytes())), r);

        let r = alt!(
            b"get hello world",
            tag!(b"get") |
            tag!(b"gets")
        );
        assert_eq!(IRResult::Ok((" hello world".as_bytes(), "get".as_bytes())), r);

        let r = alt!(
            b"gets hello world",
            tag!(b"get") |
            tag!(b"gets")
        );
        assert_eq!(IRResult::Ok(("s hello world".as_bytes(), "get".as_bytes())), r);
        let r = alt!(
            b"gets hello world",
            tag!(b"gets") |
            tag!(b"get")
        );
        assert_eq!(IRResult::Ok((" hello world".as_bytes(), "gets".as_bytes())), r);
    }
    #[test]
    fn test_split() {
        let r = split!(
            b"I love u",
            space,
            alpha
        );
        assert_eq!(IRResult::Ok(("".as_bytes(), vec!["I".as_bytes(), "love".as_bytes(), "u".as_bytes()])), r);
        let r = split!(
            b"I love u#",
            space,
            alpha
        );
        assert_eq!(IRResult::Ok(("#".as_bytes(), vec!["I".as_bytes(), "love".as_bytes(), "u".as_bytes()])), r);
        let r = split!(
            b"",
            space,
            alpha
        );
        assert_eq!(IRResult::Err(""), r);
        let r = split!(
            b" I love u",
            space,
            alpha
        );
        assert_eq!(IRResult::Err(""), r);
        let r = chain!(
            b" I love u",
            space >>
                keys: split!(space, alpha) >>
                (keys)
        ).unwrap();
        assert_eq!(("".as_bytes(), vec!["I".as_bytes(), "love".as_bytes(), "u".as_bytes()]), r);
        gen_parser!(getter_name<&[u8]>,
                    alt!(
                        tag!(b"gets") |
                        tag!(b"get")
                    )
        );
        let r = chain!(
            b"get I love u",
            getter: getter_name >>
            space >>
                keys: split!(space, alpha) >>
                (getter, keys)
        ).unwrap();
        assert_eq!(("".as_bytes(), ("get".as_bytes(), vec!["I".as_bytes(), "love".as_bytes(), "u".as_bytes()])), r);
        let r = chain!(
            b"gets I love u",
            getter: getter_name >>
                space >>
                keys: split!(space, alpha) >>
                (getter, keys)
        ).unwrap();
        assert_eq!(("".as_bytes(), ("gets".as_bytes(), vec!["I".as_bytes(), "love".as_bytes(), "u".as_bytes()])), r);
    }
}
