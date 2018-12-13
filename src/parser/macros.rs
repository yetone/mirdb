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

pub fn compare<'a, 'b>(i: &'a [u8], t: &'b [u8]) -> CompareResult {
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

pub fn take_split<'a>(i: &'a [u8], count: usize) -> (&'a [u8], &'a [u8]) {
    let r = i.split_at(count);
    (r.1, r.0)
}

#[macro_export]
macro_rules! is_not {
    ($i:expr, $e:expr) => ({
        use $crate::parser::macros::{compare, take_split, CompareResult, IRResult};
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
        use $crate::parser::macros::{compare, take_split, CompareResult, IRResult};

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
        use $crate::parser::macros::{take_split, IRResult};
        let l = $i.len();
        if l >= $size {
            IRResult::Ok(take_split($i, $size))
        } else {
            IRResult::Incomplete($size - l)
        }
    })
}

#[macro_export]
macro_rules! call (
    ($i:expr, $fun:expr) => ($fun($i));
    ($i:expr, $fun:expr, $($args:expr),*) => ($fun($i, $($args),*));
);

#[macro_export]
macro_rules! chain {
    (@inner $i:expr, ($($rest:expr),*)) => ({
        use $crate::parser::macros::IRResult;
        IRResult::Ok(($i, ($($rest),*)))
    });
    (@inner $i:expr, $field:ident : $submac:ident!($($args:tt)*)) => (
        chain!(@inner $i, $mac!($($args)*))
    );
    (@inner $i:expr, $e:ident >> $($rest:tt)*) => (
        chain!(@inner $i, call!($e) >> $($rest)*);
    );
    (@inner $i:expr, $mac:ident!($($args:tt)*) >> $($rest:tt)*) => ({
        use $crate::parser::macros::IRResult;
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
        use $crate::parser::macros::IRResult;
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
        use $crate::parser::macros::IRResult;

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
        use $crate::parser::macros::IRResult;

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
        use $crate::parser::macros::IRResult;
        IRResult::Ok(($i, $o))
    });
    (@fin $i:expr, ($($rest:tt)*)) => ({
        use $crate::parser::macros::IRResult;
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
        fn $name(i: $it) -> IRResult<$ot> {
            $mac!(i, $($args)*)
        }
    }
}

#[macro_export]
macro_rules! opt {
    ($i:expr, $mac:ident!($($args:tt)*)) => {
        {
            use $crate::parser::macros::IRResult;
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
            use $crate::parser::macros::IRResult;
            match $mac!($i, $($args)*) {
                IRResult::Ok((i, o)) => IRResult::Ok((i, o)),
                IRResult::Incomplete(v) => IRResult::Incomplete(v),
                IRResult::Err(_) => alt!(@inner $i, $($rest)*)
            }
        }
    };
    (@inner $i:expr, @fin) => {{
        use $crate::parser::macros::IRResult;
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

pub fn alpha<'a>(i: &'a [u8]) -> IRResult<&'a [u8]> {
    let position = i.iter().position(|x| !is_alphabetic(*x));
    match position {
        None => IRResult::Ok(take_split(i, i.len())),
        Some(v) if v > 0 => IRResult::Ok(take_split(i, v)),
        _ => IRResult::Err(""),
    }
}

pub fn digit<'a, T: FromStr>(i: &'a [u8]) -> IRResult<T> {
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
        Err(_e) => IRResult::Err("")
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
    fn test_alpha() {
        let r = alpha(
            b"hello world"
        ).unwrap();
        assert_eq!((" world".as_bytes(), "hello".as_bytes()), r);
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
    }
}