use std::fmt;
use std::io::ErrorKind;

use snap::Error as SnapError;

#[derive(Debug, PartialEq)]
pub enum StatusCode {
    IOError,
    NotFound,
    NotSupport,
    Other,
    SstableError(sstable::StatusCode),
    BincodeError,
    PatternError(usize),
    WALError,
    ChecksumError,
    SnapError,
    ConfigError,
    WebServerError,
}

#[derive(Debug, PartialEq)]
pub struct Status {
    pub code: StatusCode,
    pub msg: String,
}

impl Status {
    pub fn new(code: StatusCode, msg: &str) -> Self {
        let msg = if msg.is_empty() {
            format!("{:?}", code)
        } else {
            format!("{:?}: {}", code, msg)
        };
        Status { code, msg }
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl std::error::Error for Status {}

impl From<sstable::Status> for Status {
    fn from(e: sstable::Status) -> Self {
        Status::new(StatusCode::SstableError(e.code), &e.msg)
    }
}

impl From<bincode::Error> for Status {
    fn from(e: bincode::Error) -> Self {
        Status::new(StatusCode::BincodeError, &e.to_string())
    }
}

impl From<::std::io::Error> for Status {
    fn from(e: ::std::io::Error) -> Self {
        match e.kind() {
            ErrorKind::NotFound => Status::new(StatusCode::NotFound, &e.to_string()),
            _ => Status::new(StatusCode::IOError, &e.to_string()),
        }
    }
}

impl From<glob::PatternError> for Status {
    fn from(e: glob::PatternError) -> Self {
        Status::new(StatusCode::PatternError(e.pos), e.msg)
    }
}

impl From<Status> for ::std::io::Error {
    fn from(s: Status) -> Self {
        match s.code {
            StatusCode::NotFound => ::std::io::ErrorKind::NotFound.into(),
            _ => ::std::io::ErrorKind::Other.into(),
        }
    }
}

impl From<SnapError> for Status {
    fn from(e: SnapError) -> Self {
        Status::new(StatusCode::SnapError, &e.to_string())
    }
}

pub type MyResult<T> = ::std::result::Result<T, Status>;

macro_rules! err {
    ($code:expr, $msg:expr) => {
        Err($crate::error::Status::new($code, $msg))
    };
}

pub fn err<T: AsRef<str>, U>(code: StatusCode, msg: T) -> MyResult<U> {
    err!(code, msg.as_ref())
}
