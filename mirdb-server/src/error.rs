use std::error::Error;
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

impl From<sstable::Status> for Status {
    fn from(e: sstable::Status) -> Self {
        Status::new(StatusCode::SstableError(e.code), &e.msg)
    }
}

impl From<bincode::Error> for Status {
    fn from(e: bincode::Error) -> Self {
        Status::new(StatusCode::BincodeError, e.description())
    }
}

impl From<::std::io::Error> for Status {
    fn from(e: ::std::io::Error) -> Self {
        match e.kind() {
            ErrorKind::NotFound => Status::new(StatusCode::NotFound, e.description()),
            _ => Status::new(StatusCode::IOError, e.description()),
        }
    }
}

impl From<glob::PatternError> for Status {
    fn from(e: glob::PatternError) -> Self {
        Status::new(StatusCode::PatternError(e.pos), e.msg)
    }
}

impl Into<::std::io::Error> for Status {
    fn into(self) -> ::std::io::Error {
        match self.code {
            StatusCode::NotFound => ::std::io::ErrorKind::NotFound.into(),
            _ => ::std::io::ErrorKind::Other.into(),
        }
    }
}

impl From<SnapError> for Status {
    fn from(e: SnapError) -> Self {
        let code = match e {
            SnapError::Checksum { .. } => StatusCode::ChecksumError,
            _ => StatusCode::SnapError,
        };
        Status::new(code, e.description())
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
