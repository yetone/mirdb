use std::error::Error;
use std::io;
use std::result;

use cuckoofilter::CuckooError;
use snap::Error as SnapError;

#[derive(Debug, PartialEq)]
pub enum StatusCode {
    NotFound,
    IOError,
    ChecksumError,
    SnapError,
    CompressError,
    InvalidData,
    BuildError,
    BincodeError,
    CuckooError,
}

#[derive(Debug, PartialEq)]
pub struct Status {
    pub code: StatusCode,
    pub msg: String,
}

impl Status {
    fn new(code: StatusCode, msg: &str) -> Self {
        let msg = if msg.is_empty() {
            format!("{:?}", code)
        } else {
            format!("{:?}: {}", code, msg)
        };
        Status { code, msg }
    }
}

impl From<io::Error> for Status {
    fn from(e: io::Error) -> Self {
        let code = match e.kind() {
            io::ErrorKind::NotFound => StatusCode::NotFound,
            _ => StatusCode::IOError,
        };
        Status::new(code, e.description())
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

impl From<bincode::Error> for Status {
    fn from(e: bincode::Error) -> Self {
        Status::new(StatusCode::BincodeError, e.description())
    }
}

impl From<CuckooError> for Status {
    fn from(e: CuckooError) -> Self {
        Status::new(StatusCode::CuckooError, e.description())
    }
}

pub type MyResult<T> = result::Result<T, Status>;

macro_rules! err {
    ($code:expr, $msg:expr) => {
        Err($crate::error::Status {
            code: $code,
            msg: $msg.to_string(),
        })
    };
}
