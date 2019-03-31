use std::io::ErrorKind;
use std::error::Error;

#[derive(Debug)]
pub enum StatusCode {
    IOError,
    NotFound,
}

#[derive(Debug)]
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
        Status {
            code, msg
        }
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

pub type MyResult<T> = ::std::result::Result<T, Status>;

macro_rules! err {
    ($code:expr, $msg:expr) => {$crate::error::Status::new($code, $msg)};
}
