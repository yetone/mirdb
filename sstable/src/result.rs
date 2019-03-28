use std::io;
use std::result;
use std::error::Error;

#[derive(Debug)]
pub enum StatusCode {
    NotFound,
    IOError,
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

impl From<io::Error> for Status {
    fn from(e: io::Error) -> Self {
        let code = match e.kind() {
            io::ErrorKind::NotFound => StatusCode::NotFound,
            _ => StatusCode::IOError,
        };
        Status::new(code, e.description())
    }
}

pub type MyResult<T> = result::Result<T, Status>;