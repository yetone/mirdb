use std::io::Write;
use crate::error::MyResult;
use crate::utils::to_str;

#[derive(Debug, PartialEq)]
pub struct GetRespItem<'a> {
    pub(crate) key: &'a [u8],
    pub(crate) data: Vec<u8>,
    pub(crate) flags: u32,
    pub(crate) bytes: usize,
}

impl<'a> GetRespItem<'a> {
    pub fn new(key: &'a [u8], data: Vec<u8>, flags: u32, bytes: usize) -> Self {
        GetRespItem { key, data, flags, bytes }
    }
}

#[derive(Debug, PartialEq)]
pub enum Response<'a> {
    Stored,
    NotStored,
    Exists,
    NotFound,
    Get(Vec<GetRespItem<'a>>),
    Gets(Vec<GetRespItem<'a>>),
    Deleted,
    Touched,
    Ok,
    Busy(&'a [u8]),
    Badclass(&'a [u8]),
    Nospare(&'a [u8]),
    Notfull(&'a [u8]),
    Unsafe(&'a [u8]),
    Same(&'a [u8]),
    Error,
    ClientError(&'a str),
    ServerError(&'a str),
}

impl<'a> Response<'a> {
    pub fn write(&self, writer: &mut Write) -> MyResult<()> {
        match self {
            Response::Stored => {
                writer.write(b"STORED\r\n")?;
            }
            Response::NotStored => {
                writer.write(b"NOT_STORED\r\n")?;
            }
            Response::Exists => {
                writer.write(b"EXISTS\r\n")?;
            }
            Response::NotFound => {
                writer.write(b"NOT_FOUND\r\n")?;
            }
            Response::Gets(v) | Response::Get(v) => {
                for GetRespItem{ key, data, flags, bytes } in v {
                    writer.write(format!(
                        "VALUE {} {} {}\r\n",
                        to_str(key), flags, bytes
                    ).as_bytes())?;
                    writer.write(&data[..])?;
                    writer.write(b"\r\n")?;
                }
                writer.write(b"END\r\n")?;
            }
            Response::Deleted => {
                writer.write(b"DELETED\r\n")?;
            }
            Response::Touched => {
                writer.write(b"TOUCHED\r\n")?;
            }
            Response::Ok => {
                writer.write(b"OK\r\n")?;
            }
            Response::Error => {
                writer.write(b"ERROR\r\n")?;
            }
            Response::ClientError(e) => {
                writer.write(format!("CLIENT_ERROR {}\r\n", e).as_bytes())?;
            }
            Response::ServerError(e) => {
                writer.write(format!("SERVER_ERROR {}\r\n", e).as_bytes())?;
            }
            _ => {
                unimplemented!();
            }
        }
        Ok(())
    }
}
