use bytes::{BufMut, BytesMut};

use crate::error::MyResult;
use crate::utils::to_str;

#[derive(Debug, PartialEq)]
pub struct GetRespItem {
    pub(crate) key: Vec<u8>,
    pub(crate) data: Vec<u8>,
    pub(crate) flags: u32,
    pub(crate) bytes: usize,
}

impl GetRespItem {
    pub fn new(key: Vec<u8>, data: Vec<u8>, flags: u32, bytes: usize) -> Self {
        GetRespItem { key, data, flags, bytes }
    }
}

#[derive(Debug, PartialEq)]
pub enum Response {
    Stored,
    NotStored,
    Exists,
    NotFound,
    Get(Vec<GetRespItem>),
    Gets(Vec<GetRespItem>),
    Deleted,
    Touched,
    Ok,
    Busy(Vec<u8>),
    Badclass(Vec<u8>),
    Nospare(Vec<u8>),
    Notfull(Vec<u8>),
    Unsafe(Vec<u8>),
    Same(Vec<u8>),
    Error,
    ClientError(String),
    ServerError(String),
    Info(String),
}

pub trait Writer {
    fn write(&mut self, data: &[u8]) -> MyResult<()>;
}

pub struct BufferWriter<'a> {
    buf: &'a mut BytesMut,
}

impl<'a> BufferWriter<'a> {
    pub fn new(buf: &'a mut BytesMut) -> BufferWriter<'a> {
        BufferWriter { buf }
    }
}

impl<'a> Writer for BufferWriter<'a> {
    fn write(&mut self, data: &[u8]) -> MyResult<()> {
        self.buf.reserve(data.len());
        unsafe {
            self.buf.bytes_mut()[..data.len()].copy_from_slice(data);
            self.buf.advance_mut(data.len());
        }
        Ok(())
    }
}

impl Response {
    pub fn write(&self, writer: &mut Writer) -> MyResult<()> {
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
            Response::Info(s) => {
                writer.write(format!("INFO\r\n\r\n{}\r\n\r\nEND\r\n", s).as_bytes())?;
            }
            _ => {
                unimplemented!();
            }
        }
        Ok(())
    }
}
