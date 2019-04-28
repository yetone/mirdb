use std::io;

use bytes::buf::IntoBuf;
use bytes::BytesMut;
#[allow(deprecated)]
use tokio_io::codec::{Decoder, Encoder, Framed};
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_proto::pipeline::ServerProto;

use crate::error::MyResult;
use crate::error::{Status, StatusCode};
use crate::parser::parse;
use crate::parser_util::macros::IRResult;
use crate::request::Request;
use crate::response::BufferWriter;
use crate::response::Response;

pub struct ServerCodec;

impl Encoder for ServerCodec {
    type Item = Response;
    type Error = io::Error;

    fn encode(&mut self, item: Response, dst: &mut BytesMut) -> io::Result<()> {
        let mut writer = BufferWriter::new(dst);
        match item.write(&mut writer) {
            Ok(_) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

impl Decoder for ServerCodec {
    type Item = Request;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> io::Result<Option<Request>> {
        let src_len = src.len();
        let (result, src_used) = match { parse(src) } {
            IRResult::Ok((remaining, req)) => (Ok(Some(req)), src_len - remaining.len()),
            IRResult::Err(_err) => (Ok(Some(Request::Error)), src_len),
            IRResult::Incomplete(_) => (Ok(None), 0),
        };
        src.split_to(src_used);
        match result {
            Ok(x) => Ok(x),
            e @ Err(_) => e.into(),
        }
    }
}

pub struct Proto;

#[allow(deprecated)]
impl<T: AsyncWrite + AsyncRead + 'static> ServerProto<T> for Proto {
    type Request = Request;
    type Response = Response;
    type Transport = Framed<T, ServerCodec>;
    type BindTransport = io::Result<Framed<T, ServerCodec>>;

    fn bind_transport(&self, io: T) -> Self::BindTransport {
        Ok(io.framed(ServerCodec))
    }
}
