use std::io;

use bytes::buf::IntoBuf;
use bytes::BytesMut;
#[allow(deprecated)]
use tokio_io::codec::{Decoder, Encoder, Framed};
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_proto::pipeline::ServerProto;

use crate::error::{Status, StatusCode};
use crate::error::MyResult;
use crate::parser::parse;
use crate::parser_util::macros::IRResult;
use crate::request::RequestConf;
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
            Err(e) => Err(e.into())
        }
    }
}

impl Decoder for ServerCodec {
    type Item = RequestConf;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> io::Result<Option<RequestConf>> {
        let src_len = src.len();
        let (result, src_used) = match { parse(src) } {
            IRResult::Ok((remaining, req)) => {
                (Ok(Some(req)), src_len - remaining.len())
            },
            IRResult::Err(err) => {
                (err!(StatusCode::Other, err), 0)
            },
            IRResult::Incomplete(_) => {
                (Ok(None), 0)
            }
        };
        src.split_to(src_used);
        match result {
            Ok(x) => Ok(x),
            Err(e) => Err(e.into()),
        }
    }
}

pub struct Proto;

#[allow(deprecated)]
impl<T: AsyncWrite + AsyncRead + 'static> ServerProto<T> for Proto {
    type Request = RequestConf;
    type Response = Response;
    type Transport = Framed<T, ServerCodec>;
    type BindTransport = io::Result<Framed<T, ServerCodec>>;

    fn bind_transport(&self, io: T) -> Self::BindTransport {
        Ok(io.framed(ServerCodec))
    }
}
