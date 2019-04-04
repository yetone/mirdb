use std::io::Seek;
use std::io::Write;

use serde::{Deserialize, Serialize};

use crate::block_handle::BlockHandle;
use std::io::SeekFrom;
use crate::MyResult;
use bincode::{deserialize_from, serialize};
use std::io::Cursor;
use std::io::Read;
use crate::reader;
use snap::Encoder;
use snap::Decoder;

#[derive(Serialize, Deserialize)]
pub struct MetaBlock {
    pub max_key: Vec<u8>,
    pub min_key: Vec<u8>,
}

impl MetaBlock {
    pub fn new(max_key: Vec<u8>, min_key: Vec<u8>) -> Self {
        MetaBlock {
            max_key,
            min_key
        }
    }

    pub fn new_with_buffer<T: Into<Vec<u8>>>(buffer: T) -> MyResult<Self> {
        Ok(deserialize_from(Cursor::new(buffer.into()))?)
    }

    pub fn new_from_location<T: Seek + Read>(r: &mut T, location: &BlockHandle) -> MyResult<(MetaBlock, usize)> {
        let (data, offset) = reader::read_bytes(r, location)?;
        let data = Decoder::new().decompress_vec(&data)?;
        let size = data.len();
        Ok((MetaBlock::new_with_buffer(data)?, offset + size))
    }

    pub fn flush<T: Write + Seek>(&mut self, w: &mut T, offset: usize) -> MyResult<BlockHandle> {
        let buf = serialize(self)?;
        let mut encoder = Encoder::new();
        let buf = encoder.compress_vec(&buf)?;
        w.seek(SeekFrom::Start(offset as u64))?;
        let size = w.write(&buf)?;
        Ok(bh!(offset, size))
    }
}