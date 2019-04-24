use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;

use bincode::{deserialize, serialize};
use cuckoofilter::ExportedCuckooFilter;
use serde::{Deserialize, Serialize};
use snap::Decoder;
use snap::Encoder;

use crate::block_handle::BlockHandle;
use crate::reader;
use crate::types::RandomAccess;
use crate::MyResult;

#[derive(Serialize, Deserialize)]
pub struct MetaBlock {
    pub max_key: Vec<u8>,
    pub min_key: Vec<u8>,
    pub filter: ExportedCuckooFilter,
}

impl MetaBlock {
    pub fn new(max_key: Vec<u8>, min_key: Vec<u8>, filter: ExportedCuckooFilter) -> Self {
        MetaBlock {
            max_key,
            min_key,
            filter,
        }
    }

    pub fn reset(&mut self) {
        self.max_key = vec![];
        self.min_key = vec![];
        self.filter.values = vec![];
        self.filter.length = 0;
    }

    pub fn new_with_buffer<T: Into<Vec<u8>>>(buffer: T) -> MyResult<Self> {
        Ok(deserialize(&buffer.into())?)
    }

    pub fn new_from_location(
        r: &dyn RandomAccess,
        location: &BlockHandle,
    ) -> MyResult<(MetaBlock, usize)> {
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
        self.reset();
        Ok(bh!(offset, size))
    }
}
