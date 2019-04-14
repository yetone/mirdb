use integer_encoding::FixedInt;

use crate::block_handle::BlockHandle;
use crate::error::MyResult;
use crate::types::RandomAccess;

pub fn read_usize(r: &dyn RandomAccess, offset: usize) -> MyResult<(usize, usize)> {
    let mut buf = [0; 8];
    r.read_at(offset, &mut buf)?;
    let decoded = usize::decode_fixed(&mut buf);
    Ok((decoded, offset + buf.len()))
}

pub fn read_bytes(r: &dyn RandomAccess, location: &BlockHandle) -> MyResult<(Vec<u8>, usize)> {
    let mut buf = vec![0; location.size];
    let size = r.read_at(location.offset, &mut buf)?;
    Ok((buf, location.offset + size))
}
