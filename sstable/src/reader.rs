use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use integer_encoding::FixedInt;

use crate::result::MyResult;
use crate::block_handle::BlockHandle;

pub fn read_usize<T: Seek + Read>(r: &mut T, offset: usize) -> MyResult<(usize, usize)> {
    let mut buf = [0; 8];
    r.seek(SeekFrom::Start(offset as u64))?;
    r.read_exact(&mut buf)?;
    let decoded = usize::decode_fixed(&mut buf);
    Ok((decoded, offset + buf.len()))
}

pub fn read_bytes<T: Seek + Read>(r: &mut T, location: &BlockHandle) -> MyResult<(Vec<u8>, usize)> {
    let mut buf = [0; 512];
    let mut content = Vec::with_capacity(location.size);
    r.seek(SeekFrom::Start(location.offset as u64))?;
    while content.len() < location.size {
        let remain = location.size - content.len();
        let size = r.read(&mut buf)?;
        if size == 0 {
            break;
        }
        if size > remain {
            content.extend_from_slice(&buf[..remain]);
        } else {
            content.extend_from_slice(&buf[..size]);
        }
    }
    let len = content.len();
    Ok((content, location.offset + len))
}
