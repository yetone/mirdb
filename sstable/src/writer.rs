use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;

use integer_encoding::FixedInt;

use crate::error::MyResult;

pub fn write_usize<T: Seek + Write>(
    w: &mut T,
    offset: usize,
    content: usize,
) -> MyResult<(usize, usize)> {
    w.seek(SeekFrom::Start(offset as u64))?;
    let mut buf = [0; 8];
    content.encode_fixed(&mut buf);
    w.write(&buf)?;
    Ok((buf.len(), offset + buf.len()))
}

pub fn write_bytes<T: Write + Seek>(
    w: &mut T,
    offset: usize,
    content: &[u8],
) -> MyResult<(usize, usize)> {
    w.seek(SeekFrom::Start(offset as u64))?;
    w.write(content)?;
    Ok((content.len(), offset + content.len()))
}

#[cfg(test)]
mod test {
    use std::fs::File;
    use std::path::Path;

    use crate::reader::*;

    use super::*;

    #[test]
    fn test_write_usize() -> MyResult<()> {
        let path = Path::new("/tmp/test_block_writer");
        let mut f = File::create(path)?;
        let offset = 0;
        let (_, offset) = write_usize(&mut f, offset, 1)?;
        let (_, offset) = write_usize(&mut f, offset, 2)?;
        let (_, _offset) = write_usize(&mut f, offset, 3)?;
        f.flush()?;
        let f = File::open(path)?;
        let offset = 0;
        let (r, offset) = read_usize(&f, offset)?;
        assert_eq!(1, r);
        let (r, offset) = read_usize(&f, offset)?;
        assert_eq!(2, r);
        let (r, _offset) = read_usize(&f, offset)?;
        assert_eq!(3, r);
        Ok(())
    }
}
