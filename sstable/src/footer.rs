use std::io::Seek;
use std::io::Write;

use crate::block_handle::BlockHandle;
use crate::error::MyResult;
use crate::types::RandomAccess;

pub const FOOTER_LENGTH: usize = 40;
pub const FULL_FOOTER_LENGTH: usize = FOOTER_LENGTH + 8;
const MAGIC_FOOTER_ENCODED: [u8; 8] = [0x57, 0xfb, 0x80, 0x8b, 0x24, 0x75, 0x47, 0xdb];

pub struct Footer {
    meta_index_: BlockHandle,
    index_: BlockHandle,
}

impl Footer {
    pub fn new(meta_index: BlockHandle, index: BlockHandle) -> Footer {
        Footer {
            meta_index_: meta_index,
            index_: index,
        }
    }

    pub fn meta_index(&self) -> &BlockHandle {
        &self.meta_index_
    }

    pub fn index(&self) -> &BlockHandle {
        &self.index_
    }

    pub fn read(r: &dyn RandomAccess, offset: usize) -> MyResult<Self> {
        let mut buf = [0; FULL_FOOTER_LENGTH];
        r.read_at(offset, &mut buf)?;
        Ok(Footer::decode(&buf))
    }

    pub fn flush<T: Seek + Write>(&self, w: &mut T, offset: usize) -> MyResult<BlockHandle> {
        let mut buf = [0; FULL_FOOTER_LENGTH];
        self.encode(&mut buf);
        w.write_all(&buf)?;
        Ok(bh!(offset, buf.len()))
    }

    pub fn decode(from: &[u8]) -> Footer {
        assert!(from.len() >= FULL_FOOTER_LENGTH);
        assert_eq!(&from[FOOTER_LENGTH..], &MAGIC_FOOTER_ENCODED);
        let (meta, metalen) = BlockHandle::decode(&from[0..]);
        let (idx, _) = BlockHandle::decode(&from[metalen..]);

        Footer {
            meta_index_: meta,
            index_: idx,
        }
    }

    pub fn encode(&self, to: &mut [u8]) {
        assert!(to.len() >= FULL_FOOTER_LENGTH);

        let s1 = self.meta_index_.encode_to(to);
        let s2 = self.index_.encode_to(&mut to[s1..]);

        #[allow(clippy::needless_range_loop)]
        for i in s1 + s2..FOOTER_LENGTH {
            to[i] = 0;
        }

        to[FOOTER_LENGTH..FULL_FOOTER_LENGTH]
            .clone_from_slice(&MAGIC_FOOTER_ENCODED[0..(FULL_FOOTER_LENGTH - FOOTER_LENGTH)]);
    }
}

#[cfg(test)]
mod test {
    use std::fs::File;
    use std::path::Path;

    use super::*;

    #[test]
    fn test_footer() -> MyResult<()> {
        let footer = Footer::new(bh!(0, 10), bh!(11, 12));
        let mut buf = [0; FULL_FOOTER_LENGTH];
        footer.encode(&mut buf);
        let path = Path::new("/tmp/test_footer");
        let mut f = File::create(path)?;
        f.write(&buf)?;
        f.flush()?;
        let mut f = File::open(path)?;
        let footer = Footer::read(&mut f, 0)?;
        assert_eq!(0, footer.meta_index_.offset);
        assert_eq!(10, footer.meta_index_.size);
        assert_eq!(11, footer.index_.offset);
        assert_eq!(12, footer.index_.size);
        Ok(())
    }
}
