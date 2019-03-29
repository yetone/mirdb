use std::fs::File;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Seek;
use std::io::Write;
use std::path::Path;

use crate::block_builder::BlockBuilder;
use crate::block_handle::BlockHandle;
use crate::options::Options;
use crate::result::MyResult;
use crate::util::find_short_succ;
use crate::util::find_shortest_sep;
use std::io::SeekFrom;

pub const FOOTER_LENGTH: usize = 40;
pub const FULL_FOOTER_LENGTH: usize = FOOTER_LENGTH + 8;
const MAGIC_FOOTER_ENCODED: [u8; 8] = [0x57, 0xfb, 0x80, 0x8b, 0x24, 0x75, 0x47, 0xdb];

pub struct Footer {
    pub meta_index: BlockHandle,
    pub index: BlockHandle,
}

impl Footer {
    pub fn new(meta_index: BlockHandle, index: BlockHandle) -> Footer {
        Footer {
            meta_index,
            index,
        }
    }

    pub fn read<T: Seek + Read>(r: &mut T, offset: usize) -> MyResult<Self> {
        r.seek(SeekFrom::Start(offset as u64))?;
        let mut buf = [0; FULL_FOOTER_LENGTH];
        r.read_exact(&mut buf)?;
        Ok(Footer::decode(&buf))
    }

    pub fn flush<T: Seek + Write>(&self, w: &mut T, offset: usize) -> MyResult<BlockHandle> {
        let mut buf = [0; FULL_FOOTER_LENGTH];
        self.encode(&mut buf);
        w.write(&mut buf)?;
        Ok(bh!(offset, buf.len()))
    }

    pub fn decode(from: &[u8]) -> Footer {
        assert!(from.len() >= FULL_FOOTER_LENGTH);
        assert_eq!(&from[FOOTER_LENGTH..], &MAGIC_FOOTER_ENCODED);
        let (meta, metalen) = BlockHandle::decode(&from[0..]);
        let (ix, _) = BlockHandle::decode(&from[metalen..]);

        Footer {
            meta_index: meta,
            index: ix,
        }
    }

    pub fn encode(&self, to: &mut [u8]) {
        assert!(to.len() >= FULL_FOOTER_LENGTH);

        let s1 = self.meta_index.encode_to(to);
        let s2 = self.index.encode_to(&mut to[s1..]);

        for i in s1 + s2..FOOTER_LENGTH {
            to[i] = 0;
        }
        for i in FOOTER_LENGTH..FULL_FOOTER_LENGTH {
            to[i] = MAGIC_FOOTER_ENCODED[i - FOOTER_LENGTH];
        }
    }
}

#[cfg(test)]
mod test {
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
        assert_eq!(0, footer.meta_index.offset);
        assert_eq!(10, footer.meta_index.size);
        assert_eq!(11, footer.index.offset);
        assert_eq!(12, footer.index.size);
        Ok(())
    }
}
