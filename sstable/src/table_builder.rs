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

    pub fn read<T: Seek + Read>(r: &mut T) -> MyResult<Self> {
        let mut buf = [0; FULL_FOOTER_LENGTH];
        r.read_exact(&mut buf)?;
        Ok(Footer::decode(&buf))
    }

    pub fn flush<T: Seek + Write>(&self, w: &mut T, offset: usize) -> MyResult<BlockHandle> {
        let mut buf = [0; FULL_FOOTER_LENGTH];
        self.encode(&mut buf);
        w.write_all(&mut buf)?;
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

pub struct TableBuilder {
    file: File,
    opt: Options,
    offset: usize,
    data_block: BlockBuilder,
    index_block: BlockBuilder,
}

impl TableBuilder {
    pub fn new(path: &Path, opt: Options) -> MyResult<TableBuilder> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;
        Ok(TableBuilder {
            file,
            opt: opt.clone(),
            offset: 0,
            data_block: BlockBuilder::new(opt.clone()),
            index_block: BlockBuilder::new(opt),
        })
    }

    #[allow(unused)]
    fn size_estimate(&self) -> usize {
        let mut size = 0;
        size += self.data_block.size_estimate();
        size += self.index_block.size_estimate();
        size += self.offset;
        size += FULL_FOOTER_LENGTH;
        size
    }

    pub fn add(&mut self, k: &[u8], v: &[u8]) -> MyResult<()> {
        if self.data_block.size_estimate() > self.opt.block_size {
            self.write_data_block(k)?;
        }
        self.data_block.add(k, v);
        Ok(())
    }

    fn write_data_block(&mut self, next_key: &[u8]) -> MyResult<()> {
        let sep = find_shortest_sep(&self.data_block.last_key, next_key);

        let bh = self.data_block.flush(&mut self.file, self.offset)?;

        let mut handle_enc = [0; 16];
        let enc_len = bh.encode_to(&mut handle_enc);

        self.index_block.add(&sep, &handle_enc[0..enc_len]);

        self.data_block = BlockBuilder::new(self.opt.clone());
        self.offset = bh.offset + bh.size;
        Ok(())
    }

    pub fn flush(&mut self) -> MyResult<()> {
        self.write_data_block(&find_short_succ(&self.data_block.last_key))?;
        let bh = self.index_block.flush(&mut self.file, self.offset)?;
        let footer = Footer::new(bh.clone(), bh.clone());
        footer.flush(&mut self.file, bh.offset + bh.size)?;
        self.file.flush()?;
        Ok(())
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
        let footer = Footer::read(&mut f)?;
        assert_eq!(0, footer.meta_index.offset);
        assert_eq!(10, footer.meta_index.size);
        assert_eq!(11, footer.index.offset);
        assert_eq!(12, footer.index.size);
        Ok(())
    }

    fn get_data() -> Vec<(&'static [u8], &'static [u8])> {
        vec![
            ("key1".as_bytes(), "value1".as_bytes()),
            (
                "loooooooooooooooooooooooooooooooooongerkey1".as_bytes(),
                "shrtvl1".as_bytes(),
            ),
            ("medium length key 1".as_bytes(), "some value 2".as_bytes()),
            ("prefix_key1".as_bytes(), "value".as_bytes()),
            ("prefix_key2".as_bytes(), "value".as_bytes()),
            ("prefix_key3".as_bytes(), "value".as_bytes()),
        ]
    }

    #[test]
    fn test_flush() -> MyResult<()> {
        let mut t = TableBuilder::new("/tmp/x.data".as_ref(), Options::default())?;
        let data = get_data();
        for (k, v) in data {
            t.add(k, v)?;
        }
        t.flush()?;
        Ok(())
    }
}
