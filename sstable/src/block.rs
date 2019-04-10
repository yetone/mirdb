use std::io::Read;
use std::io::Seek;

use crc::crc32;
use crc::crc32::Hasher32;
use integer_encoding::FixedInt;
use snap::Decoder;

use crate::block_builder::BLOCK_CKSUM_LEN;
use crate::block_builder::BLOCK_CTYPE_LEN;
use crate::block_handle::BlockHandle;
use crate::block_iter::BlockIter;
use crate::error::MyResult;
use crate::error::StatusCode;
use crate::options::CompressType;
use crate::options::int_to_compress_type;
use crate::options::Options;
use crate::reader;
use crate::util::unmask_crc;

#[derive(Clone)]
pub struct Block {
    opt: Options,
    pub block: Vec<u8>,
}

impl Block {
    pub fn new(opt: Options) -> Self {
        Block::new_with_buffer(vec![], opt)
    }

    pub fn new_with_buffer<T: Into<Vec<u8>>>(buffer: T, opt: Options) -> Self {
        Block {
            opt,
            block: buffer.into(),
        }
    }

    pub fn new_from_location<T: Seek + Read>(r: &mut T, location: &BlockHandle, opt: Options) -> MyResult<(Block, usize)> {
        let (data, offset) = reader::read_bytes(r, location)?;
        let cksum_buf = &data[data.len() - BLOCK_CKSUM_LEN..];
        if !Block::verify_block(&data[..data.len() - BLOCK_CKSUM_LEN], unmask_crc(u32::decode_fixed(&cksum_buf))) {
            return err!(StatusCode::ChecksumError, "checksum error");
        }
        let ctype_buf = &data[data.len() - BLOCK_CTYPE_LEN - BLOCK_CKSUM_LEN..data.len() - BLOCK_CKSUM_LEN];
        let buf = &data[..data.len() - BLOCK_CKSUM_LEN - BLOCK_CTYPE_LEN];
        if let Some(ctype) = int_to_compress_type(ctype_buf[0] as u32) {
            match ctype {
                CompressType::None => Ok((Block::new_with_buffer(buf, opt), offset)),
                CompressType::Snappy => {
                    let decoded = Decoder::new().decompress_vec(&buf)?;
                    Ok((Block::new_with_buffer(decoded, opt), offset))
                }
            }
        } else {
            err!(StatusCode::InvalidData, "invalid data")
        }
    }

    fn verify_block(data: &[u8], want: u32) -> bool {
        let mut digest = crc32::Digest::new(crc32::CASTAGNOLI);
        digest.write(data);
        digest.sum32() == want
    }

    pub fn restarts_offset(&self) -> usize {
        let restarts = u32::decode_fixed(&self.block[self.block.len() - 4..]);
        let restarts_offset = self.block.len() - 4 - 4 * restarts as usize;
        restarts_offset
    }

    pub fn iter(&self) -> BlockIter {
        BlockIter::new(&self.block, self.restarts_offset())
    }
}

#[cfg(test)]
mod test {
    use std::fs::File;
    use std::io::Write;
    use std::path::Path;

    use crate::block_builder::BlockBuilder;
    use crate::types::SsIterator;
    use crate::util::to_str;

    use super::*;
    use crate::types::SsIteratorIterWrap;

    fn get_simple_data() -> Vec<(&'static [u8], &'static [u8])> {
        vec![
            ("prefix_key1".as_bytes(), "value1".as_bytes()),
            ("prefix_key2".as_bytes(), "value2".as_bytes()),
            ("prefix_key3".as_bytes(), "value3".as_bytes()),
        ]
    }

    fn get_data() -> Vec<(&'static [u8], &'static [u8])> {
        vec![
            ("key1".as_bytes(), "value1".as_bytes()),
            (
                "loooooooooooooooooooooooooooooooooongerkey1".as_bytes(),
                "shrtvl1".as_bytes(),
            ),
            ("medium length key 1".as_bytes(), "some value 2".as_bytes()),
            ("prefix_key1".as_bytes(), "value1".as_bytes()),
            ("prefix_key2".as_bytes(), "value2".as_bytes()),
            ("prefix_key3".as_bytes(), "value3".as_bytes()),
        ]
    }

    #[test]
    fn test_new() -> MyResult<()> {
        let path = Path::new("/tmp/test_data_block");
        let mut f = File::create(path)?;
        let mut b = BlockBuilder::new(Options::default());
        let data = get_data();
        for (k, v) in &data {
            b.add(*k, *v);
        }
        let bh = b.flush(&mut f, 0)?;
        f.flush()?;
        let mut f = File::open(path)?;
        let (b1, _) = Block::new_from_location(&mut f, &bh, Options::default())?;
        for (k, v) in SsIteratorIterWrap::new(&mut b1.iter()) {
            println!("k: {}, v: {}", to_str(&k[..]), to_str(&v[..]));
        }
        assert_eq!(data.len(), b1.iter().count());
        let mut bi = b1.iter();
        bi.seek("prefix_key0".as_bytes());
        assert_eq!("prefix_key1".as_bytes(), &bi.state.key[..]);
        let data = get_data();
        for (k, v) in data {
            bi.seek(k);
            assert_eq!(k, &bi.state.key[..]);
            assert_eq!(v, &bi.current_kv().unwrap().1[..]);
        }
        Ok(())
    }

    #[test]
    fn test_iter() -> MyResult<()> {
        let path = Path::new("/tmp/test_data_block_iter");
        let mut f = File::create(path)?;
        let mut b = BlockBuilder::new(Options::default());
        let data = get_simple_data();
        for (k, v) in &data {
            b.add(*k, *v);
        }
        let bh = b.flush(&mut f, 0)?;
        f.flush()?;

        let mut f = File::open(path)?;
        let (b1, _) = Block::new_from_location(&mut f, &bh, Options::default())?;

        let mut iter = b1.iter();
        assert_eq!(None, iter.current_kv());
        iter.advance();
        assert_eq!(b"prefix_key1".to_vec(), iter.current_kv().unwrap().0);
        iter.advance();
        assert_eq!(b"prefix_key2".to_vec(), iter.current_kv().unwrap().0);
        iter.prev();
        assert_eq!(b"prefix_key1".to_vec(), iter.current_kv().unwrap().0);

        let mut iter = b1.iter();
        iter.prev();
        assert_eq!(None, iter.current_kv());

        let mut iter = b1.iter();
        iter.seek_to_last();
        assert_eq!(b"prefix_key3".to_vec(), iter.current_kv().unwrap().0);
        iter.prev();
        assert_eq!(b"prefix_key2".to_vec(), iter.current_kv().unwrap().0);
        iter.prev();
        assert_eq!(b"prefix_key1".to_vec(), iter.current_kv().unwrap().0);
        iter.prev();
        assert_eq!(None, iter.current_kv());
        Ok(())
    }
}
