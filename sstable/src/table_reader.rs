use std::cell::RefCell;
use std::fs::File;
use std::io::Seek;
use std::io::SeekFrom;
use std::ops::DerefMut;
use std::path::Path;
use std::rc::Rc;

use crate::block::Block;
use crate::block_handle::BlockHandle;
use crate::cache;
use crate::footer::Footer;
use crate::footer::FULL_FOOTER_LENGTH;
use crate::options::Options;
use crate::result::MyResult;

pub struct TableReader {
    file: Rc<RefCell<File>>,
    file_size: usize,
    opt: Options,

    cache_id: cache::CacheID,
    footer: Footer,
    pub(crate) index_block: Block,
}

impl TableReader {
    pub fn new(path: &Path, opt: Options) -> MyResult<TableReader> {
        let mut f = File::open(path)?;
        let size = f.metadata()?.len() as usize;
        let footer = Footer::read(&mut f, size - FULL_FOOTER_LENGTH)?;
        let index_block = Block::new_from_location(&mut f, &footer.index, opt.clone())?.0;
        Ok(TableReader {
            file: Rc::new(RefCell::new(f)),
            file_size: size,
            cache_id: opt.block_cache.borrow_mut().new_cache_id(),
            footer,
            index_block,
            opt: opt.clone(),
        })
    }

    fn read_block(&self, bh: &BlockHandle) -> MyResult<Option<Block>> {
        let mut buf = [0; 16];
        bh.encode_to(&mut buf);
        {
            let mut bc = self.opt.block_cache.borrow_mut();
            let res = bc.get(&buf);
            if let Some(block) = res {
                return Ok(Some(block.clone()))
            }
        }
        let (block, _) = Block::new_from_location(self.file.borrow_mut().deref_mut(), bh, self.opt.clone())?;
        self.opt.block_cache.borrow_mut().insert(&buf, block.clone());
        Ok(Some(block))
    }

    pub fn get(&self, k: &[u8]) -> MyResult<Option<Vec<u8>>> {
        let mut iter = self.index_block.iter();
        iter.seek(k);
        let kv = iter.current_kv();
        if let None = kv {
            return Ok(None);
        }
        let value = iter.current_kv().unwrap().1;
        let (bh, _) = BlockHandle::decode(&value);
        let block = self.read_block(&bh)?;
        if block.is_none() {
            return Ok(None);
        }
        let block = block.unwrap();
        let mut iter = block.iter();
        iter.seek(k);
        if let Some((key, value)) = iter.current_kv() {
            if &key[..] == k {
                return Ok(Some(value));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod test {
    use crate::table_builder::TableBuilder;
    use crate::util::to_str;

    use super::*;

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
        let path = Path::new("/tmp/test_table_reader");
        let mut opt = Options::default();
        opt.block_size = 20;
        let mut t = TableBuilder::new(path, opt.clone())?;
        let data = get_data();
        for (k, v) in data {
            t.add(k, v)?;
        }
        t.flush()?;
        let t = TableReader::new(path, opt.clone())?;
        for (k, _) in t.index_block.iter() {
            println!("index key: {}", to_str(&k));
        }
        let r = t.get("prefix_key0".as_bytes())?;
        assert!(r.is_none());
        let data = get_data();
        for (k, v) in data {
            let r = t.get(k)?;
            assert!(r.is_some());
            assert_eq!(v, r.unwrap().as_slice());
        }
        Ok(())
    }
}
