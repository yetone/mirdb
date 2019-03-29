use std::cell::RefCell;
use std::fs::File;
use std::ops::DerefMut;
use std::path::Path;
use std::rc::Rc;

use crate::block::Block;
use crate::block_handle::BlockHandle;
use crate::cache;
use crate::footer::Footer;
use crate::footer::FULL_FOOTER_LENGTH;
use crate::options::Options;
use crate::error::MyResult;

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
    use std::time;

    use crate::table_builder::TableBuilder;
    use crate::util::to_str;

    use super::*;

    fn get_data() -> Vec<(String, String)> {
        let mut data = vec![
            ("key1".to_owned(), "value1".to_owned()),
            (
                "loooooooooooooooooooooooooooooooooongerkey1".to_owned(),
                "shrtvl1".to_owned(),
            ),
            ("medium length key 1".to_owned(), "some value 2".to_owned()),
        ];
        let mut key_prefix = "prefix_key".to_owned();
        let value_prefix = "value";
        let n = 1000;
        for i in 1..=n {
            if i % 10 == 0 {
                key_prefix += "a";
            }
            let key = format!("{}{}", key_prefix, i);
            let value = format!("{}{}", value_prefix, i);
            data.push((key, value));
        }
        data
    }

    #[test]
    fn test_new() -> MyResult<()> {
        let path = Path::new("/tmp/test_table_reader");
        let mut opt = Options::default();
        opt.block_size = 20;
        let mut t = TableBuilder::new(path, opt.clone())?;
        let data = get_data();
        println!("add: {}", data.len());
        let st = time::SystemTime::now();
        for (k, v) in data {
            t.add(k.as_bytes(), v.as_bytes())?;
        }
        t.flush()?;
        println!("add cost: {}ms", st.elapsed().unwrap().as_millis());
        let t = TableReader::new(path, opt.clone())?;
        let not_found_count = 1000;
        let not_found_key_prefix = "yetone";
        let mut not_found_keys = Vec::with_capacity(not_found_count);
        for i in 0..not_found_count {
            not_found_keys.push(format!("{}{}", not_found_key_prefix, i));
        }
        println!("found not found: {}", not_found_keys.len());
        let st = time::SystemTime::now();
        for k in not_found_keys {
            let r = t.get(k.as_bytes())?;
            assert!(r.is_none());
        }
        println!("not found cost: {}ms", st.elapsed().unwrap().as_millis());
        let data = get_data();
        println!("found: {}", data.len());
        let st = time::SystemTime::now();
        for (k, v) in data {
            let r = t.get(k.as_bytes())?;
            if r.is_none() {
                println!("error found: {}", to_str(k.as_bytes()));
            }
            assert!(r.is_some());
            assert_eq!(v.as_bytes(), r.unwrap().as_slice());
        }
        println!("found cost: {}ms", st.elapsed().unwrap().as_millis());
        Ok(())
    }
}
