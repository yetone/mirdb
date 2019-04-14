use std::cell::RefCell;
use std::fs::File;
use std::ops::DerefMut;
use std::path::Path;
use std::rc::Rc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;

use integer_encoding::FixedIntWriter;

use crate::block::Block;
use crate::block_handle::BlockHandle;
use crate::cache;
use crate::error::MyResult;
use crate::footer::Footer;
use crate::footer::FULL_FOOTER_LENGTH;
use crate::meta_block::MetaBlock;
use crate::options::Options;
use crate::table_iter::TableIter;
use crate::types::SsIterator;

pub struct TableReader {
    file: Rc<RefCell<File>>,
    file_size: usize,
    opt: Options,

    cache_id: cache::CacheID,
    footer: Footer,
    pub(crate) index_block: Block,
    meta_block_: MetaBlock,
    size_: usize,
    file_name_: String,

    seek_miss_count_: AtomicUsize,
}

impl TableReader {
    pub fn new(path: &Path, opt: Options) -> MyResult<TableReader> {
        let mut f = File::open(path)?;
        let size = f.metadata()?.len() as usize;
        if size <= FULL_FOOTER_LENGTH {
            println!("path: {}", path.display());
            println!("size: {}", size);
            assert!(size > FULL_FOOTER_LENGTH);
        }
        let footer = Footer::read(&mut f, size - FULL_FOOTER_LENGTH)?;
        let meta_block = MetaBlock::new_from_location(&mut f, &footer.meta_index())?.0;
        let index_block = Block::new_from_location(&mut f, &footer.index(), opt.clone())?.0;
        let metadata = f.metadata()?;
        let size_ = metadata.len() as usize;
        let file_name_ = path.file_name().expect("get file name").to_str().expect("file name to str").to_owned();
        Ok(TableReader {
            file: Rc::new(RefCell::new(f)),
            file_size: size,
            cache_id: opt.block_cache.borrow_mut().new_cache_id(),
            footer,
            index_block,
            opt: opt.clone(),
            meta_block_: meta_block,
            size_,
            file_name_,
            seek_miss_count_: AtomicUsize::new(0),
        })
    }

    fn incr_seek_miss_count(&self) {
        self.seek_miss_count_.fetch_add(1, Relaxed);
    }

    pub fn get_seek_miss_count(&self) -> usize {
        self.seek_miss_count_.load(Relaxed)
    }

    pub fn reset_seek_miss_count(&self) -> usize {
        self.seek_miss_count_.swap(0, Relaxed)
    }

    pub fn min_key(&self) -> &Vec<u8> {
        &self.meta_block_.min_key
    }

    pub fn max_key(&self) -> &Vec<u8> {
        &self.meta_block_.max_key
    }

    pub fn size(&self) -> usize {
        self.size_
    }

    pub fn file_name(&self) -> &String {
        &self.file_name_
    }

    fn gen_cache_key(&self, bh: &BlockHandle) -> cache::CacheKey {
        let mut dst = [0; 2 * 8];
        (&mut dst[..8])
            .write_fixedint(self.cache_id)
            .expect("error writing to vec");
        (&mut dst[8..])
            .write_fixedint(bh.offset as u64)
            .expect("error writing to vec");
        dst
    }

    pub(crate) fn read_block(&self, bh: &BlockHandle) -> MyResult<Option<Block>> {
        let cache_key = self.gen_cache_key(bh);
        {
            let mut bc = self.opt.block_cache.borrow_mut();
            let res = bc.get(&cache_key);
            if let Some(block) = res {
                return Ok(Some(block.clone()))
            }
        }
        let (block, _) = Block::new_from_location(self.file.borrow_mut().deref_mut(), bh, self.opt.clone())?;
        self.opt.block_cache.borrow_mut().insert(&cache_key, block.clone());
        Ok(Some(block))
    }

    pub fn iter(&self) -> TableIter {
        TableIter::new(self)
    }

    pub fn get(&self, k: &[u8]) -> MyResult<Option<Vec<u8>>> {
        if k < self.min_key() || k > self.max_key() {
            return Ok(None);
        }
        let mut iter = self.iter();
        iter.seek(k);
        if let Some(key) = iter.current_k() {
            if &key[..] == k {
                return Ok(iter.current_v());
            }
        }
        self.incr_seek_miss_count();
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
        println!("load");
        let st = time::SystemTime::now();
        let t = TableReader::new(path, opt.clone())?;
        println!("load cost: {}ms, size: {}, min_key: {}, max_key: {}", st.elapsed().unwrap().as_millis(), t.size(), to_str(t.min_key()), to_str(t.max_key()));
        let not_found_count = 1000;
        let not_found_key_prefix = "prefix_kex";
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
        println!("1st found: {}", data.len());
        let st = time::SystemTime::now();
        for (k, v) in data {
            let r = t.get(k.as_bytes())?;
            if r.is_none() {
                println!("error found: {}", to_str(k.as_bytes()));
            }
            assert!(r.is_some());
            assert_eq!(v.as_bytes(), r.unwrap().as_slice());
        }
        let first_cost = st.elapsed().unwrap().as_millis();
        println!("1st found cost: {}ms", first_cost);
        let data = get_data();
        println!("2nd found: {}", data.len());
        let st = time::SystemTime::now();
        for (k, v) in data {
            let r = t.get(k.as_bytes())?;
            if r.is_none() {
                println!("error found: {}", to_str(k.as_bytes()));
            }
            assert!(r.is_some());
            assert_eq!(v.as_bytes(), r.unwrap().as_slice());
        }
        let second_cost = st.elapsed().unwrap().as_millis();
        println!("2nd found cost: {}ms", second_cost);
        assert!(first_cost > second_cost);
        Ok(())
    }
}
