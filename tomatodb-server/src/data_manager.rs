use std::borrow::Borrow;
use std::fmt::Debug;
use std::path::Path;

use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;

use sstable::TableReader;

use crate::error::MyResult;
use crate::memtable::Memtable;
use crate::memtable_list::MemtableList;
use crate::options::Options;
use crate::sstable_reader::SstableReader;
use crate::types::Table;
use crate::utils::to_str;
use crate::utils::make_file_name;
use crate::wal::WAL;
use crate::wal::LogEntry;

pub struct DataManager<K: Ord + Clone, V: Clone> {
    mut_: Memtable<K, Option<V>>,
    imm_: MemtableList<K, Option<V>>,
    reader_: SstableReader,
    wal_: WAL<Vec<u8>, V>,
    opt_: Options,
}

unsafe impl<K: Ord + Clone, V: Clone> Sync for DataManager<K, V> {}
unsafe impl<K: Ord + Clone, V: Clone> Send for DataManager<K, V> {}

impl<K: Ord + Clone + Borrow<[u8]>, V: Clone + Serialize + DeserializeOwned + Debug> DataManager<K, V> {
    pub fn new(opt: Options) -> MyResult<Self> {
        let mut dm = DataManager {
            mut_: Memtable::new(opt.mem_table_max_size, opt.mem_table_max_height),
            imm_: MemtableList::new(opt.clone(), opt.imm_mem_table_max_count, opt.imm_mem_table_max_size, opt.imm_mem_table_max_height),
            reader_: SstableReader::new(opt.clone())?,
            wal_: WAL::new(opt.clone())?,
            opt_: opt.clone(),
        };
        dm.redo()?;
        Ok(dm)
    }

    pub fn redo(&mut self) -> MyResult<()> {
        if self.wal_.seg_count() > 0 {
            let work_dir = Path::new(&self.opt_.work_dir);
            for seg in &mut self.wal_.segs {
                let path = work_dir.join(make_file_name(self.reader_.manifest_builder_mut().new_file_number(), "sst"));
                if let Some((_, reader)) = seg.build_sstable(self.opt_.clone(), &path)? {
                    self.reader_.add(0, reader)?;
                }
                seg.delete()?;
            }
            self.wal_ = WAL::new(self.opt_.clone())?;
            assert_eq!(0, self.wal_.seg_count());
        }
        Ok(())
    }

    pub fn insert(&mut self, k: K, v: V) -> MyResult<Option<V>> {
        self.insert_(k, Some(v))
    }

    fn insert_(&mut self, k: K, v: Option<V>) -> MyResult<Option<V>> {
        self.wal_.append(&LogEntry::new(k.borrow().to_vec(), v.clone()))?;

        if self.mut_.is_full() {
            if self.imm_.is_full() {
                let work_dir = Path::new(&self.opt_.work_dir);
                for memtable in self.imm_.iter() {
                    let path = work_dir.join(make_file_name(self.reader_.manifest_builder_mut().new_file_number(), "sst"));
                    if let Some((_, reader)) = memtable.build_sstable(self.opt_.clone(), &path)? {
                        self.reader_.add(0, reader)?;
                    }
                    self.wal_.consume_seg()?;
                }
                self.imm_.clear();
            }
            self.imm_.push(self.mut_.clone());
            self.mut_.clear();
            self.wal_.new_seg()?;
        }

        Ok(if let Some(v) = self.mut_.insert(k, v) {
            v
        } else {
            None
        })
    }

    pub fn get<Q: ?Sized>(&self, k: &Q) -> MyResult<Option<V>>
        where K: Borrow<Q>,
              Q: Ord + Borrow<[u8]> {
        let mut r = self.mut_.get(k);
        if r.is_none()  {
            r = self.imm_.get(k);
        }
        println!("get: {}", to_str(k.borrow()));
        println!("r: {:?}", r);
        // TODO: optimize this shit codes
        // TODO: plz zero copy
        Ok(
            if let Some(r) = r {
                if let Some(r) = r {
                    Some(r.clone())
                } else {
                    None
                }
            } else {
                let x: Option<Option<V>> = self.reader_.get(k.borrow())?;
                println!("x: {:?}", x);
                if let Some(x) = x {
                    x.clone()
                } else {
                    None
                }
            }
        )
    }

    pub fn remove(&mut self, k: &K) -> MyResult<Option<V>>
        where K: Borrow<[u8]> {
        let r = self.get(k.borrow())?;
        println!("remove: {}", to_str(k.borrow()));
        if !r.is_none() {
            println!("is not none");
            self.insert_(k.clone(), None)?;
        }
        Ok(r)
    }

    #[cfg(test)]
    fn clear_memtables(&mut self) {
        self.mut_.clear();
        self.imm_.clear();
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::get_test_opt;

    fn get_data() -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut kvs = Vec::with_capacity(3);
        kvs.push((b"a".to_vec(), b"abcasldkfjaoiwejfawoejfoaisjdflaskdjfoias".to_vec()));
        kvs.push((b"b".to_vec(), b"bbcasdlfjasldfj".to_vec()));
        kvs.push((b"c".to_vec(), b"cbcasldfjowiejfoaisdjfalskdfj".to_vec()));
        kvs
    }

    #[test]
    fn test_fault_tolerance() -> MyResult<()> {
        let mut opt = get_test_opt();
        opt.imm_mem_table_max_count = 3;

        let mut dm = DataManager::new(opt.clone())?;

        let data = get_data();

        for (k, v) in &data {
            dm.insert(k.clone(), v.clone())?;
        }

        for (k, v) in &data {
            let r = dm.get(k)?;
            assert_eq!(Some(v.clone()), r);
        }

        // mock abnormal exit
        dm.clear_memtables();

        // cannot get data
        for (k, _v) in &data {
            let r = dm.get(k)?;
            assert_eq!(None, r);
        }

        // load from wal
        let mut dm = DataManager::new(opt.clone())?;

        // can get data now!
        for (k, v) in &data {
            let r = dm.get(k)?;
            assert_eq!(Some(v.clone()), r);
        }

        dm.insert(b"d".to_vec(), b"xixi".to_vec())?;
        assert_eq!(Some(b"xixi".to_vec()), dm.get(b"d".to_vec().as_slice())?);

        for (k, v) in &data {
            let r = dm.get(k)?;
            assert_eq!(Some(v.clone()), r);
        }

        Ok(())
    }
}