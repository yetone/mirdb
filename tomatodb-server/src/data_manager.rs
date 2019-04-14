use std::borrow::Borrow;
use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::RwLockWriteGuard;

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
use crate::utils::make_file_name;
use crate::utils::read_unlock;
use crate::utils::to_str;
use crate::utils::write_unlock;
use crate::wal::LogEntry;
use crate::wal::WAL;

pub struct DataManager<K: Ord + Clone, V: Clone> {
    mut_: Arc<RwLock<Memtable<K, Option<V>>>>,
    imm_: Arc<RwLock<MemtableList<K, Option<V>>>>,
    readers_: Arc<RwLock<SstableReader>>,
    wal_: Arc<RwLock<WAL<Vec<u8>, Option<V>>>>,
    opt_: Options,
    last_compact_keys_: Vec<Vec<u8>>,
}

unsafe impl<K: Ord + Clone + Sync, V: Clone + Sync> Sync for DataManager<K, V> {}
unsafe impl<K: Ord + Clone + Send, V: Clone + Send> Send for DataManager<K, V> {}

impl<K: Ord + Clone + Borrow<[u8]>, V: Clone + Serialize + DeserializeOwned + Debug> DataManager<K, V> {
    pub fn new(opt: Options) -> MyResult<Self> {
        let mut dm = DataManager {
            mut_: Arc::new(RwLock::new(Memtable::new(opt.mem_table_max_size, opt.mem_table_max_height))),
            imm_: Arc::new(RwLock::new(MemtableList::new(opt.clone(), opt.imm_mem_table_max_count, opt.mem_table_max_size, opt.mem_table_max_height))),
            readers_: Arc::new(RwLock::new(SstableReader::new(opt.clone())?)),
            wal_: Arc::new(RwLock::new(WAL::new(opt.clone())?)),
            opt_: opt.clone(),
            last_compact_keys_: Vec::with_capacity(opt.max_level),
        };
        dm.redo()?;
        Ok(dm)
    }

    fn new_file_number(&self) -> usize {
        let mut readers = write_unlock(&self.readers_);
        readers.manifest_builder_mut().new_file_number()
    }

    pub fn redo(&mut self) -> MyResult<()> {
        {
            let mut wal = write_unlock(&self.wal_);

            if wal.seg_count() <= 0 {
                return Ok(());
            }

            let work_dir = Path::new(&self.opt_.work_dir);

            for seg in &mut wal.segs {
                let path = work_dir.join(make_file_name(self.new_file_number(), "sst"));
                if let Some((_, reader)) = seg.build_sstable(&self.opt_, &path)? {
                    let mut readers = write_unlock(&self.readers_);
                    readers.add(0, reader)?;
                }
                seg.delete()?;
            }
        }

        self.wal_ = Arc::new(RwLock::new(WAL::new(self.opt_.clone())?));

        assert_eq!(0, read_unlock(&self.wal_).seg_count());

        Ok(())
    }

    pub fn insert(&self, k: K, v: V) -> MyResult<Option<V>> {
        self.insert_(k, Some(v))
    }

    fn insert_(&self, k: K, v: Option<V>) -> MyResult<Option<V>> {
        let mut wal = write_unlock(&self.wal_);
        wal.append(&LogEntry::new(k.borrow().to_vec(), v.clone()))?;

        let mut muttable = write_unlock(&self.mut_);
        let mut immuttable = write_unlock(&self.imm_);

        if muttable.is_full() {
            if immuttable.is_full() {
                self.minor_compaction(&mut wal, &mut immuttable)?;
            }
            immuttable.push(muttable.clone());
            muttable.clear();
            wal.new_seg()?;
        }

        Ok(if let Some(v) = muttable.insert(k, v) {
            v
        } else {
            None
        })
    }

    pub fn get<Q: ?Sized>(&self, k: &Q) -> MyResult<Option<V>>
        where K: Borrow<Q>,
              Q: Ord + Borrow<[u8]> {

        let muttable = read_unlock(&self.mut_);
        let immuttable = read_unlock(&self.imm_);

        let mut r = muttable.get(k);
        if r.is_none()  {
            r = immuttable.get(k);
        }

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
                let readers = read_unlock(&self.readers_);
                let x: Option<Option<V>> = readers.get(k.borrow())?;
                if let Some(x) = x {
                    x.clone()
                } else {
                    None
                }
            }
        )
    }

    pub fn remove(&self, k: &K) -> MyResult<Option<V>>
        where K: Borrow<[u8]> {
        let r = self.get(k.borrow())?;
        if !r.is_none() {
            self.insert_(k.clone(), None)?;
        }
        Ok(r)
    }

    fn minor_compaction(&self, wal: &mut RwLockWriteGuard<WAL<Vec<u8>, Option<V>>>, immuttable: &mut RwLockWriteGuard<MemtableList<K, Option<V>>>) -> MyResult<()> {
        let work_dir = Path::new(&self.opt_.work_dir);
        for memtable in immuttable.iter() {
            let path = work_dir.join(make_file_name(self.new_file_number(), "sst"));
            if let Some((_, reader)) = memtable.build_sstable(&self.opt_, &path)? {
                let mut readers = write_unlock(&self.readers_);
                readers.add(0, reader)?;
            }
            wal.consume_seg()?;
        }
        immuttable.clear();
        Ok(())
    }

    fn major_compaction(&self) -> MyResult<()> {
        let levels = {
            let readers = read_unlock(&self.readers_);
            readers.compute_compaction_levels()
        };
        if levels.len() > 0 {
            self.size_compaction(levels)?;
        } else {
            self.seek_compaction()?;
        }
        Ok(())
    }

    fn size_compaction(&self, levels: Vec<usize>) -> MyResult<()> {
        // TODO: process all levels
        let level = levels[0];

        if level == self.opt_.max_level - 1 {
            return Ok(());
        }

        let readers_group = read_unlock(&self.readers_);
        let readers = readers_group.get_readers(level);

        let mut inputs0 = vec![];

        if level == 0 {
            for reader in readers {
                inputs0.push(reader);
            }
        } else {
            let last_compact_key = self.last_compact_keys_.get(level);

            for reader in readers {
                if last_compact_key.is_none() || reader.max_key() > last_compact_key.unwrap() {
                    inputs0.push(reader);
                    break;
                }
            }

            if inputs0.is_empty() {
                inputs0.push(&readers[0]);
            }
        }

        let mut min = None;
        let mut max = None;

        for i in &inputs0 {
            if let Some(min_) = min {
                min = if min_ < i.min_key() {
                    Some(min_)
                } else {
                    Some(i.min_key())
                };
            } else {
                min = Some(i.min_key());
            }
            if let Some(max_) = max {
                max = if max_ > i.max_key() {
                    Some(max_)
                } else {
                    Some(i.max_key())
                };
            } else {
                max = Some(i.max_key());
            }
        }

        let min_key = min.unwrap();
        let max_key = max.unwrap();
        let readers = readers_group.get_readers(level + 1);
        let inputs1 = self.get_other_readers(&min_key, &max_key, readers);

        let mut new_readers = vec![];


        let mut readers_group = write_unlock(&self.readers_);

        for reader in inputs0 {
            readers_group.remove(level, reader.file_name())?;
        }
        for reader in inputs1 {
            readers_group.remove(level + 1, reader.file_name())?;
        }
        for reader in new_readers {
            readers_group.add(level + 1, reader)?;
        }
        Ok(())
    }

    fn seek_compaction(&self) -> MyResult<()> {
        Ok(())
    }

    fn get_other_readers<'a>(&'a self, min_key: &Vec<u8>, max_key: &Vec<u8>, readers: &'a Vec<TableReader>) -> Vec<&'a TableReader> {
        let mut res = vec![];
        for reader in readers {
            if reader.max_key() >= min_key {
                res.push(reader);
                continue;
            }
            if reader.min_key() <= max_key {
                res.push(reader);
                continue;
            }
            if reader.min_key() > max_key {
                break;
            }
        }
        res
    }

    #[cfg(test)]
    fn clear_memtables(&self) {
        let mut muttable = write_unlock(&self.mut_);
        let mut immuttable = write_unlock(&self.imm_);
        muttable.clear();
        immuttable.clear();
    }
}

#[cfg(test)]
mod test {
    use crate::test_utils::get_test_opt;

    use super::*;

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

        let dm = DataManager::new(opt.clone())?;

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
        let dm = DataManager::new(opt.clone())?;

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

        // load from wal
        let dm: DataManager<Vec<u8>, Vec<u8>> = DataManager::new(opt.clone())?;

        // can get data now!
        for (k, v) in &data {
            let r = dm.get(k)?;
            assert_eq!(Some(v.clone()), r);
        }
        assert_eq!(Some(b"xixi".to_vec()), dm.get(b"d".to_vec().as_slice())?);

        Ok(())
    }
}