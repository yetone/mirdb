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
use crate::sstable_builder::build_sstable;
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
                let (_, reader) = seg.build_sstable(self.opt_.clone(), &path)?;
                self.reader_.add(0, reader)?;
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
                    let (_, reader) = build_sstable(self.opt_.clone(), &path, memtable)?;
                    self.reader_.add(0, reader)?;
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
}