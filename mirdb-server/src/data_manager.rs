use log::info;
use std::borrow::Borrow;
use std::fmt::Debug;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::RwLockWriteGuard;
use std::thread;
use std::time;
use std::time::Duration;

use bincode::deserialize;
use bincode::serialize;
use serde::Deserialize;
use serde::Serialize;

use sstable::SsIterator;
use sstable::TableBuilder;
use sstable::TableReader;

use crate::error::MyResult;
use crate::memtable::Memtable;
use crate::memtable_list::MemtableList;
use crate::merger::Merger;
use crate::options::Options;
use crate::slice::Slice;
use crate::sstable_reader::SstableReader;
use crate::store::StoreKey;
use crate::store::StorePayload;
use crate::types::Table;
use crate::utils::make_file_name;
use crate::utils::read_lock;
use crate::utils::to_str;
use crate::utils::write_lock;
use crate::wal::WAL;

pub struct DataManager {
    mut_: Arc<RwLock<Memtable<Slice, Slice>>>,
    imm_: Arc<RwLock<MemtableList<Slice, Slice>>>,
    readers_: Arc<RwLock<SstableReader>>,
    wal_: Arc<RwLock<WAL>>,
    opt_: Options,
    next_file_number_: AtomicUsize,
    last_compact_keys_: Vec<Vec<u8>>,
}

unsafe impl Sync for DataManager {}
unsafe impl Send for DataManager {}

impl DataManager {
    pub fn new(opt: Options) -> MyResult<Arc<Self>> {
        let readers_ = Arc::new(RwLock::new(SstableReader::new(opt.clone())?));
        let next_file_number = {
            let readers = read_lock(&readers_);
            readers.manifest_builder().next_file_number()
        };
        let mut dm = DataManager {
            mut_: Arc::new(RwLock::new(Memtable::new(
                opt.mem_table_max_size,
                opt.mem_table_max_height,
            ))),
            imm_: Arc::new(RwLock::new(MemtableList::new(
                opt.clone(),
                opt.imm_mem_table_max_count,
                opt.mem_table_max_size,
                opt.mem_table_max_height,
            ))),
            readers_,
            next_file_number_: AtomicUsize::new(next_file_number),
            wal_: Arc::new(RwLock::new(WAL::new(opt.clone())?)),
            opt_: opt.clone(),
            last_compact_keys_: Vec::with_capacity(opt.max_level),
        };
        dm.redo()?;
        Ok(Arc::new(dm))
    }

    pub fn background_thread(dma: Arc<Self>) {
        let dm = dma.clone();
        let _ = thread::spawn(move || {
            let d = Duration::from_millis(dm.opt().thread_sleep_ms as u64);
            loop {
                dm.major_compaction().unwrap();
                thread::sleep(d);
            }
        });
        let dm = dma.clone();
        let _ = thread::spawn(move || {
            let d = Duration::from_millis(dm.opt().thread_sleep_ms as u64);
            loop {
                dm.minor_compaction().unwrap();
                thread::sleep(d);
            }
        });
    }

    fn new_file_number(&self) -> usize {
        self.next_file_number_.fetch_add(1, Relaxed)
    }

    pub fn opt(&self) -> &Options {
        &self.opt_
    }

    pub fn info(&self) -> String {
        let readers = read_lock(&self.readers_);
        readers.manifest_builder().to_string()
    }

    pub fn redo(&mut self) -> MyResult<()> {
        {
            let mut wal = write_lock(&self.wal_);

            if wal.seg_count() == 0 {
                return Ok(());
            }

            info!("redoing...");

            let work_dir = Path::new(&self.opt_.work_dir);

            let mut threads = Vec::with_capacity(wal.segs.len());

            for seg in &wal.segs {
                let opt = self.opt_.clone();
                let path = work_dir.join(make_file_name(self.new_file_number(), "sst"));
                let seg = seg.clone()?;
                threads.push(thread::spawn(move || {
                    info!("building sstable {:?}...", path);
                    let st = time::SystemTime::now();
                    let t = seg.build_sstable(&opt, &path).unwrap();
                    info!(
                        "build sstable {:?} cost: {}ms",
                        path,
                        st.elapsed().unwrap().as_millis()
                    );
                    t.map(|_| path)
                }));
            }

            let table_opt = self.opt_.get_table_opt();

            let readers = threads
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .filter(Option::is_some)
                .map(Option::unwrap)
                .map(|path| TableReader::new(&path, table_opt.clone()).unwrap())
                .collect();

            {
                let mut readers_group = write_lock(&self.readers_);
                readers_group.add_readers(0, readers)?;
            }

            for seg in &mut wal.segs {
                seg.delete()?;
            }
        }

        self.wal_ = Arc::new(RwLock::new(WAL::new(self.opt_.clone())?));

        assert_eq!(0, read_lock(&self.wal_).seg_count());

        info!("redo done!");

        Ok(())
    }

    pub fn insert(&self, k: StoreKey, v: StorePayload) -> MyResult<Option<StorePayload>> {
        self.insert_with_option(k, Some(v))
    }

    fn insert_with_option(
        &self,
        k: StoreKey,
        v: Option<StorePayload>,
    ) -> MyResult<Option<StorePayload>> {
        let encoded_v = serialize(&v)?;
        let r = self.insert_(k, Slice::from(encoded_v))?;
        Ok(r.and_then(|_| v))
    }

    fn insert_(&self, k: Slice, v: Slice) -> MyResult<Option<Slice>> {
        let mut wal = write_lock(&self.wal_);
        wal.append(&k, &v)?;

        let mut muttable = write_lock(&self.mut_);
        let r = muttable.insert(k, v);

        if wal.current_seg_size()? >= self.opt_.mem_table_max_size {
            let copied = muttable.clone();
            {
                let mut immuttable = write_lock(&self.imm_);
                immuttable.add(copied);
            }
            muttable.clear();
            wal.new_seg()?;
        }

        Ok(r)
    }

    pub fn get<K: ?Sized>(&self, k: &K) -> MyResult<Option<StorePayload>>
    where
        K: Borrow<StoreKey>,
    {
        let k = k.borrow();

        let muttable = read_lock(&self.mut_);
        let immuttable = read_lock(&self.imm_);

        let mut r = muttable.get(k);
        if r.is_none() {
            r = immuttable.get(k);
        }

        if let Some(r) = r {
            Ok(deserialize(r.borrow())?)
        } else {
            let readers = read_lock(&self.readers_);
            let x: Option<Slice> = readers.get(k)?;
            Ok(x.and_then(|x| deserialize(x.borrow()).unwrap()))
        }
    }

    pub fn remove<K>(&self, k: &K) -> MyResult<Option<StorePayload>>
    where
        K: Borrow<StoreKey>,
    {
        let r = self.get(k.borrow())?;
        if r.is_some() {
            self.insert_with_option(k.borrow().clone(), None)?;
        }
        Ok(r)
    }

    fn minor_compaction(&self) -> MyResult<()> {
        let imm = read_lock(&self.imm_);
        let c = imm.table_count();
        if c == 0 {
            return Ok(());
        }
        drop(imm);

        let mut wal = write_lock(&self.wal_);
        let imm = read_lock(&self.imm_);

        let mut iter = imm.tables_iter().rev();
        let work_dir = Path::new(&self.opt_.work_dir);
        for _ in 0..c {
            let memtable = iter.next().unwrap();
            let path = work_dir.join(make_file_name(self.new_file_number(), "sst"));
            if let Some((_, reader)) = memtable.build_sstable(&self.opt_, &path)? {
                let mut readers = write_lock(&self.readers_);
                readers.add(0, reader)?;
            }
            wal.consume_seg()?;
        }
        drop(imm);
        drop(wal);
        let mut imm = write_lock(&self.imm_);
        for _ in 0..c {
            imm.consume();
        }
        Ok(())
    }

    pub fn major_compaction(&self) -> MyResult<()> {
        let levels = {
            let readers = read_lock(&self.readers_);
            readers.compute_compaction_levels()
        };
        if !levels.is_empty() {
            info!("size compaction: {:?}", levels);
            self.size_compaction(levels)?;
        } else {
            self.seek_compaction()?;
        }
        Ok(())
    }

    fn size_compaction(&self, levels: Vec<usize>) -> MyResult<()> {
        // TODO: process all levels
        let level = levels[0];

        if level >= self.opt_.max_level - 1 {
            return Ok(());
        }

        let readers_group = read_lock(&self.readers_);
        let readers = readers_group.get_readers(level);

        let mut inputs0: Vec<&TableReader>;

        if level == 0 {
            inputs0 = readers.iter().rev().collect();
        } else {
            let last_compact_key = self.last_compact_keys_.get(level);

            inputs0 = readers
                .iter()
                .filter(|reader| {
                    last_compact_key.is_none() || reader.max_key() > last_compact_key.unwrap()
                })
                .collect();

            if inputs0.is_empty() {
                inputs0.push(&readers[0]);
            }
        }

        let (max, min) = inputs0.iter().fold((None, None), |a, b| {
            if let (Some(max), Some(min)) = a {
                (
                    Some(::std::cmp::max(max, b.max_key())),
                    Some(::std::cmp::min(min, b.min_key())),
                )
            } else {
                (Some(b.max_key()), Some(b.min_key()))
            }
        });

        let min_key = min.unwrap();
        let max_key = max.unwrap();
        let readers = readers_group.get_readers(level + 1);
        let inputs1 = self.get_other_readers(&min_key, &max_key, readers);

        let mut iters = vec![];

        for reader in &inputs0 {
            iters.push(reader.iter());
        }

        for reader in &inputs1 {
            iters.push(reader.iter());
        }

        let mut merger = Merger::new(iters);

        let work_dir = Path::new(&self.opt_.work_dir);

        let table_opt = self.opt_.get_table_opt();
        let mut table = None;
        let mut new_readers = vec![];

        while let Some((k, v)) = merger.next() {
            if table.is_none() {
                let path = work_dir.join(make_file_name(self.new_file_number(), "sst"));
                table = Some(TableBuilder::new(&path, table_opt.clone())?);
            }

            let is_full = {
                let table_ = table.as_mut().unwrap();

                table_.add(&k, &v)?;

                table_.total_size_estimate() >= self.opt_.sst_max_size
            };

            if !is_full {
                continue;
            }

            let mut table_ = table.take().unwrap();
            let path = &table_.path().clone();
            table_.flush()?;
            let reader = TableReader::new(path, table_opt.clone())?;
            new_readers.push(reader);
        }

        if let Some(mut table_) = table.take() {
            let path = &table_.path().clone();
            table_.flush()?;
            let reader = TableReader::new(&path, table_opt.clone())?;
            new_readers.push(reader);
        }

        let file_names0 = inputs0.iter().map(|x| x.file_name().clone()).collect();
        let file_names1 = inputs1.iter().map(|x| x.file_name().clone()).collect();

        drop(readers_group);

        let mut readers_group = write_lock(&self.readers_);

        readers_group.remove_by_file_names(level, &file_names0)?;
        readers_group.remove_by_file_names(level + 1, &file_names1)?;

        readers_group.add_readers(level + 1, new_readers)?;

        Ok(())
    }

    fn seek_compaction(&self) -> MyResult<()> {
        Ok(())
    }

    fn get_other_readers<'a>(
        &'a self,
        min_key: &[u8],
        max_key: &[u8],
        readers: &'a [TableReader],
    ) -> Vec<&'a TableReader> {
        readers
            .iter()
            .take_while(|x| x.min_key().as_slice() <= max_key)
            .filter(|x| x.max_key().as_slice() >= min_key)
            .collect()
    }

    #[cfg(test)]
    fn clear_memtables(&self) {
        let mut muttable = write_lock(&self.mut_);
        let mut immuttable = write_lock(&self.imm_);
        muttable.clear();
        immuttable.clear();
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::time;

    use crate::test_utils::get_test_opt;

    use super::*;

    fn make_key(k: Vec<u8>) -> StoreKey {
        Slice::from(k)
    }

    fn make_payload(v: Vec<u8>) -> StorePayload {
        StorePayload::new(Slice::from(v), 0, 0, 0, 0)
    }

    fn get_data() -> HashMap<StoreKey, StorePayload> {
        (b'a'..=b'f')
            .into_iter()
            .map(|x| (make_key(vec![x]), make_payload(vec![x; 20])))
            .collect::<HashMap<_, _>>()
    }

    #[test]
    fn test_fault_tolerance() -> MyResult<()> {
        let mut opt = get_test_opt();
        opt.imm_mem_table_max_count = 3;
        opt.mem_table_max_size = 20;
        opt.sst_max_size = 60;
        opt.l0_compaction_trigger = 1;

        let dm = DataManager::new(opt.clone())?;

        let mut data = get_data();

        let mut meet_b = false;
        let mut updated_b = false;

        let st = time::SystemTime::now();
        for (k, v) in &data {
            println!("{}: {}", to_str(k), to_str(&v.data));
            dm.insert(k.clone(), v.clone())?;
            if meet_b {
                dm.insert(make_key(b"b".to_vec()), make_payload(b"none".to_vec()))?;
                updated_b = true;
            }
            if k == &b"b".to_vec() {
                meet_b = true;
            }
        }
        println!("insert cost: {}ms", st.elapsed().unwrap().as_millis());

        if updated_b {
            data.insert(make_key(b"b".to_vec()), make_payload(b"none".to_vec()));
        }

        let st = time::SystemTime::now();
        for (k, v) in &data {
            let r = dm.get(k)?;
            assert_eq!(Some(v.clone()), r);
        }
        println!("get cost: {}ms", st.elapsed().unwrap().as_millis());

        let deleted = vec![make_key(b"e".to_vec())];
        for k in &deleted {
            data.remove(k);
            dm.remove(k)?;
        }

        for k in &deleted {
            let r = dm.get(k)?;
            assert_eq!(None, r)
        }

        // mock abnormal exit
        dm.clear_memtables();

        // cannot get data
        for (k, _v) in &data {
            let r = dm.get(k)?;
            assert_eq!(None, r);
        }

        // load from wal
        println!("loading from wal");
        let st = time::SystemTime::now();
        let dm = DataManager::new(opt.clone())?;
        println!("load wal cost: {}ms", st.elapsed().unwrap().as_millis());

        // can get data now!
        let st = time::SystemTime::now();
        for (k, v) in &data {
            let r = dm.get(k)?;
            assert_eq!(Some(v.clone()), r);
        }
        println!("get cost: {}ms", st.elapsed().unwrap().as_millis());

        data.insert(make_key(b"x".to_vec()), make_payload(b"xxx".to_vec()));
        data.insert(make_key(b"y".to_vec()), make_payload(b"yyy".to_vec()));
        data.insert(make_key(b"z".to_vec()), make_payload(b"zzz".to_vec()));
        dm.insert(make_key(b"x".to_vec()), make_payload(b"xxx".to_vec()))?;
        dm.insert(make_key(b"y".to_vec()), make_payload(b"yyy".to_vec()))?;
        dm.insert(make_key(b"z".to_vec()), make_payload(b"zzz".to_vec()))?;

        for (k, v) in &data {
            let r = dm.get(k)?;
            assert_eq!(Some(v.clone()), r);
        }

        for k in &deleted {
            let r = dm.get(k)?;
            assert_eq!(None, r)
        }

        // load from wal
        println!("loading from wal");
        let st = time::SystemTime::now();
        let dm: Arc<DataManager> = DataManager::new(opt.clone())?;
        println!("load wal cost: {}ms", st.elapsed().unwrap().as_millis());

        // can get data now!
        for (k, v) in &data {
            let r = dm.get(k)?;
            assert_eq!(Some(v.clone()), r);
        }

        println!("loading from wal");
        let st = time::SystemTime::now();
        let dm: Arc<DataManager> = DataManager::new(opt.clone())?;
        println!("load wal cost: {}ms", st.elapsed().unwrap().as_millis());

        // compaction
        let st = time::SystemTime::now();
        dm.minor_compaction()?;
        println!(
            "minor compaction cost: {}ms",
            st.elapsed().unwrap().as_millis()
        );
        let st = time::SystemTime::now();
        dm.major_compaction()?;
        println!(
            "major compaction cost: {}ms",
            st.elapsed().unwrap().as_millis()
        );
        println!("info: {}", dm.info());

        // can get data now!
        for (k, v) in &data {
            let r = dm.get(k)?;
            assert_eq!(Some(v.clone()), r);
        }

        Ok(())
    }
}
