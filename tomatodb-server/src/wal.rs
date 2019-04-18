use std::borrow::Borrow;
use std::cmp::min;
use std::collections::linked_list::Iter as LinkedListIter;
use std::collections::LinkedList;
use std::fmt::Debug;
use std::fs::File;
use std::fs::OpenOptions;
use std::fs::remove_file;
use std::io::Cursor;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::marker::PhantomData;
use std::num::Wrapping;
use std::ops::DerefMut;
use std::path::Path;
use std::path::PathBuf;
use std::ptr;

use bincode::deserialize;
use bincode::serialize;
use glob::glob;
use integer_encoding::FixedInt;
use memmap::Mmap;
use serde::{Deserialize, Serialize};
use snap::Decoder;
use snap::Encoder;

use skip_list::SkipList;
use sstable::RandomAccess;
use sstable::TableBuilder;
use sstable::TableReader;

use crate::error::err;
use crate::error::MyResult;
use crate::error::StatusCode;
use crate::options::Options;
use crate::sstable_builder::skiplist_to_sstable;
use crate::utils::make_file_name;

fn padding(len: usize) -> usize {
    4usize.wrapping_sub(len) & 7
}

fn copy_memory(src: &[u8], dst: &mut [u8]) {
    let len_src = src.len();
    assert!(dst.len() >= len_src);

    unsafe {
        ptr::copy_nonoverlapping(
            src.as_ptr(),
            dst.as_mut_ptr(),
            len_src
        );
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct LogEntry<K, V> {
    k: K,
    v: V,
}

impl<K, V> LogEntry<K, V> {
    pub fn new(k: K, v: V) -> Self {
        LogEntry { k, v }
    }

    pub fn key(&self) -> &K {
        &self.k
    }

    pub fn value(&self) -> &V {
        &self.v
    }

    pub fn kv(self) -> (K, V) {
        (self.k, self.v)
    }
}

pub struct WALSeg<K, V> {
    file: File,
    size_: usize,
    path: PathBuf,
    k: PhantomData<K>,
    v: PhantomData<V>,
}

impl<K: Serialize, V: Serialize> WALSeg<K, V> {
    pub fn new<T: AsRef<Path>>(path: T, _capacity: usize) -> MyResult<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.as_ref())?;

        Ok(WALSeg {
            file,
            size_: 0,
            path: path.as_ref().to_path_buf(),
            k: PhantomData,
            v: PhantomData,
        })
    }

    pub fn iter(&self) -> MyResult<WALSegIter<K, V>> {
        WALSegIter::new(&self.path)
    }

    pub fn size(&self) -> usize {
        self.size_
    }

    pub fn append(&mut self, entry: &LogEntry<K, V>) -> MyResult<()> {
        let entry_buf = serialize(entry)?;
        let entry_buf = Encoder::new().compress_vec(&entry_buf)?;

        let entry_size = entry_buf.len();
        let size_space = u32::required_space();

        let padding = padding(entry_size);

        let mut buf = vec![0; size_space + entry_size + padding];

        // size
        (entry_size as u32).encode_fixed(&mut buf[..size_space]);

        // entry
        copy_memory(&entry_buf, &mut buf[size_space..]);

        // padding
        if padding > 0 {
            let zeros: [u8; 8] = [0; 8];
            copy_memory(&zeros[..padding], &mut buf[size_space + entry_size..]);
        }

        self.file.write(&buf)?;

        self.file.flush()?;

        self.size_ += buf.len();

        Ok(())
    }

    pub fn clone(&self) -> MyResult<Self> {
        Self::new(&self.path, 0)
    }

    pub fn delete(&self) -> MyResult<()> {
        remove_file(&self.path)?;
        Ok(())
    }
}

impl<V> WALSeg<Vec<u8>, V>
    where for<'de> V: Serialize + Deserialize<'de> {

    fn to_skiplist(&self, opt: &Options) -> MyResult<SkipList<Vec<u8>, V>> {
        let mut map = SkipList::new(opt.mem_table_max_height);
        for entry in self.iter()? {
            map.insert(entry.k, entry.v);
        }
        Ok(map)
    }

    pub fn build_sstable(&self, opt: &Options, path: &Path) -> MyResult<Option<(String, TableReader)>> {
        let map = self.to_skiplist(opt)?;
        skiplist_to_sstable(&map, opt, path)
    }
}

pub struct WALSegIter<K, V> {
    offset: usize,
    mmap: Mmap,
    file_size: usize,
    k: PhantomData<K>,
    v: PhantomData<V>,
}

impl<K, V> WALSegIter<K, V> {
    pub fn new<T: AsRef<Path>>(path: T) -> MyResult<Self> {
        let file = OpenOptions::new()
            .read(true)
            .open(&path)?;

        let file_size = file.metadata()?.len() as usize;

        let mmap = unsafe { Mmap::map(&file)? };

        Ok(WALSegIter {
            file_size,
            mmap,
            offset: 0,
            k: PhantomData,
            v: PhantomData,
        })
    }
}

impl<K, V> Iterator for WALSegIter<K, V>
    where
            for<'de> K: Deserialize<'de>,
            for<'de> V: Deserialize<'de> {

    type Item = LogEntry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.file_size {
            return None;
        }

        let size = u32::decode_fixed(&self.mmap[self.offset..self.offset + u32::required_space()]) as usize;

        if size == 0 {
            return None
        }

        let offset = self.offset + u32::required_space();
        let data = &self.mmap[offset..offset + size];
        let data = Decoder::new().decompress_vec(&data).expect("snap decompress wal file error");
        let entry: LogEntry<K, V> = deserialize(&data).expect("deserialize from wal file error");
        self.offset = offset + size + padding(size);

        Some(entry)
    }
}

pub struct WAL<K, V> {
    opt: Options,
    pub segs: LinkedList<WALSeg<K, V>>,
    current_file_num: usize,
}

impl<K: Serialize, V: Serialize> WAL<K, V> {
    pub fn new(opt: Options) -> MyResult<Self> {
        let path = Path::new(&opt.work_dir);
        let mut paths = vec![];
        for entry in glob(path.join("*.wal").to_str().expect("path to str"))? {
            match entry {
                Ok(path) => paths.push(path),
                _ => (),
            }
        }
        paths.sort();
        let segs = paths.iter().map(|p| {
            let seg = WALSeg::new(&p.as_path(), opt.mem_table_max_size).expect("new wal seg");
            if seg.file.metadata().unwrap().len() == 0 {
                remove_file(&seg.path).unwrap();
                None
            } else {
                Some(seg)
            }
        }).filter(|x| x.is_some())
            .map(|x| x.unwrap())
            .collect();
        Ok(WAL {
            opt,
            segs,
            current_file_num: 0,
        })
    }

    pub fn seg_count(&self) -> usize {
        self.segs.len()
    }

    pub fn append(&mut self, entry: &LogEntry<K, V>) -> MyResult<()> {
        if self.seg_count() == 0 {
            self.new_seg()?;
        }
        if let Some(seg) = &mut self.segs.back_mut() {
            return seg.append(entry);
        }
        err(StatusCode::WALError, "cannot get the tail wal seg")
    }

    pub fn truncate(&mut self, n: usize) -> MyResult<()> {
        for _ in 0..n {
            self.consume_seg()?;
        }
        Ok(())
    }

    pub fn consume_seg(&mut self) -> MyResult<()> {
        if let Some(seg) = &mut self.segs.pop_front() {
            seg.delete()?;
        }
        Ok(())
    }

    pub fn new_seg(&mut self) -> MyResult<()> {
        let file_num = self.new_file_num();
        let file_name = make_file_name(file_num, "wal");
        let path = Path::new(&self.opt.work_dir);
        let path = path.join(file_name);
        let seg = WALSeg::new(path.as_path(), self.opt.mem_table_max_size)?;
        self.segs.push_back(seg);
        Ok(())
    }

    pub fn current_seg_size(&self) -> MyResult<usize> {
        if let Some(seg) = self.segs.back() {
            return Ok(seg.size());
        }
        Ok(0)
    }

    fn new_file_num(&mut self) -> usize {
        let n = self.current_file_num;
        self.current_file_num += 1;
        n
    }

    pub fn iter(&self) -> MyResult<WALIter<K, V>> {
        Ok(WALIter::new(&self))
    }
}

pub struct WALIter<'a, K, V> {
    segs_iter: LinkedListIter<'a, WALSeg<K, V>>,
    seg_iter: Option<WALSegIter<K, V>>,
}

impl<'a, K, V> WALIter<'a, K, V> {
    pub fn new(wal: &'a WAL<K, V>) -> Self {
        WALIter {
            segs_iter: wal.segs.iter(),
            seg_iter: None,
        }
    }
}

impl<'a, K, V> Iterator for WALIter<'a, K, V>
    where
            for<'de> K: Serialize + Deserialize<'de>,
            for<'de> V: Serialize + Deserialize<'de> {

    type Item = LogEntry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(seg_iter) = &mut self.seg_iter {
            let n = seg_iter.next();
            if n.is_some() {
                return n;
            }
        }
        if let Some(seg) = self.segs_iter.next() {
            self.seg_iter = Some(seg.iter().expect("get wal seg iter"));
            return self.next();
        }
        None
    }
}

#[cfg(test)]
mod test {
    use crate::test_utils::get_test_opt;

    use super::*;

    #[test]
    fn test_wal_seg() -> MyResult<()> {
        use std::time;
        let p = Path::new("/tmp/wal");
        if p.exists() {
            remove_file(p)?;
        }
        let mut seg = WALSeg::new(&p, 1024)?;
        let mut kvs = Vec::with_capacity(3);
        kvs.push((b"a".to_vec(), b"abcasldkfjaoiwejfawoejfoaisjdflaskdjfoias".to_vec()));
        kvs.push((b"b".to_vec(), b"bbcasdlfjasldfj".to_vec()));
        kvs.push((b"c".to_vec(), b"cbcasldfjowiejfoaisdjfalskdfj".to_vec()));
        let st = time::SystemTime::now();
        for (k, v) in &kvs {
            let entry = LogEntry::new(k.clone(), Some(v.clone()));
            seg.append(&entry)?;
        }
        println!("append cost: {}us", st.elapsed().unwrap().as_micros());
        let mut iter = seg.iter()?;
        let st = time::SystemTime::now();
        for (k, v) in &kvs {
            let entry = LogEntry::new(k.clone(), Some(v.clone()));
            assert_eq!(Some(entry), iter.next());
        }
        println!("iter cost: {}us", st.elapsed().unwrap().as_micros());
        assert_eq!(None, iter.next());
        Ok(())
    }

    #[test]
    fn test_wal() -> MyResult<()> {
        let opt = get_test_opt();
        let mut wal = WAL::new(opt.clone())?;
        let mut kvs = Vec::with_capacity(3);
        kvs.push((b"a".to_vec(), b"abcasldkfjaoiwejfawoejfoaisjdflaskdjfoias".to_vec()));
        kvs.push((b"b".to_vec(), b"bbcasdlfjasldfj".to_vec()));
        kvs.push((b"c".to_vec(), b"cbcasldfjowiejfoaisdjfalskdfj".to_vec()));
        for (k, v) in &kvs {
            let entry = LogEntry::new(k.clone(), Some(v.clone()));
            wal.new_seg()?;
            wal.append(&entry)?;
        }
        let mut wal = WAL::new(opt.clone())?;
        let mut iter = wal.iter()?;
        for (k, v) in &kvs {
            let entry = LogEntry::new(k.clone(), Some(v.clone()));
            assert_eq!(Some(entry), iter.next());
        }
        assert_eq!(None, iter.next());
        wal.truncate(1)?;
        let mut iter = wal.iter()?;
        for (i, (k, v)) in kvs.iter().enumerate() {
            if i == 0 {
                continue;
            }
            let entry = LogEntry::new(k.clone(), Some(v.clone()));
            assert_eq!(Some(entry), iter.next());
        }
        assert_eq!(None, iter.next());
        wal.truncate(1)?;
        let wal = WAL::new(opt.clone())?;
        let mut iter = wal.iter()?;
        for (i, (k, v)) in kvs.iter().enumerate() {
            if i <= 1 {
                continue;
            }
            let entry = LogEntry::new(k.clone(), Some(v.clone()));
            assert_eq!(Some(entry), iter.next());
        }
        assert_eq!(None, iter.next());
        Ok(())
    }
}