use std::borrow::Borrow;
use std::cmp::min;
use std::collections::linked_list::Iter as LinkedListIter;
use std::collections::LinkedList;
use std::fmt::Debug;
use std::fs::remove_file;
use std::fs::File;
use std::fs::OpenOptions;
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

use glob::glob;
use integer_encoding::FixedInt;
use memmap::Mmap;
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
use crate::slice::Slice;
use crate::sstable_builder::skiplist_to_sstable;
use crate::utils::make_file_name;

fn padding(len: usize) -> usize {
    4usize.wrapping_sub(len) & 7
}

fn copy_memory(src: &[u8], dst: &mut [u8]) {
    let len_src = src.len();
    assert!(dst.len() >= len_src);

    unsafe {
        ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), len_src);
    }
}

pub struct WALSeg {
    file: File,
    size_: usize,
    path: PathBuf,
}

impl WALSeg {
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
        })
    }

    pub fn iter(&self) -> MyResult<WALSegIter> {
        WALSegIter::new(&self.path)
    }

    pub fn size(&self) -> usize {
        self.size_
    }

    pub fn append(&mut self, key: &Slice, value: &Slice) -> MyResult<()> {
        let mut encoder = Encoder::new();
        let key_buf = encoder.compress_vec(key.as_ref())?;
        let value_buf = encoder.compress_vec(value.as_ref())?;

        let key_size = key_buf.len();
        let value_size = value_buf.len();
        let size_space = u32::required_space();

        let padding = padding(key_size + value_size);

        let mut buf = vec![0; size_space * 2 + key_size + value_size + padding];

        // size
        ((key_size + value_size) as u32).encode_fixed(&mut buf[..size_space]);

        // key size
        (key_size as u32).encode_fixed(&mut buf[size_space..size_space * 2]);

        // key
        copy_memory(&key_buf, &mut buf[size_space * 2..]);

        // value
        copy_memory(&value_buf, &mut buf[size_space * 2 + key_size..]);

        // padding
        if padding > 0 {
            let zeros: [u8; 8] = [0; 8];
            copy_memory(
                &zeros[..padding],
                &mut buf[size_space * 2 + key_size + value_size..],
            );
        }

        self.file.write_all(&buf)?;

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

    pub fn to_skiplist(&self, opt: &Options) -> MyResult<SkipList<Slice, Slice>> {
        let mut map = SkipList::new(opt.mem_table_max_height);
        for (k, v) in self.iter()? {
            map.insert(k, v);
        }
        Ok(map)
    }

    pub fn build_sstable(
        &self,
        opt: &Options,
        path: &Path,
    ) -> MyResult<Option<(String, TableReader)>> {
        let map = self.to_skiplist(opt)?;
        skiplist_to_sstable(&map, opt, path)
    }
}

pub struct WALSegIter {
    offset: usize,
    mmap: Mmap,
    file_size: usize,
}

impl WALSegIter {
    pub fn new<T: AsRef<Path>>(path: T) -> MyResult<Self> {
        let file = OpenOptions::new().read(true).open(&path)?;

        let file_size = file.metadata()?.len() as usize;

        let mmap = unsafe { Mmap::map(&file)? };

        Ok(WALSegIter {
            file_size,
            mmap,
            offset: 0,
        })
    }
}

impl Iterator for WALSegIter {
    type Item = (Slice, Slice);

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.file_size {
            return None;
        }

        let size = u32::decode_fixed(&self.mmap[self.offset..self.offset + u32::required_space()])
            as usize;

        if size == 0 {
            return None;
        }

        let offset = self.offset + u32::required_space();

        let key_size =
            u32::decode_fixed(&self.mmap[offset..offset + u32::required_space()]) as usize;

        let offset = offset + u32::required_space();

        let data = &self.mmap[offset..offset + size];
        let key_data = &data[..key_size];
        let value_data = &data[key_size..];
        let key = Decoder::new()
            .decompress_vec(&key_data)
            .expect("snap decompress key in wal file error");
        let value = Decoder::new()
            .decompress_vec(&value_data)
            .expect("snap decompress value in wal file error");

        self.offset = offset + size + padding(size);

        Some((Slice::from(key), Slice::from(value)))
    }
}

pub struct WAL {
    opt: Options,
    pub segs: LinkedList<WALSeg>,
    current_file_num: usize,
}

impl WAL {
    pub fn new(opt: Options) -> MyResult<Self> {
        let path = Path::new(&opt.work_dir);
        let mut paths = vec![];
        for entry in glob(path.join("*.wal").to_str().expect("path to str"))? {
            if let Ok(path) = entry {
                paths.push(path);
            }
        }
        paths.sort();
        let segs = paths
            .iter()
            .map(|p| {
                let seg = WALSeg::new(&p.as_path(), opt.mem_table_max_size).expect("new wal seg");
                if seg.file.metadata().unwrap().len() == 0 {
                    remove_file(&seg.path).unwrap();
                    None
                } else {
                    Some(seg)
                }
            })
            .filter(Option::is_some)
            .map(Option::unwrap)
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

    pub fn append(&mut self, key: &Slice, value: &Slice) -> MyResult<()> {
        if self.seg_count() == 0 {
            self.new_seg()?;
        }
        if let Some(seg) = &mut self.segs.back_mut() {
            return seg.append(key, value);
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

    pub fn iter(&self) -> MyResult<WALIter> {
        Ok(WALIter::new(&self))
    }
}

pub struct WALIter<'a> {
    segs_iter: LinkedListIter<'a, WALSeg>,
    seg_iter: Option<WALSegIter>,
}

impl<'a> WALIter<'a> {
    pub fn new(wal: &'a WAL) -> Self {
        WALIter {
            segs_iter: wal.segs.iter(),
            seg_iter: None,
        }
    }
}

impl<'a> Iterator for WALIter<'a> {
    type Item = (Slice, Slice);

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
        kvs.push((
            b"a".to_vec(),
            b"abcasldkfjaoiwejfawoejfoaisjdflaskdjfoias".to_vec(),
        ));
        kvs.push((b"b".to_vec(), b"bbcasdlfjasldfj".to_vec()));
        kvs.push((b"c".to_vec(), b"cbcasldfjowiejfoaisdjfalskdfj".to_vec()));
        let st = time::SystemTime::now();
        for (k, v) in &kvs {
            seg.append(&Slice::from(k.clone()), &Slice::from(v.clone()))?;
        }
        println!("append cost: {}us", st.elapsed().unwrap().as_micros());
        let mut iter = seg.iter()?;
        let st = time::SystemTime::now();
        for (k, v) in &kvs {
            assert_eq!(
                Some((Slice::from(k.clone()), Slice::from(v.clone()))),
                iter.next()
            );
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
        kvs.push((
            b"a".to_vec(),
            b"abcasldkfjaoiwejfawoejfoaisjdflaskdjfoias".to_vec(),
        ));
        kvs.push((b"b".to_vec(), b"bbcasdlfjasldfj".to_vec()));
        kvs.push((b"c".to_vec(), b"cbcasldfjowiejfoaisdjfalskdfj".to_vec()));
        for (k, v) in &kvs {
            wal.new_seg()?;
            wal.append(&Slice::from(k.clone()), &Slice::from(v.clone()))?;
        }
        let mut wal = WAL::new(opt.clone())?;
        let mut iter = wal.iter()?;
        for (k, v) in &kvs {
            assert_eq!(
                Some((Slice::from(k.clone()), Slice::from(v.clone()))),
                iter.next()
            );
        }
        assert_eq!(None, iter.next());
        wal.truncate(1)?;
        let mut iter = wal.iter()?;
        for (i, (k, v)) in kvs.iter().enumerate() {
            if i == 0 {
                continue;
            }
            assert_eq!(
                Some((Slice::from(k.clone()), Slice::from(v.clone()))),
                iter.next()
            );
        }
        assert_eq!(None, iter.next());
        wal.truncate(1)?;
        let wal = WAL::new(opt.clone())?;
        let mut iter = wal.iter()?;
        for (i, (k, v)) in kvs.iter().enumerate() {
            if i <= 1 {
                continue;
            }
            assert_eq!(
                Some((Slice::from(k.clone()), Slice::from(v.clone()))),
                iter.next()
            );
        }
        assert_eq!(None, iter.next());
        Ok(())
    }
}
