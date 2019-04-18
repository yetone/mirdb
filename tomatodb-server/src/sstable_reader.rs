use std::borrow::Borrow;
use std::collections::HashMap;
use std::fs::remove_file;
use std::io::Cursor;
use std::path::Path;

use bincode::deserialize;
use serde::Deserialize;

use sstable::TableReader;

use crate::error::MyResult;
use crate::manifest::FileMeta;
use crate::manifest::ManifestBuilder;
use crate::options::Options;
use crate::store::StoreKey;
use crate::store::StorePayload;
use crate::utils::to_str;

pub struct SstableReader {
    opt_: Options,
    readers_: Vec<Vec<TableReader>>,
    manifest_builder_: ManifestBuilder,
}

fn table_reader_to_file_meta(reader: &TableReader) -> FileMeta {
    FileMeta {
        file_name: reader.file_name().clone(),
    }
}

fn sort_readers(readers: &mut Vec<TableReader>) {
    readers.sort_by(|a, b| a.min_key().cmp(&b.min_key()))
}

impl SstableReader {
    pub fn new(opt: Options) -> MyResult<Self> {
        let readers_ = Vec::with_capacity(opt.max_level);
        let mut r = SstableReader {
            opt_: opt.clone(),
            readers_,
            manifest_builder_: ManifestBuilder::new(opt)?
        };
        r.load()?;
        Ok(r)
    }

    fn load_reader(&self, file_meta: &FileMeta) -> MyResult<TableReader> {
        let path = Path::new(&self.opt_.work_dir);
        let path = path.join(&file_meta.file_name);
        Ok(TableReader::new(&path, self.opt_.to_table_opt())?)
    }

    pub fn get_readers(&self, level: usize) -> &Vec<TableReader> {
        assert!(level < self.opt_.max_level);
        &self.readers_[level]
    }

    pub fn search_readers(&self, level: usize, key: &[u8]) -> Vec<&TableReader> {
        let mut res = vec![];

        if self.readers_.len() <= level {
            return res;
        }

        let readers = self.get_readers(level);

        if level == 0 {
            for reader in readers.iter().rev() {
                if &(reader.min_key())[..] <= key && &(reader.max_key())[..] >= key {
                    res.push(reader);
                }
            }
        } else if readers.len() > 0 {
            let mut left = 0;
            let mut right = readers.len() - 1;

            while left < right {
                let middle = (left + right + 1) / 2;
                if &readers[middle].min_key()[..] < key {
                    left = middle;
                } else {
                    right = middle - 1;
                }
            }

            assert_eq!(left, right);

            for i in left..readers.len() {
                let reader = &readers[i];
                if &(reader.min_key())[..] <= key && &(reader.max_key())[..] >= key {
                    res.push(reader);
                    continue;
                }
                if &(reader.min_key())[..] > key  {
                    break;
                }
            }
        }

        res
    }

    pub fn load(&mut self) -> MyResult<()> {
        for i in 0..self.opt_.max_level {
            let mut readers = vec![];
            if let Some(fms) = self.manifest_builder_.file_metas(i) {
                if readers.len() < fms.len() {
                    readers.reserve(fms.len() - readers.len());
                }
                for fm in fms {
                    let reader = self.load_reader(fm)?;
                    readers.push(reader);
                }
                if i != 0 {
                    sort_readers(&mut readers);
                }
            }
            self.readers_.push(readers);
        }
        Ok(())
    }

    pub fn add(&mut self, level: usize, reader: TableReader) -> MyResult<()> {
        self.add_readers(level, vec![reader])
    }

    pub fn add_readers(&mut self, level: usize, readers: Vec<TableReader>) -> MyResult<()> {
        assert!(level < self.opt_.max_level);

        for reader in readers {
            self.manifest_builder_.add_file_meta(level, table_reader_to_file_meta(&reader));
            let readers = &mut self.readers_[level];
            readers.push(reader);
            if level != 0 {
                sort_readers(readers);
            }
        }

        self.manifest_builder_.flush()?;
        Ok(())
    }

    pub fn remove_by_file_names(&mut self, level: usize, file_names: &Vec<String>) -> MyResult<()> {
        assert!(level < self.opt_.max_level);

        for file_name in file_names {
            self.manifest_builder_.remove_file_meta_by_file_name(level, &file_name);
            let readers = &mut self.readers_[level];
            let mut i = 0;
            for reader in readers.iter() {
                if &reader.file_name() == &file_name {
                    break;
                }
                i += 1;
            }
            if i < readers.len() {
                readers.remove(i);
            }
        }

        self.manifest_builder_.flush()?;

        let work_dir = Path::new(&self.opt_.work_dir);
        for file_name in file_names {
            let path = work_dir.join(file_name);
            remove_file(&path)?;
        }

        Ok(())
    }

    pub fn manifest_builder(&self) -> &ManifestBuilder {
        &self.manifest_builder_
    }

    pub fn manifest_builder_mut(&mut self) -> &mut ManifestBuilder {
        &mut self.manifest_builder_
    }

    pub fn get<K, V>(&self, k: K) -> MyResult<Option<V>>
        where
            K: Borrow<[u8]>,
            for<'de> V: Deserialize<'de> {
        for i in 0..self.opt_.max_level {
            let readers = self.search_readers(i, k.borrow());
            for reader in readers {
                if let Some(encoded) = reader.get(k.borrow())? {
                    let decoded = deserialize(&encoded)?;
                    return Ok(Some(decoded));
                }
            }
        }
        Ok(None)
    }

    pub fn compute_compaction_levels(&self) -> Vec<usize> {
        let mut scores = Vec::with_capacity(self.opt_.max_level);
        for i in 0..self.opt_.max_level {
            let readers = self.get_readers(i);
            let score = if i == 0 {
                readers.len() as f64 / self.opt_.l0_compaction_trigger as f64
            } else {
                readers.iter().map(|x| x.size()).sum::<usize>() as f64 / self.max_bytes_for_level(i)
            };
            if score >= 1. {
                scores.push((i, score))
            }
        }
        scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        scores.iter().map(|x| x.0).collect()
    }

    fn max_bytes_for_level(&self, level: usize) -> f64 {
        let mut level = level;
        let mut result = 10. * 1048576.;
        while level > 1 {
            result *= 10.;
            level -= 1;
        }
        result
    }
}