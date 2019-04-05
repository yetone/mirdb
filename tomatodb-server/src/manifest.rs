use std::fs::File;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use bincode::deserialize_from;
use bincode::serialize;
use serde::{Deserialize, Serialize};

use crate::error::MyResult;
use crate::options::Options;

const MANIFEST_FILENAME: &'static str = "MANIFEST";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMeta {
    pub max_key: Vec<u8>,
    pub min_key: Vec<u8>,
    pub file_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LevelMeta {
    pub file_metas: Vec<FileMeta>,
}

impl LevelMeta {
    pub fn new() -> Self {
        LevelMeta {
            file_metas: vec![],
        }
    }

    pub fn push_file_meta(&mut self, file_meta: FileMeta) {
        self.file_metas.push(file_meta);
    }

    pub fn seek_file_min_key(&self, min_key: &[u8]) -> usize {
        let mut l = 0;
        let mut r = self.file_metas.len() - 1;
        while l < r {
            let m = (l + r) / 2;
            if &(self.file_metas[m].min_key)[..] < min_key {
                l = m + 1;
            } else {
                r = m;
            }
        }
        l
    }

    pub fn add_file_meta(&mut self, file_meta: FileMeta) {
        if self.file_metas.len() == 0 {
            self.push_file_meta(file_meta);
            return;
        }
        let i = self.seek_file_min_key(&file_meta.min_key);
        self.file_metas.insert(i, file_meta);
    }

    pub fn search_file_meta(&self, key: &[u8]) -> Option<&FileMeta> {
        if self.file_metas.len() == 0 {
            return None;
        }
        let i = self.seek_file_min_key(key);
        Some(&self.file_metas[i])
    }

    pub fn remove_file_meta_by_file_name(&mut self, file_name: &String) {
        let mut i = 0;
        while i < self.file_metas.len() {
            let file_meta = &self.file_metas[i];
            i += 1;
            if &file_meta.file_name == file_name {
                break
            }
        }
        if i < self.file_metas.len() {
            self.file_metas.remove(i);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub level_metas: Vec<LevelMeta>,
    pub current_file_number: usize,
}

pub struct ManifestBuilder {
    opt: Options,
    manifest_: Manifest,
}

impl Manifest {
    fn new(opt: &Options) -> Self {
        Manifest {
            level_metas: Vec::with_capacity(opt.max_level),
            current_file_number: 0,
        }
    }

    pub fn new_file_number(&mut self) -> usize {
        let r = self.current_file_number;
        self.current_file_number += 1;
        r
    }

    pub fn gen_path(opt: &Options) -> PathBuf {
        let p = Path::new(&opt.work_dir);
        p.join(MANIFEST_FILENAME)
    }

    pub fn load(opt: &Options) -> MyResult<Self> {
        let p = Manifest::gen_path(opt);
        if !p.exists() {
            return Ok(Manifest::new(opt));
        }
        let f = File::open(&p)?;
        Ok(deserialize_from(f)?)
    }

    pub fn flush<T: Write>(&self, w: &mut T) -> MyResult<()> {
        w.write(&serialize(self)?)?;
        Ok(())
    }

    fn ensure_level(&mut self, level: usize) {
        while self.level_metas.len() < level + 1 {
            self.level_metas.push(LevelMeta::new());
        }
    }

    pub fn add_file_meta(&mut self, level: usize, file_meta: FileMeta) {
        self.ensure_level(level);
        if level == 0 {
            self.level_metas[level].push_file_meta(file_meta);
        } else {
            self.level_metas[level].add_file_meta(file_meta);
        }
    }

    pub fn remove_file_meta_by_file_name(&mut self, level: usize, file_name: &String) {
        self.ensure_level(level);
        self.level_metas[level].remove_file_meta_by_file_name(file_name)

    }

    pub fn file_metas(&self, level: usize) -> Option<&Vec<FileMeta>> {
        if self.level_metas.len() <= level {
            None
        } else {
            Some(&self.level_metas[level].file_metas)
        }
    }

    pub fn search_file_metas(&self, level: usize, key: &[u8]) -> Vec<&FileMeta> {
        let mut res = vec![];
        if self.level_metas.len() <= level {
            return res;
        } else if level == 0 {
            if let Some(file_metas) = self.file_metas(level) {
                let mut l = file_metas.len();
                while l > 0 {
                    l -= 1;
                    let file_meta = &file_metas[l];
                    if &(file_meta.min_key)[..] <= key && &(file_meta.max_key)[..] >= key {
                        res.push(file_meta);
                    }
                }
            }
        } else if let Some(file_meta) = self.level_metas[level].search_file_meta(key) {
            res.push(file_meta);
        }
        res
    }
}

impl ManifestBuilder {
    pub fn new(opt: Options) -> MyResult<Self> {
        Ok(ManifestBuilder {
            opt: opt.clone(),
            manifest_: Manifest::load(&opt)?,
        })
    }

    pub fn file_metas(&self, level: usize) -> Option<&Vec<FileMeta>> {
        self.manifest_.file_metas(level)
    }

    pub fn add_file_meta(&mut self, level: usize, file_meta: FileMeta) {
        assert!(level <= self.opt.max_level);
        self.manifest_.add_file_meta(level, file_meta)
    }

    pub fn remove_file_meta_by_file_name(&mut self, level: usize, file_name: &String) {
        assert!(level <= self.opt.max_level);
        self.manifest_.remove_file_meta_by_file_name(level, file_name)
    }

    pub fn search_file_metas(&self, level: usize, key: &[u8]) -> Vec<&FileMeta> {
        assert!(level <= self.opt.max_level);
        self.manifest_.search_file_metas(level, key)
    }

    pub fn manifest(&self) -> &Manifest {
        &self.manifest_
    }

    pub fn flush(&self) -> MyResult<()> {
        let mut file_ = File::create(Manifest::gen_path(&self.opt))?;
        self.manifest_.flush(&mut file_)
    }

    pub fn new_file_number(&mut self) -> usize {
        self.manifest_.new_file_number()
    }
}
