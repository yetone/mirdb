use std::borrow::Borrow;
use std::collections::HashMap;
use std::io::Cursor;
use std::path::Path;

use bincode::deserialize_from;
use serde::de::DeserializeOwned;

use sstable::TableReader;

use crate::error::MyResult;
use crate::manifest::FileMeta;
use crate::manifest::ManifestBuilder;
use crate::options::Options;
use crate::store::StoreKey;
use crate::store::StorePayload;

pub struct SstableReader {
    opt_: Options,
    readers_: HashMap<String, TableReader>,
    manifest_builder_: ManifestBuilder,
}

fn table_reader_to_file_meta(reader: &TableReader) -> FileMeta {
    FileMeta {
        max_key: reader.max_key().clone(),
        min_key: reader.min_key().clone(),
        file_name: reader.file_name().clone(),
    }
}

impl SstableReader {
    pub fn new(opt: Options) -> MyResult<Self> {
        let mut r = SstableReader {
            opt_: opt.clone(),
            readers_: HashMap::new(),
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

    pub fn load(&mut self) -> MyResult<()> {
        for i in 0..self.opt_.max_level {
            if let Some(fms) = self.manifest_builder_.file_metas(i) {
                for fm in fms {
                    let reader = self.load_reader(fm)?;
                    println!("reader: {}", reader.file_name());
                    self.readers_.insert(reader.file_name().clone(), reader);
                }
            }
        }
        Ok(())
    }

    pub fn add(&mut self, level: usize, reader: TableReader) -> MyResult<()> {
        self.manifest_builder_.add_file_meta(level, table_reader_to_file_meta(&reader));
        self.readers_.insert(reader.file_name().to_owned(), reader);
        self.manifest_builder_.flush()?;
        Ok(())
    }

    pub fn manifest_builder(&self) -> &ManifestBuilder {
        &self.manifest_builder_
    }

    pub fn manifest_builder_mut(&mut self) -> &mut ManifestBuilder {
        &mut self.manifest_builder_
    }

    pub fn get<K: Borrow<[u8]>, V: DeserializeOwned>(&self, k: K) -> MyResult<Option<V>> {
        for i in 0..self.opt_.max_level {
            let fms = self.manifest_builder_.search_file_metas(i, k.borrow());
            for fm in fms {
                let reader = self.readers_.get(&fm.file_name)
                    .expect(&format!("cannot get table reader: {}", &fm.file_name));
                if let Some(encoded) = reader.get(k.borrow())? {
                    let buff = Cursor::new(encoded);
                    let decoded = deserialize_from(buff)?;
                    return Ok(Some(decoded));
                }
            }
        }
        Ok(None)
    }
}