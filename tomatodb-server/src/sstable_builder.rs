use std::borrow::Borrow;
use std::fs::File;
use std::path::Path;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use bincode::{deserialize, serialize};
use serde::Serialize;

use sstable::TableBuilder;
use sstable::TableReader;

use crate::error::MyResult;
use crate::memtable::Memtable;
use crate::options::Options;
use crate::store::StoreKey;
use crate::store::StorePayload;

pub trait SstableBuilder<'a, K: 'a + Ord + Clone + Borrow<[u8]>, V: 'a + Clone + Serialize> {
    type IterType: Iterator<Item = (&'a K, &'a V)>;

    fn kv_iter(&self) -> Self::IterType;

    fn build_sstable(&self, opt: Options, path: &Path) -> MyResult<Option<(String, TableReader)>> {
        let table_opt = opt.to_table_opt();
        let mut tb = TableBuilder::new(&path, table_opt.clone())?;
        for (k, v) in self.kv_iter() {
            tb.add(k.borrow(), &serialize(v)?)?;
        }
        tb.flush()?;
        Ok(Some((path.to_str().unwrap().to_owned(), TableReader::new(path, table_opt.clone())?)))
    }
}

