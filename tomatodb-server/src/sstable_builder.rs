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

pub fn build_sstable<K: Ord + Clone + Borrow<[u8]>, V: Clone + Serialize>(opt: Options, level: usize, table: &Memtable<K, Option<V>>) -> MyResult<(String, TableReader)> {
    let work_dir = Path::new(&opt.work_dir);
    let p = work_dir.join(format!("{}-{}.sst", level,
                                  SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()));
    let table_opt = opt.to_table_opt();
    let mut tb = TableBuilder::new(&p, table_opt.clone())?;
    for (k, v) in table.iter() {
        tb.add(k.borrow(), &serialize(v)?)?;
    }
    tb.flush()?;
    Ok((p.to_str().unwrap().to_owned(), TableReader::new(p.as_path(), table_opt.clone())?))
}
