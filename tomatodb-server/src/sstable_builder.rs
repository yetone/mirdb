use std::borrow::Borrow;
use std::fs::File;
use std::path::Path;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use bincode::{deserialize, serialize};
use serde::Serialize;

use skip_list::SkipList;
use sstable::TableBuilder;
use sstable::TableReader;

use crate::error::MyResult;
use crate::options::Options;
use crate::store::StoreKey;
use crate::store::StorePayload;

pub fn skiplist_to_sstable<K: Borrow<[u8]>, V: Serialize>(map: &SkipList<K, V>, opt: &Options, path: &Path) -> MyResult<Option<(String, TableReader)>> {

    if map.length() == 0 {
        return Ok(None);
    }

    let table_opt = opt.to_table_opt();
    let mut tb = TableBuilder::new(&path, table_opt.clone())?;

    for (k, v) in map.iter() {
        tb.add(k.borrow(), &serialize(v)?)?;
    }

    tb.flush()?;

    Ok(Some((path.to_str().unwrap().to_owned(), TableReader::new(path, table_opt.clone())?)))
}

