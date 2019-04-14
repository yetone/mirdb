use std::fs::File;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use crate::block_builder::BlockBuilder;
use crate::footer::Footer;
use crate::footer::FULL_FOOTER_LENGTH;
use crate::options::Options;
use crate::error::MyResult;
use crate::util::find_short_succ;
use crate::util::find_shortest_sep;
use crate::meta_block::MetaBlock;
use std::path::PathBuf;

pub struct TableBuilder {
    file: File,
    path_: PathBuf,
    opt: Options,
    offset: usize,
    total_size_estimate_: usize,
    data_block: BlockBuilder,
    index_block: BlockBuilder,
    min_key: Option<Vec<u8>>,
    max_key: Option<Vec<u8>>,
}

impl TableBuilder {
    pub fn new(path: &Path, opt: Options) -> MyResult<TableBuilder> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;
        Ok(TableBuilder {
            file,
            path_: path.to_path_buf(),
            opt: opt.clone(),
            offset: 0,
            total_size_estimate_: 0,
            data_block: BlockBuilder::new(opt.clone()),
            index_block: BlockBuilder::new(opt),
            min_key: None,
            max_key: None,
        })
    }

    pub fn path(&self) -> &PathBuf {
        &self.path_
    }

    #[allow(unused)]
    fn current_size_estimate(&self) -> usize {
        let mut size = 0;
        size += self.data_block.size_estimate();
        size += self.index_block.size_estimate();
        size += self.offset;
        size += FULL_FOOTER_LENGTH;
        size
    }

    pub fn total_size_estimate(&self) -> usize {
        self.total_size_estimate_
    }

    pub fn add(&mut self, k: &[u8], v: &[u8]) -> MyResult<()> {
        self.total_size_estimate_ += k.len() + v.len();
        if self.data_block.size_estimate() > self.opt.block_size {
            self.write_data_block(k)?;
        }
        self.data_block.add(k, v);
        if let None = &self.min_key {
            self.min_key = Some(k.to_vec());
        }
        self.max_key = Some(k.to_vec());
        Ok(())
    }

    fn write_data_block(&mut self, next_key: &[u8]) -> MyResult<()> {
        let sep = find_shortest_sep(&self.data_block.last_key, next_key);

        let bh = self.data_block.flush(&mut self.file, self.offset)?;
        self.offset = bh.offset + bh.size;

        let mut bh_buf = [0; 16];
        let bh_size = bh.encode_to(&mut bh_buf);

        self.index_block.add(&sep, &bh_buf[0..bh_size]);

        self.data_block = BlockBuilder::new(self.opt.clone());

        Ok(())
    }

    pub fn flush(mut self) -> MyResult<()> {
        self.write_data_block(&find_short_succ(&self.data_block.last_key))?;
        let mut meta_block = MetaBlock::new(
            self.max_key.expect("max key"),
            self.min_key.expect("min key")
        );
        let meta_bh = meta_block.flush(&mut self.file, self.offset)?;
        self.offset = meta_bh.offset + meta_bh.size;
        let index_bh = self.index_block.flush(&mut self.file, self.offset)?;
        self.offset = index_bh.offset + index_bh.size;
        let footer = Footer::new(meta_bh.clone(), index_bh.clone());
        footer.flush(&mut self.file, self.offset)?;
        self.file.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn get_data() -> Vec<(&'static [u8], &'static [u8])> {
        vec![
            ("key1".as_bytes(), "value1".as_bytes()),
            (
                "loooooooooooooooooooooooooooooooooongerkey1".as_bytes(),
                "shrtvl1".as_bytes(),
            ),
            ("medium length key 1".as_bytes(), "some value 2".as_bytes()),
            ("prefix_key1".as_bytes(), "value".as_bytes()),
            ("prefix_key2".as_bytes(), "value".as_bytes()),
            ("prefix_key3".as_bytes(), "value".as_bytes()),
        ]
    }

    #[test]
    fn test_flush() -> MyResult<()> {
        let mut t = TableBuilder::new("/tmp/x.data".as_ref(), Options::default())?;
        let data = get_data();
        for (k, v) in data {
            t.add(k, v)?;
        }
        t.flush()?;
        Ok(())
    }
}
