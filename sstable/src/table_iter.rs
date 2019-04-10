use crate::block::Block;
use crate::block_handle::BlockHandle;
use crate::block_iter::BlockIter;
use crate::block_iter::BlockIterState;
use crate::TableReader;
use crate::types::SsIterator;

pub struct TableIter<'a> {
    table: &'a TableReader,
    pub(crate) index_iter: BlockIter<'a>,
    data_iter_state: BlockIterState,
    data_block: Option<Block>,
}

impl<'a> TableIter<'a> {
    pub fn new(table: &'a TableReader) -> Self {
        Self {
            table,
            index_iter: table.index_block.iter(),
            data_iter_state: BlockIterState::new(0),
            data_block: None
        }
    }

    fn data_iter(&self) -> Option<BlockIter> {
        match &self.data_block {
            Some(ref v) => Some(
                BlockIter::new_with_state(&v.block, self.data_iter_state.clone())
            ),
            _ => None
        }
    }

    pub fn seek_to_last(&mut self) {
        self.reset();
        self.index_iter.seek_to_last();
        self.index_iter.prev();
        self.advance();

        assert!(self.valid());
    }
}

impl<'a> SsIterator for TableIter<'a> {
    fn valid(&self) -> bool {
        let data_iter = self.data_iter();
        data_iter.is_some() && data_iter.as_ref().unwrap().valid()
    }

    fn advance(&mut self) -> bool {
        if let Some(data_iter) = &mut self.data_iter() {
            if data_iter.advance() {
                self.data_iter_state = data_iter.state.clone();
                return true;
            }
            self.data_iter_state = data_iter.state.clone();
        }

        if !self.index_iter.advance() {
            return false;
        }

        if let Some((_k, v)) = self.index_iter.current_kv() {
            let (bh, _) = BlockHandle::decode(&v);
            match self.table.read_block(&bh) {
                Ok(Some(block)) => {
                    self.data_iter_state = BlockIterState::new(block.restarts_offset());
                    self.data_block = Some(block);
                    return self.advance();
                },
                Ok(None) => return false,
                Err(_) => return self.advance(),
            }
        }

        self.reset();
        false
    }

    fn prev(&mut self) -> bool {
        if let Some(data_iter) = &mut self.data_iter() {
            if data_iter.prev() {
                self.data_iter_state = data_iter.state.clone();
                return true;
            }

            self.data_iter_state = data_iter.state.clone();
        }

        if !self.index_iter.prev() {
            return false;
        }

        if let Some((_k, v)) = self.index_iter.current_kv() {
            let (bh, _) = BlockHandle::decode(&v);
            match self.table.read_block(&bh) {
                Ok(Some(block)) => {
                    let mut iter = block.iter();
                    iter.advance();
                    if iter.state.key.is_empty() {
                        return false;
                    }
                    iter.seek_to_last();
                    self.data_iter_state = iter.state.clone();
                    self.data_block = Some(block);
                    return true;
                },
                Ok(None) => return false,
                Err(_) => return self.prev(),
            }
        }

        self.reset();
        false
    }

    fn current_k(&self) -> Option<Vec<u8>> {
        self.data_iter().and_then(|x| x.current_k())
    }

    fn current_kv(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.data_iter().and_then(|x| x.current_kv())
    }

    fn reset(&mut self) {
        self.index_iter.reset();
        self.data_block = None;
        self.data_iter_state.reset();
    }

    fn seek(&mut self, key: &[u8]) {
        self.reset();
        self.index_iter.seek(key);
        if let Some((_k, v)) = self.index_iter.current_kv() {
            let (bh, _) = BlockHandle::decode(&v);
            match self.table.read_block(&bh) {
                Ok(Some(block)) => {
                    let mut iter = block.iter();
                    iter.seek(key);
                    self.data_iter_state = iter.state.clone();
                    self.data_block = Some(block);
                },
                _ => ()
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use crate::MyResult;
    use crate::Options;
    use crate::table_builder::TableBuilder;

    use super::*;

    fn get_data() -> Vec<(String, String)> {
        let mut data = vec![];
        let mut key_prefix = "prefix_key".to_owned();
        let value_prefix = "value";
        let n = 3;
        for i in 1..=n {
            if i % 10 == 0 {
                key_prefix += "a";
            }
            let key = format!("{}{}", key_prefix, i);
            let value = format!("{}{}", value_prefix, i);
            data.push((key, value));
        }
        data
    }

    #[test]
    fn test_seek() -> MyResult<()> {
        let path = Path::new("/tmp/test_table_iter");
        let mut opt = Options::default();
        opt.block_size = 2;
        let mut t = TableBuilder::new(path, opt.clone())?;
        let data = get_data();
        for (k, v) in data {
            t.add(k.as_bytes(), v.as_bytes())?;
        }
        t.flush()?;
        let t = TableReader::new(path, opt.clone())?;

        let mut iter = TableIter::new(&t);
        assert_eq!(None, iter.current_kv());
        iter.seek("prefix_key2".as_bytes());
        assert_eq!(b"prefix_key2".to_vec(), iter.current_kv().unwrap().0);
        Ok(())
    }

    #[test]
    fn test_advance_prev() -> MyResult<()> {
        let path = Path::new("/tmp/test_table_iter");
        let mut opt = Options::default();
        opt.block_size = 2;
        let mut t = TableBuilder::new(path, opt.clone())?;
        let data = get_data();
        for (k, v) in data {
            t.add(k.as_bytes(), v.as_bytes())?;
        }
        t.flush()?;
        let t = TableReader::new(path, opt.clone())?;

        let mut iter = TableIter::new(&t);
        assert_eq!(None, iter.current_kv());
        iter.advance();
        assert_eq!(b"prefix_key1".to_vec(), iter.current_kv().unwrap().0);
        iter.advance();
        assert_eq!(b"prefix_key2".to_vec(), iter.current_kv().unwrap().0);
        iter.advance();
        assert_eq!(b"prefix_key3".to_vec(), iter.current_kv().unwrap().0);
        iter.advance();
        assert_eq!(None, iter.current_kv());

        let mut iter = TableIter::new(&t);
        iter.seek_to_last();
        assert_eq!(b"prefix_key3".to_vec(), iter.current_kv().unwrap().0);
        iter.prev();
        assert_eq!(b"prefix_key2".to_vec(), iter.current_kv().unwrap().0);
        iter.prev();
        assert_eq!(b"prefix_key1".to_vec(), iter.current_kv().unwrap().0);
        iter.prev();
        assert_eq!(None, iter.current_kv());
        Ok(())
    }
}
