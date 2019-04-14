use sstable::SsIterator;
use sstable::TableIter;
use crate::utils::to_str;

pub struct Merger<'a> {
    iters: Vec<TableIter<'a>>,
    i: Option<usize>,
}

impl<'a> Merger<'a> {
    pub fn new(iters: Vec<TableIter<'a>>) -> Self {
        Self {
            iters,
            i: None,
        }
    }
}

impl<'a> SsIterator for Merger<'a> {
    fn valid(&self) -> bool {
        self.iters.iter().any(|x| x.valid())
    }

    fn advance(&mut self) -> bool {
        let mut pk: Option<Vec<u8>> = None;
        let mut pi = None;
        let mut i = 0;
        while i < self.iters.len() {
            let nk = {
                let iter = &mut self.iters[i];
                if iter.advance() {
                    iter.current_k()
                } else {
                    None
                }
            };
            if nk.is_none() {
                i += 1;
                continue;
            }
            let nk = nk.unwrap();
            if let Some(ref pk_) = pk {
                if pk_ > &nk {
                    let iter: &mut TableIter = &mut self.iters[pi.unwrap()];
                    iter.prev();
                    pk = Some(nk);
                    pi = Some(i);
                } else if pk_ < &nk {
                    let iter = &mut self.iters[i];
                    iter.prev();
                }
            } else {
                pk = Some(nk);
                pi = Some(i);
            }
            i += 1;
        }
        self.i = pi;
        pk.is_some()
    }

    fn prev(&mut self) -> bool {
        let mut pk = None;
        let mut pi = None;
        let mut i = 0;
        while i < self.iters.len() {
            let nk = {
                let iter = &mut self.iters[i];
                if iter.prev() {
                    iter.current_k()
                } else {
                    None
                }
            };
            if nk.is_none() {
                i += 1;
                continue;
            }
            let nk = nk.unwrap();
            if let Some(pk_) = &pk {
                if pk_ < &nk {
                    let iter: &mut TableIter = &mut self.iters[pi.unwrap()];
                    iter.advance();
                    pk = Some(nk);
                    pi = Some(i);
                } else if pk_ > &nk {
                    let iter = &mut self.iters[i];
                    iter.advance();
                }
            } else {
                pk = Some(nk);
                pi = Some(i);
            }
            i += 1;
        }
        self.i = pi;
        self.current_k().is_some()
    }

    fn current_k(&self) -> Option<Vec<u8>> {
        self.i.and_then(|i| self.iters[i].current_k())
    }

    fn current_v(&self) -> Option<Vec<u8>> {
        self.i.and_then(|i| self.iters[i].current_v())
    }

    fn reset(&mut self) {
        for iter in &mut self.iters {
            iter.reset()
        }
        self.i = None;
    }

    fn seek(&mut self, _key: &[u8]) {
        unimplemented!()
    }

    fn seek_to_last(&mut self) {
        unimplemented!()
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use sstable::TableBuilder;
    use sstable::TableReader;

    use crate::options::Options;

    use super::*;
    use crate::utils::to_str;
    use crate::error::MyResult;

    #[test]
    fn test() -> MyResult<()> {
        let mut opt = Options::default();
        opt.block_size = 20;
        let opt = opt.to_table_opt();
        let mut ts = vec![];
        let path = Path::new("/tmp/test_merger0");
        let mut t = TableBuilder::new(path, opt.clone())?;
        t.add("b".as_bytes(), "b".as_bytes())?;
        t.add("c".as_bytes(), "0".as_bytes())?;
        t.flush()?;
        let t = TableReader::new(path, opt.clone())?;
        ts.push(t.iter());
        let path = Path::new("/tmp/test_merger1");
        let mut t = TableBuilder::new(path, opt.clone())?;
        t.add("c".as_bytes(), "c".as_bytes())?;
        t.add("d".as_bytes(), "d".as_bytes())?;
        t.add("e".as_bytes(), "0".as_bytes())?;
        t.flush()?;
        let t = TableReader::new(path, opt.clone())?;
        ts.push(t.iter());
        let path = Path::new("/tmp/test_merger2");
        let mut t = TableBuilder::new(path, opt.clone())?;
        t.add("a".as_bytes(), "a".as_bytes())?;
        t.add("e".as_bytes(), "e".as_bytes())?;
        t.add("f".as_bytes(), "f".as_bytes())?;
        t.flush()?;
        let t = TableReader::new(path, opt.clone())?;
        ts.push(t.iter());
        ts.reverse();
        let mut m = Merger::new(ts);
        while let Some((k, v)) = m.next() {
            println!("{}: {}", to_str(&k), to_str(&v));
            assert_eq!(k, v);
        }
        Ok(())
    }
}