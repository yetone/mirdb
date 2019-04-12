pub trait SsIterator {
    fn valid(&self) -> bool;
    fn advance(&mut self) -> bool;
    fn prev(&mut self) -> bool;
    fn current_k(&self) -> Option<Vec<u8>>;
    fn current_v(&self) -> Option<Vec<u8>>;
    fn reset(&mut self);
    fn seek(&mut self, key: &[u8]);
    fn seek_to_last(&mut self);

    fn seek_to_first(&mut self) {
        self.reset();
        self.advance();
    }

    fn current_kv(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        if self.valid() {
            if let Some(k) = self.current_k() {
                if let Some(v) = self.current_v() {
                    return Some((k, v));
                }
            }
        }
        None
    }

    fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        if !self.advance() {
            None
        } else {
            self.current_kv()
        }
    }

    fn count(&mut self) -> usize {
        self.reset();
        let mut count = 0;

        while let Some(_) = self.next() {
            count += 1;
        }
        count
    }
}

pub struct SsIteratorIterWrap<'a, T> {
    inner: &'a mut T,
}

impl<'a, T: SsIterator> SsIteratorIterWrap<'a, T> {
    pub fn new(iter: &'a mut T) -> Self {
        Self {
            inner: iter
        }
    }
}

impl<'a, T: SsIterator> Iterator for SsIteratorIterWrap<'a, T> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}
