use std::path::Path;
use std::fs::File;
use crate::error::MyResult;
use crate::request::Request;

pub struct WAL {
    file: File,
}

impl WAL {
    pub fn new(path: &Path) -> MyResult<Self> {
        Ok(WAL {
            file: File::create(path)?
        })
    }

    pub fn add(_req: Request) -> MyResult<()> {
        Ok(())
    }
}

pub struct WALIter {
    file: File,
}

impl WALIter {
    pub fn new(path: &Path) -> MyResult<Self> {
        Ok(WALIter {
            file: File::create(path)?
        })
    }
}

//impl Iterator for WALIter {
//    type Item = Request;
//
//    fn next(&mut self) -> Option<Self::Item> {
//        self.file.read
//    }
//}