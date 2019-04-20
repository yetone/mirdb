use rand::prelude::ThreadRng;
use rand::Rng;

pub trait HeightGenerator {
    fn gen_height(&mut self, max: usize) -> usize;
}

pub struct GenHeight {
    rng: ThreadRng,
}

unsafe impl Send for GenHeight {}

impl GenHeight {
    pub fn new() -> Self {
        GenHeight {
            rng: rand::thread_rng(),
        }
    }
}

impl HeightGenerator for GenHeight {
    fn gen_height(&mut self, max_height: usize) -> usize {
        let mut l = 0;
        while self.rng.gen_range::<usize, usize, usize>(0, 2) > 0 && l < max_height {
            l += 1;
        }
        l
    }
}
