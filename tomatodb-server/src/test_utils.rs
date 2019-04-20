#![cfg(test)]

use std::fs::create_dir_all;
use std::fs::remove_dir_all;
use std::path::Path;

use rand::distributions::Alphanumeric;
use rand::thread_rng;
use rand::Rng;

use crate::options::Options;

pub fn get_test_opt() -> Options {
    let rand_string: String = thread_rng().sample_iter(&Alphanumeric).take(30).collect();
    let mut opt = Options::default();
    opt.work_dir = "/tmp/tomatodbtest/".to_string() + &rand_string;
    if Path::new(&opt.work_dir).exists() {
        remove_dir_all(&opt.work_dir).expect("remove work dir error!");
    }
    create_dir_all(&opt.work_dir).expect("create work dir error!");
    opt.mem_table_max_size = 1;
    opt.imm_mem_table_max_count = 1;
    opt
}
