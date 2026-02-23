//! MirDB Library
//!
//! A persistent key-value store with memcached protocol support.

#![allow(unused_imports, unused_macros, dead_code)]

#[macro_use]
mod utils;
#[macro_use]
mod error;
pub mod request;
mod response;
#[macro_use]
mod parser_util;
mod config;
mod data_manager;
mod manifest;
mod memtable;
mod memtable_list;
mod merger;
pub mod options;
mod parser;
mod proto;
pub mod slice;
mod sstable_builder;
mod sstable_reader;
pub mod store;
mod test_utils;
mod thread_pool;
mod types;
mod wal;
pub mod http;

pub use http::handlers::get_homepage_html;
