//! MirDB - A persistent key-value store with Memcached protocol support.
//!
//! This library provides the core functionality for MirDB including:
//! - HTTP server for serving the homepage
//! - Memcached protocol implementation
//! - LSM-tree based storage engine

#![allow(unused_imports, unused_macros, dead_code)]

#[macro_use]
mod utils;
#[macro_use]
mod error;
#[macro_use]
mod request;
mod response;
#[macro_use]
mod parser_util;
mod config;
mod data_manager;
mod manifest;
mod memtable;
mod memtable_list;
mod merger;
mod options;
mod parser;
mod proto;
mod slice;
mod sstable_builder;
mod sstable_reader;
mod store;
mod test_utils;
mod thread_pool;
mod types;
mod wal;

/// HTTP server module for serving the MirDB homepage.
pub mod http;

pub use self::http::run_http_server;
