//! MirDB - A persistent key-value store with Memcached protocol support.
//!
//! This library provides the core functionality for MirDB, including:
//! - Storage engine with LSM-tree architecture
//! - Web API module for HTTP endpoints
//! - Memcached protocol parser

#![allow(unused_imports, unused_macros, dead_code)]

#[macro_use]
mod utils;
#[macro_use]
mod error;
#[macro_use]
pub mod request;
pub mod response;
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
pub mod slice;
mod sstable_builder;
mod sstable_reader;
mod store;
mod test_utils;
mod thread_pool;
mod types;
mod wal;

// Web module - new addition for homepage and metrics API
pub mod web;

// Re-export key types for external use
pub use config::Config;
pub use data_manager::DataManager;
pub use options::Options;
pub use store::Store;
