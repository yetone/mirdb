use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use crate::slice::Slice;
use crate::store::{Store, StoreKey, StorePayload};
use crate::request::SetterType;
use crate::response::Response;
use crate::request::Request;
use std::sync::atomic::{AtomicU64, Ordering};

/// Represents a single entry in the Raft log
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub data: Vec<u8>,
}

/// Commands that can be stored in the log
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum StoreCommand {
    Set { key: Vec<u8>, value: Vec<u8>, flags: u32, ttl: u32 },
    Delete { key: Vec<u8> },
}

/// Core Raft log structure that maintains log entries
pub struct RaftLog {
    entries: Arc<RwLock<Vec<LogEntry>>>,
    commit_index: Arc<RwLock<u64>>,
    applied_index: Arc<AtomicU64>,
    first_index: Arc<RwLock<u64>>,
}

impl RaftLog {
    pub fn new() -> Self {
        Self {
            entries: Arc::new(RwLock::new(Vec::new())),
            commit_index: Arc::new(RwLock::new(0)),
            applied_index: Arc::new(AtomicU64::new(0)),
            first_index: Arc::new(RwLock::new(1)),
        }
    }

    /// Append a new entry to the log
    /// Returns the index of the newly appended entry
    pub fn append(&self, term: u64, data: Vec<u8>) -> u64 {
        let mut entries = self.entries.write().unwrap();
        let index = entries.len() as u64 + 1;
        entries.push(LogEntry { index, term, data });
        index
    }

    /// Append multiple entries (for log replication)
    pub fn append_entries(&self, entries: Vec<LogEntry>) -> Result<(), LogError> {
        let mut current_entries = self.entries.write().unwrap();

        // Ensure log continuity - entries must be contiguous
        for entry in entries {
            let expected_index = current_entries.len() as u64 + 1;
            if entry.index != expected_index {
                return Err(LogError::IndexMismatch {
                    expected: expected_index,
                    got: entry.index,
                });
            }
            current_entries.push(entry);
        }

        Ok(())
    }

    /// Get an entry at a specific index
    pub fn get_entry(&self, index: u64) -> Option<LogEntry> {
        let entries = self.entries.read().unwrap();
        entries.get((index - 1) as usize).cloned()
    }

    /// Get a range of entries (for log replication)
    pub fn get_entries(&self, start: u64, end: u64) -> Vec<LogEntry> {
        let entries = self.entries.read().unwrap();
        let start_idx = (start - 1) as usize;
        let end_idx = (end - 1) as usize;

        entries[start_idx..=end_idx].to_vec()
    }

    /// Get the last log index
    pub fn last_index(&self) -> u64 {
        self.entries.read().unwrap().len() as u64
    }

    /// Get the last log term
    pub fn last_term(&self) -> u64 {
        let entries = self.entries.read().unwrap();
        entries.last().map(|e| e.term).unwrap_or(0)
    }

    /// Set the commit index (when majority confirms)
    pub fn set_commit_index(&self, index: u64) -> Result<(), LogError> {
        let last = self.last_index();
        if index > last {
            return Err(LogError::InvalidCommitIndex {
                commit: index,
                last,
            });
        }

        *self.commit_index.write().unwrap() = index;
        Ok(())
    }

    /// Get the current commit index
    pub fn get_commit_index(&self) -> u64 {
        *self.commit_index.read().unwrap()
    }

    /// Get the current applied index
    pub fn get_applied_index(&self) -> u64 {
        self.applied_index.load(Ordering::SeqCst)
    }

    /// Apply an entry to the state machine (idempotent)
    pub fn apply_entry(&self, entry: &LogEntry, store: &Store) -> Result<(), ApplyError> {
        let current_applied = self.applied_index.load(Ordering::SeqCst);

        // Check if already applied (idempotency)
        if entry.index <= current_applied {
            return Ok(());
        }

        // Deserialize command
        let cmd: StoreCommand = bincode::deserialize(&entry.data)
            .map_err(|e| ApplyError::DeserializationFailed(e.to_string()))?;

        // Apply to store
        match cmd {
            StoreCommand::Set { key, value, flags, ttl } => {
                let created_at = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();

                let sp = StorePayload::new(
                    Slice::from(value),
                    flags,
                    ttl,
                    value.len(),
                    created_at,
                );

                store.apply(Request::Setter {
                    setter: SetterType::Set,
                    key: Slice::from(key),
                    flags,
                    ttl,
                    bytes: sp.bytes,
                    payload: sp.data.clone(),
                    no_reply: false,
                }).map_err(|e| ApplyError::ApplyFailed(e.msg))?;
            }
            StoreCommand::Delete { key } => {
                store.apply(Request::Deleter {
                    key: Slice::from(key),
                    no_reply: false,
                }).map_err(|e| ApplyError::ApplyFailed(e.msg))?;
            }
        }

        // Update applied index
        self.applied_index.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    /// Apply all committed entries up to commit index
    pub fn apply_committed(&self, store: &Store) -> Result<u64, ApplyError> {
        let commit_index = self.get_commit_index();
        let applied_index = self.get_applied_index();

        if commit_index <= applied_index {
            return Ok(applied_index);
        }

        for idx in (applied_index + 1)..=commit_index {
            if let Some(entry) = self.get_entry(idx) {
                self.apply_entry(&entry, store)?;
            } else {
                return Err(ApplyError::MissingEntry { index: idx });
            }
        }

        Ok(self.get_applied_index())
    }

    /// Truncate logs from a given index (for consistency during leader changes)
    pub fn truncate(&self, from_index: u64) -> Result<(), LogError> {
        let mut entries = self.entries.write().unwrap();
        let current_len = entries.len() as u64;

        if from_index > current_len + 1 {
            return Err(LogError::InvalidTruncateIndex {
                from: from_index,
                last: current_len,
            });
        }

        entries.truncate((from_index - 1) as usize);
        Ok(())
    }

    /// Check if logs match another log (for consistency checking)
    pub fn logs_match_up_to(&self, last_idx: u64, last_term: u64) -> bool {
        if last_idx == 0 {
            return true; // Empty logs always match
        }

        if let Some(entry) = self.get_entry(last_idx) {
            entry.term == last_term
        } else {
            false
        }
    }
}

/// Errors related to log operations
#[derive(Debug, Clone)]
pub enum LogError {
    IndexMismatch { expected: u64, got: u64 },
    InvalidCommitIndex { commit: u64, last: u64 },
    InvalidTruncateIndex { from: u64, last: u64 },
}

impl std::fmt::Display for LogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogError::IndexMismatch { expected, got } => {
                write!(f, "Log index mismatch: expected {}, got {}", expected, got)
            }
            LogError::InvalidCommitIndex { commit, last } => {
                write!(f, "Invalid commit index {} (last index is {})", commit, last)
            }
            LogError::InvalidTruncateIndex { from, last } => {
                write!(f, "Invalid truncate index {} (last index is {})", from, last)
            }
        }
    }
}

impl std::error::Error for LogError {}

/// Errors related to applying log entries
#[derive(Debug, Clone)]
pub enum ApplyError {
    DeserializationFailed(String),
    ApplyFailed(String),
    MissingEntry { index: u64 },
    StoreError(String),
}

impl std::fmt::Display for ApplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApplyError::DeserializationFailed(e) => write!(f, "Deserialization failed: {}", e),
            ApplyError::ApplyFailed(e) => write!(f, "Apply failed: {}", e),
            ApplyError::MissingEntry { index } => write!(f, "Missing log entry at index {}", index),
            ApplyError::StoreError(e) => write!(f, "Store error: {}", e),
        }
    }
}

impl std::error::Error for ApplyError {}

/// Raft consensus module for leader election and log replication
pub mod consensus {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[derive(Debug, Clone, Copy, PartialEq)]
    pub enum NodeState {
        Follower,
        Candidate,
        Leader,
    }

    pub struct RaftConsensus {
        node_id: u64,
        current_term: Arc<AtomicU64>,
        voted_for: Arc<RwLock<Option<u64>>>,
        state: Arc<RwLock<NodeState>>,
        log: Arc<RaftLog>,
    }

    impl RaftConsensus {
        pub fn new(node_id: u64) -> Self {
            Self {
                node_id,
                current_term: Arc::new(AtomicU64::new(0)),
                voted_for: Arc::new(RwLock::new(None)),
                state: Arc::new(RwLock::new(NodeState::Follower)),
                log: Arc::new(RaftLog::new()),
            }
        }

        pub fn get_node_id(&self) -> u64 {
            self.node_id
        }

        pub fn get_current_term(&self) -> u64 {
            self.current_term.load(Ordering::SeqCst)
        }

        pub fn get_state(&self) -> NodeState {
            *self.state.read().unwrap()
        }

        pub fn log(&self) -> Arc<RaftLog> {
            self.log.clone()
        }

        /// Start a new election (becomes candidate)
        pub fn start_election(&self) -> u64 {
            let new_term = self.current_term.load(Ordering::SeqCst) + 1;
            self.current_term.store(new_term, Ordering::SeqCst);
            *self.state.write().unwrap() = NodeState::Candidate;
            *self.voted_for.write().unwrap() = Some(self.node_id);
            new_term
        }

        /// Vote for a candidate
        pub fn vote(&self, candidate_id: u64, candidate_term: u64, last_log_index: u64, last_log_term: u64) -> bool {
            let current_term = self.current_term.load(Ordering::SeqCst);

            if candidate_term < current_term {
                return false;
            }

            if candidate_term > current_term {
                self.current_term.store(candidate_term, Ordering::SeqCst);
                *self.state.write().unwrap() = NodeState::Follower;
                *self.voted_for.write().unwrap() = None;
            }

            // Check if logs are at least as up-to-date as ours
            let last_index = self.log.last_index();
            let last_term = self.log.last_term();

            if last_index != 0 {
                if last_log_term < last_term {
                    return false;
                }
                if last_log_term == last_term && last_log_index < last_index {
                    return false;
                }
            }

            let mut voted_for = self.voted_for.write().unwrap();
            if voted_for.is_none() || voted_for.unwrap() == candidate_id {
                *voted_for = Some(candidate_id);
                true
            } else {
                false
            }
        }

        /// Become leader
        pub fn become_leader(&self) {
            *self.state.write().unwrap() = NodeState::Leader;
        }

        /// Step down to follower
        pub fn become_follower(&self, term: u64) {
            self.current_term.store(term, Ordering::SeqCst);
            *self.state.write().unwrap() = NodeState::Follower;
            *self.voted_for.write().unwrap() = None;
        }

        /// Check if logs match up to a certain index/term
        pub fn check_log_matching(&self, prev_log_index: u64, prev_log_term: u64) -> bool {
            if prev_log_index == 0 {
                return true;
            }

            match self.log.get_entry(prev_log_index) {
                Some(entry) => entry.term == prev_log_term,
                None => false,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::options::Options;
    use crate::store::Store;

    #[test]
    fn test_raft_log_append_and_retrieve() {
        let log = RaftLog::new();

        let data1 = bincode::serialize(&StoreCommand::Set {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
            flags: 0,
            ttl: 0,
        }).unwrap();

        let idx1 = log.append(1, data1);
        assert_eq!(idx1, 1);
        assert_eq!(log.last_index(), 1);

        let data2 = bincode::serialize(&StoreCommand::Set {
            key: b"key2".to_vec(),
            value: b"value2".to_vec(),
            flags: 0,
            ttl: 0,
        }).unwrap();

        let idx2 = log.append(1, data2);
        assert_eq!(idx2, 2);
        assert_eq!(log.last_index(), 2);

        // Verify entries can be retrieved
        let entry1 = log.get_entry(1).unwrap();
        assert_eq!(entry1.index, 1);
        assert_eq!(entry1.term, 1);

        let entry2 = log.get_entry(2).unwrap();
        assert_eq!(entry2.index, 2);
        assert_eq!(entry2.term, 1);
    }

    #[test]
    fn test_log_index_continuity() {
        let log = RaftLog::new();

        // Append entries with continuous indices
        for i in 1..=5 {
            let data = bincode::serialize(&StoreCommand::Set {
                key: format!("key{}", i).into_bytes(),
                value: format!("value{}", i).into_bytes(),
                flags: 0,
                ttl: 0,
            }).unwrap();
            log.append(1, data);
        }

        // Verify indices are continuous
        for i in 1..=5 {
            assert!(log.get_entry(i).is_some(), "Entry {} should exist", i);
        }

        // Verify last index
        assert_eq!(log.last_index(), 5);
    }

    #[test]
    fn test_commit_index_management() {
        let log = RaftLog::new();
        let opt = Options::default();
        let store = Store::new(opt).unwrap();

        // Append entries
        let idx1 = log.append(1, vec![1, 2, 3]);
        let idx2 = log.append(1, vec![4, 5, 6]);

        // Set commit index
        log.set_commit_index(idx1).unwrap();
        assert_eq!(log.get_commit_index(), 1);

        // Can advance commit index
        log.set_commit_index(idx2).unwrap();
        assert_eq!(log.get_commit_index(), 2);

        // Cannot commit beyond last index
        let result = log.set_commit_index(10);
        assert!(result.is_err());
    }

    #[test]
    fn test_apply_entry_idempotency() {
        let opt = Options::default();
        let store = Store::new(opt).unwrap();
        let log = RaftLog::new();

        let data = bincode::serialize(&StoreCommand::Set {
            key: b"test_key".to_vec(),
            value: b"test_value".to_vec(),
            flags: 0,
            ttl: 0,
        }).unwrap();

        let entry = LogEntry {
            index: 1,
            term: 1,
            data,
        };

        // Apply entry first time
        log.append(1, entry.data.clone());
        log.apply_entry(&entry, &store).unwrap();
        assert_eq!(log.get_applied_index(), 1);

        // Try to apply same entry again (should be idempotent)
        log.apply_entry(&entry, &store).unwrap();
        assert_eq!(log.get_applied_index(), 1); // Applied index should not increase
    }

    #[test]
    fn test_log_matching() {
        let log = RaftLog::new();

        log.append(1, vec![1]);
        log.append(1, vec![2]);
        log.append(2, vec![3]);

        // Logs should match for index 1, term 1
        assert!(log.logs_match_up_to(1, 1));

        // Should not match for wrong term
        assert!(!log.logs_match_up_to(1, 2));

        // Empty logs always match
        let empty_log = RaftLog::new();
        assert!(empty_log.logs_match_up_to(0, 0));
    }

    #[test]
    fn test_truncate_logs() {
        let log = RaftLog::new();

        for i in 1..=5 {
            log.append(1, vec![i]);
        }

        assert_eq!(log.last_index(), 5);

        // Truncate from index 3 (removes entries 3, 4, 5)
        log.truncate(3).unwrap();
        assert_eq!(log.last_index(), 2);

        // Should not be able to truncate beyond end
        let result = log.truncate(10);
        assert!(result.is_err());
    }
}
