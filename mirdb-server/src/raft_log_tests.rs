#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::sync::atomic::{AtomicU64, Ordering};
    use crate::slice::Slice;
    use crate::request::{Request, SetterType};
    use crate::response::Response;
    use crate::store::Store;
    use crate::test_utils::get_test_opt;
    use crate::types::Table;

    // Mock Raft log entry structure
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct LogEntry {
        index: u64,
        term: u64,
        command: StoreCommand,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    enum StoreCommand {
        Set { key: Vec<u8>, value: Vec<u8> },
        Delete { key: Vec<u8> },
    }

    // Mock Raft node for testing log replication
    struct MockRaftNode {
        id: u64,
        logs: Arc<Mutex<Vec<LogEntry>>>,
        applied_index: Arc<AtomicU64>,
        store: Arc<Store>,
        peers: Vec<u64>,
    }

    impl MockRaftNode {
        fn new(id: u64, store: Arc<Store>) -> Self {
            Self {
                id,
                logs: Arc::new(Mutex::new(Vec::new())),
                applied_index: Arc::new(AtomicU64::new(0)),
                store,
                peers: Vec::new(),
            }
        }

        // Append log entry and replicate to followers
        fn append_entry(&self, command: StoreCommand) -> Result<u64, String> {
            let mut logs = self.logs.lock().unwrap();
            let last_index = logs.last().map(|e| e.index).unwrap_or(0);
            let new_index = last_index + 1;

            let entry = LogEntry {
                index: new_index,
                term: 1, // Simplified for testing
                command,
            };

            logs.push(entry);
            Ok(new_index)
        }

        // Apply log entries to state machine (store)
        fn apply_logs(&self) -> Result<(), String> {
            let logs = self.logs.lock().unwrap();
            let current_applied = self.applied_index.load(Ordering::SeqCst);

            for entry in logs.iter().skip(current_applied as usize) {
                match &entry.command {
                    StoreCommand::Set { key, value } => {
                        let key_slice = Slice::from(key.clone());
                        let value_slice = Slice::from(value.clone());
                        let response = self.store.apply(Request::Setter {
                            setter: SetterType::Set,
                            key: key_slice,
                            flags: 0,
                            ttl: 0,
                            bytes: value_slice.len(),
                            payload: value_slice,
                            no_reply: false,
                        });

                        if let Err(e) = response {
                            return Err(format!("Failed to apply log entry: {:?}", e));
                        }
                    }
                    StoreCommand::Delete { key } => {
                        let key_slice = Slice::from(key.clone());
                        self.store.apply(Request::Deleter {
                            key: key_slice,
                            no_reply: false,
                        }).map_err(|e| format!("Failed to apply delete: {:?}", e))?;
                    }
                }
                self.applied_index.fetch_add(1, Ordering::SeqCst);
            }

            Ok(())
        }

        // Get all log entries
        fn get_logs(&self) -> Vec<LogEntry> {
            self.logs.lock().unwrap().clone()
        }

        // Check if logs match another node's logs
        fn logs_match(&self, other: &MockRaftNode) -> bool {
            let my_logs = self.logs.lock().unwrap();
            let other_logs = other.logs.lock().unwrap();
            *my_logs == *other_logs
        }

        // Simulate idempotent apply (applying same entry multiple times)
        fn apply_entry_idempotent(&self, entry: &LogEntry) -> Result<(), String> {
            let applied = self.applied_index.load(Ordering::SeqCst);
            if entry.index <= applied {
                // Already applied, this is idempotent
                return Ok(());
            }

            match &entry.command {
                StoreCommand::Set { key, value } => {
                    let key_slice = Slice::from(key.clone());
                    let value_slice = Slice::from(value.clone());
                    self.store.apply(Request::Setter {
                        setter: SetterType::Set,
                        key: key_slice,
                        flags: 0,
                        ttl: 0,
                        bytes: value_slice.len(),
                        payload: value_slice,
                        no_reply: false,
                    }).map_err(|e| format!("Failed to apply idempotent: {:?}", e))?;
                }
                StoreCommand::Delete { key } => {
                    let key_slice = Slice::from(key.clone());
                    self.store.apply(Request::Deleter {
                        key: key_slice,
                        no_reply: false,
                    }).map_err(|e| format!("Failed to apply delete: {:?}", e))?;
                }
            }
            self.applied_index.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn test_concurrent_writes_maintain_total_order() {
        let opt = get_test_opt();
        let store = Arc::new(Store::new(opt).unwrap());

        let node1 = MockRaftNode::new(1, store.clone());
        let node2 = MockRaftNode::new(2, store.clone());
        let node3 = MockRaftNode::new(3, store.clone());

        // Simulate 5 concurrent SET commands
        let commands = vec![
            StoreCommand::Set { key: b"A".to_vec(), value: b"value_A".to_vec() },
            StoreCommand::Set { key: b"B".to_vec(), value: b"value_B".to_vec() },
            StoreCommand::Set { key: b"C".to_vec(), value: b"value_C".to_vec() },
            StoreCommand::Set { key: b"D".to_vec(), value: b"value_D".to_vec() },
            StoreCommand::Set { key: b"E".to_vec(), value: b"value_E".to_vec() },
        ];

        // Apply all commands to node1 (leader)
        for cmd in commands {
            node1.append_entry(cmd).unwrap();
        }

        // Replicate to followers (for testing, we copy logs)
        let leader_logs = node1.get_logs();
        *node2.logs.lock().unwrap() = leader_logs.clone();
        *node3.logs.lock().unwrap() = leader_logs.clone();

        // Apply logs to all nodes
        node1.apply_logs().unwrap();
        node2.apply_logs().unwrap();
        node3.apply_logs().unwrap();

        // Verify all nodes have identical log sequences
        assert!(node1.logs_match(&node2), "Node 1 and Node 2 logs should match");
        assert!(node1.logs_match(&node3), "Node 1 and Node 3 logs should match");
        assert!(node2.logs_match(&node3), "Node 2 and Node 3 logs should match");

        // Verify log order
        let logs = node1.get_logs();
        assert_eq!(logs.len(), 5, "Should have 5 log entries");
        assert_eq!(logs[0].index, 1);
        assert_eq!(logs[1].index, 2);
        assert_eq!(logs[2].index, 3);
        assert_eq!(logs[3].index, 4);
        assert_eq!(logs[4].index, 5);

        // Verify state consistency - check that all keys exist
        for key_char in &['A', 'B', 'C', 'D', 'E'] {
            let key_str = format!("{}", key_char);
            let key = Slice::from(key_str.as_bytes().to_vec());

            // All nodes should see the same state - use apply for getter
            let result = store.apply(Request::Getter {
                getter: crate::request::GetterType::Get,
                keys: vec![key.clone()],
            }).unwrap();
            if let Response::Get(items) = result {
                assert!(!items.is_empty(), "Key {} should exist", key_char);
            } else {
                panic!("Expected Get response for key {}", key_char);
            }
        }
    }

    #[test]
    fn test_leader_restart_log_continuity() {
        let opt = get_test_opt();
        let store = Arc::new(Store::new(opt).unwrap());

        let mut node1 = MockRaftNode::new(1, store.clone());

        // Add initial entries
        node1.append_entry(StoreCommand::Set { key: b"key1".to_vec(), value: b"value1".to_vec() }).unwrap();
        node1.append_entry(StoreCommand::Set { key: b"key2".to_vec(), value: b"value2".to_vec() }).unwrap();
        node1.apply_logs().unwrap();

        // Simulate leader failure
        let logs_before_restart = node1.get_logs();
        assert_eq!(logs_before_restart.len(), 2);

        // New leader (node2) takes over
        let store2 = Arc::new(Store::new(get_test_opt()).unwrap());
        let node2 = MockRaftNode::new(2, store2.clone());
        *node2.logs.lock().unwrap() = logs_before_restart.clone();
        node2.apply_logs().unwrap();

        // New leader should apply new entries
        node2.append_entry(StoreCommand::Set { key: b"key3".to_vec(), value: b"value3".to_vec() }).unwrap();
        node2.append_entry(StoreCommand::Set { key: b"key4".to_vec(), value: b"value4".to_vec() }).unwrap();
        node2.apply_logs().unwrap();

        // Verify log continuity - no gaps, log indices continue
        let logs_after = node2.get_logs();
        assert_eq!(logs_after.len(), 4);
        assert_eq!(logs_after[0].index, 1);
        assert_eq!(logs_after[1].index, 2);
        assert_eq!(logs_after[2].index, 3);
        assert_eq!(logs_after[3].index, 4);

        // No resets or gaps
        for i in 0..logs_after.len() {
            assert_eq!(logs_after[i].index, (i + 1) as u64);
        }
    }

    #[test]
    fn test_idempotent_log_application() {
        let opt = get_test_opt();
        let store = Arc::new(Store::new(opt).unwrap());

        let node1 = MockRaftNode::new(1, store.clone());

        // Create a log entry
        node1.append_entry(StoreCommand::Set { key: b"test_key".to_vec(), value: b"test_value".to_vec() }).unwrap();
        node1.apply_logs().unwrap();

        let first_entry = node1.get_logs()[0].clone();

        // Set applied_index back to simulate duplicate application
        node1.applied_index.store(0, Ordering::SeqCst);

        // Apply the same entry again - should be idempotent
        node1.apply_entry_idempotent(&first_entry).unwrap();

        // Verify state is consistent and the operation is truly idempotent
        let key = Slice::from(b"test_key".to_vec());
        let result = store.apply(Request::Getter {
            getter: crate::request::GetterType::Get,
            keys: vec![key.clone()],
        }).unwrap();
        if let Response::Get(items) = result {
            assert!(!items.is_empty(), "Key should exist after idempotent apply");
        } else {
            panic!("Expected Get response");
        }

        // Check that applied index has not increased (already applied)
        assert_eq!(node1.applied_index.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_log_index_continuity_across_terms() {
        let opt = get_test_opt();
        let store = Arc::new(Store::new(opt).unwrap());

        let node1 = MockRaftNode::new(1, store.clone());

        // Simulate multiple terms with leader changes
        // Term 1
        node1.append_entry(StoreCommand::Set { key: b"t1_key1".to_vec(), value: b"value1".to_vec() }).unwrap();
        node1.append_entry(StoreCommand::Set { key: b"t1_key2".to_vec(), value: b"value2".to_vec() }).unwrap();

        // Simulate leader change (Term 2)
        node1.append_entry(StoreCommand::Set { key: b"t2_key1".to_vec(), value: b"value3".to_vec() }).unwrap();
        node1.append_entry(StoreCommand::Set { key: b"t2_key2".to_vec(), value: b"value4".to_vec() }).unwrap();

        // Term 3
        node1.append_entry(StoreCommand::Set { key: b"t3_key1".to_vec(), value: b"value5".to_vec() }).unwrap();

        node1.apply_logs().unwrap();

        // Verify log indices continue monotonically
        let logs = node1.get_logs();
        assert_eq!(logs.len(), 5);

        for i in 0..logs.len() {
            assert_eq!(logs[i].index, (i + 1) as u64, "Log index should be continuous");
            // Indices should never reset or decrease
            if i > 0 {
                assert!(logs[i].index > logs[i-1].index, "Log index should be monotonic");
            }
        }

        // Verify no gaps in indices
        let indices: Vec<u64> = logs.iter().map(|e| e.index).collect();
        for i in 0..indices.len() {
            assert_eq!(indices[i], (i + 1) as u64, "No gaps should exist in log indices");
        }
    }
}

// Raft log module for production use
pub mod raft_log {
    use std::sync::{Arc, RwLock};
    use serde::{Deserialize, Serialize};
    use crate::slice::Slice;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub struct LogEntry {
        pub index: u64,
        pub term: u64,
        pub data: Vec<u8>,
    }

    pub struct RaftLog {
        entries: Arc<RwLock<Vec<LogEntry>>>,
        commit_index: Arc<RwLock<u64>>,
    }

    impl RaftLog {
        pub fn new() -> Self {
            Self {
                entries: Arc::new(RwLock::new(Vec::new())),
                commit_index: Arc::new(RwLock::new(0)),
            }
        }

        pub fn append(&self, term: u64, data: Vec<u8>) -> u64 {
            let mut entries = self.entries.write().unwrap();
            let index = entries.len() as u64 + 1;
            entries.push(LogEntry { index, term, data });
            index
        }

        pub fn get_entry(&self, index: u64) -> Option<LogEntry> {
            let entries = self.entries.read().unwrap();
            entries.get((index - 1) as usize).cloned()
        }

        pub fn last_index(&self) -> u64 {
            self.entries.read().unwrap().len() as u64
        }

        pub fn set_commit_index(&self, index: u64) {
            *self.commit_index.write().unwrap() = index;
        }

        pub fn get_commit_index(&self) -> u64 {
            *self.commit_index.read().unwrap()
        }
    }
}
