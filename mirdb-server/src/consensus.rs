use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::MyResult;
use crate::options::Options;
use crate::slice::Slice;
use crate::store::{Store, StoreKey, StorePayload};
use crate::wal::WAL;
use crate::request::{Request, Response, SetterType};

/// Simulated distributed consensus for testing failure recovery scenarios.
/// This implements a simplified leader-based replication system where:
/// - Leader handles all writes and replicates to followers
/// - Followers store replicated data and can recover from WAL
/// - Writes are committed when written to leader's WAL (simplified)
#[derive(Clone)]
pub struct ConsensusNode {
    /// Node ID in the cluster (0, 1, 2 for 3-node cluster)
    node_id: u64,
    /// Current term for leader election
    current_term: Arc<AtomicU64>,
    /// Node role: Leader, Follower, or Candidate
    role: Arc<RwLock<NodeRole>>,
    /// Cluster configuration (other node addresses)
    cluster_config: ClusterConfig,
    /// Underlying storage with WAL
    store: Arc<Store>,
    /// Replicated log entries (for followers)
    replicated_log: Arc<RwLock<Vec<LogEntry>>>,
    /// Last applied log index
    last_applied: Arc<AtomicU64>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum NodeRole {
    Leader,
    Follower,
    Candidate,
}

#[derive(Clone, Debug)]
pub struct ClusterConfig {
    pub node_id: u64,
    pub nodes: HashMap<u64, String>, // node_id -> address
}

#[derive(Clone, Debug)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub command: Command,
}

#[derive(Clone, Debug)]
pub enum Command {
    Set { key: StoreKey, value: StorePayload },
    Delete { key: StoreKey },
    Noop,
}

impl ConsensusNode {
    /// Create a new consensus node
    pub fn new(node_id: u64, opt: Options, cluster_config: ClusterConfig) -> MyResult<Self> {
        let store = Store::new(opt)?;

        Ok(ConsensusNode {
            node_id,
            current_term: Arc::new(AtomicU64::new(1)),
            role: Arc::new(RwLock::new(NodeRole::Follower)),
            cluster_config,
            store: Arc::new(store),
            replicated_log: Arc::new(RwLock::new(Vec::new())),
            last_applied: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Start as leader (for testing)
    pub fn become_leader(&self) {
        let mut role = self.role.write().unwrap();
        *role = NodeRole::Leader;
    }

    /// Start as follower (for testing)
    pub fn become_follower(&self) {
        let mut role = self.role.write().unwrap();
        *role = NodeRole::Follower;
    }

    /// Get current role
    pub fn get_role(&self) -> NodeRole {
        self.role.read().unwrap().clone()
    }

    /// Get current term
    pub fn get_current_term(&self) -> u64 {
        self.current_term.load(Ordering::SeqCst)
    }

    /// Apply a write request through consensus
    /// In real Raft, this would replicate to quorum before committing
    /// For testing, we simulate the consensus and rely on WAL for durability
    pub fn apply_write(&self, request: Request) -> MyResult<Response> {
        let role = self.get_role();

        match role {
            NodeRole::Leader => {
                // Leader: apply to local store and replicate
                let response = self.store.apply(request.clone())?;

                // Simulate replication to followers (would be async in real implementation)
                self.replicate_to_followers(&request)?;

                Ok(response)
            }
            NodeRole::Follower => {
                // Follower: redirect to leader or reject
                // For testing, we'll just apply locally but mark as follower write
                self.store.apply(request)
            }
            NodeRole::Candidate => {
                // Candidate: cannot accept writes during election
                Err(crate::error::err(
                    crate::error::StatusCode::Unavailable,
                    "Node is candidate, cannot accept writes",
                )?)
            }
        }
    }

    /// Simulate replication to followers (for testing)
    fn replicate_to_followers(&self, _request: &Request) -> MyResult<()> {
        // In a real implementation, this would send RPCs to follower nodes
        // For testing purposes, we simulate that replication happens
        Ok(())
    }

    /// Apply a replicated log entry (for followers)
    pub fn apply_log_entry(&self, entry: LogEntry) -> MyResult<()> {
        // Update last applied
        self.last_applied.store(entry.index, Ordering::SeqCst);

        // Apply the command based on type
        match entry.command {
            Command::Set { ref key, ref value } => {
                self.store.apply(Request::Setter {
                    setter: SetterType::Set,
                    key: key.clone(),
                    flags: value.flags,
                    ttl: value.ttl,
                    bytes: value.bytes,
                    payload: value.data.clone(),
                    no_reply: false,
                })?;
            }
            Command::Delete { ref key } => {
                self.store.apply(Request::Deleter {
                    key: key.clone(),
                    no_reply: false,
                })?;
            }
            Command::Noop => {
                // No operation, just mark applied
            }
        }

        Ok(())
    }

    /// Get value from store
    pub fn get(&self, key: &StoreKey) -> MyResult<Option<StorePayload>> {
        // Try to get from underlying store
        match self.store.apply(Request::Getter {
            getter: crate::request::GetterType::Get,
            keys: vec![key.clone()],
        })? {
            crate::response::Response::Get(mut items) => {
                if items.is_empty() {
                    Ok(None)
                } else {
                    let item = items.remove(0);
                    Ok(Some(StorePayload::new(
                        item.data,
                        item.flags,
                        0,
                        item.bytes,
                        0,
                    )))
                }
            }
            _ => Ok(None),
        }
    }

    /// Create a log entry for a write operation
    pub fn create_log_entry(&self, term: u64, command: Command) -> LogEntry {
        let index = self.last_applied.load(Ordering::SeqCst) + 1;
        LogEntry {
            index,
            term,
            command,
        }
    }

    /// Simulate node failure by clearing in-memory state
    /// The WAL will persist and can be replayed on restart
    pub fn simulate_failure(&self) {
        // In a real system, this would be process termination
        // For testing, we just note that the node is "failed"
        let mut role = self.role.write().unwrap();
        *role = NodeRole::Follower; // Reset role on "restart"
    }

    /// Get node ID
    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    /// Get store info for debugging
    pub fn info(&self) -> String {
        self.store.apply(crate::request::Request::Info)
            .map(|r| format!("{:?}", r))
            .unwrap_or_else(|_| "Error getting info".to_string())
    }
}

/// Create a log entry from a write request
pub fn request_to_log_entry(term: u64, request: &Request) -> Option<LogEntry> {
    match request {
        Request::Setter { key, flags, ttl, bytes, payload, .. } => {
            Some(LogEntry {
                index: 0, // Will be set when appending
                term,
                command: Command::Set {
                    key: key.clone(),
                    value: StorePayload::new(payload.clone(), *flags, *ttl, *bytes, 0),
                },
            })
        }
        Request::Deleter { key, .. } => {
            Some(LogEntry {
                index: 0,
                term,
                command: Command::Delete { key: key.clone() },
            })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::slice::Slice;
    use crate::store::StorePayload;

    #[test]
    fn test_consensus_node_creation() {
        let opt = crate::options::Options::default();
        let mut nodes = HashMap::new();
        nodes.insert(0, "127.0.0.1:9000".to_string());
        nodes.insert(1, "127.0.0.1:9001".to_string());
        nodes.insert(2, "127.0.0.1:9002".to_string());

        let config = ClusterConfig {
            node_id: 0,
            nodes,
        };

        let node = ConsensusNode::new(0, opt, config).unwrap();
        assert_eq!(node.node_id(), 0);
        assert_eq!(node.get_role(), NodeRole::Follower);
    }

    #[test]
    fn test_leader_role() {
        let opt = crate::options::Options::default();
        let config = ClusterConfig {
            node_id: 0,
            nodes: HashMap::new(),
        };

        let node = ConsensusNode::new(0, opt, config).unwrap();
        node.become_leader();
        assert_eq!(node.get_role(), NodeRole::Leader);
    }
}
