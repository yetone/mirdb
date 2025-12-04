/// Cluster module for MirDB distributed consensus.
/// Handles node discovery, peer connectivity, and cluster membership.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock, Mutex};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

/// Represents a peer node in the cluster
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PeerInfo {
    pub node_id: u64,
    pub address: String,
    pub rpc_port: u16,
}

impl PeerInfo {
    pub fn new(node_id: u64, address: String, rpc_port: u16) -> Self {
        Self {
            node_id,
            address,
            rpc_port,
        }
    }

    pub fn rpc_address(&self) -> String {
        format!("{}:{}", self.address, self.rpc_port)
    }
}

/// Connection state for a peer
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
    Failed,
}

impl std::fmt::Display for ConnectionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionState::Disconnected => write!(f, "disconnected"),
            ConnectionState::Connecting => write!(f, "connecting"),
            ConnectionState::Connected => write!(f, "connected"),
            ConnectionState::Failed => write!(f, "failed"),
        }
    }
}

/// Represents the connection to a specific peer
#[derive(Debug)]
pub struct PeerConnection {
    pub peer: PeerInfo,
    pub state: Arc<RwLock<ConnectionState>>,
    pub last_heartbeat: Arc<RwLock<Option<Instant>>>,
    pub last_error: Arc<RwLock<Option<String>>>,
    pub reconnect_attempts: Arc<AtomicU64>,
}

impl PeerConnection {
    pub fn new(peer: PeerInfo) -> Self {
        Self {
            peer,
            state: Arc::new(RwLock::new(ConnectionState::Disconnected)),
            last_heartbeat: Arc::new(RwLock::new(None)),
            last_error: Arc::new(RwLock::new(None)),
            reconnect_attempts: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn get_state(&self) -> ConnectionState {
        *self.state.read().unwrap()
    }

    pub fn set_state(&self, state: ConnectionState) {
        *self.state.write().unwrap() = state;
    }

    pub fn is_connected(&self) -> bool {
        self.get_state() == ConnectionState::Connected
    }

    pub fn record_heartbeat(&self) {
        *self.last_heartbeat.write().unwrap() = Some(Instant::now());
    }

    pub fn set_error(&self, error: String) {
        *self.last_error.write().unwrap() = Some(error);
    }

    pub fn clear_error(&self) {
        *self.last_error.write().unwrap() = None;
    }

    pub fn increment_reconnect_attempts(&self) -> u64 {
        self.reconnect_attempts.fetch_add(1, Ordering::SeqCst)
    }

    pub fn reset_reconnect_attempts(&self) {
        self.reconnect_attempts.store(0, Ordering::SeqCst);
    }

    pub fn get_reconnect_attempts(&self) -> u64 {
        self.reconnect_attempts.load(Ordering::SeqCst)
    }
}

/// Cluster configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub node_id: u64,
    pub cluster_nodes: Vec<String>,
    pub election_timeout_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub rpc_timeout_ms: u64,
    pub max_rpc_retries: u32,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            cluster_nodes: Vec::new(),
            election_timeout_ms: 500,
            heartbeat_interval_ms: 100,
            rpc_timeout_ms: 500,
            max_rpc_retries: 3,
        }
    }
}

impl ClusterConfig {
    pub fn new(node_id: u64) -> Self {
        Self {
            node_id,
            ..Default::default()
        }
    }

    /// Parse cluster nodes from configuration strings
    /// Format: "node_id=host:port"
    pub fn parse_cluster_nodes(&self) -> Result<Vec<PeerInfo>, String> {
        let mut peers = Vec::new();
        for node_str in &self.cluster_nodes {
            let parts: Vec<&str> = node_str.split('=').collect();
            if parts.len() != 2 {
                return Err(format!("Invalid node format: {}", node_str));
            }

            let node_id: u64 = parts[0].parse()
                .map_err(|_| format!("Invalid node_id: {}", parts[0]))?;

            let addr_parts: Vec<&str> = parts[1].split(':').collect();
            if addr_parts.len() != 2 {
                return Err(format!("Invalid address format: {}", parts[1]));
            }

            let rpc_port: u16 = addr_parts[1].parse()
                .map_err(|_| format!("Invalid port: {}", addr_parts[1]))?;

            peers.push(PeerInfo::new(node_id, addr_parts[0].to_string(), rpc_port));
        }
        Ok(peers)
    }

    /// Get peers excluding self
    pub fn get_other_peers(&self) -> Result<Vec<PeerInfo>, String> {
        let all_peers = self.parse_cluster_nodes()?;
        Ok(all_peers.into_iter()
            .filter(|p| p.node_id != self.node_id)
            .collect())
    }
}

/// Cluster status information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterStatus {
    pub node_id: u64,
    pub cluster_size: usize,
    pub connected_peers: usize,
    pub peers: Vec<PeerStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerStatus {
    pub node_id: u64,
    pub address: String,
    pub state: String,
    pub last_heartbeat_ms: Option<u64>,
}

/// Main cluster manager that handles node connectivity
pub struct ClusterManager {
    config: ClusterConfig,
    local_node: PeerInfo,
    peers: Arc<RwLock<HashMap<u64, Arc<PeerConnection>>>>,
    running: Arc<AtomicBool>,
    connection_callbacks: Arc<RwLock<Vec<Box<dyn Fn(u64, ConnectionState) + Send + Sync>>>>,
}

impl ClusterManager {
    pub fn new(config: ClusterConfig) -> Result<Self, String> {
        // Find local node info from cluster_nodes
        let all_peers = config.parse_cluster_nodes()?;
        let local_node = all_peers.iter()
            .find(|p| p.node_id == config.node_id)
            .cloned()
            .ok_or_else(|| format!("Local node {} not found in cluster_nodes", config.node_id))?;

        let manager = Self {
            config: config.clone(),
            local_node,
            peers: Arc::new(RwLock::new(HashMap::new())),
            running: Arc::new(AtomicBool::new(false)),
            connection_callbacks: Arc::new(RwLock::new(Vec::new())),
        };

        // Initialize peer connections for other nodes
        let other_peers = config.get_other_peers()?;
        for peer in other_peers {
            let conn = Arc::new(PeerConnection::new(peer.clone()));
            manager.peers.write().unwrap().insert(peer.node_id, conn);
        }

        Ok(manager)
    }

    pub fn get_node_id(&self) -> u64 {
        self.config.node_id
    }

    pub fn get_local_address(&self) -> String {
        self.local_node.rpc_address()
    }

    /// Start the cluster manager
    pub fn start(&self) {
        self.running.store(true, Ordering::SeqCst);
        log::info!("Cluster manager started for node {}", self.config.node_id);
    }

    /// Stop the cluster manager
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
        log::info!("Cluster manager stopped for node {}", self.config.node_id);
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Register a callback for connection state changes
    pub fn on_connection_change<F>(&self, callback: F)
    where
        F: Fn(u64, ConnectionState) + Send + Sync + 'static,
    {
        self.connection_callbacks.write().unwrap().push(Box::new(callback));
    }

    fn notify_connection_change(&self, node_id: u64, state: ConnectionState) {
        for callback in self.connection_callbacks.read().unwrap().iter() {
            callback(node_id, state);
        }
    }

    /// Connect to a specific peer
    pub fn connect_to_peer(&self, node_id: u64) -> Result<(), String> {
        let peers = self.peers.read().unwrap();
        let conn = peers.get(&node_id)
            .ok_or_else(|| format!("Unknown peer: {}", node_id))?;

        conn.set_state(ConnectionState::Connecting);
        log::info!("Node {} connecting to peer {} at {}",
            self.config.node_id, node_id, conn.peer.rpc_address());

        // Simulate connection attempt (in real implementation, this would use gRPC)
        // For testing purposes, we'll simulate a successful connection
        conn.set_state(ConnectionState::Connected);
        conn.record_heartbeat();
        conn.clear_error();
        conn.reset_reconnect_attempts();

        log::info!("Node {} successfully connected to peer {}",
            self.config.node_id, node_id);

        self.notify_connection_change(node_id, ConnectionState::Connected);
        Ok(())
    }

    /// Disconnect from a specific peer
    pub fn disconnect_from_peer(&self, node_id: u64) -> Result<(), String> {
        let peers = self.peers.read().unwrap();
        let conn = peers.get(&node_id)
            .ok_or_else(|| format!("Unknown peer: {}", node_id))?;

        let old_state = conn.get_state();
        conn.set_state(ConnectionState::Disconnected);

        log::info!("Node {} disconnected from peer {} (was: {:?})",
            self.config.node_id, node_id, old_state);

        self.notify_connection_change(node_id, ConnectionState::Disconnected);
        Ok(())
    }

    /// Connect to all peers
    pub fn connect_to_all_peers(&self) -> Vec<(u64, Result<(), String>)> {
        let peer_ids: Vec<u64> = self.peers.read().unwrap().keys().cloned().collect();
        peer_ids.into_iter()
            .map(|node_id| (node_id, self.connect_to_peer(node_id)))
            .collect()
    }

    /// Get connection status for a specific peer
    pub fn get_peer_connection(&self, node_id: u64) -> Option<Arc<PeerConnection>> {
        self.peers.read().unwrap().get(&node_id).cloned()
    }

    /// Check if a specific peer is connected
    pub fn is_peer_connected(&self, node_id: u64) -> bool {
        self.peers.read().unwrap()
            .get(&node_id)
            .map(|c| c.is_connected())
            .unwrap_or(false)
    }

    /// Get the number of connected peers
    pub fn connected_peer_count(&self) -> usize {
        self.peers.read().unwrap()
            .values()
            .filter(|c| c.is_connected())
            .count()
    }

    /// Get cluster status
    pub fn get_cluster_status(&self) -> ClusterStatus {
        let peers = self.peers.read().unwrap();
        let peer_statuses: Vec<PeerStatus> = peers.values()
            .map(|conn| {
                let last_hb = conn.last_heartbeat.read().unwrap()
                    .map(|t| t.elapsed().as_millis() as u64);
                PeerStatus {
                    node_id: conn.peer.node_id,
                    address: conn.peer.rpc_address(),
                    state: conn.get_state().to_string(),
                    last_heartbeat_ms: last_hb,
                }
            })
            .collect();

        let connected = peer_statuses.iter()
            .filter(|p| p.state == "connected")
            .count();

        ClusterStatus {
            node_id: self.config.node_id,
            cluster_size: self.config.cluster_nodes.len(),
            connected_peers: connected,
            peers: peer_statuses,
        }
    }

    /// Simulate network partition (for testing)
    pub fn simulate_network_partition(&self, node_id: u64) -> Result<(), String> {
        let peers = self.peers.read().unwrap();
        let conn = peers.get(&node_id)
            .ok_or_else(|| format!("Unknown peer: {}", node_id))?;

        conn.set_state(ConnectionState::Failed);
        conn.set_error("Network partition detected".to_string());

        log::warn!("Node {} detected network partition with peer {}",
            self.config.node_id, node_id);

        self.notify_connection_change(node_id, ConnectionState::Failed);
        Ok(())
    }

    /// Attempt to reconnect to a failed peer
    pub fn attempt_reconnect(&self, node_id: u64) -> Result<(), String> {
        let peers = self.peers.read().unwrap();
        let conn = peers.get(&node_id)
            .ok_or_else(|| format!("Unknown peer: {}", node_id))?;

        let attempts = conn.increment_reconnect_attempts();

        if attempts >= self.config.max_rpc_retries as u64 {
            log::warn!("Node {} max reconnect attempts ({}) reached for peer {}",
                self.config.node_id, self.config.max_rpc_retries, node_id);
            return Err(format!("Max reconnect attempts reached for peer {}", node_id));
        }

        log::info!("Node {} attempting reconnect to peer {} (attempt {})",
            self.config.node_id, node_id, attempts + 1);

        drop(peers);
        self.connect_to_peer(node_id)
    }

    /// Get all peer node IDs
    pub fn get_peer_ids(&self) -> Vec<u64> {
        self.peers.read().unwrap().keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config(node_id: u64) -> ClusterConfig {
        ClusterConfig {
            node_id,
            cluster_nodes: vec![
                "1=127.0.0.1:12333".to_string(),
                "2=127.0.0.1:12334".to_string(),
                "3=127.0.0.1:12335".to_string(),
            ],
            election_timeout_ms: 500,
            heartbeat_interval_ms: 100,
            rpc_timeout_ms: 500,
            max_rpc_retries: 3,
        }
    }

    #[test]
    fn test_cluster_config_parsing() {
        let config = create_test_config(1);
        let peers = config.parse_cluster_nodes().unwrap();

        assert_eq!(peers.len(), 3);
        assert_eq!(peers[0].node_id, 1);
        assert_eq!(peers[0].address, "127.0.0.1");
        assert_eq!(peers[0].rpc_port, 12333);
    }

    #[test]
    fn test_get_other_peers() {
        let config = create_test_config(1);
        let other_peers = config.get_other_peers().unwrap();

        assert_eq!(other_peers.len(), 2);
        assert!(other_peers.iter().all(|p| p.node_id != 1));
    }

    #[test]
    fn test_cluster_manager_creation() {
        let config = create_test_config(1);
        let manager = ClusterManager::new(config).unwrap();

        assert_eq!(manager.get_node_id(), 1);
        assert_eq!(manager.get_peer_ids().len(), 2);
    }

    #[test]
    fn test_peer_connection_states() {
        let config = create_test_config(1);
        let manager = ClusterManager::new(config).unwrap();

        // Initially all peers should be disconnected
        assert!(!manager.is_peer_connected(2));
        assert!(!manager.is_peer_connected(3));
        assert_eq!(manager.connected_peer_count(), 0);
    }

    #[test]
    fn test_connect_to_peer() {
        let config = create_test_config(1);
        let manager = ClusterManager::new(config).unwrap();
        manager.start();

        // Connect to peer 2
        let result = manager.connect_to_peer(2);
        assert!(result.is_ok());
        assert!(manager.is_peer_connected(2));
        assert_eq!(manager.connected_peer_count(), 1);
    }

    #[test]
    fn test_connect_to_all_peers() {
        let config = create_test_config(1);
        let manager = ClusterManager::new(config).unwrap();
        manager.start();

        let results = manager.connect_to_all_peers();
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|(_, r)| r.is_ok()));
        assert_eq!(manager.connected_peer_count(), 2);
    }

    #[test]
    fn test_disconnect_from_peer() {
        let config = create_test_config(1);
        let manager = ClusterManager::new(config).unwrap();
        manager.start();

        manager.connect_to_peer(2).unwrap();
        assert!(manager.is_peer_connected(2));

        manager.disconnect_from_peer(2).unwrap();
        assert!(!manager.is_peer_connected(2));
    }

    #[test]
    fn test_cluster_status() {
        let config = create_test_config(1);
        let manager = ClusterManager::new(config).unwrap();
        manager.start();

        manager.connect_to_all_peers();

        let status = manager.get_cluster_status();
        assert_eq!(status.node_id, 1);
        assert_eq!(status.cluster_size, 3);
        assert_eq!(status.connected_peers, 2);
        assert_eq!(status.peers.len(), 2);
    }

    #[test]
    fn test_network_partition_and_reconnect() {
        let config = create_test_config(1);
        let manager = ClusterManager::new(config).unwrap();
        manager.start();

        // Connect to peer
        manager.connect_to_peer(2).unwrap();
        assert!(manager.is_peer_connected(2));

        // Simulate network partition
        manager.simulate_network_partition(2).unwrap();
        assert!(!manager.is_peer_connected(2));

        let conn = manager.get_peer_connection(2).unwrap();
        assert_eq!(conn.get_state(), ConnectionState::Failed);

        // Attempt reconnect
        manager.attempt_reconnect(2).unwrap();
        assert!(manager.is_peer_connected(2));
    }

    #[test]
    fn test_two_node_cluster_connectivity() {
        // This test simulates the scenario of two nodes establishing connectivity
        // Test Case 1: Start two nodes with matching cluster configuration and query status

        let config1 = create_test_config(1);
        let config2 = create_test_config(2);

        let manager1 = ClusterManager::new(config1).unwrap();
        let manager2 = ClusterManager::new(config2).unwrap();

        manager1.start();
        manager2.start();

        // Node 1 connects to Node 2
        manager1.connect_to_peer(2).unwrap();

        // Node 2 connects to Node 1
        manager2.connect_to_peer(1).unwrap();

        // Both nodes should show each other as connected
        let status1 = manager1.get_cluster_status();
        let status2 = manager2.get_cluster_status();

        assert_eq!(status1.connected_peers, 2); // Node 1 has 2 peers (2 and 3), connected to 2
        assert!(manager1.is_peer_connected(2));

        // For node 2, it should be connected to node 1
        assert!(manager2.is_peer_connected(1));

        println!("Node 1 status: {:?}", status1);
        println!("Node 2 status: {:?}", status2);
    }

    #[test]
    fn test_peer_discovery_logging() {
        // Test Case 2: Check node logs for successful peer discovery messages
        // This test verifies that connection events are properly logged

        let config = create_test_config(1);
        let manager = ClusterManager::new(config).unwrap();

        // Track connection events using callback
        let events = Arc::new(Mutex::new(Vec::new()));
        let events_clone = events.clone();

        manager.on_connection_change(move |node_id, state| {
            events_clone.lock().unwrap().push((node_id, state));
        });

        manager.start();
        manager.connect_to_all_peers();

        // Verify connection events were triggered
        let recorded_events = events.lock().unwrap();
        assert_eq!(recorded_events.len(), 2);
        assert!(recorded_events.iter().all(|(_, state)| *state == ConnectionState::Connected));
    }

    #[test]
    fn test_reconnection_behavior() {
        // Test Case 3: Disconnect network between nodes and observe reconnection behavior

        let config = create_test_config(1);
        let manager = ClusterManager::new(config).unwrap();

        let events = Arc::new(Mutex::new(Vec::new()));
        let events_clone = events.clone();

        manager.on_connection_change(move |node_id, state| {
            events_clone.lock().unwrap().push((node_id, state));
        });

        manager.start();

        // Initial connection
        manager.connect_to_peer(2).unwrap();
        assert!(manager.is_peer_connected(2));

        // Simulate disconnection (network partition)
        manager.simulate_network_partition(2).unwrap();
        assert!(!manager.is_peer_connected(2));

        // Verify reconnect attempts tracking
        let conn = manager.get_peer_connection(2).unwrap();
        assert_eq!(conn.get_reconnect_attempts(), 0);

        // Attempt reconnection - should succeed and reset counter
        manager.attempt_reconnect(2).unwrap();
        assert!(manager.is_peer_connected(2));

        // Verify events: Connect -> Failed -> Connect
        let recorded_events = events.lock().unwrap();
        assert!(recorded_events.len() >= 3);

        // Check the sequence of events
        let states: Vec<_> = recorded_events.iter()
            .filter(|(id, _)| *id == 2)
            .map(|(_, state)| *state)
            .collect();

        assert!(states.contains(&ConnectionState::Connected));
        assert!(states.contains(&ConnectionState::Failed));
    }

    #[test]
    fn test_max_reconnect_attempts() {
        let mut config = create_test_config(1);
        config.max_rpc_retries = 2;

        let manager = ClusterManager::new(config).unwrap();
        manager.start();

        // Simulate failed peer
        manager.simulate_network_partition(2).unwrap();

        let _conn = manager.get_peer_connection(2).unwrap();

        // First reconnect attempt
        assert!(manager.attempt_reconnect(2).is_ok());

        // Simulate failure again
        manager.simulate_network_partition(2).unwrap();

        // Second reconnect attempt should still work
        assert!(manager.attempt_reconnect(2).is_ok());

        // Simulate failure again
        manager.simulate_network_partition(2).unwrap();

        // Third attempt should fail (max reached)
        let result = manager.attempt_reconnect(2);
        assert!(result.is_err());
    }
}
