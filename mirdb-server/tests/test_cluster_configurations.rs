/// Test suite for different cluster configurations.
/// Validates quorum logic for 3-node, 5-node, and 7-node clusters.
///
/// This module tests the Raft consensus quorum requirements:
/// - 3-node cluster: quorum = 2, tolerates 1 failure
/// - 5-node cluster: quorum = 3, tolerates 2 failures
/// - 7-node cluster: quorum = 4, tolerates 3 failures
///
/// For a cluster of N nodes, quorum = N/2 + 1 (majority)

/// Calculates the quorum (majority) required for a cluster of given size
fn calculate_quorum(cluster_size: usize) -> usize {
    cluster_size / 2 + 1
}

/// Determines if we have quorum given alive nodes
fn has_quorum(cluster_size: usize, alive_nodes: usize) -> bool {
    alive_nodes >= calculate_quorum(cluster_size)
}

/// Represents a mock cluster node for testing
#[derive(Debug, Clone)]
struct MockNode {
    id: usize,
    alive: bool,
}

/// Represents a mock cluster for testing quorum logic
struct MockCluster {
    nodes: Vec<MockNode>,
}

impl MockCluster {
    fn new(size: usize) -> Self {
        let nodes = (1..=size)
            .map(|id| MockNode { id, alive: true })
            .collect();
        Self { nodes }
    }

    fn cluster_size(&self) -> usize {
        self.nodes.len()
    }

    fn alive_count(&self) -> usize {
        self.nodes.iter().filter(|n| n.alive).count()
    }

    fn has_quorum(&self) -> bool {
        has_quorum(self.cluster_size(), self.alive_count())
    }

    fn kill_node(&mut self, id: usize) {
        if let Some(node) = self.nodes.iter_mut().find(|n| n.id == id) {
            node.alive = false;
        }
    }

    fn revive_node(&mut self, id: usize) {
        if let Some(node) = self.nodes.iter_mut().find(|n| n.id == id) {
            node.alive = true;
        }
    }

    /// Simulates a write operation - succeeds only if quorum is available
    fn write(&self, _key: &str, _value: &str) -> Result<(), &'static str> {
        if self.has_quorum() {
            Ok(())
        } else {
            Err("No quorum available - write failed")
        }
    }

    /// Simulates a read operation - can succeed with any alive node
    fn read(&self, _key: &str) -> Result<Option<String>, &'static str> {
        if self.alive_count() > 0 {
            Ok(Some("value".to_string()))
        } else {
            Err("No alive nodes")
        }
    }
}

// =============================================================================
// TEST CASE 1: 3-node cluster, kill 1 node
// Expected: Writes continue successfully with 2 remaining nodes
// =============================================================================

#[test]
fn test_3_node_cluster_kill_1_node() {
    println!("\n=== TEST CASE 1: 3-node cluster, kill 1 node ===");
    println!("Expected: Writes continue successfully with 2 remaining nodes");

    let mut cluster = MockCluster::new(3);

    // Verify initial state - all 3 nodes alive
    assert_eq!(cluster.alive_count(), 3);
    assert!(cluster.has_quorum(), "3-node cluster should have quorum with all nodes");

    // Initial write should succeed
    let result = cluster.write("test1", "value1");
    assert!(result.is_ok(), "Initial write should succeed with 3 nodes");
    println!("  ✓ Initial write successful with 3 nodes");

    // Kill node 3
    cluster.kill_node(3);
    assert_eq!(cluster.alive_count(), 2, "Should have 2 nodes alive after killing 1");

    // Quorum for 3 nodes is 2, so we still have quorum
    assert!(cluster.has_quorum(), "Should still have quorum with 2/3 nodes");

    // Write should still succeed
    let result = cluster.write("test2", "value2");
    assert!(result.is_ok(), "Write should succeed with 2 nodes (quorum maintained)");
    println!("  ✓ Write succeeded after killing 1 node (quorum: 2/3 maintained)");

    // Verify data can be read
    let result = cluster.read("test2");
    assert!(result.is_ok());
    println!("  ✓ Read successful with 2 remaining nodes");

    println!("✓ TEST CASE 1 PASSED: 3-node cluster survives 1 node failure");
}

// =============================================================================
// TEST CASE 2: 3-node cluster, kill 2 nodes
// Expected: Write fails or times out (no majority available)
// =============================================================================

#[test]
fn test_3_node_cluster_kill_2_nodes() {
    println!("\n=== TEST CASE 2: 3-node cluster, kill 2 nodes ===");
    println!("Expected: Write fails (no majority available)");

    let mut cluster = MockCluster::new(3);

    // Verify initial state
    assert_eq!(cluster.alive_count(), 3);
    assert!(cluster.has_quorum());

    // Initial write should succeed
    let result = cluster.write("test1", "value1");
    assert!(result.is_ok(), "Initial write should succeed");
    println!("  ✓ Initial write successful with 3 nodes");

    // Kill nodes 2 and 3
    cluster.kill_node(2);
    cluster.kill_node(3);
    assert_eq!(cluster.alive_count(), 1, "Should have 1 node alive after killing 2");

    // Quorum for 3 nodes is 2, so we DON'T have quorum with only 1 node
    assert!(!cluster.has_quorum(), "Should NOT have quorum with 1/3 nodes");

    // Write should FAIL
    let result = cluster.write("test2", "value2");
    assert!(result.is_err(), "Write should fail without quorum (only 1 of 3 nodes)");
    println!("  ✓ Write correctly failed without quorum (1/3 nodes, need 2)");

    // Read might still work (stale read from remaining node)
    let result = cluster.read("test1");
    assert!(result.is_ok(), "Read from single node should still work (stale data)");
    println!("  ✓ Read succeeded from single remaining node");

    println!("✓ TEST CASE 2 PASSED: 3-node cluster correctly rejects writes below quorum");
}

// =============================================================================
// TEST CASE 3: 5-node cluster, kill 2 nodes
// Expected: Writes continue successfully with 3 remaining nodes
// =============================================================================

#[test]
fn test_5_node_cluster_kill_2_nodes() {
    println!("\n=== TEST CASE 3: 5-node cluster, kill 2 nodes ===");
    println!("Expected: Writes continue successfully with 3 remaining nodes");

    let mut cluster = MockCluster::new(5);

    // Verify initial state - all 5 nodes alive
    assert_eq!(cluster.alive_count(), 5);
    assert_eq!(calculate_quorum(5), 3, "Quorum for 5-node cluster should be 3");
    assert!(cluster.has_quorum());

    // Initial write should succeed
    let result = cluster.write("test1", "value1");
    assert!(result.is_ok(), "Initial write should succeed with 5 nodes");
    println!("  ✓ Initial write successful with 5 nodes");

    // Kill nodes 4 and 5
    cluster.kill_node(4);
    cluster.kill_node(5);
    assert_eq!(cluster.alive_count(), 3, "Should have 3 nodes alive after killing 2");

    // Quorum for 5 nodes is 3, so we still have quorum
    assert!(cluster.has_quorum(), "Should still have quorum with 3/5 nodes");

    // Write should still succeed
    let result = cluster.write("test2", "value2");
    assert!(result.is_ok(), "Write should succeed with 3 nodes (quorum maintained)");
    println!("  ✓ Write succeeded after killing 2 nodes (quorum: 3/5 maintained)");

    // Verify data can be read
    let result = cluster.read("test2");
    assert!(result.is_ok());
    println!("  ✓ Read successful with 3 remaining nodes");

    println!("✓ TEST CASE 3 PASSED: 5-node cluster survives 2 node failures");
}

// =============================================================================
// TEST CASE 4: 5-node cluster, kill 3 nodes
// Expected: Write fails (only 2 nodes remaining, no majority)
// =============================================================================

#[test]
fn test_5_node_cluster_kill_3_nodes() {
    println!("\n=== TEST CASE 4: 5-node cluster, kill 3 nodes ===");
    println!("Expected: Write fails (only 2 nodes remaining, no majority)");

    let mut cluster = MockCluster::new(5);

    // Verify initial state
    assert_eq!(cluster.alive_count(), 5);
    assert!(cluster.has_quorum());

    // Initial write should succeed
    let result = cluster.write("test1", "value1");
    assert!(result.is_ok(), "Initial write should succeed");
    println!("  ✓ Initial write successful with 5 nodes");

    // Kill nodes 3, 4, and 5
    cluster.kill_node(3);
    cluster.kill_node(4);
    cluster.kill_node(5);
    assert_eq!(cluster.alive_count(), 2, "Should have 2 nodes alive after killing 3");

    // Quorum for 5 nodes is 3, so we DON'T have quorum with only 2 nodes
    assert!(!cluster.has_quorum(), "Should NOT have quorum with 2/5 nodes");

    // Write should FAIL
    let result = cluster.write("test2", "value2");
    assert!(result.is_err(), "Write should fail without quorum (only 2 of 5 nodes)");
    println!("  ✓ Write correctly failed without quorum (2/5 nodes, need 3)");

    println!("✓ TEST CASE 4 PASSED: 5-node cluster correctly rejects writes below quorum");
}

// =============================================================================
// TEST CASE 5: 7-node cluster - comprehensive test
// Expected: 7 nodes form cluster; can tolerate 3 node failures;
//           read/write operations succeed until below quorum
// =============================================================================

#[test]
fn test_7_node_cluster_comprehensive() {
    println!("\n=== TEST CASE 5: 7-node cluster comprehensive test ===");
    println!("Expected: Can tolerate 3 failures; fails when quorum lost");

    let mut cluster = MockCluster::new(7);

    // Verify initial state - all 7 nodes alive
    assert_eq!(cluster.alive_count(), 7);
    assert_eq!(calculate_quorum(7), 4, "Quorum for 7-node cluster should be 4");
    assert!(cluster.has_quorum());
    println!("  ✓ 7-node cluster formed successfully");

    // Test write operation
    let result = cluster.write("test1", "value1");
    assert!(result.is_ok(), "Write should succeed with 7 nodes");
    println!("  ✓ Write successful with all 7 nodes");

    // Test read operation
    let result = cluster.read("test1");
    assert!(result.is_ok());
    println!("  ✓ Read successful with all 7 nodes");

    // === Test fault tolerance: kill 2 nodes (5 alive, quorum=4) ===
    println!("\n  Testing fault tolerance: killing 2 nodes...");
    cluster.kill_node(6);
    cluster.kill_node(7);
    assert_eq!(cluster.alive_count(), 5);
    assert!(cluster.has_quorum(), "Should have quorum with 5/7 nodes");

    let result = cluster.write("test2", "value2");
    assert!(result.is_ok(), "Write should succeed with 5 nodes");
    println!("  ✓ Write succeeded after killing 2 nodes (5/7 alive, quorum=4)");

    // === Kill one more node (4 alive, exactly at quorum) ===
    println!("\n  Testing at quorum boundary: killing 1 more node...");
    cluster.kill_node(5);
    assert_eq!(cluster.alive_count(), 4);
    assert!(cluster.has_quorum(), "Should still have quorum with exactly 4/7 nodes");

    let result = cluster.write("test3", "value3");
    assert!(result.is_ok(), "Write should succeed at quorum boundary (4 nodes)");
    println!("  ✓ Write succeeded at quorum boundary (4/7 alive, quorum=4)");

    // === Kill one more node (3 alive, below quorum) ===
    println!("\n  Testing below quorum: killing 1 more node...");
    cluster.kill_node(4);
    assert_eq!(cluster.alive_count(), 3);
    assert!(!cluster.has_quorum(), "Should NOT have quorum with 3/7 nodes");

    let result = cluster.write("test4", "value4");
    assert!(result.is_err(), "Write should fail below quorum");
    println!("  ✓ Write correctly failed below quorum (3/7 alive, need 4)");

    // === Verify reads still work from remaining nodes ===
    let result = cluster.read("test3");
    assert!(result.is_ok(), "Reads should still work from remaining nodes");
    println!("  ✓ Read successful from remaining 3 nodes (stale data)");

    println!("\n✓ TEST CASE 5 PASSED: 7-node cluster handles failures correctly");
}

// =============================================================================
// Additional tests for edge cases
// =============================================================================

#[test]
fn test_quorum_calculation() {
    // Test quorum calculations for various cluster sizes
    assert_eq!(calculate_quorum(3), 2, "3-node quorum should be 2");
    assert_eq!(calculate_quorum(5), 3, "5-node quorum should be 3");
    assert_eq!(calculate_quorum(7), 4, "7-node quorum should be 4");
    assert_eq!(calculate_quorum(9), 5, "9-node quorum should be 5");

    println!("✓ Quorum calculations verified for cluster sizes 3, 5, 7, 9");
}

#[test]
fn test_fault_tolerance_limits() {
    // 3-node cluster: tolerates 1 failure
    let mut cluster_3 = MockCluster::new(3);
    cluster_3.kill_node(3);
    assert!(cluster_3.has_quorum(), "3-node should survive 1 failure");
    cluster_3.kill_node(2);
    assert!(!cluster_3.has_quorum(), "3-node should NOT survive 2 failures");

    // 5-node cluster: tolerates 2 failures
    let mut cluster_5 = MockCluster::new(5);
    cluster_5.kill_node(4);
    cluster_5.kill_node(5);
    assert!(cluster_5.has_quorum(), "5-node should survive 2 failures");
    cluster_5.kill_node(3);
    assert!(!cluster_5.has_quorum(), "5-node should NOT survive 3 failures");

    // 7-node cluster: tolerates 3 failures
    let mut cluster_7 = MockCluster::new(7);
    cluster_7.kill_node(5);
    cluster_7.kill_node(6);
    cluster_7.kill_node(7);
    assert!(cluster_7.has_quorum(), "7-node should survive 3 failures");
    cluster_7.kill_node(4);
    assert!(!cluster_7.has_quorum(), "7-node should NOT survive 4 failures");

    println!("✓ Fault tolerance limits verified for 3, 5, 7 node clusters");
}

#[test]
fn test_node_recovery() {
    // Test that a cluster can regain quorum when nodes recover
    let mut cluster = MockCluster::new(5);

    // Kill enough nodes to lose quorum
    cluster.kill_node(3);
    cluster.kill_node(4);
    cluster.kill_node(5);
    assert!(!cluster.has_quorum(), "Should not have quorum with 2/5 nodes");

    // Revive one node - should regain quorum
    cluster.revive_node(3);
    assert!(cluster.has_quorum(), "Should regain quorum when node 3 recovers (3/5)");

    // Write should succeed again
    let result = cluster.write("recovery_test", "value");
    assert!(result.is_ok(), "Write should succeed after quorum recovery");

    println!("✓ Node recovery and quorum regain verified");
}

fn main() {
    println!("Running cluster configuration tests...\n");

    // These would be run by cargo test, but we can call them manually too
    test_quorum_calculation();
    test_3_node_cluster_kill_1_node();
    test_3_node_cluster_kill_2_nodes();
    test_5_node_cluster_kill_2_nodes();
    test_5_node_cluster_kill_3_nodes();
    test_7_node_cluster_comprehensive();
    test_fault_tolerance_limits();
    test_node_recovery();

    println!("\n========================================");
    println!("ALL CLUSTER CONFIGURATION TESTS PASSED!");
    println!("========================================");
}
