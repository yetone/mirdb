#!/usr/bin/env python3
"""
Integration tests for different cluster configurations.
Tests 3-node, 5-node, and 7-node clusters with various failure scenarios.
"""

import sys
import time
import subprocess
import socket
import threading
import signal
import os
import memcache
from typing import List, Dict, Optional


class ClusterNode:
    """Represents a single node in the MirDB cluster."""

    def __init__(self, node_id: int, port: int, config_path: str):
        self.node_id = node_id
        self.port = port
        self.config_path = config_path
        self.process: Optional[subprocess.Popen] = None
        self.client: Optional[memcache.Client] = None

    def start(self):
        """Start the MirDB server process."""
        print(f"  Starting node {self.node_id} on port {self.port}...")
        self.process = subprocess.Popen(
            ["./target/debug/mirdb-server", "-c", self.config_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd="/workspace",
            preexec_fn=os.setsid
        )

        # Wait for server to start
        time.sleep(3)

        # Verify it's running
        if not self.is_alive():
            raise RuntimeError(f"Node {self.node_id} failed to start on port {self.port}")

        # Create memcached client
        self.client = memcache.Client([f'127.0.0.1:{self.port}'], debug=0)
        print(f"  ✓ Node {self.node_id} started successfully")

    def stop(self):
        """Stop the MirDB server process."""
        if self.process:
            print(f"  Stopping node {self.node_id}")
            try:
                os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)
                self.process.wait(timeout=5)
            except (subprocess.TimeoutExpired, ProcessLookupError):
                try:
                    os.killpg(os.getpgid(self.process.pid), signal.SIGKILL)
                except ProcessLookupError:
                    pass
            self.process = None
            self.client = None

    def kill(self):
        """Forcefully kill the node (simulates crash)."""
        if self.process:
            print(f"  Killing node {self.node_id} (simulating crash)")
            try:
                os.killpg(os.getpgid(self.process.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass
            self.process = None
            self.client = None
            time.sleep(1)  # Give it time to actually die

    def is_alive(self) -> bool:
        """Check if the node is alive and accepting connections."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex(('127.0.0.1', self.port))
            sock.close()
            return result == 0
        except:
            return False

    def write(self, key: str, value: str) -> bool:
        """Write a value to the node."""
        if not self.client:
            return False
        try:
            return self.client.set(key, value)
        except:
            return False

    def read(self, key: str):
        """Read a value from the node."""
        if not self.client:
            return None
        try:
            return self.client.get(key)
        except:
            return None


class Cluster:
    """Represents a MirDB cluster with multiple nodes."""

    def __init__(self, node_count: int):
        self.node_count = node_count
        self.nodes: Dict[int, ClusterNode] = {}
        self._create_nodes()

    def _create_nodes(self):
        """Create and configure nodes for the cluster."""
        base_port = 13000
        for i in range(self.node_count):
            node_id = i + 1
            port = base_port + node_id
            config_path = f"/workspace/etc/mirdb-node{node_id}.toml"

            # Create a config file for this node
            self._create_node_config(node_id, port)

            self.nodes[node_id] = ClusterNode(node_id, port, config_path)

    def _create_node_config(self, node_id: int, port: int):
        """Create a configuration file for a node."""
        config_content = f"""# MirDB Node {node_id} Configuration

[mirdb]
path = "/tmp/mirdb-node{node_id}"
host = "127.0.0.1"
port = {port}
memtable_size = 4194304  # 4MB

[raft]
node_id = {node_id}
cluster_nodes = [{', '.join(str(i) for i in range(1, self.node_count + 1))}]
election_timeout_ms = 500
heartbeat_interval_ms = 100
"""
        config_path = f"/workspace/etc/mirdb-node{node_id}.toml"
        with open(config_path, 'w') as f:
            f.write(config_content)

    def start(self):
        """Start all nodes in the cluster."""
        print(f"\nStarting {self.node_count}-node cluster...")
        for node in self.nodes.values():
            node.start()
            time.sleep(1)  # Stagger starts slightly

        # Wait for cluster formation and leader election
        print(f"  Waiting for leader election...")
        time.sleep(5)

        # Verify all nodes are alive
        alive_count = sum(1 for node in self.nodes.values() if node.is_alive())
        if alive_count != self.node_count:
            raise RuntimeError(f"Only {alive_count}/{self.node_count} nodes are alive after startup")

        print(f"  ✓ Cluster started with {alive_count} nodes alive")

    def stop(self):
        """Stop all nodes in the cluster."""
        print(f"\nStopping {self.node_count}-node cluster...")
        for node in self.nodes.values():
            node.stop()
        print(f"  ✓ Cluster stopped")

    def get_leader(self) -> Optional[ClusterNode]:
        """Find the leader node by attempting writes on each node."""
        # In a real implementation, we'd check Raft state
        # For testing, we'll assume the first node that accepts writes is the leader
        test_key = f"leader_test_{int(time.time())}"
        test_value = "leader_value"

        for node in self.nodes.values():
            if node.is_alive() and node.write(test_key, test_value):
                # Verify we can read it back
                if node.read(test_key) == test_value:
                    return node
        return None

    def get_alive_nodes(self) -> List[ClusterNode]:
        """Get all alive nodes."""
        return [node for node in self.nodes.values() if node.is_alive()]

    def get_alive_count(self) -> int:
        """Get count of alive nodes."""
        return len(self.get_alive_nodes())

    def write_to_leader(self, key: str, value: str) -> bool:
        """Write to the cluster via the leader."""
        leader = self.get_leader()
        if not leader:
            print("  ERROR: No leader found")
            return False
        return leader.write(key, value)

    def read_from_any(self, key: str):
        """Read from any alive node."""
        for node in self.get_alive_nodes():
            value = node.read(key)
            if value is not None:
                return value
        return None


def test_3_node_cluster_kill_1():
    """
    Test Case 1: Deploy 3-node cluster; kill 1 node; attempt writes
    Expected: Writes continue successfully with 2 remaining nodes
    """
    print("\n" + "="*60)
    print("TEST CASE 1: 3-node cluster, kill 1 node")
    print("Expected: Writes continue with 2 remaining nodes")
    print("="*60)

    cluster = Cluster(3)
    try:
        cluster.start()

        # Initial write
        print("\n  Initial write to cluster...")
        assert cluster.write_to_leader("test1", "value1"), "Initial write failed"
        print("  ✓ Initial write successful")

        # Kill node 3
        cluster.nodes[3].kill()
        time.sleep(2)

        # Verify we have quorum (2 nodes alive)
        alive_count = cluster.get_alive_count()
        print(f"  Nodes alive: {alive_count}/3")
        assert alive_count == 2, "Should have 2 nodes alive"

        # Attempt write - should succeed with 2 nodes (majority of 3 is 2)
        print("  Attempting write after killing 1 node...")
        result = cluster.write_to_leader("test2", "value2")
        if result:
            print("  ✓ Write succeeded (quorum maintained)")

            # Verify data persisted
            value = cluster.read_from_any("test2")
            assert value == "value2", f"Read returned {value}, expected value2"
            print("  ✓ Data verified across cluster")
        else:
            raise AssertionError("Write should succeed with 2 nodes alive")

        print("\n✓ TEST CASE 1 PASSED")
        return True

    except Exception as e:
        print(f"\n✗ TEST CASE 1 FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        cluster.stop()


def test_3_node_cluster_kill_2():
    """
    Test Case 2: Deploy 3-node cluster; kill 2 nodes; attempt writes
    Expected: Write fails or times out (no majority available)
    """
    print("\n" + "="*60)
    print("TEST CASE 2: 3-node cluster, kill 2 nodes")
    print("Expected: Write fails (no majority available)")
    print("="*60)

    cluster = Cluster(3)
    try:
        cluster.start()

        # Initial write
        print("\n  Initial write to cluster...")
        assert cluster.write_to_leader("test1", "value1"), "Initial write failed"
        print("  ✓ Initial write successful")

        # Kill nodes 2 and 3
        cluster.nodes[2].kill()
        cluster.nodes[3].kill()
        time.sleep(2)

        # Verify only 1 node alive (no quorum)
        alive_count = cluster.get_alive_count()
        print(f"  Nodes alive: {alive_count}/3")
        assert alive_count == 1, "Should have 1 node alive"

        # Attempt write - should fail (no majority)
        print("  Attempting write after killing 2 nodes...")
        result = cluster.write_to_leader("test2", "value2")

        if result:
            print("  ✗ UNEXPECTED: Write succeeded without quorum")
            return False
        else:
            print("  ✓ Write failed as expected (no quorum)")

        print("\n✓ TEST CASE 2 PASSED")
        return True

    except Exception as e:
        print(f"\n✗ TEST CASE 2 FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        cluster.stop()


def test_5_node_cluster_kill_2():
    """
    Test Case 3: Deploy 5-node cluster; kill 2 nodes; attempt writes
    Expected: Writes continue successfully with 3 remaining nodes
    """
    print("\n" + "="*60)
    print("TEST CASE 3: 5-node cluster, kill 2 nodes")
    print("Expected: Writes continue with 3 remaining nodes")
    print("="*60)

    cluster = Cluster(5)
    try:
        cluster.start()

        # Initial write
        print("\n  Initial write to cluster...")
        assert cluster.write_to_leader("test1", "value1"), "Initial write failed"
        print("  ✓ Initial write successful")

        # Kill nodes 4 and 5
        cluster.nodes[4].kill()
        cluster.nodes[5].kill()
        time.sleep(2)

        # Verify we have quorum (3 nodes alive)
        alive_count = cluster.get_alive_count()
        print(f"  Nodes alive: {alive_count}/5")
        assert alive_count == 3, "Should have 3 nodes alive"

        # Attempt write - should succeed with 3 nodes (majority of 5 is 3)
        print("  Attempting write after killing 2 nodes...")
        result = cluster.write_to_leader("test2", "value2")
        if result:
            print("  ✓ Write succeeded (quorum maintained)")

            # Verify data persisted
            value = cluster.read_from_any("test2")
            assert value == "value2", f"Read returned {value}, expected value2"
            print("  ✓ Data verified across cluster")
        else:
            raise AssertionError("Write should succeed with 3 nodes alive (quorum)")

        print("\n✓ TEST CASE 3 PASSED")
        return True

    except Exception as e:
        print(f"\n✗ TEST CASE 3 FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        cluster.stop()


def test_5_node_cluster_kill_3():
    """
    Test Case 4: Deploy 5-node cluster; kill 3 nodes; attempt writes
    Expected: Write fails (only 2 nodes remaining, no majority)
    """
    print("\n" + "="*60)
    print("TEST CASE 4: 5-node cluster, kill 3 nodes")
    print("Expected: Write fails (only 2 nodes remaining)")
    print("="*60)

    cluster = Cluster(5)
    try:
        cluster.start()

        # Initial write
        print("\n  Initial write to cluster...")
        assert cluster.write_to_leader("test1", "value1"), "Initial write failed"
        print("  ✓ Initial write successful")

        # Kill nodes 3, 4, and 5
        cluster.nodes[3].kill()
        cluster.nodes[4].kill()
        cluster.nodes[5].kill()
        time.sleep(2)

        # Verify only 2 nodes alive (no quorum)
        alive_count = cluster.get_alive_count()
        print(f"  Nodes alive: {alive_count}/5")
        assert alive_count == 2, "Should have 2 nodes alive"

        # Attempt write - should fail (no majority)
        print("  Attempting write after killing 3 nodes...")
        result = cluster.write_to_leader("test2", "value2")

        if result:
            print("  ✗ UNEXPECTED: Write succeeded without quorum")
            return False
        else:
            print("  ✓ Write failed as expected (no quorum)")

        print("\n✓ TEST CASE 4 PASSED")
        return True

    except Exception as e:
        print(f"\n✗ TEST CASE 4 FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        cluster.stop()


def test_7_node_cluster():
    """
    Test Case 5: Start 7-node cluster and verify correct operation
    Expected: 7 nodes form cluster; can tolerate 3 node failures; read/write operations succeed
    """
    print("\n" + "="*60)
    print("TEST CASE 5: 7-node cluster formation and operation")
    print("Expected: All 7 nodes functional, can tolerate failures")
    print("="*60)

    cluster = Cluster(7)
    try:
        cluster.start()

        # Verify all 7 nodes are alive
        alive_count = cluster.get_alive_count()
        print(f"  Nodes alive: {alive_count}/7")
        assert alive_count == 7, "All 7 nodes should be alive"

        # Test write operation
        print("\n  Testing write operation...")
        assert cluster.write_to_leader("test1", "value1"), "Initial write failed"
        print("  ✓ Write successful")

        # Test read operation
        value = cluster.read_from_any("test1")
        assert value == "value1", f"Read returned {value}, expected value1"
        print("  ✓ Read successful")

        # Test fault tolerance: kill 2 nodes (should still have quorum of 5)
        print("\n  Testing fault tolerance: killing 2 nodes...")
        cluster.nodes[6].kill()
        cluster.nodes[7].kill()
        time.sleep(2)

        alive_count = cluster.get_alive_count()
        print(f"  Nodes alive: {alive_count}/7")
        assert alive_count == 5, "Should have 5 nodes alive"

        # Write should still work
        result = cluster.write_to_leader("test2", "value2")
        assert result, "Write should succeed with 5 nodes (quorum is 4)"
        print("  ✓ Write succeeded after killing 2 nodes")

        # Kill one more node (4 nodes alive, quorum needed is 4)
        print("\n  Killing 1 more node (4 alive, 4 needed for quorum)...")
        cluster.nodes[5].kill()
        time.sleep(2)

        alive_count = cluster.get_alive_count()
        print(f"  Nodes alive: {alive_count}/7")
        assert alive_count == 4, "Should have 4 nodes alive"

        # Write should still work (exactly at quorum)
        result = cluster.write_to_leader("test3", "value3")
        assert result, "Write should succeed with exactly 4 nodes (quorum is 4)"
        print("  ✓ Write succeeded at quorum boundary")

        # Kill one more node (3 nodes alive, below quorum)
        print("\n  Killing 1 more node (3 alive, below quorum of 4)...")
        cluster.nodes[4].kill()
        time.sleep(2)

        alive_count = cluster.get_alive_count()
        print(f"  Nodes alive: {alive_count}/7")
        assert alive_count == 3, "Should have 3 nodes alive"

        # Write should fail now
        result = cluster.write_to_leader("test4", "value4")
        assert not result, "Write should fail with only 3 nodes (below quorum)"
        print("  ✓ Write failed as expected below quorum")

        print("\n✓ TEST CASE 5 PASSED")
        return True

    except Exception as e:
        print(f"\n✗ TEST CASE 5 FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        cluster.stop()


def run_all_tests():
    """Run all cluster configuration tests."""
    print("="*60)
    print("CLUSTER CONFIGURATION TESTS")
    print("Validating 3, 5, and 7 node cluster configurations")
    print("="*60)

    # Track results
    results = []

    # Test Case 1: 3-node cluster, kill 1 node
    results.append(("Test 1: 3-node, kill 1", test_3_node_cluster_kill_1()))

    # Test Case 2: 3-node cluster, kill 2 nodes
    results.append(("Test 2: 3-node, kill 2", test_3_node_cluster_kill_2()))

    # Test Case 3: 5-node cluster, kill 2 nodes
    results.append(("Test 3: 5-node, kill 2", test_5_node_cluster_kill_2()))

    # Test Case 4: 5-node cluster, kill 3 nodes
    results.append(("Test 4: 5-node, kill 3", test_5_node_cluster_kill_3()))

    # Test Case 5: 7-node cluster
    results.append(("Test 5: 7-node cluster", test_7_node_cluster()))

    # Print summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    for name, passed in results:
        status = "✓ PASSED" if passed else "✗ FAILED"
        print(f"{name}: {status}")

    total = len(results)
    passed = sum(1 for _, p in results if p)
    print(f"\nTotal: {passed}/{total} tests passed")

    if passed == total:
        print("\n✓✓✓ ALL TESTS PASSED ✓✓✓")
        sys.exit(0)
    else:
        print(f"\n✗✗✗ {total - passed} TEST(S) FAILED ✗✗✗")
        sys.exit(1)


if __name__ == "__main__":
    run_all_tests()
