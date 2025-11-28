#!/usr/bin/env python3
"""
Integration tests for backward compatibility with memcached protocol.
Tests that existing memcached clients work unchanged with distributed MirDB cluster.
"""

import sys
import time
import subprocess
import socket
import sys

# Try to import memcache client
try:
    import memcache
except ImportError:
    print("memcache library not available. Installing...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "python3-memcached"])
    import memcache


def test_basic_set_and_get():
    """Test basic SET and GET commands with standard memcached client."""
    print("Test 1: Basic SET and GET")

    # Connect to the server
    mc = memcache.Client(['127.0.0.1:12333'], debug=1)

    # Test SET
    result = mc.set('test_key_1', 'test_value_1')
    print(f"  SET test_key_1: {result}")
    assert result, "SET should succeed"

    # Test GET
    value = mc.get('test_key_1')
    print(f"  GET test_key_1: {value}")
    assert value == 'test_value_1', f"GET returned {value}, expected test_value_1"

    # Test multi-get
    mc.set('test_key_2', 'test_value_2')
    mc.set('test_key_3', 'test_value_3')
    values = mc.get_multi(['test_key_1', 'test_key_2', 'test_key_3'])
    print(f"  Multi-GET: {values}")
    assert values == {
        'test_key_1': 'test_value_1',
        'test_key_2': 'test_value_2',
        'test_key_3': 'test_value_3'
    }, f"Multi-get returned {values}"

    print("  ✓ Test 1 passed\n")


def test_all_storage_commands():
    """Test all memcached storage commands: ADD, REPLACE, APPEND, PREPEND."""
    print("Test 2: All Storage Commands (ADD, REPLACE, APPEND, PREPEND)")

    mc = memcache.Client(['127.0.0.1:12333'], debug=1)

    # Test ADD (only succeeds if key doesn't exist)
    mc.delete('add_key')
    result = mc.add('add_key', 'add_value')
    print(f"  ADD new key: {result}")
    assert result, "ADD on non-existent key should succeed"

    result2 = mc.add('add_key', 'new_value')
    print(f"  ADD existing key: {result2}")
    assert not result2, "ADD on existing key should fail"

    # Test REPLACE (only succeeds if key exists)
    mc.set('replace_key', 'original_value')
    result = mc.replace('replace_key', 'replaced_value')
    print(f"  REPLACE existing key: {result}")
    assert result, "REPLACE on existing key should succeed"

    mc.delete('nonexistent_replace')
    result = mc.replace('nonexistent_replace', 'value')
    print(f"  REPLACE non-existent key: {result}")
    assert not result, "REPLACE on non-existent key should fail"

    # Test APPEND
    mc.set('append_key', 'prefix_')
    result = mc.append('append_key', 'suffix')
    print(f"  APPEND: {result}")
    assert result, "APPEND should succeed"

    value = mc.get('append_key')
    print(f"  After APPEND: {value}")
    assert value == 'prefix_suffix', f"APPEND result should be 'prefix_suffix', got {value}"

    # Test PREPEND
    mc.set('prepend_key', '_suffix')
    result = mc.prepend('prepend_key', 'prefix')
    print(f"  PREPEND: {result}")
    assert result, "PREPEND should succeed"

    value = mc.get('prepend_key')
    print(f"  After PREPEND: {value}")
    assert value == 'prefix_suffix', f"PREPEND result should be 'prefix_suffix', got {value}"

    print("  ✓ Test 2 passed\n")


def test_delete_command():
    """Test DELETE command."""
    print("Test 3: DELETE Command")

    mc = memcache.Client(['127.0.0.1:12333'], debug=1)

    # Set a key then delete it
    mc.set('delete_key', 'delete_value')
    result = mc.delete('delete_key')
    print(f"  DELETE existing key: {result}")
    assert result, "DELETE on existing key should succeed"

    value = mc.get('delete_key')
    print(f"  GET after DELETE: {value}")
    assert value is None, "GET after DELETE should return None"

    # Delete non-existent key
    result = mc.delete('nonexistent_key')
    print(f"  DELETE non-existent key: {result}")
    assert not result, "DELETE on non-existent key should fail"

    print("  ✓ Test 3 passed\n")


def test_ttl_expiration():
    """Test TTL (Time To Live) expiration."""
    print("Test 4: TTL Expiration")

    mc = memcache.Client(['127.0.0.1:12333'], debug=1)

    # Set with 1 second TTL
    mc.set('ttl_key', 'ttl_value', time=1)
    value = mc.get('ttl_key')
    print(f"  GET before expiration: {value}")
    assert value == 'ttl_value', "GET before expiration should return value"

    # Wait for expiration
    time.sleep(2)
    value = mc.get('ttl_key')
    print(f"  GET after expiration: {value}")
    assert value is None, "GET after expiration should return None"

    # Set with 0 (no expiration)
    mc.set('no_ttl_key', 'no_ttl_value', time=0)
    value = mc.get('no_ttl_key')
    print(f"  GET with no TTL: {value}")
    assert value == 'no_ttl_value', "GET with no ttl should return value"

    print("  ✓ Test 4 passed\n")


def test_error_responses():
    """Test error handling."""
    print("Test 5: Error Responses")

    # TODO: Test CLIENT_ERROR and SERVER_ERROR scenarios
    # These would require sending malformed commands or causing server errors

    print("  ⚠ Skipped (requires malformed commands or server errors)\n")


def run_all_tests():
    """Run all backward compatibility tests."""
    print("=" * 60)
    print("Backward Compatibility Tests with Memcached Protocol")
    print("=" * 60)
    print()

    try:
        # First, start the MirDB server in the background
        print("Starting MirDB server...")
        server_process = subprocess.Popen(
            ["./target/debug/mirdb-server", "-c", "etc/mirdb.toml"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd="/workspace"
        )

        # Wait for server to start
        time.sleep(2)

        # Check if server is running
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex(('127.0.0.1', 12333))
        sock.close()

        if result != 0:
            print("ERROR: Cannot connect to server on 127.0.0.1:12333")
            print("Make sure the server is built and running")
            server_process.terminate()
            sys.exit(1)

        print("Server started successfully")
        print()

        # Run tests
        test_basic_set_and_get()
        test_all_storage_commands()
        test_delete_command()
        test_ttl_expiration()
        test_error_responses()

        print("=" * 60)
        print("All backward compatibility tests passed! ✓")
        print("=" * 60)

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        # Clean up server process
        if 'server_process' in locals():
            server_process.terminate()
            server_process.wait()


if __name__ == "__main__":
    run_all_tests()
