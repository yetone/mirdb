#!/bin/bash
# Test script for Scenario 3: Server Status and Version Display
# This script verifies that the HTML, CSS, and handler files contain the required elements

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
HTML_FILE="$PROJECT_ROOT/assets/index.html"
CSS_FILE="$PROJECT_ROOT/assets/css/style.css"
HANDLERS_FILE="$PROJECT_ROOT/src/homepage/handlers.rs"
STATE_FILE="$PROJECT_ROOT/src/homepage/state.rs"

TESTS_PASSED=0
TESTS_FAILED=0

# Test helper function
assert_contains() {
    local file="$1"
    local pattern="$2"
    local description="$3"

    if grep -q "$pattern" "$file" 2>/dev/null; then
        echo "PASS: $description"
        ((TESTS_PASSED++))
    else
        echo "FAIL: $description (pattern: $pattern not found in $(basename $file))"
        ((TESTS_FAILED++))
    fi
}

echo "=========================================="
echo "Scenario 3: Server Status and Version Display"
echo "=========================================="
echo ""

echo "=== Test Case 1: Homepage displays running status ==="
assert_contains "$HTML_FILE" 'id="version-info"' "HTML has version-info section id"
assert_contains "$HTML_FILE" 'class="version-info"' "HTML has version-info class"
assert_contains "$HTML_FILE" 'id="server-version"' "HTML has server-version element id"
assert_contains "$HTML_FILE" 'class="server-version"' "HTML has server-version class"
assert_contains "$HTML_FILE" 'id="server-status"' "HTML has server-status element id"
assert_contains "$HTML_FILE" 'class="server-status"' "HTML has server-status class"
assert_contains "$HTML_FILE" 'id="status-indicator"' "HTML has status-indicator element id"
assert_contains "$HTML_FILE" 'class="status-indicator"' "HTML has status-indicator class"
assert_contains "$HTML_FILE" 'id="status-text"' "HTML has status-text element id"
echo ""

echo "=== Test Case 2: Version display element ==="
assert_contains "$HTML_FILE" 'Version:' "HTML has Version: text"
assert_contains "$HTML_FILE" 'Status:' "HTML has Status: text"
assert_contains "$HTML_FILE" "fetch('/api/status')" "HTML has fetch API call for status"
assert_contains "$HTML_FILE" 'data.version' "JavaScript accesses data.version"
assert_contains "$HTML_FILE" 'data.running' "JavaScript accesses data.running"
assert_contains "$HTML_FILE" 'Running' "JavaScript displays Running text"
assert_contains "$HTML_FILE" 'Stopped' "JavaScript displays Stopped text"
echo ""

echo "=== Test Case 3: GET /api/status endpoint ==="
assert_contains "$HANDLERS_FILE" 'handle_status' "handlers.rs exports handle_status function"
assert_contains "$HANDLERS_FILE" 'is_running' "handlers.rs checks running status"
assert_contains "$HANDLERS_FILE" 'get_version' "handlers.rs gets version"
assert_contains "$HANDLERS_FILE" '"running"' "Status response has running field"
assert_contains "$HANDLERS_FILE" '"version"' "Status response has version field"
assert_contains "$STATE_FILE" 'pub fn get_version' "state.rs exports get_version"
assert_contains "$STATE_FILE" 'pub fn is_running' "state.rs exports is_running"
assert_contains "$STATE_FILE" 'CARGO_PKG_VERSION' "state.rs uses CARGO_PKG_VERSION"
echo ""

echo "=== CSS Tests ==="
assert_contains "$CSS_FILE" '.version-info' "CSS has version-info styles"
assert_contains "$CSS_FILE" '.server-version' "CSS has server-version styles"
assert_contains "$CSS_FILE" '.server-status' "CSS has server-status styles"
assert_contains "$CSS_FILE" '.status-indicator' "CSS has status-indicator styles"
assert_contains "$CSS_FILE" '.status-running' "CSS has status-running indicator styles"
assert_contains "$CSS_FILE" '.status-stopped' "CSS has status-stopped indicator styles"
assert_contains "$CSS_FILE" 'border-radius: 50%' "CSS status indicator is round"
assert_contains "$CSS_FILE" '#22c55e' "CSS running indicator has green color"
echo ""

echo "=========================================="
echo "Test Results: $TESTS_PASSED passed, $TESTS_FAILED failed"
echo "=========================================="

if [ $TESTS_FAILED -gt 0 ]; then
    exit 1
fi

echo ""
echo "All tests passed!"
exit 0
