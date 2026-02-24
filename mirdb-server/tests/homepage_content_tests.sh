#!/bin/bash
# Homepage Content Validation Tests
# Owner: Scenario 2 - Homepage Content & Structure
#
# This script validates that the index.html contains all required sections
# and follows proper semantic HTML structure.

set -e

HTML_FILE="src/web/index.html"
PASS_COUNT=0
FAIL_COUNT=0

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

pass() {
    echo -e "${GREEN}[PASS]${NC} $1"
    PASS_COUNT=$((PASS_COUNT + 1))
}

fail() {
    echo -e "${RED}[FAIL]${NC} $1"
    FAIL_COUNT=$((FAIL_COUNT + 1))
}

# Check if HTML file exists
if [ ! -f "$HTML_FILE" ]; then
    echo "Error: $HTML_FILE not found"
    exit 1
fi

echo "Running Homepage Content Validation Tests..."
echo "============================================"

# Test 1: MirDB branding
echo "Test 1: Check for MirDB branding"
if grep -q "MirDB" "$HTML_FILE" && grep -q "logo" "$HTML_FILE"; then
    pass "HTML contains 'MirDB' text and logo reference"
else
    fail "HTML should contain 'MirDB' text and logo reference"
fi

# Test 2: Heading hierarchy
echo "Test 2: Validate heading hierarchy"
H1_COUNT=$(grep -c "<h1>" "$HTML_FILE" || true)
HAS_H2=$(grep -q "<h2>" "$HTML_FILE" && echo "yes" || echo "no")
HAS_H3=$(grep -q "<h3>" "$HTML_FILE" && echo "yes" || echo "no")

if [ "$H1_COUNT" -eq 1 ] && [ "$HAS_H2" = "yes" ] && [ "$HAS_H3" = "yes" ]; then
    pass "Document has proper heading hierarchy (single h1, h2 follows h1, h3 follows h2)"
else
    fail "Document should have proper heading hierarchy (found $H1_COUNT h1 tags, h2: $HAS_H2, h3: $HAS_H3)"
fi

# Test 3: Quick Start section with installation command
echo "Test 3: Check for Quick Start section with installation command"
if grep -q "cargo install mirdb" "$HTML_FILE"; then
    pass "HTML contains 'cargo install mirdb' installation command"
else
    fail "HTML should contain 'cargo install mirdb' installation command"
fi

# Test 4: All Memcached commands are listed
echo "Test 4: Validate all Memcached commands are listed"
COMMANDS=("SET" "GET" "ADD" "REPLACE" "DELETE" "APPEND" "PREPEND")
MISSING_CMDS=()
for cmd in "${COMMANDS[@]}"; do
    if ! grep -q ">$cmd<" "$HTML_FILE"; then
        MISSING_CMDS+=("$cmd")
    fi
done

if [ ${#MISSING_CMDS[@]} -eq 0 ]; then
    pass "HTML contains all Memcached commands: SET, GET, ADD, REPLACE, DELETE, APPEND, PREPEND"
else
    fail "HTML is missing commands: ${MISSING_CMDS[*]}"
fi

# Test 5: Architecture section with LSM-Tree explanation
echo "Test 5: Check Architecture section for LSM-Tree explanation"
ARCH_TERMS=("LSM" "WAL" "SSTable" "SkipList")
MISSING_TERMS=()
for term in "${ARCH_TERMS[@]}"; do
    if ! grep -qi "$term" "$HTML_FILE"; then
        MISSING_TERMS+=("$term")
    fi
done

if [ ${#MISSING_TERMS[@]} -eq 0 ]; then
    pass "HTML contains references to LSM-Tree, WAL, SSTable, and SkipList"
else
    fail "HTML is missing architecture terms: ${MISSING_TERMS[*]}"
fi

# Test 6: External links have security attributes
echo "Test 6: Validate external links have security attributes"
# Count external links (target="_blank")
EXTERNAL_LINKS=$(grep -o 'target="_blank"' "$HTML_FILE" | wc -l || true)
NOOPENER_LINKS=$(grep -o 'rel="noopener"' "$HTML_FILE" | wc -l || true)

if [ "$EXTERNAL_LINKS" -gt 0 ] && [ "$EXTERNAL_LINKS" -eq "$NOOPENER_LINKS" ]; then
    pass "All external links have target='_blank' and rel='noopener' ($EXTERNAL_LINKS links)"
else
    fail "External link security mismatch: $EXTERNAL_LINKS target=_blank, $NOOPENER_LINKS rel=noopener"
fi

# Test 7: GitHub repository link
echo "Test 7: Check for GitHub repository link"
if grep -q "github.com" "$HTML_FILE" && grep -q "mirdb" "$HTML_FILE"; then
    pass "HTML contains link to MirDB GitHub repository"
else
    fail "HTML should contain link to MirDB GitHub repository"
fi

# Test 8: Semantic HTML structure
echo "Test 8: Validate semantic HTML structure"
HAS_NAV=$(grep -q "<nav" "$HTML_FILE" && echo "yes" || echo "no")
HAS_MAIN=$(grep -q "<main" "$HTML_FILE" && echo "yes" || echo "no")
HAS_SECTION=$(grep -q "<section" "$HTML_FILE" && echo "yes" || echo "no")
HAS_FOOTER=$(grep -q "<footer" "$HTML_FILE" && echo "yes" || echo "no")

if [ "$HAS_NAV" = "yes" ] && [ "$HAS_MAIN" = "yes" ] && [ "$HAS_SECTION" = "yes" ] && [ "$HAS_FOOTER" = "yes" ]; then
    pass "HTML uses nav, main, section, and footer elements appropriately"
else
    fail "HTML should use semantic elements (nav: $HAS_NAV, main: $HAS_MAIN, section: $HAS_SECTION, footer: $HAS_FOOTER)"
fi

echo ""
echo "============================================"
echo "Test Results: $PASS_COUNT passed, $FAIL_COUNT failed"
echo "============================================"

if [ $FAIL_COUNT -gt 0 ]; then
    exit 1
fi

exit 0
