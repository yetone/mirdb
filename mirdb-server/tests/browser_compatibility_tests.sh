#!/bin/bash
#
# Browser Compatibility and Performance Validation Tests
# Owner: Scenario 7 - Integration & E2E Testing
#
# This script validates browser compatibility requirements through static analysis:
# - Cross-browser CSS compatibility (standard properties, no vendor-only prefixes)
# - Performance optimization indicators (minification potential, asset sizes)
# - JavaScript compatibility (ES5-safe or polyfilled)
# - HTML standards compliance
#
# For full E2E browser testing, use: npx playwright test
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HTML_FILE="${SCRIPT_DIR}/../src/web/index.html"
CSS_FILE="${SCRIPT_DIR}/../src/web/styles.css"
JS_FILE="${SCRIPT_DIR}/../src/web/script.js"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

TESTS_PASSED=0
TESTS_FAILED=0
TESTS_SKIPPED=0

pass() {
    echo -e "${GREEN}[PASS]${NC} $1"
    TESTS_PASSED=$((TESTS_PASSED + 1))
}

fail() {
    echo -e "${RED}[FAIL]${NC} $1"
    TESTS_FAILED=$((TESTS_FAILED + 1))
}

skip() {
    echo -e "${YELLOW}[SKIP]${NC} $1"
    TESTS_SKIPPED=$((TESTS_SKIPPED + 1))
}

info() {
    echo -e "       $1"
}

echo "============================================"
echo "Browser Compatibility & Performance Tests"
echo "============================================"
echo ""

# Check if files exist
if [ ! -f "$HTML_FILE" ]; then
    echo "Error: $HTML_FILE not found"
    exit 1
fi

if [ ! -f "$CSS_FILE" ]; then
    echo "Error: $CSS_FILE not found"
    exit 1
fi

if [ ! -f "$JS_FILE" ]; then
    echo "Error: $JS_FILE not found"
    exit 1
fi

# ============================================
# HTML Standards Compliance Tests
# ============================================
echo ""
echo "--- HTML Standards Compliance ---"

# Test 1: HTML5 doctype
echo "Test 1: HTML5 DOCTYPE declaration"
if grep -qi "<!DOCTYPE html>" "$HTML_FILE"; then
    pass "HTML5 DOCTYPE present"
else
    fail "Missing HTML5 DOCTYPE declaration"
fi

# Test 2: Charset UTF-8
echo "Test 2: UTF-8 charset declaration"
if grep -qi 'charset="UTF-8"\|charset=UTF-8' "$HTML_FILE"; then
    pass "UTF-8 charset declared"
else
    fail "Missing UTF-8 charset declaration"
fi

# Test 3: Viewport meta tag
echo "Test 3: Viewport meta tag for mobile"
if grep -qi 'name="viewport"' "$HTML_FILE"; then
    if grep -qi 'width=device-width' "$HTML_FILE"; then
        pass "Viewport meta tag with device-width present"
    else
        fail "Viewport meta tag missing width=device-width"
    fi
else
    fail "Missing viewport meta tag"
fi

# Test 4: Language attribute
echo "Test 4: HTML lang attribute"
if grep -qi '<html lang=' "$HTML_FILE"; then
    pass "HTML lang attribute present"
else
    fail "Missing HTML lang attribute for accessibility"
fi

# ============================================
# CSS Browser Compatibility Tests
# ============================================
echo ""
echo "--- CSS Browser Compatibility ---"

# Test 5: CSS custom properties (supported in all modern browsers)
echo "Test 5: CSS custom properties usage"
if grep -q '\-\-[a-zA-Z]' "$CSS_FILE"; then
    pass "CSS custom properties (variables) used"
    info "Supported in Chrome 49+, Firefox 31+, Safari 9.1+, Edge 15+"
else
    skip "No CSS custom properties found"
fi

# Test 6: Flexbox usage (widely supported)
echo "Test 6: Flexbox layout usage"
if grep -q 'display:\s*flex' "$CSS_FILE"; then
    pass "Flexbox layout used"
    info "Supported in Chrome 29+, Firefox 28+, Safari 9+, Edge 12+"
else
    skip "No flexbox layout found"
fi

# Test 7: CSS Grid usage
echo "Test 7: CSS Grid layout usage"
if grep -q 'display:\s*grid\|grid-template' "$CSS_FILE"; then
    pass "CSS Grid layout used"
    info "Supported in Chrome 57+, Firefox 52+, Safari 10.1+, Edge 16+"
else
    skip "No CSS Grid layout found"
fi

# Test 8: Webkit prefixes for Safari compatibility
echo "Test 8: WebKit prefixes for Safari"
if grep -q '\-webkit-' "$CSS_FILE"; then
    WEBKIT_COUNT=$(grep -c '\-webkit-' "$CSS_FILE" || true)
    pass "WebKit prefixes present ($WEBKIT_COUNT occurrences)"
else
    skip "No WebKit prefixes (may need for older Safari)"
fi

# Test 9: Standard CSS properties (no proprietary-only)
echo "Test 9: Using standard CSS properties"
# Check for IE-specific properties that have no standard equivalent
if grep -qi 'filter:\s*alpha\|behavior:\s*url\|-ms-filter' "$CSS_FILE"; then
    fail "Contains IE-specific proprietary properties"
else
    pass "No IE-only proprietary properties"
fi

# Test 10: backdrop-filter with fallback
echo "Test 10: backdrop-filter browser support"
if grep -q 'backdrop-filter' "$CSS_FILE"; then
    if grep -q '\-webkit-backdrop-filter' "$CSS_FILE"; then
        pass "backdrop-filter with WebKit prefix for Safari"
    else
        fail "backdrop-filter used without -webkit- prefix for Safari"
    fi
else
    skip "backdrop-filter not used"
fi

# ============================================
# JavaScript Compatibility Tests
# ============================================
echo ""
echo "--- JavaScript Compatibility ---"

# Test 11: Strict mode enabled
echo "Test 11: JavaScript strict mode"
if grep -q "'use strict'\|\"use strict\"" "$JS_FILE"; then
    pass "Strict mode enabled"
else
    fail "Missing strict mode declaration"
fi

# Test 12: Arrow functions usage (ES6)
echo "Test 12: ES6 arrow functions check"
if grep -q '=>' "$JS_FILE"; then
    skip "Contains arrow functions (ES6) - requires modern browsers"
    info "Supported in Chrome 45+, Firefox 22+, Safari 10+, Edge 12+"
else
    pass "No arrow functions - ES5 compatible"
fi

# Test 13: Let/const declarations (ES6)
echo "Test 13: ES6 let/const check"
if grep -Eq '\blet\s|\bconst\s' "$JS_FILE"; then
    skip "Contains let/const (ES6) - requires modern browsers"
    info "Supported in Chrome 49+, Firefox 44+, Safari 10+, Edge 12+"
else
    pass "Uses var declarations - ES5 compatible"
fi

# Test 14: Template literals check
echo "Test 14: ES6 template literals check"
if grep -q '`' "$JS_FILE"; then
    skip "Contains template literals - requires modern browsers"
else
    pass "No template literals - ES5 compatible"
fi

# Test 15: Clipboard API with fallback
echo "Test 15: Clipboard API fallback"
if grep -q 'navigator.clipboard' "$JS_FILE"; then
    if grep -q 'execCommand\|fallback' "$JS_FILE"; then
        pass "Clipboard API with fallback for older browsers"
    else
        fail "Clipboard API used without fallback"
    fi
else
    pass "Clipboard API not used (or uses alternative)"
fi

# Test 16: localStorage availability check
echo "Test 16: localStorage error handling"
if grep -q 'localStorage' "$JS_FILE"; then
    # Check if there's any try-catch or feature detection
    if grep -q 'try\|catch\|typeof' "$JS_FILE"; then
        pass "localStorage with error handling/feature detection"
    else
        skip "localStorage used without explicit error handling"
    fi
else
    skip "localStorage not used"
fi

# Test 17: Event listener usage
echo "Test 17: Standard event handling"
if grep -q 'addEventListener' "$JS_FILE"; then
    pass "Using standard addEventListener"
else
    fail "Not using standard addEventListener"
fi

# ============================================
# Performance Tests
# ============================================
echo ""
echo "--- Performance Validation ---"

# Test 18: Asset file sizes
echo "Test 18: Asset file sizes (NFR-5: <200KB total)"
HTML_SIZE=$(wc -c < "$HTML_FILE")
CSS_SIZE=$(wc -c < "$CSS_FILE")
JS_SIZE=$(wc -c < "$JS_FILE")
TOTAL_SIZE=$((HTML_SIZE + CSS_SIZE + JS_SIZE))
MAX_SIZE=$((200 * 1024))

info "HTML: ${HTML_SIZE} bytes"
info "CSS: ${CSS_SIZE} bytes"
info "JS: ${JS_SIZE} bytes"
info "Total: ${TOTAL_SIZE} bytes ($((TOTAL_SIZE / 1024)) KB)"

if [ "$TOTAL_SIZE" -lt "$MAX_SIZE" ]; then
    pass "Total asset size under 200KB ($((TOTAL_SIZE / 1024))KB)"
else
    fail "Total asset size exceeds 200KB limit ($((TOTAL_SIZE / 1024))KB)"
fi

# Test 19: No external dependencies
echo "Test 19: Self-contained assets (no external CDN)"
if grep -qi 'cdnjs\|unpkg\|jsdelivr\|googleapis.com/css\|cloudflare' "$HTML_FILE"; then
    fail "External CDN dependencies found"
else
    pass "No external CDN dependencies - fully self-contained"
fi

# Test 20: Inline styles minimized
echo "Test 20: Minimal inline styles"
INLINE_STYLE_COUNT=$(grep -c 'style="' "$HTML_FILE" || true)
if [ "$INLINE_STYLE_COUNT" -lt 5 ]; then
    pass "Minimal inline styles ($INLINE_STYLE_COUNT occurrences)"
else
    fail "Too many inline styles ($INLINE_STYLE_COUNT) - prefer CSS classes"
fi

# Test 21: CSS uses efficient selectors
echo "Test 21: Efficient CSS selectors"
# Check for overly specific selectors (more than 4 levels deep)
if grep -E '^\s*[a-z]+\s+[a-z]+\s+[a-z]+\s+[a-z]+\s+[a-z]+\s*\{' "$CSS_FILE" >/dev/null 2>&1; then
    fail "Contains deeply nested selectors (5+ levels)"
else
    pass "CSS selectors are efficiently structured"
fi

# ============================================
# Cross-Browser Feature Tests
# ============================================
echo ""
echo "--- Cross-Browser Feature Support ---"

# Test 22: prefers-color-scheme media query
echo "Test 22: System theme preference support"
if grep -q 'prefers-color-scheme' "$CSS_FILE" || grep -q 'prefers-color-scheme' "$JS_FILE"; then
    pass "System color scheme preference supported"
else
    skip "No system theme preference detection"
fi

# Test 23: prefers-reduced-motion support
echo "Test 23: Reduced motion accessibility"
if grep -q 'prefers-reduced-motion' "$CSS_FILE"; then
    pass "Respects prefers-reduced-motion preference"
else
    fail "Missing prefers-reduced-motion support"
fi

# Test 24: Smooth scroll behavior
echo "Test 24: Smooth scroll behavior"
if grep -q 'scroll-behavior:\s*smooth' "$CSS_FILE"; then
    pass "Smooth scroll behavior defined"
else
    skip "No smooth scroll behavior"
fi

# Test 25: Focus-visible pseudo-class
echo "Test 25: Modern focus-visible support"
if grep -q ':focus-visible' "$CSS_FILE"; then
    pass "Using :focus-visible for better keyboard accessibility"
    info "Supported in Chrome 86+, Firefox 85+, Safari 15.4+, Edge 86+"
else
    if grep -q ':focus' "$CSS_FILE"; then
        pass "Using :focus (broader support than :focus-visible)"
    else
        fail "Missing focus styles"
    fi
fi

# Test 26: External links security
echo "Test 26: External link security attributes"
EXTERNAL_LINKS=$(grep -c 'target="_blank"' "$HTML_FILE" || true)
NOOPENER_COUNT=$(grep -c 'rel="noopener"' "$HTML_FILE" || true)
if [ "$EXTERNAL_LINKS" -eq "$NOOPENER_COUNT" ]; then
    pass "All external links have rel='noopener' ($EXTERNAL_LINKS links)"
else
    fail "External links missing rel='noopener' ($NOOPENER_COUNT of $EXTERNAL_LINKS)"
fi

# ============================================
# Summary
# ============================================
echo ""
echo "============================================"
echo "Test Results Summary"
echo "============================================"
echo -e "Passed:  ${GREEN}$TESTS_PASSED${NC}"
echo -e "Failed:  ${RED}$TESTS_FAILED${NC}"
echo -e "Skipped: ${YELLOW}$TESTS_SKIPPED${NC}"
echo ""

if [ $TESTS_FAILED -gt 0 ]; then
    echo -e "${RED}Some tests failed!${NC}"
    echo ""
    echo "Note: For full browser E2E testing, run:"
    echo "  npx playwright test"
    exit 1
else
    echo -e "${GREEN}All compatibility checks passed!${NC}"
    echo ""
    echo "For comprehensive browser testing, run:"
    echo "  npx playwright test"
    exit 0
fi
