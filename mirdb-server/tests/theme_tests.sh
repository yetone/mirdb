#!/bin/bash
#
# Shell-based tests for MirDB Homepage JavaScript (script.js)
# Owner: Scenario 4 - Theme Toggle & Interactions
#
# Tests validate the JavaScript file contains required functionality:
# - Theme toggle function with dark-theme class manipulation
# - localStorage persistence for theme preference
# - System preference detection (prefers-color-scheme)
# - Copy-to-clipboard functionality with feedback
# - Mobile menu toggle functionality
# - Keyboard accessibility (Enter/Space activation)
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JS_FILE="${SCRIPT_DIR}/../src/web/script.js"
HTML_FILE="${SCRIPT_DIR}/../src/web/index.html"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

TESTS_PASSED=0
TESTS_FAILED=0

pass() {
    echo -e "${GREEN}PASS${NC}: $1"
    TESTS_PASSED=$((TESTS_PASSED + 1))
}

fail() {
    echo -e "${RED}FAIL${NC}: $1"
    TESTS_FAILED=$((TESTS_FAILED + 1))
}

echo "============================================"
echo "MirDB Homepage JavaScript Tests"
echo "============================================"
echo ""

# Test 1: Check script.js file exists
echo "Test 1: script.js file exists"
if [ -f "$JS_FILE" ]; then
    pass "script.js file exists"
else
    fail "script.js file not found at $JS_FILE"
fi

# Test 2: Check for toggleTheme function
echo "Test 2: toggleTheme function exists"
if grep -q "function toggleTheme" "$JS_FILE"; then
    pass "toggleTheme function defined"
else
    fail "toggleTheme function not found"
fi

# Test 3: Check for dark-theme class manipulation
echo "Test 3: Dark theme class manipulation"
if grep -q "dark-theme" "$JS_FILE" && grep -q "classList.add\|classList.remove\|classList.contains" "$JS_FILE"; then
    pass "Dark theme class manipulation present"
else
    fail "Dark theme class manipulation not found"
fi

# Test 4: Check for localStorage usage for theme persistence
echo "Test 4: localStorage persistence for theme"
if grep -q "localStorage.getItem" "$JS_FILE" && grep -q "localStorage.setItem" "$JS_FILE"; then
    pass "localStorage get/set for theme persistence"
else
    fail "localStorage operations not found"
fi

# Test 5: Check theme key is stored in localStorage
echo "Test 5: Theme key constant defined"
if grep -q "THEME_KEY\|'theme'\|\"theme\"" "$JS_FILE"; then
    pass "Theme key constant/value present"
else
    fail "Theme key not found"
fi

# Test 6: Check for system preference detection (prefers-color-scheme)
echo "Test 6: System preference detection"
if grep -q "prefers-color-scheme" "$JS_FILE"; then
    pass "System preference detection (prefers-color-scheme) present"
else
    fail "System preference detection not found"
fi

# Test 7: Check for matchMedia usage
echo "Test 7: matchMedia API usage"
if grep -q "matchMedia" "$JS_FILE"; then
    pass "matchMedia API used for system preference"
else
    fail "matchMedia not found"
fi

# Test 8: Check for copyToClipboard function
echo "Test 8: copyToClipboard function exists"
if grep -q "function copyToClipboard\|copyToClipboard" "$JS_FILE"; then
    pass "copyToClipboard function defined"
else
    fail "copyToClipboard function not found"
fi

# Test 9: Check for Clipboard API usage
echo "Test 9: Clipboard API usage"
if grep -q "navigator.clipboard" "$JS_FILE"; then
    pass "Clipboard API (navigator.clipboard) used"
else
    fail "Clipboard API not found"
fi

# Test 10: Check for copy feedback mechanism
echo "Test 10: Copy feedback mechanism"
if grep -q "Copied\|copied\|COPIED_CLASS" "$JS_FILE"; then
    pass "Copy feedback mechanism present"
else
    fail "Copy feedback not found"
fi

# Test 11: Check for 2-second feedback duration
echo "Test 11: Copy feedback duration (2 seconds)"
if grep -q "2000" "$JS_FILE"; then
    pass "2-second (2000ms) feedback duration"
else
    fail "2000ms feedback duration not found"
fi

# Test 12: Check for mobile menu toggle function
echo "Test 12: Mobile menu toggle function"
if grep -q "toggleMobileMenu\|mobile.*toggle\|MOBILE_OPEN_CLASS" "$JS_FILE"; then
    pass "Mobile menu toggle function present"
else
    fail "Mobile menu toggle not found"
fi

# Test 13: Check for aria-expanded attribute handling
echo "Test 13: ARIA expanded attribute handling"
if grep -q "aria-expanded" "$JS_FILE"; then
    pass "aria-expanded attribute handling present"
else
    fail "aria-expanded handling not found"
fi

# Test 14: Check for keyboard event handling (Enter/Space)
echo "Test 14: Keyboard accessibility (Enter/Space keys)"
if grep -q "Enter\|' '" "$JS_FILE" && grep -q "keydown\|keypress\|keyup" "$JS_FILE"; then
    pass "Keyboard accessibility (Enter/Space) implemented"
else
    fail "Keyboard event handling not found"
fi

# Test 15: Check for event listener setup
echo "Test 15: Event listeners setup"
if grep -q "addEventListener" "$JS_FILE"; then
    pass "Event listeners set up"
else
    fail "addEventListener not found"
fi

# Test 16: Check for click event handling
echo "Test 16: Click event handling"
if grep -q "'click'\|\"click\"" "$JS_FILE"; then
    pass "Click event handling present"
else
    fail "Click event handling not found"
fi

# Test 17: Check for initTheme function
echo "Test 17: initTheme initialization function"
if grep -q "initTheme\|init.*Theme" "$JS_FILE"; then
    pass "initTheme function present"
else
    fail "initTheme function not found"
fi

# Test 18: Check for DOMContentLoaded or ready state check
echo "Test 18: DOM ready initialization"
if grep -q "DOMContentLoaded\|readyState" "$JS_FILE"; then
    pass "DOM ready initialization present"
else
    fail "DOM ready check not found"
fi

# Test 19: Check for theme toggle button reference
echo "Test 19: Theme toggle button element reference"
if grep -q "theme-toggle" "$JS_FILE"; then
    pass "Theme toggle button referenced"
else
    fail "Theme toggle button reference not found"
fi

# Test 20: Check for mobile menu button reference
echo "Test 20: Mobile menu toggle button reference"
if grep -q "mobile-menu-toggle" "$JS_FILE"; then
    pass "Mobile menu toggle button referenced"
else
    fail "Mobile menu toggle button reference not found"
fi

# Test 21: Check for copy button handling
echo "Test 21: Copy button handling"
if grep -q "copy-btn\|data-copy" "$JS_FILE"; then
    pass "Copy button handling present"
else
    fail "Copy button handling not found"
fi

# Test 22: Check for fallback copy mechanism (older browsers)
echo "Test 22: Fallback copy mechanism"
if grep -q "execCommand\|fallback.*[Cc]opy\|textarea" "$JS_FILE"; then
    pass "Fallback copy mechanism for older browsers"
else
    fail "Fallback copy mechanism not found"
fi

# Test 23: Check HTML has theme toggle button with ID
echo "Test 23: HTML has theme toggle button"
if grep -q 'id="theme-toggle"' "$HTML_FILE"; then
    pass "HTML contains theme toggle button with ID"
else
    fail "Theme toggle button ID not found in HTML"
fi

# Test 24: Check HTML has mobile menu toggle with ID
echo "Test 24: HTML has mobile menu toggle button"
if grep -q 'id="mobile-menu-toggle"' "$HTML_FILE"; then
    pass "HTML contains mobile menu toggle button with ID"
else
    fail "Mobile menu toggle button ID not found in HTML"
fi

# Test 25: Check HTML has copy buttons with data-copy attribute
echo "Test 25: HTML has copy buttons with data-copy"
if grep -q 'class="copy-btn"' "$HTML_FILE" && grep -q 'data-copy=' "$HTML_FILE"; then
    pass "HTML contains copy buttons with data-copy attribute"
else
    fail "Copy buttons with data-copy not found in HTML"
fi

# Test 26: Check script.js is linked in HTML
echo "Test 26: script.js linked in HTML"
if grep -q 'src="/script.js"\|src="script.js"' "$HTML_FILE"; then
    pass "script.js is linked in HTML"
else
    fail "script.js not linked in HTML"
fi

# Test 27: Check for proper IIFE wrapper or strict mode
echo "Test 27: Strict mode or IIFE wrapper"
if grep -q "'use strict'\|\"use strict\"\|(function()" "$JS_FILE"; then
    pass "Strict mode or IIFE wrapper present"
else
    fail "Strict mode or IIFE wrapper not found"
fi

# Test 28: Check for system theme change listener
echo "Test 28: System theme change listener"
if grep -q "addEventListener.*change\|change.*listener" "$JS_FILE" && grep -q "matchMedia" "$JS_FILE"; then
    pass "System theme change listener present"
else
    fail "System theme change listener not found"
fi

echo ""
echo "============================================"
echo "Test Results"
echo "============================================"
echo "Passed: $TESTS_PASSED"
echo "Failed: $TESTS_FAILED"
echo ""

if [ $TESTS_FAILED -gt 0 ]; then
    echo -e "${RED}Some tests failed!${NC}"
    exit 1
else
    echo -e "${GREEN}All tests passed!${NC}"
    exit 0
fi
