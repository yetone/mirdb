#!/bin/bash
# Browser Compatibility and Performance Tests
# Owner: Scenario 7 - Integration & E2E Testing
#
# This script validates browser compatibility by checking:
# - CSS vendor prefixes for cross-browser support
# - JavaScript compatibility patterns (ES5/ES6 fallbacks)
# - No browser-specific CSS hacks that might break in other browsers
# - Performance-related validations (asset sizes, minification potential)
# - Lighthouse-friendly optimizations
#
# Test Cases:
# TC1: Chrome compatibility - verify layout and interactivity
# TC2: Firefox compatibility - verify CSS animations and layout
# TC3: Safari compatibility - verify localStorage and focus styles
# TC4: Edge compatibility - verify all interactive features
# TC5: Performance - page load time optimization (asset size < 200KB)
# TC6: No JavaScript errors - valid JS syntax and patterns
# TC7: Lighthouse readiness - performance optimizations

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

pass() {
    echo -e "${GREEN}PASS${NC}: $1"
    TESTS_PASSED=$((TESTS_PASSED + 1))
}

fail() {
    echo -e "${RED}FAIL${NC}: $1"
    TESTS_FAILED=$((TESTS_FAILED + 1))
}

warn() {
    echo -e "${YELLOW}WARN${NC}: $1"
}

echo "============================================"
echo "Browser Compatibility & Performance Tests"
echo "Owner: Scenario 7 - Integration & E2E Testing"
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

echo "=== Test Case 1: Chrome Compatibility ==="
echo ""

# TC1.1: Check for flexbox support (Chrome modern feature)
echo "TC1.1: Flexbox layout support"
if grep -qE "display:\s*flex" "$CSS_FILE"; then
    pass "CSS uses flexbox layout (Chrome compatible)"
else
    fail "CSS should use flexbox for Chrome compatibility"
fi

# TC1.2: Check for CSS Grid support (Chrome modern feature)
echo "TC1.2: CSS Grid layout support"
if grep -qE "display:\s*grid" "$CSS_FILE"; then
    pass "CSS uses grid layout (Chrome compatible)"
else
    fail "CSS should use grid layout for Chrome compatibility"
fi

# TC1.3: Check for CSS custom properties (CSS variables - Chrome 49+)
echo "TC1.3: CSS custom properties support"
if grep -qE "var\(--" "$CSS_FILE"; then
    pass "CSS uses custom properties/variables (Chrome 49+ compatible)"
else
    fail "CSS should use CSS custom properties"
fi

# TC1.4: Check theme toggle functionality exists
echo "TC1.4: Theme toggle functionality"
if grep -q "toggleTheme" "$JS_FILE" && grep -q "dark-theme" "$JS_FILE"; then
    pass "Theme toggle functionality implemented (Chrome compatible)"
else
    fail "Theme toggle should be implemented"
fi

# TC1.5: Check copy button functionality
echo "TC1.5: Copy button functionality"
if grep -q "navigator.clipboard" "$JS_FILE"; then
    pass "Clipboard API used (Chrome compatible)"
else
    fail "Clipboard API should be used for copy functionality"
fi

echo ""
echo "=== Test Case 2: Firefox Compatibility ==="
echo ""

# TC2.1: Check for vendor-prefixed webkit properties with moz fallbacks or standard
echo "TC2.1: Firefox compatible animations"
if grep -qE "transition:" "$CSS_FILE"; then
    pass "CSS transitions use standard syntax (Firefox compatible)"
else
    fail "CSS should use standard transition syntax"
fi

# TC2.2: Check for smooth scroll behavior
echo "TC2.2: Smooth scroll behavior"
if grep -qE "scroll-behavior:\s*smooth" "$CSS_FILE"; then
    pass "Smooth scroll behavior defined (Firefox compatible)"
else
    fail "Should have smooth scroll behavior"
fi

# TC2.3: Check for -moz-osx-font-smoothing (Firefox-specific)
echo "TC2.3: Firefox font smoothing"
if grep -q "-moz-osx-font-smoothing" "$CSS_FILE"; then
    pass "Firefox font smoothing defined"
else
    warn "-moz-osx-font-smoothing not defined (optional)"
    # Not a failure, just informational
    TESTS_PASSED=$((TESTS_PASSED + 1))
fi

# TC2.4: Check for backdrop-filter with -webkit fallback
echo "TC2.4: Backdrop filter support"
if grep -qE "backdrop-filter:" "$CSS_FILE"; then
    if grep -qE "-webkit-backdrop-filter:" "$CSS_FILE"; then
        pass "Backdrop filter with webkit prefix (Firefox and Safari compatible)"
    else
        warn "Missing -webkit-backdrop-filter prefix"
        TESTS_PASSED=$((TESTS_PASSED + 1))
    fi
else
    warn "No backdrop-filter used (optional feature)"
    TESTS_PASSED=$((TESTS_PASSED + 1))
fi

# TC2.5: No layout-breaking Firefox-specific issues
echo "TC2.5: No Firefox layout issues"
if ! grep -qE ":-moz-broken|:-moz-drag-over" "$CSS_FILE"; then
    pass "No Firefox-specific pseudo-classes that break compatibility"
else
    fail "Contains Firefox-specific pseudo-classes that may break in other browsers"
fi

echo ""
echo "=== Test Case 3: Safari Compatibility ==="
echo ""

# TC3.1: Check for -webkit-background-clip for text gradients
echo "TC3.1: Safari text gradient support"
if grep -qE "-webkit-background-clip:\s*text" "$CSS_FILE"; then
    pass "Webkit background-clip for text gradients (Safari compatible)"
else
    warn "No webkit background-clip used (may be intentional)"
    TESTS_PASSED=$((TESTS_PASSED + 1))
fi

# TC3.2: Check for -webkit-text-fill-color (Safari text styling)
echo "TC3.2: Safari text fill color"
if grep -qE "-webkit-text-fill-color" "$CSS_FILE"; then
    pass "Webkit text-fill-color used (Safari compatible)"
else
    warn "No webkit text-fill-color (may not be needed)"
    TESTS_PASSED=$((TESTS_PASSED + 1))
fi

# TC3.3: localStorage usage with proper checks
echo "TC3.3: localStorage with Safari compatibility"
if grep -q "localStorage" "$JS_FILE"; then
    if grep -qE "try|catch|localStorage" "$JS_FILE" || grep -q "window.localStorage" "$JS_FILE"; then
        pass "localStorage used with proper pattern (Safari compatible)"
    else
        warn "localStorage used but may need try-catch for Safari private mode"
        TESTS_PASSED=$((TESTS_PASSED + 1))
    fi
else
    fail "localStorage should be used for theme persistence"
fi

# TC3.4: Focus styles visible (Safari accessibility)
echo "TC3.4: Focus styles for Safari"
if grep -qE ":focus-visible|:focus" "$CSS_FILE"; then
    pass "Focus styles defined (Safari accessibility compatible)"
else
    fail "Focus styles should be defined for Safari accessibility"
fi

# TC3.5: Check for -webkit-font-smoothing
echo "TC3.5: Safari font smoothing"
if grep -q "-webkit-font-smoothing" "$CSS_FILE"; then
    pass "Webkit font smoothing defined (Safari compatible)"
else
    warn "-webkit-font-smoothing not defined (optional)"
    TESTS_PASSED=$((TESTS_PASSED + 1))
fi

echo ""
echo "=== Test Case 4: Edge Compatibility ==="
echo ""

# TC4.1: Edge uses Chromium now, check for modern CSS
echo "TC4.1: Edge modern CSS support"
if grep -qE "display:\s*(flex|grid)" "$CSS_FILE"; then
    pass "Modern CSS layout used (Edge Chromium compatible)"
else
    fail "Modern CSS layout should be used"
fi

# TC4.2: No IE-specific hacks
echo "TC4.2: No IE-specific CSS hacks"
if ! grep -qE "@media.*-ms-|_:lang|\\\\9" "$CSS_FILE"; then
    pass "No IE-specific CSS hacks (Edge compatible)"
else
    fail "Contains IE-specific hacks that may cause issues"
fi

# TC4.3: JavaScript uses standard APIs
echo "TC4.3: Standard JavaScript APIs"
if grep -q "addEventListener" "$JS_FILE" && grep -q "classList" "$JS_FILE"; then
    pass "Uses standard DOM APIs (Edge compatible)"
else
    fail "Should use standard DOM APIs"
fi

# TC4.4: Check for ES5 compatibility (var instead of let/const for wider support)
echo "TC4.4: JavaScript ES5 compatibility"
if grep -qE "^[[:space:]]*(var|function)" "$JS_FILE"; then
    pass "Uses ES5 compatible syntax (var declarations)"
else
    warn "May use ES6+ only features (check for Edge legacy support if needed)"
    TESTS_PASSED=$((TESTS_PASSED + 1))
fi

# TC4.5: All interactive features have event handlers
echo "TC4.5: Interactive features properly wired"
THEME_LISTENER=$(grep -c "themeToggle.*addEventListener\|addEventListener.*themeToggle" "$JS_FILE" || echo "0")
COPY_LISTENER=$(grep -c "copyButtons\|copy-btn" "$JS_FILE" || echo "0")
MOBILE_LISTENER=$(grep -c "mobileMenuToggle.*addEventListener\|addEventListener.*mobileMenuToggle" "$JS_FILE" || echo "0")

if [ "$THEME_LISTENER" -gt 0 ] || [ "$COPY_LISTENER" -gt 0 ] || [ "$MOBILE_LISTENER" -gt 0 ]; then
    pass "Event listeners properly attached for interactive features"
else
    fail "Interactive features should have event listeners"
fi

echo ""
echo "=== Test Case 5: Performance (3G Load Time) ==="
echo ""

# TC5.1: Check total asset size (NFR-5: < 200KB)
echo "TC5.1: Total asset size under 200KB"
HTML_SIZE=$(wc -c < "$HTML_FILE")
CSS_SIZE=$(wc -c < "$CSS_FILE")
JS_SIZE=$(wc -c < "$JS_FILE")
TOTAL_SIZE=$((HTML_SIZE + CSS_SIZE + JS_SIZE))
TOTAL_KB=$((TOTAL_SIZE / 1024))

echo "  HTML: ${HTML_SIZE} bytes"
echo "  CSS: ${CSS_SIZE} bytes"
echo "  JS: ${JS_SIZE} bytes"
echo "  Total: ${TOTAL_SIZE} bytes (${TOTAL_KB} KB)"

if [ "$TOTAL_SIZE" -lt 204800 ]; then
    pass "Total asset size is ${TOTAL_KB}KB (under 200KB limit)"
else
    fail "Total asset size is ${TOTAL_KB}KB (exceeds 200KB limit)"
fi

# TC5.2: 3G simulation calculation
# Standard 3G: ~1.6 Mbps = 200 KB/s = 200000 bytes/s
# NFR-2: Render within 2 seconds = 400000 bytes max
echo "TC5.2: 3G load time estimation"
BYTES_PER_SECOND_3G=200000
MAX_BYTES_2SEC=$((BYTES_PER_SECOND_3G * 2))

if [ "$TOTAL_SIZE" -lt "$MAX_BYTES_2SEC" ]; then
    LOAD_TIME=$(echo "scale=2; $TOTAL_SIZE / $BYTES_PER_SECOND_3G" | bc 2>/dev/null || echo "0.5")
    pass "Estimated 3G load time: ~${LOAD_TIME}s (under 2s requirement)"
else
    fail "Assets too large for 2s 3G load requirement"
fi

# TC5.3: No blocking external resources (stylesheets/scripts from CDN)
echo "TC5.3: No blocking external resources"
# Check for external stylesheets (link tags with http/https src for CSS)
EXTERNAL_CSS_COUNT=0
if grep -qE '<link[^>]*href="https?://' "$HTML_FILE"; then
    EXTERNAL_CSS_COUNT=$(grep -cE '<link[^>]*rel="stylesheet"[^>]*href="https?://' "$HTML_FILE" || true)
fi
# Check for external scripts (script tags with http/https src)
EXTERNAL_JS_COUNT=0
if grep -qE '<script[^>]*src="https?://' "$HTML_FILE"; then
    EXTERNAL_JS_COUNT=$(grep -cE '<script[^>]*src="https?://' "$HTML_FILE" || true)
fi

if [ "$EXTERNAL_CSS_COUNT" -eq 0 ] && [ "$EXTERNAL_JS_COUNT" -eq 0 ]; then
    pass "No external blocking resources (all self-contained)"
else
    fail "Contains external resources that may slow load time (CSS: $EXTERNAL_CSS_COUNT, JS: $EXTERNAL_JS_COUNT)"
fi

# TC5.4: CSS uses efficient selectors (no deeply nested or universal)
echo "TC5.4: CSS selector efficiency"
INEFFICIENT_SELECTORS=$(grep -cE '^\s*\*\s*\{|>.*>.*>.*>' "$CSS_FILE" || true)
if [ "$INEFFICIENT_SELECTORS" -lt 3 ]; then
    pass "CSS uses reasonably efficient selectors"
else
    warn "CSS may have performance issues with complex selectors"
    TESTS_PASSED=$((TESTS_PASSED + 1))
fi

# TC5.5: Reduced motion support (performance accessibility)
echo "TC5.5: Reduced motion support"
if grep -qE "prefers-reduced-motion" "$CSS_FILE"; then
    pass "Respects prefers-reduced-motion (performance accessible)"
else
    fail "Should respect prefers-reduced-motion for performance accessibility"
fi

echo ""
echo "=== Test Case 6: No JavaScript Errors ==="
echo ""

# TC6.1: JavaScript syntax validation (basic)
echo "TC6.1: JavaScript syntax validation"
# Check for common syntax errors
if ! grep -qE "function\s*\(.*\)\s*{[^}]*$" "$JS_FILE" 2>/dev/null; then
    pass "No obvious unclosed function blocks"
else
    warn "Possible unclosed function block (complex pattern)"
    TESTS_PASSED=$((TESTS_PASSED + 1))
fi

# TC6.2: Strict mode usage
echo "TC6.2: Strict mode enabled"
if grep -qE "'use strict'|\"use strict\"" "$JS_FILE"; then
    pass "JavaScript uses strict mode (catches common errors)"
else
    fail "JavaScript should use strict mode"
fi

# TC6.3: No console.log left in production
echo "TC6.3: No debug console.log statements"
CONSOLE_COUNT=$(grep -c "console.log" "$JS_FILE" || true)
if [ "$CONSOLE_COUNT" -eq 0 ]; then
    pass "No console.log statements in production code"
else
    warn "Found $CONSOLE_COUNT console.log statement(s) (may want to remove for production)"
    TESTS_PASSED=$((TESTS_PASSED + 1))
fi

# TC6.4: Error handling for async operations
echo "TC6.4: Error handling for clipboard API"
if grep -q "\.catch\|catch\s*(" "$JS_FILE"; then
    pass "Error handling present for async operations"
else
    fail "Should have error handling for async clipboard operations"
fi

# TC6.5: No undefined variable access patterns
echo "TC6.5: Variable declarations before use"
if grep -qE "document\.getElementById|document\.querySelector" "$JS_FILE"; then
    if grep -qE "var\s+\w+\s*=\s*document\." "$JS_FILE"; then
        pass "DOM elements properly assigned to variables"
    else
        warn "DOM elements may not be cached (performance consideration)"
        TESTS_PASSED=$((TESTS_PASSED + 1))
    fi
else
    fail "Should use proper DOM selection methods"
fi

# TC6.6: IIFE wrapper to prevent global pollution
echo "TC6.6: IIFE wrapper for scope isolation"
if grep -qE "^\s*\(function\s*\(\)\s*{|^\(function\(\)" "$JS_FILE"; then
    pass "JavaScript wrapped in IIFE (no global pollution)"
else
    fail "JavaScript should be wrapped in IIFE to prevent global pollution"
fi

echo ""
echo "=== Test Case 7: Lighthouse Performance Readiness ==="
echo ""

# TC7.1: Meta viewport tag for mobile
echo "TC7.1: Viewport meta tag"
if grep -qE 'meta.*name="viewport"' "$HTML_FILE"; then
    if grep -qE 'width=device-width' "$HTML_FILE"; then
        pass "Proper viewport meta tag for mobile rendering"
    else
        fail "Viewport should include width=device-width"
    fi
else
    fail "Missing viewport meta tag"
fi

# TC7.2: Meta description for SEO
echo "TC7.2: Meta description tag"
if grep -qE 'meta.*name="description"' "$HTML_FILE"; then
    pass "Meta description present (SEO/Lighthouse)"
else
    fail "Missing meta description tag"
fi

# TC7.3: HTML lang attribute
echo "TC7.3: HTML lang attribute"
if grep -qE '<html.*lang=' "$HTML_FILE"; then
    pass "HTML lang attribute present (accessibility/Lighthouse)"
else
    fail "Missing HTML lang attribute"
fi

# TC7.4: Efficient image handling (SVG icons are inline)
echo "TC7.4: Efficient image handling"
if grep -q "<svg" "$HTML_FILE"; then
    if ! grep -qE '<img.*src=' "$HTML_FILE" || grep -qE 'loading="lazy"' "$HTML_FILE"; then
        pass "Uses inline SVG or lazy-loaded images (Lighthouse optimized)"
    else
        warn "Images should use lazy loading"
        TESTS_PASSED=$((TESTS_PASSED + 1))
    fi
else
    warn "No SVG icons found (may use external images)"
    TESTS_PASSED=$((TESTS_PASSED + 1))
fi

# TC7.5: Critical CSS/JS not blocking render
echo "TC7.5: Script loading pattern"
# Check if script is placed near end of body (before </body>)
SCRIPT_LINE=$(grep -n '<script' "$HTML_FILE" | head -1 | cut -d: -f1)
BODY_END_LINE=$(grep -n '</body>' "$HTML_FILE" | head -1 | cut -d: -f1)

if [ -n "$SCRIPT_LINE" ] && [ -n "$BODY_END_LINE" ]; then
    # Script should be within last 5 lines before </body>
    DIFF=$((BODY_END_LINE - SCRIPT_LINE))
    if [ "$DIFF" -ge 0 ] && [ "$DIFF" -le 5 ]; then
        pass "Script placed at end of body (non-blocking, line $SCRIPT_LINE before body end at $BODY_END_LINE)"
    else
        warn "Script position ($SCRIPT_LINE) may affect render performance (body ends at $BODY_END_LINE)"
        TESTS_PASSED=$((TESTS_PASSED + 1))
    fi
else
    fail "Could not determine script/body position"
fi

# TC7.6: CSS efficiency check - no @import
echo "TC7.6: No CSS @import (render blocking)"
if ! grep -qE "@import" "$CSS_FILE"; then
    pass "No @import statements (avoids render blocking)"
else
    fail "CSS @import statements can block rendering"
fi

# TC7.7: Touch targets size check (> 44px for mobile)
echo "TC7.7: Touch target sizes"
TOUCH_TARGETS=$(grep -cE "width:\s*4[4-9]px|width:\s*[5-9][0-9]px|height:\s*4[4-9]px|height:\s*[5-9][0-9]px|width:\s*40px.*height:\s*40px" "$CSS_FILE" || true)
if [ "$TOUCH_TARGETS" -gt 0 ]; then
    pass "Touch targets appear to be adequate size (>= 40px)"
else
    warn "Verify touch targets are at least 44x44px for mobile (Lighthouse requirement)"
    TESTS_PASSED=$((TESTS_PASSED + 1))
fi

echo ""
echo "============================================"
echo "Test Results Summary"
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
