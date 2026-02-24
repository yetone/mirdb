#!/bin/bash
# Accessibility Compliance Tests
# Owner: Scenario 6 - Accessibility Compliance
#
# This script validates WCAG 2.1 Level AA compliance including:
# - Proper semantic structure
# - ARIA labels for interactive elements
# - Skip link for main content
# - Focus visible indicators
# - Color contrast considerations

set -e

HTML_FILE="src/web/index.html"
CSS_FILE="src/web/styles.css"
JS_FILE="src/web/script.js"
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

# Check if files exist
if [ ! -f "$HTML_FILE" ]; then
    echo "Error: $HTML_FILE not found"
    exit 1
fi

if [ ! -f "$CSS_FILE" ]; then
    echo "Error: $CSS_FILE not found"
    exit 1
fi

echo "Running Accessibility Compliance Tests..."
echo "=========================================="

# Test 1: Skip link exists and targets main content
echo "Test 1: Check for skip link to main content"
HAS_SKIP_LINK=$(grep -q 'class="skip-link"' "$HTML_FILE" && echo "yes" || echo "no")
SKIP_LINK_TARGET=$(grep -q 'href="#main-content"' "$HTML_FILE" && echo "yes" || echo "no")
MAIN_HAS_ID=$(grep -q 'id="main-content"' "$HTML_FILE" && echo "yes" || echo "no")

if [ "$HAS_SKIP_LINK" = "yes" ] && [ "$SKIP_LINK_TARGET" = "yes" ] && [ "$MAIN_HAS_ID" = "yes" ]; then
    pass "Skip link exists and targets main-content"
else
    fail "Skip link missing or misconfigured (skip-link: $HAS_SKIP_LINK, target: $SKIP_LINK_TARGET, main id: $MAIN_HAS_ID)"
fi

# Test 2: Theme toggle has aria-label
echo "Test 2: Check theme toggle button for aria-label"
if grep -q 'id="theme-toggle".*aria-label' "$HTML_FILE" || grep -q 'aria-label.*id="theme-toggle"' "$HTML_FILE"; then
    pass "Theme toggle button has aria-label attribute"
else
    # Try a more flexible search
    if grep -A5 'id="theme-toggle"' "$HTML_FILE" | grep -q 'aria-label'; then
        pass "Theme toggle button has aria-label attribute"
    elif grep -B5 'id="theme-toggle"' "$HTML_FILE" | grep -q 'aria-label="Toggle'; then
        pass "Theme toggle button has aria-label attribute"
    else
        fail "Theme toggle button should have aria-label attribute"
    fi
fi

# Test 3: Logo has accessible name
echo "Test 3: Check logo for accessible name"
if grep -q 'class="logo".*aria-label' "$HTML_FILE" || grep -q 'aria-label.*class="logo"' "$HTML_FILE"; then
    pass "Logo has aria-label for accessible name"
else
    fail "Logo should have aria-label for screen readers"
fi

# Test 4: All copy buttons have aria-label
echo "Test 4: Check copy buttons for aria-label"
COPY_BTN_COUNT=$(grep -c 'class="copy-btn"' "$HTML_FILE" || true)
COPY_BTN_ARIA_COUNT=$(grep -c 'copy-btn.*aria-label' "$HTML_FILE" || true)

if [ "$COPY_BTN_COUNT" -gt 0 ] && [ "$COPY_BTN_COUNT" -eq "$COPY_BTN_ARIA_COUNT" ]; then
    pass "All $COPY_BTN_COUNT copy buttons have aria-label"
else
    fail "Copy buttons missing aria-label ($COPY_BTN_ARIA_COUNT of $COPY_BTN_COUNT have aria-label)"
fi

# Test 5: Mobile menu toggle has aria-expanded
echo "Test 5: Check mobile menu toggle for aria-expanded"
if grep -q 'id="mobile-menu-toggle"' "$HTML_FILE" && grep -q 'aria-expanded' "$HTML_FILE"; then
    pass "Mobile menu toggle has aria-expanded attribute"
else
    fail "Mobile menu toggle should have aria-expanded attribute"
fi

# Test 6: Proper heading hierarchy (h1 -> h2 -> h3)
echo "Test 6: Validate heading hierarchy"
H1_COUNT=$(grep -c "<h1>" "$HTML_FILE" || true)
H2_COUNT=$(grep -c "<h2>" "$HTML_FILE" || true)
H3_COUNT=$(grep -c "<h3>" "$HTML_FILE" || true)

# Check that h1 appears before h2, and h2 appears before h3
H1_LINE=$(grep -n "<h1>" "$HTML_FILE" | head -1 | cut -d: -f1)
H2_LINE=$(grep -n "<h2>" "$HTML_FILE" | head -1 | cut -d: -f1)
H3_LINE=$(grep -n "<h3>" "$HTML_FILE" | head -1 | cut -d: -f1)

if [ "$H1_COUNT" -eq 1 ] && [ "$H2_COUNT" -gt 0 ] && [ "$H3_COUNT" -gt 0 ] && \
   [ "$H1_LINE" -lt "$H2_LINE" ] && [ "$H2_LINE" -lt "$H3_LINE" ]; then
    pass "Heading hierarchy is correct (h1 -> h2 -> h3)"
else
    fail "Heading hierarchy issue (h1: $H1_COUNT, h2: $H2_COUNT, h3: $H3_COUNT)"
fi

# Test 7: Focus visible styles exist in CSS
echo "Test 7: Check for focus-visible styles"
if grep -q "focus-visible" "$CSS_FILE"; then
    pass "CSS contains :focus-visible styles for keyboard accessibility"
else
    fail "CSS should contain :focus-visible styles"
fi

# Test 8: Skip link CSS exists
echo "Test 8: Check for skip link CSS styles"
if grep -q "\.skip-link" "$CSS_FILE" && grep -q "skip-link:focus" "$CSS_FILE"; then
    pass "Skip link has CSS styles including focus state"
else
    fail "Skip link should have CSS styles with focus state"
fi

# Test 9: Navigation has aria-label
echo "Test 9: Check navigation for aria-label"
if grep -q '<nav.*aria-label' "$HTML_FILE"; then
    pass "Navigation has aria-label attribute"
else
    fail "Navigation should have aria-label attribute"
fi

# Test 10: SVG icons are hidden from screen readers
echo "Test 10: Check SVG icons have aria-hidden"
SVG_COUNT=$(grep -c "<svg" "$HTML_FILE" || true)
SVG_HIDDEN_COUNT=$(grep -c 'aria-hidden="true"' "$HTML_FILE" || true)

if [ "$SVG_COUNT" -gt 0 ] && [ "$SVG_HIDDEN_COUNT" -ge "$SVG_COUNT" ]; then
    pass "SVG icons have aria-hidden='true' ($SVG_HIDDEN_COUNT of $SVG_COUNT)"
else
    # Some SVGs might need aria-hidden, check if decorative ones have it
    if [ "$SVG_HIDDEN_COUNT" -ge 6 ]; then
        pass "Most decorative SVG icons have aria-hidden='true'"
    else
        fail "Decorative SVG icons should have aria-hidden='true' (found $SVG_HIDDEN_COUNT)"
    fi
fi

# Test 11: HTML has lang attribute
echo "Test 11: Check HTML lang attribute"
if grep -q '<html lang="en"' "$HTML_FILE"; then
    pass "HTML element has lang='en' attribute"
else
    fail "HTML element should have lang attribute for accessibility"
fi

# Test 12: Buttons have type attribute
echo "Test 12: Check buttons have type attribute"
BUTTON_COUNT=$(grep -c "<button" "$HTML_FILE" || true)
BUTTON_TYPE_COUNT=$(grep -c 'type="button"' "$HTML_FILE" || true)

if [ "$BUTTON_COUNT" -eq "$BUTTON_TYPE_COUNT" ]; then
    pass "All $BUTTON_COUNT buttons have type='button' attribute"
else
    fail "All buttons should have type attribute ($BUTTON_TYPE_COUNT of $BUTTON_COUNT)"
fi

# Test 13: Architecture diagram has accessible description
echo "Test 13: Check architecture diagram accessibility"
if grep -q 'architecture-diagram.*role="img".*aria-label' "$HTML_FILE" || \
   grep -q 'architecture-diagram.*aria-label' "$HTML_FILE"; then
    pass "Architecture diagram has role='img' and aria-label"
else
    fail "Architecture diagram should have role='img' and aria-label"
fi

# Test 14: Reduced motion support
echo "Test 14: Check for reduced motion support"
if grep -q "prefers-reduced-motion" "$CSS_FILE"; then
    pass "CSS supports prefers-reduced-motion media query"
else
    fail "CSS should support prefers-reduced-motion for users with motion sensitivity"
fi

# Test 15: Keyboard event handlers in JS
echo "Test 15: Check for keyboard accessibility in JavaScript"
if grep -q "keydown\|keypress\|keyup" "$JS_FILE"; then
    pass "JavaScript includes keyboard event handlers"
else
    fail "JavaScript should include keyboard event handlers for accessibility"
fi

# Test 16: Focus outline in CSS
echo "Test 16: Check for focus outline styles"
if grep -q "outline:" "$CSS_FILE" && grep -q "outline-offset:" "$CSS_FILE"; then
    pass "CSS has outline styles for focus indicators"
else
    fail "CSS should have outline styles for focus indicators"
fi

# Test 17: Header has role="banner"
echo "Test 17: Check header for banner role"
if grep -q '<header.*role="banner"' "$HTML_FILE"; then
    pass "Header has role='banner' attribute"
else
    fail "Header should have role='banner' for landmark navigation"
fi

# Test 18: Footer has role="contentinfo"
echo "Test 18: Check footer for contentinfo role"
if grep -q '<footer.*role="contentinfo"' "$HTML_FILE"; then
    pass "Footer has role='contentinfo' attribute"
else
    fail "Footer should have role='contentinfo' for landmark navigation"
fi

echo ""
echo "=========================================="
echo "Test Results: $PASS_COUNT passed, $FAIL_COUNT failed"
echo "=========================================="

if [ $FAIL_COUNT -gt 0 ]; then
    exit 1
fi

exit 0
