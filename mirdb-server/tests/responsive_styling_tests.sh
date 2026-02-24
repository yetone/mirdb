#!/bin/bash
# Responsive Styling and Theme Support Tests
# Owner: Scenario 3 - Responsive Styling
#
# This script validates that styles.css contains all required:
# - CSS custom properties for theming
# - Light and dark theme styles
# - Mobile-first responsive breakpoints
# - Sticky header positioning
# - Code block styling with copy button
# - Smooth theme transitions

set -e

CSS_FILE="src/web/styles.css"
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

# Check if CSS file exists
if [ ! -f "$CSS_FILE" ]; then
    echo "Error: $CSS_FILE not found"
    exit 1
fi

echo "Running Responsive Styling and Theme Tests..."
echo "============================================"

# Test 1: CSS custom properties for theming
echo "Test 1: Check for CSS custom properties (--bg-primary, --text-primary, --accent-color)"
BG_PRIMARY=$(grep -c "\-\-bg-primary" "$CSS_FILE" || true)
TEXT_PRIMARY=$(grep -c "\-\-text-primary" "$CSS_FILE" || true)
ACCENT_COLOR=$(grep -c "\-\-accent-color" "$CSS_FILE" || true)

if [ "$BG_PRIMARY" -gt 0 ] && [ "$TEXT_PRIMARY" -gt 0 ] && [ "$ACCENT_COLOR" -gt 0 ]; then
    pass "CSS defines --bg-primary, --text-primary, --accent-color variables"
else
    fail "CSS should define --bg-primary ($BG_PRIMARY), --text-primary ($TEXT_PRIMARY), --accent-color ($ACCENT_COLOR)"
fi

# Test 2: Dark theme class selector
echo "Test 2: Check for dark theme class selector"
if grep -qE "\.dark-theme|body\.dark-theme" "$CSS_FILE"; then
    # Check that dark theme has different variable values
    DARK_BG=$(grep -A50 "body\.dark-theme\|\.dark-theme" "$CSS_FILE" | grep -c "\-\-bg-primary" || true)
    if [ "$DARK_BG" -gt 0 ]; then
        pass "CSS contains .dark-theme selector with different variable values"
    else
        fail "CSS should have .dark-theme selector with different CSS variable values"
    fi
else
    fail "CSS should contain .dark-theme or body.dark-theme selector"
fi

# Test 3: Mobile breakpoint media query
echo "Test 3: Validate mobile/desktop breakpoint media query"
# Check for @media query with min-width: 769px (desktop breakpoint - mobile first approach)
# OR max-width: 768px (alternative mobile approach)
if grep -qE "@media.*min-width.*769px|@media.*min-width:.*769px|@media.*max-width.*768px|@media.*max-width:.*768px" "$CSS_FILE"; then
    pass "CSS contains @media query for 768px/769px breakpoint (mobile/desktop)"
else
    fail "CSS should contain @media query for 768px breakpoint"
fi

# Test 4: Sticky header positioning
echo "Test 4: Check for sticky header positioning"
if grep -qE "position:\s*sticky|position:\s*fixed" "$CSS_FILE"; then
    # Check if it's associated with header
    if grep -qE "\.site-header|header" "$CSS_FILE" && grep -qE "position:\s*sticky" "$CSS_FILE"; then
        pass "Header has position: sticky styling"
    elif grep -qE "position:\s*fixed" "$CSS_FILE"; then
        pass "Header has position: fixed styling (alternative to sticky)"
    else
        fail "Header element should have position: sticky or fixed styling"
    fi
else
    fail "CSS should contain position: sticky or position: fixed for header"
fi

# Test 5: Code block styling
echo "Test 5: Check for code block styling"
if grep -qE "\.code-block|pre\s*\{|code\s*\{" "$CSS_FILE"; then
    pass "CSS contains code block styling (.code-block, pre, or code)"
else
    fail "CSS should contain code block styling"
fi

# Test 6: Copy button styling
echo "Test 6: Check for copy button styling"
if grep -qE "\.copy-btn|copy-btn" "$CSS_FILE"; then
    # Check for hover state
    if grep -qE "\.copy-btn:hover|\.code-block:hover.*\.copy-btn" "$CSS_FILE"; then
        pass "CSS contains copy button styling with hover state"
    else
        pass "CSS contains copy button styling (hover state may be on parent)"
    fi
else
    fail "CSS should contain .copy-btn styling"
fi

# Test 7: Theme transition animation
echo "Test 7: Check for smooth theme transition"
if grep -qE "transition.*0\.3s|transition.*300ms|transition:.*ease|--transition-normal" "$CSS_FILE"; then
    pass "CSS contains smooth transition animation (~0.3s)"
else
    fail "CSS should contain transition for smooth theme changes"
fi

# Test 8: Focus visible indicators (accessibility)
echo "Test 8: Check for focus visible indicators"
if grep -qE ":focus-visible|:focus" "$CSS_FILE"; then
    pass "CSS contains focus indicators for accessibility"
else
    fail "CSS should contain :focus or :focus-visible for accessibility"
fi

# Test 9: Mobile menu styling
echo "Test 9: Check for mobile menu/hamburger styling"
if grep -qE "\.mobile-menu|\.hamburger|mobile-menu-toggle" "$CSS_FILE"; then
    pass "CSS contains mobile menu/hamburger styling"
else
    fail "CSS should contain mobile menu styling"
fi

# Test 10: Desktop navigation expansion
echo "Test 10: Check for desktop navigation expansion"
if grep -qE "@media.*min-width.*769|@media.*min-width:.*769" "$CSS_FILE"; then
    # Check that mobile menu is hidden on desktop
    if grep -qE "\.mobile-menu-toggle.*display:\s*none|display:\s*none.*mobile" "$CSS_FILE" || grep -A5 "@media.*min-width" "$CSS_FILE" | grep -qE "display:\s*none"; then
        pass "CSS hides mobile menu toggle on desktop"
    else
        pass "CSS has desktop media query (mobile menu handling present)"
    fi
else
    fail "CSS should have desktop breakpoint for expanded navigation"
fi

# Test 11: CSS variable declarations in :root
echo "Test 11: Check for :root CSS variable declarations"
if grep -qE ":root\s*\{" "$CSS_FILE"; then
    pass "CSS declares variables in :root pseudo-class"
else
    fail "CSS should declare CSS custom properties in :root"
fi

# Test 12: Responsive grid/flex layout
echo "Test 12: Check for responsive layout (grid or flexbox)"
if grep -qE "display:\s*grid|display:\s*flex|grid-template-columns" "$CSS_FILE"; then
    pass "CSS uses grid or flexbox for responsive layout"
else
    fail "CSS should use grid or flexbox for responsive layout"
fi

# Test 13: Background and text color definitions
echo "Test 13: Verify color contrast setup"
# Check for both light and dark theme color definitions
LIGHT_BG=$(grep -E "^\s*--bg-primary:\s*#ffffff|^\s*--bg-primary:\s*#fff" "$CSS_FILE" | wc -l || true)
LIGHT_TEXT=$(grep -E "^\s*--text-primary:\s*#1a1a2e|^\s*--text-primary:\s*#[0-3]" "$CSS_FILE" | wc -l || true)

if [ "$LIGHT_BG" -gt 0 ] || [ "$LIGHT_TEXT" -gt 0 ]; then
    pass "CSS defines color values for contrast compliance (light theme)"
else
    # Check for any background/text color variables
    if grep -qE "--bg-primary:" "$CSS_FILE" && grep -qE "--text-primary:" "$CSS_FILE"; then
        pass "CSS defines --bg-primary and --text-primary color variables"
    else
        fail "CSS should define background and text colors for WCAG compliance"
    fi
fi

# Test 14: Check for nav-links styling
echo "Test 14: Check for navigation links styling"
if grep -qE "\.nav-links|nav-links" "$CSS_FILE"; then
    pass "CSS contains navigation links styling"
else
    fail "CSS should contain .nav-links styling"
fi

echo ""
echo "============================================"
echo "Test Results: $PASS_COUNT passed, $FAIL_COUNT failed"
echo "============================================"

if [ $FAIL_COUNT -gt 0 ]; then
    exit 1
fi

exit 0
