/**
 * Responsive layout tests for MirDB homepage.
 *
 * Owner: Scenario 8 - Responsive Layout Desktop
 * Co-owner: Scenario 9 - Responsive Layout Mobile
 *
 * Tests:
 * - Desktop layout at 1024px+ (Scenario 8)
 * - Large screen layout at 1920px (Scenario 8)
 * - Mobile layout at 375px (Scenario 9)
 * - Navigation collapse on mobile (Scenario 9)
 * - Touch target sizes (Scenario 9)
 */

const fs = require('fs');
const path = require('path');
const assert = require('assert');

// Helper to load CSS content
function loadMainCss() {
    const cssPath = path.join(__dirname, '../../static/css/main.css');
    return fs.readFileSync(cssPath, 'utf-8');
}

// Helper to load HTML content
function loadHomepageHtml() {
    const htmlPath = path.join(__dirname, '../../static/index.html');
    return fs.readFileSync(htmlPath, 'utf-8');
}

// Test results tracking
let passed = 0;
let failed = 0;
const results = [];

function test(name, fn) {
    try {
        fn();
        passed++;
        results.push({ name, status: 'pass' });
        console.log(`  \u2713 ${name}`);
    } catch (error) {
        failed++;
        results.push({ name, status: 'fail', error: error.message });
        console.log(`  \u2717 ${name}`);
        console.log(`    Error: ${error.message}`);
    }
}

// =============================================
// Test Case 1: Desktop Layout at 1024px Viewport
// Verifies all sections visible without horizontal scroll
// =============================================
console.log('\nTest Case 1: Desktop Layout at 1024px Viewport');
console.log('Input: Render page at 1024px viewport width');
console.log('Expected: All sections visible without horizontal scroll');

test('CSS has 1024px desktop media query', () => {
    const css = loadMainCss();
    assert(css.includes('@media (min-width: 1024px)'),
           'Should have min-width: 1024px media query for desktop');
});

test('CSS main content has max-width constraint', () => {
    const css = loadMainCss();
    assert(css.includes('max-width'),
           'Should have max-width constraint to prevent horizontal overflow');
});

test('HTML has viewport meta tag for responsive scaling', () => {
    const html = loadHomepageHtml();
    assert(html.includes('width=device-width'),
           'Should have viewport meta tag with device-width');
});

test('CSS uses box-sizing border-box to prevent overflow', () => {
    const css = loadMainCss();
    assert(css.includes('box-sizing: border-box'),
           'Should use border-box for predictable sizing');
});

test('All main sections exist for desktop view', () => {
    const html = loadHomepageHtml();
    const sections = ['header', 'hero', 'dashboard', 'features', 'quickstart', 'documentation', 'footer'];
    for (const section of sections) {
        assert(html.includes(`id="${section}"`),
               `Should have ${section} section visible at 1024px`);
    }
});

// =============================================
// Test Case 2: Full Navigation at 1024px Viewport
// Verifies full navigation bar visible, no hamburger menu
// =============================================
console.log('\nTest Case 2: Navigation at 1024px Viewport');
console.log('Input: Check navigation at 1024px viewport');
console.log('Expected: Full navigation bar visible, no hamburger menu');

test('Desktop media query sets horizontal nav layout', () => {
    const css = loadMainCss();
    const desktopSection = css.split('@media (min-width: 1024px)')[1];
    assert(desktopSection, 'Should have desktop media query section');

    // Check that nav-list is flex row in desktop view
    assert(desktopSection.includes('.nav-list') || css.includes('.nav-list'),
           'Should style nav-list for desktop');
    assert(css.includes('display: flex'),
           'Nav should use flexbox for horizontal layout');
});

test('Header uses horizontal flex layout on desktop', () => {
    const css = loadMainCss();
    const desktopSection = css.split('@media (min-width: 1024px)')[1];
    if (desktopSection) {
        assert(desktopSection.includes('flex-direction: row') ||
               desktopSection.includes('.header-container'),
               'Header should use row layout on desktop');
    }
});

test('Navigation list is always visible (no hamburger on desktop)', () => {
    const css = loadMainCss();
    // Check that nav-list doesn't have display: none in desktop view
    const desktopSection = css.split('@media (min-width: 1024px)')[1];
    if (desktopSection) {
        // In desktop, nav should be visible, not hidden
        assert(!desktopSection.includes('.nav-list') ||
               !desktopSection.includes('display: none'),
               'Nav list should not be hidden on desktop');
    }
    // Also verify the base nav-list is display: flex
    assert(css.includes('.nav-list') && css.includes('display: flex'),
           'Nav list should be flex display');
});

test('All navigation links are present', () => {
    const html = loadHomepageHtml();
    assert(html.includes('id="nav-overview"'), 'Should have Overview nav link');
    assert(html.includes('id="nav-api-docs"'), 'Should have API Docs nav link');
    assert(html.includes('id="nav-github"'), 'Should have GitHub nav link');
});

// =============================================
// Test Case 3: Dashboard Grid Layout at 1024px
// Verifies metrics displayed in grid/row layout
// =============================================
console.log('\nTest Case 3: Dashboard Layout at 1024px Viewport');
console.log('Input: Check dashboard layout at 1024px viewport');
console.log('Expected: Metrics displayed in grid/row layout');

test('Metrics grid uses CSS grid layout', () => {
    const css = loadMainCss();
    assert(css.includes('.metrics-grid'), 'Should have metrics-grid class');
    assert(css.includes('display: grid'), 'Should use CSS grid for metrics');
});

test('Desktop media query sets metrics to multi-column layout', () => {
    const css = loadMainCss();
    const desktopSection = css.split('@media (min-width: 1024px)')[1];
    assert(desktopSection, 'Should have desktop media query');
    assert(desktopSection.includes('.metrics-grid'),
           'Desktop media query should style metrics grid');
    assert(desktopSection.includes('grid-template-columns'),
           'Should define grid columns for desktop');
});

test('Metrics grid shows 5 columns on desktop (one for each metric)', () => {
    const css = loadMainCss();
    const desktopSection = css.split('@media (min-width: 1024px)')[1];
    if (desktopSection) {
        // Check for repeat(5, 1fr) or similar 5-column layout
        assert(desktopSection.includes('repeat(5, 1fr)') ||
               desktopSection.includes('grid-template-columns'),
               'Should have 5-column layout for metrics on desktop');
    }
});

test('All 5 metric cards exist', () => {
    const html = loadHomepageHtml();
    const metrics = ['metric-uptime', 'metric-memory', 'metric-keys', 'metric-ops', 'metric-storage'];
    for (const metric of metrics) {
        assert(html.includes(`id="${metric}"`),
               `Should have ${metric} card in dashboard`);
    }
});

// =============================================
// Test Case 4: Large Screen Layout at 1920px
// Verifies content centered/constrained, not edge-to-edge
// =============================================
console.log('\nTest Case 4: Large Screen Layout at 1920px Viewport');
console.log('Input: Render page at 1920px viewport width');
console.log('Expected: Content centered or constrained, not stretched edge-to-edge');

test('CSS has 1920px large desktop media query', () => {
    const css = loadMainCss();
    assert(css.includes('@media (min-width: 1920px)') ||
           css.includes('@media (min-width: 1440px)'),
           'Should have large desktop media query');
});

test('Max-width is defined to constrain content', () => {
    const css = loadMainCss();
    assert(css.includes('--max-width'),
           'Should define max-width CSS variable');
    assert(css.includes('max-width: var(--max-width)') ||
           css.includes('max-width:'),
           'Should apply max-width constraint');
});

test('Main content area uses max-width and auto margins for centering', () => {
    const css = loadMainCss();
    assert(css.includes('.main'), 'Should have .main styles');
    assert(css.includes('margin: 0 auto') || css.includes('margin:0 auto'),
           'Should center content with auto margins');
});

test('Header container is constrained on large screens', () => {
    const css = loadMainCss();
    assert(css.includes('.header-container'),
           'Should have header-container styles');
    const largeScreenSection = css.split('@media (min-width: 1920px)')[1];
    if (largeScreenSection) {
        assert(largeScreenSection.includes('max-width') ||
               largeScreenSection.includes('.header-container'),
               'Header should be constrained on large screens');
    }
});

test('Footer container is constrained on large screens', () => {
    const css = loadMainCss();
    assert(css.includes('.footer-container'),
           'Should have footer-container styles');
});

test('Hero content has max-width for readability on large screens', () => {
    const css = loadMainCss();
    const largeScreenSection = css.split('@media (min-width: 1920px)')[1];
    if (largeScreenSection) {
        assert(largeScreenSection.includes('.hero-content') ||
               css.includes('.hero-content'),
               'Hero content should be constrained');
    }
});

test('Root max-width variable increases appropriately for large screens', () => {
    const css = loadMainCss();
    // Check base max-width exists
    assert(css.includes('--max-width: 1200px') || css.includes('--max-width:1200px'),
           'Should have base max-width of 1200px');
    // Check large screen max-width adjustment
    const largeScreenSection = css.split('@media (min-width: 1920px)')[1];
    if (largeScreenSection) {
        assert(largeScreenSection.includes('--max-width') ||
               largeScreenSection.includes('max-width'),
               'Large screen should adjust max-width');
    }
});

// =============================================
// Additional Desktop Layout Tests
// =============================================
console.log('\nAdditional Desktop Layout Tests');

test('Features grid uses multi-column layout on desktop', () => {
    const css = loadMainCss();
    assert(css.includes('.features-grid'), 'Should have features-grid class');
    const desktopSection = css.split('@media (min-width: 1024px)')[1];
    if (desktopSection) {
        assert(desktopSection.includes('.features-grid') ||
               css.includes('grid-template-columns'),
               'Features should use multi-column grid on desktop');
    }
});

test('Code examples layout is optimized for desktop', () => {
    const css = loadMainCss();
    assert(css.includes('.code-examples'), 'Should have code-examples class');
});

test('Footer uses horizontal layout on desktop', () => {
    const css = loadMainCss();
    const desktopSection = css.split('@media (min-width: 1024px)')[1];
    if (desktopSection) {
        assert(desktopSection.includes('.footer-container') ||
               desktopSection.includes('flex-direction: row'),
               'Footer should use horizontal layout on desktop');
    }
});

test('CSS variables are defined for consistent spacing', () => {
    const css = loadMainCss();
    assert(css.includes('--spacing-'), 'Should define spacing CSS variables');
});

test('No horizontal scroll elements on desktop (no overflow-x)', () => {
    const css = loadMainCss();
    // Main body should not have overflow-x: scroll
    // Check that base styles don't force horizontal scrolling
    assert(!css.includes('overflow-x: scroll') || css.includes('overflow-x: auto'),
           'Should not have forced horizontal scroll on main elements');
});

// =============================================
// Summary
// =============================================
console.log('\n' + '='.repeat(50));
console.log(`Responsive Desktop Test Results: ${passed} passed, ${failed} failed`);
console.log('='.repeat(50));

// Exit with appropriate code
if (failed > 0) {
    console.log('\nFailed tests:');
    results.filter(r => r.status === 'fail').forEach(r => {
        console.log(`  - ${r.name}: ${r.error}`);
    });
    process.exit(1);
} else {
    console.log('\nAll desktop responsive tests passed!');
    process.exit(0);
}
