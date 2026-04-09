/**
 * Homepage rendering and functionality tests.
 *
 * Owner: Scenario 1 - Homepage Core Rendering
 * Co-owners: Multiple scenarios for specific tests
 *
 * Test areas:
 * - All sections render correctly (Scenario 1)
 * - Status indicator updates (Scenario 2)
 * - Metrics refresh (Scenario 4)
 * - Documentation links work (Scenario 6)
 * - Theme toggle works (Scenario 7)
 * - Performance metrics (Scenario 11)
 */

const fs = require('fs');
const path = require('path');
const assert = require('assert');

// Helper to load HTML content
function loadHomepageHtml() {
    const htmlPath = path.join(__dirname, '../../static/index.html');
    return fs.readFileSync(htmlPath, 'utf-8');
}

// Helper to load CSS content
function loadMainCss() {
    const cssPath = path.join(__dirname, '../../static/css/main.css');
    return fs.readFileSync(cssPath, 'utf-8');
}

// Helper to check if HTML contains element with ID
function hasElementWithId(html, id) {
    return html.includes(`id="${id}"`);
}

// Helper to check if HTML contains text
function containsText(html, text) {
    return html.includes(text);
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
        console.log(`  ✓ ${name}`);
    } catch (error) {
        failed++;
        results.push({ name, status: 'fail', error: error.message });
        console.log(`  ✗ ${name}`);
        console.log(`    Error: ${error.message}`);
    }
}

// =============================================
// Test Case 1: HTTP 200 response simulation
// Verifies homepage HTML file exists and is valid
// =============================================
console.log('\nTest Case 1: Homepage HTML Exists and Valid');

test('homepage HTML file exists', () => {
    const html = loadHomepageHtml();
    assert(html.length > 0, 'HTML file should have content');
});

test('HTML has DOCTYPE declaration', () => {
    const html = loadHomepageHtml();
    assert(html.includes('<!DOCTYPE html>'), 'Should have DOCTYPE declaration');
});

test('HTML has html tag', () => {
    const html = loadHomepageHtml();
    assert(html.includes('<html'), 'Should have html tag');
});

test('HTML has head section', () => {
    const html = loadHomepageHtml();
    assert(html.includes('<head>'), 'Should have head section');
});

test('HTML has body section', () => {
    const html = loadHomepageHtml();
    assert(html.includes('<body>'), 'Should have body section');
});

test('HTML has closing html tag', () => {
    const html = loadHomepageHtml();
    assert(html.includes('</html>'), 'Should have closing html tag');
});

// =============================================
// Test Case 2: Header elements verification
// Verifies header contains MirDB branding, name, and version
// =============================================
console.log('\nTest Case 2: Header Contains Logo, Name, and Version');

test('header section exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'header'), 'Should have header element');
});

test('logo element exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'logo'), 'Should have logo element');
});

test('MirDB branding is present', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'MirDB'), 'Should contain MirDB branding');
});

test('version element exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'version'), 'Should have version element');
});

test('version number is displayed', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'v0.1.0') || containsText(html, 'v0.'), 'Should display version number');
});

test('product name MirDB is displayed in logo', () => {
    const html = loadHomepageHtml();
    assert(html.includes('MirDB'), 'Should have MirDB product name displayed');
});

// =============================================
// Test Case 3: Navigation links verification
// Verifies navigation includes Overview, API Docs, and GitHub
// =============================================
console.log('\nTest Case 3: Navigation Links');

test('navigation section exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'nav'), 'Should have navigation element');
});

test('Overview nav link exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'nav-overview'), 'Should have Overview nav link');
});

test('Overview link text present', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'Overview'), 'Should have Overview link text');
});

test('API Docs nav link exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'nav-api-docs'), 'Should have API Docs nav link');
});

test('API Docs link text present', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'API Docs'), 'Should have API Docs link text');
});

test('GitHub nav link exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'nav-github'), 'Should have GitHub nav link');
});

test('GitHub link text present', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'GitHub'), 'Should have GitHub link text');
});

test('navigation links have valid URLs', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'href="https://github.com'), 'Should have GitHub URL');
});

// =============================================
// Test Case 4: Hero section verification
// Verifies hero section displays tagline and CTA
// =============================================
console.log('\nTest Case 4: Hero Section Content');

test('hero section exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'hero'), 'Should have hero section');
});

test('tagline element exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'tagline'), 'Should have tagline element');
});

test('correct tagline text is displayed', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'A Persistent Key-Value Store with Memcached Protocol'),
           'Should display the correct tagline');
});

test('CTA button exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'cta-button'), 'Should have CTA button');
});

test('Get Started in Seconds CTA text present', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'Get Started in Seconds'),
           'Should have "Get Started in Seconds" CTA text');
});

test('status indicator exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'status-indicator'), 'Should have status indicator');
});

// =============================================
// Test Case 5: Dashboard metrics panel verification
// Verifies dashboard shows all required metrics
// =============================================
console.log('\nTest Case 5: Dashboard Metrics Panel');

test('dashboard section exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'dashboard'), 'Should have dashboard section');
});

test('uptime metric card exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'metric-uptime'), 'Should have uptime metric card');
});

test('uptime label present', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'Uptime'), 'Should have Uptime label');
});

test('memory metric card exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'metric-memory'), 'Should have memory metric card');
});

test('memory label present', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'Memory'), 'Should have Memory label');
});

test('keys metric card exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'metric-keys'), 'Should have keys metric card');
});

test('keys label present', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'Keys'), 'Should have Keys label');
});

test('ops metric card exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'metric-ops'), 'Should have ops metric card');
});

test('ops/sec label present', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'Ops/sec') || containsText(html, 'ops'), 'Should have Ops/sec label');
});

test('storage metric card exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'metric-storage'), 'Should have storage metric card');
});

test('storage label present', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'Storage'), 'Should have Storage label');
});

test('all five metrics have data-metric attributes', () => {
    const html = loadHomepageHtml();
    const metrics = ['uptime', 'memory', 'keys', 'ops', 'storage'];
    for (const metric of metrics) {
        assert(html.includes(`data-metric="${metric}"`) || html.includes(`id="metric-${metric}"`),
               `Dashboard should have ${metric} metric`);
    }
});

// =============================================
// Test Case 6: Footer content verification
// Verifies footer contains required links and license
// =============================================
console.log('\nTest Case 6: Footer Content');

test('footer section exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'footer'), 'Should have footer section');
});

test('Memcached protocol link in footer exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'footer-protocol'), 'Should have protocol link in footer');
});

test('Memcached Protocol link text present', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'Memcached Protocol'), 'Should have Memcached Protocol link text');
});

test('GitHub link in footer exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'footer-github'), 'Should have GitHub link in footer');
});

test('license section in footer exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'footer-license'), 'Should have license section in footer');
});

test('license information displayed', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'MIT License') || containsText(html, 'License'),
           'Should display license information');
});

// =============================================
// Additional structural tests
// =============================================
console.log('\nAdditional Structure Tests');

test('UTF-8 charset meta tag present', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'charset="UTF-8"') || containsText(html, 'charset=UTF-8'),
           'Should have UTF-8 charset meta tag');
});

test('viewport meta tag present', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'viewport'), 'Should have viewport meta tag');
});

test('CSS stylesheet linked', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'main.css'), 'Should link to main.css stylesheet');
});

test('page title exists', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, '<title>'), 'Should have title tag');
});

test('page title contains MirDB', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'MirDB'), 'Title should contain MirDB');
});

test('content section exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'content'), 'Should have main content section');
});

test('features section exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'features'), 'Should have features section');
});

test('quickstart section exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'quickstart'), 'Should have quickstart section');
});

// =============================================
// CSS file tests
// =============================================
console.log('\nCSS File Tests');

test('CSS file exists and has content', () => {
    const css = loadMainCss();
    assert(css.length > 0, 'CSS file should not be empty');
});

test('CSS has header styles', () => {
    const css = loadMainCss();
    assert(css.includes('.header'), 'Should have header styles');
});

test('CSS has hero styles', () => {
    const css = loadMainCss();
    assert(css.includes('.hero'), 'Should have hero styles');
});

test('CSS has dashboard styles', () => {
    const css = loadMainCss();
    assert(css.includes('.dashboard'), 'Should have dashboard styles');
});

test('CSS has footer styles', () => {
    const css = loadMainCss();
    assert(css.includes('.footer'), 'Should have footer styles');
});

test('CSS has metric card styles', () => {
    const css = loadMainCss();
    assert(css.includes('.metric-card'), 'Should have metric card styles');
});

test('CSS has navigation styles', () => {
    const css = loadMainCss();
    assert(css.includes('.nav'), 'Should have navigation styles');
});

test('CSS has CTA button styles', () => {
    const css = loadMainCss();
    assert(css.includes('.cta-button'), 'Should have CTA button styles');
});

test('CSS has media queries for responsiveness', () => {
    const css = loadMainCss();
    assert(css.includes('@media'), 'Should have media queries');
});

// =============================================
// Summary
// =============================================
console.log('\n' + '='.repeat(50));
console.log(`Test Results: ${passed} passed, ${failed} failed`);
console.log('='.repeat(50));

// Exit with appropriate code
if (failed > 0) {
    console.log('\nFailed tests:');
    results.filter(r => r.status === 'fail').forEach(r => {
        console.log(`  - ${r.name}: ${r.error}`);
    });
    process.exit(1);
} else {
    console.log('\nAll tests passed!');
    process.exit(0);
}
