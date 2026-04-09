/**
 * Documentation Links E2E Tests - Scenario 6
 *
 * Owner: Scenario 6 - Documentation Links
 *
 * Tests:
 * - README link element exists with proper href
 * - Memcached protocol link element exists with proper href
 * - README link navigable (target="_blank")
 * - Protocol documentation link navigable
 * - GitHub repository link in navigation
 */

'use strict';

const fs = require('fs');
const path = require('path');
const assert = require('assert');

// Test results tracking
let passed = 0;
let failed = 0;
const results = [];

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

console.log('\n==================================================');
console.log('Scenario 6: Documentation Links E2E Tests');
console.log('==================================================\n');

// =============================================
// Test Case 1: README link element exists
// =============================================
console.log('Test Case 1: README Link Element');

test('README link element with id="doc-link-readme" exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'doc-link-readme'),
           'Should have README link with id="doc-link-readme"');
});

test('README link text is present', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, '>README<') || containsText(html, '>README</a>'),
           'Should have README link text');
});

test('README link points to GitHub README', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'href="https://github.com/yetone/mirdb/blob/master/README.md"'),
           'README link should point to GitHub README');
});

// =============================================
// Test Case 2: Memcached protocol link element
// =============================================
console.log('\nTest Case 2: Memcached Protocol Link Element');

test('Memcached protocol link element with id="doc-link-protocol" exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'doc-link-protocol'),
           'Should have protocol link with id="doc-link-protocol"');
});

test('Memcached Protocol link text is present', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'Memcached Protocol'),
           'Should have "Memcached Protocol" link text');
});

test('Protocol link points to official wiki', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'href="https://github.com/memcached/memcached/wiki/Protocols"'),
           'Protocol link should point to official protocol wiki');
});

// =============================================
// Test Case 3: README link is navigable
// =============================================
console.log('\nTest Case 3: README Link Navigation');

test('README link opens in new tab (target="_blank")', () => {
    const html = loadHomepageHtml();
    // Find the README link section and verify it has target="_blank"
    const readmeLinkSection = html.substring(
        html.indexOf('id="doc-link-readme"') - 100,
        html.indexOf('id="doc-link-readme"') + 100
    );
    assert(readmeLinkSection.includes('target="_blank"'),
           'README link should open in new tab');
});

test('README link has rel="noopener" for security', () => {
    const html = loadHomepageHtml();
    const readmeLinkSection = html.substring(
        html.indexOf('id="doc-link-readme"') - 100,
        html.indexOf('id="doc-link-readme"') + 100
    );
    assert(readmeLinkSection.includes('rel="noopener"'),
           'README link should have rel="noopener" for security');
});

// =============================================
// Test Case 4: Protocol documentation link is navigable
// =============================================
console.log('\nTest Case 4: Protocol Link Navigation');

test('Protocol link opens in new tab (target="_blank")', () => {
    const html = loadHomepageHtml();
    const protocolLinkSection = html.substring(
        html.indexOf('id="doc-link-protocol"') - 100,
        html.indexOf('id="doc-link-protocol"') + 100
    );
    assert(protocolLinkSection.includes('target="_blank"'),
           'Protocol link should open in new tab');
});

test('Protocol link has rel="noopener" for security', () => {
    const html = loadHomepageHtml();
    const protocolLinkSection = html.substring(
        html.indexOf('id="doc-link-protocol"') - 100,
        html.indexOf('id="doc-link-protocol"') + 100
    );
    assert(protocolLinkSection.includes('rel="noopener"'),
           'Protocol link should have rel="noopener" for security');
});

// =============================================
// Test Case 5: GitHub repository link in navigation
// =============================================
console.log('\nTest Case 5: GitHub Navigation Link');

test('GitHub link in navigation with id="nav-github" exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'nav-github'),
           'Should have GitHub link with id="nav-github"');
});

test('GitHub navigation link text is present', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, '>GitHub<') || containsText(html, '>GitHub</a>'),
           'Navigation should have GitHub link text');
});

test('GitHub link points to correct repository', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'href="https://github.com/yetone/mirdb"'),
           'GitHub link should point to https://github.com/yetone/mirdb');
});

// =============================================
// Additional Tests: Documentation Section Structure
// =============================================
console.log('\nAdditional: Documentation Section Structure');

test('Documentation section container exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'documentation'),
           'Should have documentation section with id="documentation"');
});

test('Documentation section title exists', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, '>Documentation<'),
           'Documentation section should have "Documentation" title');
});

test('Documentation description text exists', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'Learn more about MirDB'),
           'Documentation section should have description text');
});

test('Documentation links have doc-link class for styling', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'class="doc-link"'),
           'Documentation links should have "doc-link" class');
});

test('Footer has Memcached protocol link', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'footer-protocol'),
           'Footer should have Memcached protocol link');
});

test('Footer has GitHub repository link', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'footer-github'),
           'Footer should have GitHub link');
});

test('Footer protocol link points to correct URL', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'footer-protocol') &&
           containsText(html, 'memcached/memcached/wiki/Protocols'),
           'Footer protocol link should point to official wiki');
});

test('Footer GitHub link points to correct URL', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'footer-github') &&
           containsText(html, 'github.com/yetone/mirdb'),
           'Footer GitHub link should point to repository');
});

// =============================================
// CSS Tests: Documentation Link Styles
// =============================================
console.log('\nCSS: Documentation Link Styles');

test('CSS has .documentation section styles', () => {
    const css = loadMainCss();
    assert(css.includes('.documentation'),
           'CSS should have .documentation styles');
});

test('CSS has .doc-links styles', () => {
    const css = loadMainCss();
    assert(css.includes('.doc-links'),
           'CSS should have .doc-links styles');
});

test('CSS has .doc-link hover styles', () => {
    const css = loadMainCss();
    assert(css.includes('.doc-link:hover') || css.includes('.doc-links li a:hover'),
           'CSS should have documentation link hover styles');
});

test('CSS has .doc-description styles', () => {
    const css = loadMainCss();
    assert(css.includes('.doc-description'),
           'CSS should have .doc-description styles');
});

// =============================================
// Security Tests: External Links
// =============================================
console.log('\nSecurity: External Link Attributes');

test('All target="_blank" links have rel="noopener"', () => {
    const html = loadHomepageHtml();
    const targetBlankCount = (html.match(/target="_blank"/g) || []).length;
    const noopenerCount = (html.match(/rel="noopener"/g) || []).length;
    assert(noopenerCount >= targetBlankCount,
           `All target="_blank" links should have rel="noopener" (${targetBlankCount} vs ${noopenerCount})`);
});

// =============================================
// Summary
// =============================================
console.log('\n' + '='.repeat(50));
console.log(`Test Results: ${passed} passed, ${failed} failed`);
console.log('='.repeat(50));

// Export results for scenario tracking
const testSummary = {
    scenario: 6,
    name: 'Documentation Links',
    total: passed + failed,
    passed: passed,
    failed: failed,
    tests: results
};

// Write results to JSON for scenario reporting
const resultsPath = path.join(__dirname, '../../.something/test_results_scenario_6.json');
try {
    fs.writeFileSync(resultsPath, JSON.stringify(testSummary, null, 2));
    console.log(`\nResults written to: ${resultsPath}`);
} catch (e) {
    // Directory might not exist, that's ok
}

// Exit with appropriate code
if (failed > 0) {
    console.log('\nFailed tests:');
    results.filter(r => r.status === 'fail').forEach(r => {
        console.log(`  - ${r.name}: ${r.error}`);
    });
    process.exit(1);
} else {
    console.log('\nAll Scenario 6 tests passed!');
    process.exit(0);
}
