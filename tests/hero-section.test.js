/**
 * Hero Section Tests
 * Tests for verifying the hero section displays product name, logo, tagline,
 * and call-to-action buttons correctly (REQ-1)
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

// Load the HTML file
const htmlPath = path.join(__dirname, '..', 'index.html');
const html = fs.readFileSync(htmlPath, 'utf8');

// Parse with JSDOM
const dom = new JSDOM(html);
const document = dom.window.document;

// Test results tracking
let passed = 0;
let failed = 0;
const results = [];

function test(name, fn) {
    try {
        fn();
        passed++;
        results.push({ name, status: 'pass' });
        console.log(`\x1b[32m✓\x1b[0m ${name}`);
    } catch (error) {
        failed++;
        results.push({ name, status: 'fail', error: error.message });
        console.log(`\x1b[31m✗\x1b[0m ${name}`);
        console.log(`  Error: ${error.message}`);
    }
}

function assert(condition, message) {
    if (!condition) {
        throw new Error(message);
    }
}

console.log('\n=== Hero Section Display Tests ===\n');

// Test Case 1: Hero section contains product logo (logo.gif)
// Type: e2e
// Expected: Hero section contains product logo (logo.gif) displayed as an image element
test('TC1: Hero section contains product logo (logo.gif) displayed as an image element', () => {
    const heroSection = document.querySelector('.hero');
    assert(heroSection, 'Hero section should exist');

    const logo = heroSection.querySelector('img.logo');
    assert(logo, 'Logo image should exist with class "logo"');

    const logoSrc = logo.getAttribute('src');
    assert(logoSrc, 'Logo should have a src attribute');
    assert(logoSrc.includes('logo.gif'), 'Logo src should contain "logo.gif"');
});

// Test Case 2: Product name 'MirDB' is visible in the hero section
// Type: e2e
// Expected: Product name 'MirDB' is visible in the hero section
test('TC2: Product name MirDB is visible in the hero section', () => {
    const heroSection = document.querySelector('.hero');
    assert(heroSection, 'Hero section should exist');

    const heading = heroSection.querySelector('h1');
    assert(heading, 'Hero section should contain an h1 element');

    const headingText = heading.textContent.trim();
    assert(headingText === 'MirDB', `Product name should be "MirDB", got "${headingText}"`);
});

// Test Case 3: Tagline contains 'Persistent Key-Value Store' and 'Memcached Protocol'
// Type: unit
// Expected: Tagline contains 'Persistent Key-Value Store' and 'Memcached Protocol'
test('TC3: Tagline contains Persistent Key-Value Store and Memcached Protocol', () => {
    const tagline = document.querySelector('.tagline');
    assert(tagline, 'Tagline element should exist with class "tagline"');

    const taglineText = tagline.textContent;
    assert(taglineText.includes('Persistent Key-Value Store'),
        'Tagline should contain "Persistent Key-Value Store"');
    assert(taglineText.includes('Memcached Protocol'),
        'Tagline should contain "Memcached Protocol"');
});

// Test Case 4: 'Get Started' button scrolls to quick-start section
// Type: e2e
// Expected: Page scrolls to the quick-start section
test('TC4: Get Started button links to quick-start section', () => {
    const heroSection = document.querySelector('.hero');
    assert(heroSection, 'Hero section should exist');

    const getStartedBtn = heroSection.querySelector('a.btn-primary');
    assert(getStartedBtn, 'Get Started button should exist with class "btn-primary"');

    const btnText = getStartedBtn.textContent.trim();
    assert(btnText === 'Get Started', `Button text should be "Get Started", got "${btnText}"`);

    const href = getStartedBtn.getAttribute('href');
    assert(href === '#quick-start', `Button should link to "#quick-start", got "${href}"`);

    // Verify quick-start section exists
    const quickStartSection = document.getElementById('quick-start');
    assert(quickStartSection, 'Quick-start section should exist for smooth scroll target');
});

// Test Case 5: 'View on GitHub' button links to GitHub repository
// Type: e2e
// Expected: Link navigates to GitHub repository URL
test('TC5: View on GitHub button links to GitHub repository URL', () => {
    const heroSection = document.querySelector('.hero');
    assert(heroSection, 'Hero section should exist');

    const githubBtn = heroSection.querySelector('a.btn-secondary');
    assert(githubBtn, 'View on GitHub button should exist with class "btn-secondary"');

    const btnText = githubBtn.textContent.trim();
    assert(btnText === 'View on GitHub', `Button text should be "View on GitHub", got "${btnText}"`);

    const href = githubBtn.getAttribute('href');
    assert(href === 'https://github.com/yetone/mirdb',
        `Button should link to "https://github.com/yetone/mirdb", got "${href}"`);

    // Verify external link attributes
    const target = githubBtn.getAttribute('target');
    assert(target === '_blank', 'GitHub link should open in new tab (target="_blank")');

    const rel = githubBtn.getAttribute('rel');
    assert(rel && rel.includes('noopener'), 'GitHub link should have rel="noopener" for security');
});

// Summary
console.log(`\n=== Test Summary ===`);
console.log(`Passed: ${passed}`);
console.log(`Failed: ${failed}`);
console.log(`Total: ${passed + failed}`);

// Exit with appropriate code
process.exit(failed > 0 ? 1 : 0);
