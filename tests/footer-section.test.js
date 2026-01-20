/**
 * Footer Section Tests
 * Tests for verifying the footer contains links to documentation, GitHub,
 * and community resources (REQ-10)
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

console.log('\n=== Footer Section Tests ===\n');

// Test Case 1: Check footer element exists
// Type: unit
// Expected: Page contains a footer element or section at the bottom
test('TC1: Page contains a footer element at the bottom', () => {
    const footer = document.querySelector('footer');
    assert(footer, 'Footer element should exist');

    // Verify footer is at the end of the body
    const body = document.querySelector('body');
    const lastChild = body.lastElementChild;
    assert(lastChild.tagName.toLowerCase() === 'footer',
        'Footer should be the last element in the body');
});

// Test Case 2: Verify GitHub link in footer
// Type: unit
// Expected: Footer contains a link to the GitHub repository
test('TC2: Footer contains a link to the GitHub repository', () => {
    const footer = document.querySelector('footer');
    assert(footer, 'Footer element should exist');

    const links = footer.querySelectorAll('a');
    const githubLink = Array.from(links).find(link => {
        const href = link.getAttribute('href');
        return href && href.includes('github.com/yetone/mirdb');
    });

    assert(githubLink, 'Footer should contain a GitHub link');

    const linkText = githubLink.textContent.trim().toLowerCase();
    assert(linkText.includes('github') || githubLink.getAttribute('href').includes('github'),
        'GitHub link should be identifiable as GitHub');

    // Verify external link attributes for security
    const target = githubLink.getAttribute('target');
    assert(target === '_blank', 'GitHub link should open in new tab (target="_blank")');

    const rel = githubLink.getAttribute('rel');
    assert(rel && rel.includes('noopener'),
        'GitHub link should have rel="noopener" for security');
});

// Test Case 3: Verify documentation link in footer
// Type: unit
// Expected: Footer contains a link labeled 'Documentation' or similar
test('TC3: Footer contains a Documentation link', () => {
    const footer = document.querySelector('footer');
    assert(footer, 'Footer element should exist');

    const links = footer.querySelectorAll('a');
    const docLink = Array.from(links).find(link => {
        const text = link.textContent.toLowerCase();
        const href = link.getAttribute('href') || '';
        return text.includes('doc') ||
               text.includes('readme') ||
               href.includes('README') ||
               href.includes('docs');
    });

    assert(docLink, 'Footer should contain a Documentation link');

    const href = docLink.getAttribute('href');
    assert(href, 'Documentation link should have an href attribute');

    // Verify external link attributes for security
    const target = docLink.getAttribute('target');
    assert(target === '_blank', 'Documentation link should open in new tab');

    const rel = docLink.getAttribute('rel');
    assert(rel && rel.includes('noopener'),
        'Documentation link should have rel="noopener" for security');
});

// Test Case 4: Verify license link in footer
// Type: unit
// Expected: Footer contains a link to license information
test('TC4: Footer contains a License link', () => {
    const footer = document.querySelector('footer');
    assert(footer, 'Footer element should exist');

    const links = footer.querySelectorAll('a');
    const licenseLink = Array.from(links).find(link => {
        const text = link.textContent.toLowerCase();
        const href = link.getAttribute('href') || '';
        return text.includes('license') ||
               href.toLowerCase().includes('license');
    });

    assert(licenseLink, 'Footer should contain a License link');

    const href = licenseLink.getAttribute('href');
    assert(href, 'License link should have an href attribute');
    assert(href.toLowerCase().includes('license'),
        'License link href should point to license information');

    // Verify external link attributes for security
    const target = licenseLink.getAttribute('target');
    assert(target === '_blank', 'License link should open in new tab');

    const rel = licenseLink.getAttribute('rel');
    assert(rel && rel.includes('noopener'),
        'License link should have rel="noopener" for security');
});

// Test Case 5: Check for project attribution
// Type: unit
// Expected: Footer contains attribution text or copyright notice
test('TC5: Footer contains attribution text or copyright notice', () => {
    const footer = document.querySelector('footer');
    assert(footer, 'Footer element should exist');

    const footerText = footer.textContent.toLowerCase();

    // Check for common attribution patterns
    const hasProjectName = footerText.includes('mirdb');
    const hasCopyright = footerText.includes('©') ||
                         footerText.includes('copyright') ||
                         footerText.includes('built with') ||
                         footerText.includes('made with');

    assert(hasProjectName || hasCopyright,
        'Footer should contain project attribution (project name or copyright notice)');

    // Check for copyright paragraph element
    const copyrightElement = footer.querySelector('.copyright') ||
                            footer.querySelector('p');
    assert(copyrightElement,
        'Footer should have a dedicated element for copyright/attribution');

    const copyrightText = copyrightElement.textContent;
    assert(copyrightText.length > 0,
        'Copyright/attribution text should not be empty');
});

// Summary
console.log(`\n=== Test Summary ===`);
console.log(`Passed: ${passed}`);
console.log(`Failed: ${failed}`);
console.log(`Total: ${passed + failed}`);

// Exit with appropriate code
process.exit(failed > 0 ? 1 : 0);
