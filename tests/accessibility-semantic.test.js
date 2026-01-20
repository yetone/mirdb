/**
 * Accessibility - Semantic HTML Tests
 * Tests for verifying the homepage uses proper semantic HTML structure (NFR-3)
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

console.log('\n=== Accessibility - Semantic HTML Tests ===\n');

// Test Case 1: Check for single h1 element
// Type: unit
// Expected: Page contains exactly one h1 element
test('TC1: Page contains exactly one h1 element', () => {
    const h1Elements = document.querySelectorAll('h1');
    assert(h1Elements.length === 1,
        `Page should contain exactly one h1 element, found ${h1Elements.length}`);

    // Verify h1 has meaningful content
    const h1Text = h1Elements[0].textContent.trim();
    assert(h1Text.length > 0, 'h1 element should have meaningful content');
});

// Test Case 2: Verify heading hierarchy
// Type: unit
// Expected: Headings follow proper hierarchy without skipping levels
test('TC2: Headings follow proper hierarchy without skipping levels', () => {
    const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
    assert(headings.length > 0, 'Page should contain heading elements');

    let previousLevel = 0;
    const errors = [];

    headings.forEach((heading, index) => {
        const level = parseInt(heading.tagName.charAt(1));

        // First heading should be h1
        if (index === 0) {
            if (level !== 1) {
                errors.push(`First heading should be h1, found h${level}`);
            }
        } else {
            // Each subsequent heading should not skip more than one level
            // e.g., h1 -> h2 or h1 -> h1 is OK, but h1 -> h3 is not
            if (level > previousLevel + 1) {
                errors.push(`Heading hierarchy skipped: h${previousLevel} followed by h${level} ("${heading.textContent.trim()}")`);
            }
        }

        previousLevel = level;
    });

    assert(errors.length === 0, errors.join('; '));
});

// Test Case 3: Check for main element
// Type: unit
// Expected: Page contains a main element for primary content
test('TC3: Page contains a main element for primary content', () => {
    const mainElement = document.querySelector('main');
    assert(mainElement, 'Page should contain a main element for primary content');

    // Verify main element contains content
    const mainContent = mainElement.innerHTML.trim();
    assert(mainContent.length > 0, 'main element should contain content');
});

// Test Case 4: Verify semantic footer element
// Type: unit
// Expected: Footer content is wrapped in a footer element
test('TC4: Footer content is wrapped in a footer element', () => {
    const footerElement = document.querySelector('footer');
    assert(footerElement, 'Page should contain a footer element');

    // Verify footer contains expected content (links, copyright, etc.)
    const footerLinks = footerElement.querySelectorAll('a');
    assert(footerLinks.length > 0, 'Footer should contain navigation links');

    // Check footer has meaningful content
    const footerText = footerElement.textContent.trim();
    assert(footerText.length > 0, 'Footer should have meaningful content');
});

// Additional semantic HTML checks for comprehensive coverage
test('TC5: Page uses semantic section elements', () => {
    const sections = document.querySelectorAll('section');
    assert(sections.length > 0, 'Page should use section elements for content organization');

    // Verify sections have identifying headers or aria-labels
    sections.forEach((section, index) => {
        const hasHeading = section.querySelector('h1, h2, h3, h4, h5, h6');
        const hasAriaLabel = section.hasAttribute('aria-label') || section.hasAttribute('aria-labelledby');
        assert(hasHeading || hasAriaLabel,
            `Section ${index + 1} should have a heading or aria-label for accessibility`);
    });
});

test('TC6: Page uses semantic header element', () => {
    const headerElement = document.querySelector('header');
    assert(headerElement, 'Page should contain a header element');

    // Verify header contains the main heading
    const h1InHeader = headerElement.querySelector('h1');
    assert(h1InHeader, 'Header element should contain the main h1 heading');
});

// Summary
console.log(`\n=== Test Summary ===`);
console.log(`Passed: ${passed}`);
console.log(`Failed: ${failed}`);
console.log(`Total: ${passed + failed}`);

// Exit with appropriate code
process.exit(failed > 0 ? 1 : 0);
