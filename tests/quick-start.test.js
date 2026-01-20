/**
 * Quick Start Section Tests
 * Tests for verifying the quick-start section displays installation and usage instructions
 * with code examples (REQ-3, REQ-4)
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

console.log('\n=== Quick Start Section Tests ===\n');

// Test Case 1: Check for installation command presence
// Type: unit
// Expected: Section contains 'cargo' build or run commands in a code block
test('TC1: Installation command presence - cargo build/run commands in code block', () => {
    const quickStartSection = document.getElementById('quick-start');
    assert(quickStartSection, 'Quick start section should exist');

    const codeBlocks = quickStartSection.querySelectorAll('pre code');
    assert(codeBlocks.length > 0, 'Quick start section should contain code blocks');

    let hasCargoCommand = false;
    codeBlocks.forEach(codeBlock => {
        const text = codeBlock.textContent;
        if (text.includes('cargo build') || text.includes('cargo run')) {
            hasCargoCommand = true;
        }
    });

    assert(hasCargoCommand, 'Code blocks should contain cargo build or cargo run commands');
});

// Test Case 2: Verify code blocks are formatted correctly
// Type: e2e
// Expected: Code examples are displayed in monospace font with code block styling
test('TC2: Code blocks formatted correctly - monospace font and styling', () => {
    const quickStartSection = document.getElementById('quick-start');
    assert(quickStartSection, 'Quick start section should exist');

    const preElements = quickStartSection.querySelectorAll('pre');
    assert(preElements.length > 0, 'Quick start section should contain pre elements');

    const codeElements = quickStartSection.querySelectorAll('pre code');
    assert(codeElements.length > 0, 'Pre elements should contain code elements');

    // Verify the code elements exist (styling is defined in CSS)
    codeElements.forEach((codeEl, index) => {
        assert(codeEl.textContent.trim().length > 0, `Code block ${index + 1} should have content`);
        // Check that code element has language class or is within pre
        const parent = codeEl.parentElement;
        assert(parent.tagName === 'PRE', `Code element ${index + 1} should be inside a pre element`);
    });
});

// Test Case 3: Check for telnet usage example
// Type: unit
// Expected: Usage example includes telnet connection command or similar client connection
test('TC3: Telnet usage example present', () => {
    const quickStartSection = document.getElementById('quick-start');
    assert(quickStartSection, 'Quick start section should exist');

    const codeBlocks = quickStartSection.querySelectorAll('pre code');

    let hasTelnetExample = false;
    codeBlocks.forEach(codeBlock => {
        const text = codeBlock.textContent;
        if (text.includes('telnet') || text.includes('localhost 12333') || text.includes('connect')) {
            hasTelnetExample = true;
        }
    });

    assert(hasTelnetExample, 'Usage example should include telnet connection command');
});

// Test Case 4: Verify usage.gif is displayed
// Type: e2e
// Expected: Image element with source pointing to usage.gif is present and visible
test('TC4: Usage.gif is displayed', () => {
    const quickStartSection = document.getElementById('quick-start');
    assert(quickStartSection, 'Quick start section should exist');

    const images = quickStartSection.querySelectorAll('img');

    let hasUsageGif = false;
    let usageGifElement = null;
    images.forEach(img => {
        const src = img.getAttribute('src');
        if (src && src.includes('usage.gif')) {
            hasUsageGif = true;
            usageGifElement = img;
        }
    });

    assert(hasUsageGif, 'Quick start section should contain usage.gif image');
    assert(usageGifElement, 'Usage.gif element should exist');

    // Check the image has the usage-gif class for visibility styling
    assert(usageGifElement.classList.contains('usage-gif'), 'Usage.gif should have the usage-gif class');
});

// Test Case 5: Verify usage.gif has alt text
// Type: unit
// Expected: Usage GIF image has appropriate alt text for accessibility
test('TC5: Usage.gif has alt text for accessibility', () => {
    const quickStartSection = document.getElementById('quick-start');
    assert(quickStartSection, 'Quick start section should exist');

    const images = quickStartSection.querySelectorAll('img');

    let usageGifElement = null;
    images.forEach(img => {
        const src = img.getAttribute('src');
        if (src && src.includes('usage.gif')) {
            usageGifElement = img;
        }
    });

    assert(usageGifElement, 'Usage.gif element should exist');

    const altText = usageGifElement.getAttribute('alt');
    assert(altText, 'Usage.gif should have an alt attribute');
    assert(altText.length > 0, 'Usage.gif alt text should not be empty');
    assert(altText.length >= 10, 'Usage.gif alt text should be descriptive (at least 10 characters)');

    // Check alt text contains relevant keywords
    const altLower = altText.toLowerCase();
    const hasRelevantKeywords = altLower.includes('mirdb') ||
                                altLower.includes('usage') ||
                                altLower.includes('demonstration') ||
                                altLower.includes('demo') ||
                                altLower.includes('terminal') ||
                                altLower.includes('command');
    assert(hasRelevantKeywords, 'Alt text should contain relevant keywords describing the usage demonstration');
});

// Summary
console.log(`\n=== Test Summary ===`);
console.log(`Passed: ${passed}`);
console.log(`Failed: ${failed}`);
console.log(`Total: ${passed + failed}`);

// Exit with appropriate code
process.exit(failed > 0 ? 1 : 0);
