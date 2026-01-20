/**
 * Accessibility - Images Tests
 * Tests for verifying all images have appropriate alt text for accessibility (NFR-3)
 * WCAG 2.1 AA compliance requires all images to have alt text
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

console.log('\n=== Accessibility - Images Tests ===\n');

// Test Case 1: Check logo image alt text
// Type: unit
// Expected: Logo image has non-empty alt attribute describing the logo
test('TC1: Logo image has non-empty alt attribute describing the logo', () => {
    const heroSection = document.querySelector('.hero');
    assert(heroSection, 'Hero section should exist');

    const logo = heroSection.querySelector('img.logo');
    assert(logo, 'Logo image should exist with class "logo"');

    const src = logo.getAttribute('src');
    assert(src && src.includes('logo.gif'), 'Logo image should have src containing logo.gif');

    const altText = logo.getAttribute('alt');
    assert(altText !== null, 'Logo image must have an alt attribute');
    assert(altText.length > 0, 'Logo alt text should not be empty');
    assert(altText.length >= 10, 'Logo alt text should be descriptive (at least 10 characters)');

    // Check alt text contains relevant keywords
    const altLower = altText.toLowerCase();
    const hasRelevantKeywords = altLower.includes('mirdb') ||
                                altLower.includes('logo') ||
                                altLower.includes('database');
    assert(hasRelevantKeywords, `Alt text should describe the logo content, got "${altText}"`);

    console.log(`  Logo alt text: "${altText}"`);
});

// Test Case 2: Check usage.gif alt text
// Type: unit
// Expected: Usage GIF has alt text describing the demonstration content
test('TC2: Usage.gif has alt text describing the demonstration content', () => {
    const quickStartSection = document.getElementById('quick-start');
    assert(quickStartSection, 'Quick start section should exist');

    const usageGif = quickStartSection.querySelector('img[src*="usage.gif"]');
    assert(usageGif, 'Usage.gif image should exist in the quick-start section');

    const altText = usageGif.getAttribute('alt');
    assert(altText !== null, 'Usage.gif must have an alt attribute');
    assert(altText.length > 0, 'Usage.gif alt text should not be empty');
    assert(altText.length >= 10, 'Usage.gif alt text should be descriptive (at least 10 characters)');

    // Check alt text contains relevant keywords describing the demonstration
    const altLower = altText.toLowerCase();
    const hasRelevantKeywords = altLower.includes('usage') ||
                                altLower.includes('demonstration') ||
                                altLower.includes('demo') ||
                                altLower.includes('terminal') ||
                                altLower.includes('command') ||
                                altLower.includes('mirdb');
    assert(hasRelevantKeywords, `Alt text should describe the demonstration content, got "${altText}"`);

    console.log(`  Usage.gif alt text: "${altText}"`);
});

// Test Case 3: Verify all img elements have alt attribute
// Type: unit
// Expected: Every img element has an alt attribute (empty or descriptive)
test('TC3: Every img element has an alt attribute (empty or descriptive)', () => {
    const allImages = document.querySelectorAll('img');
    assert(allImages.length > 0, 'Page should contain at least one image');

    console.log(`  Found ${allImages.length} images on the page`);

    const missingAlt = [];
    allImages.forEach((img, index) => {
        const src = img.getAttribute('src') || 'unknown';
        const alt = img.getAttribute('alt');

        // Check if alt attribute exists (can be empty for decorative images)
        if (alt === null) {
            missingAlt.push({ index: index + 1, src });
        }
    });

    if (missingAlt.length > 0) {
        const details = missingAlt.map(m => `Image ${m.index}: ${m.src}`).join(', ');
        assert(false, `${missingAlt.length} image(s) missing alt attribute: ${details}`);
    }

    console.log(`  All ${allImages.length} images have alt attributes`);
});

// Test Case 4: Check badge images for alt text
// Type: unit
// Expected: CI status badges have appropriate alt text describing their purpose
test('TC4: CI status badges have appropriate alt text describing their purpose', () => {
    const badgesContainer = document.querySelector('.badges');
    assert(badgesContainer, 'Badges container should exist');

    const badgeImages = badgesContainer.querySelectorAll('img');
    assert(badgeImages.length > 0, 'Badges container should contain at least one badge image');

    console.log(`  Found ${badgeImages.length} badge image(s)`);

    badgeImages.forEach((badge, index) => {
        const src = badge.getAttribute('src');
        const altText = badge.getAttribute('alt');

        assert(altText !== null, `Badge image ${index + 1} must have an alt attribute`);
        assert(altText.length > 0, `Badge image ${index + 1} alt text should not be empty`);

        // Check alt text mentions CI/build/status
        const altLower = altText.toLowerCase();
        const hasRelevantKeywords = altLower.includes('ci') ||
                                    altLower.includes('circleci') ||
                                    altLower.includes('build') ||
                                    altLower.includes('status') ||
                                    altLower.includes('badge');
        assert(hasRelevantKeywords,
            `Badge ${index + 1} alt text should describe its purpose (CI/build/status), got "${altText}"`);

        console.log(`  Badge ${index + 1} alt text: "${altText}"`);
    });
});

// Additional test: Verify alt text is not just filenames
test('TC5: Alt text is descriptive and not just filenames', () => {
    const allImages = document.querySelectorAll('img');

    allImages.forEach((img, index) => {
        const src = img.getAttribute('src') || '';
        const altText = img.getAttribute('alt') || '';

        if (altText.length > 0) {
            // Check that alt text is not just the filename
            const srcFilename = src.split('/').pop().replace(/\.[^.]+$/, '');
            const altIsJustFilename = altText.toLowerCase() === srcFilename.toLowerCase();

            assert(!altIsJustFilename,
                `Image ${index + 1} alt text should be descriptive, not just the filename "${srcFilename}"`);
        }
    });

    console.log(`  All images have descriptive alt text (not just filenames)`);
});

// Summary
console.log(`\n=== Test Summary ===`);
console.log(`Passed: ${passed}`);
console.log(`Failed: ${failed}`);
console.log(`Total: ${passed + failed}`);

// Exit with appropriate code
process.exit(failed > 0 ? 1 : 0);
