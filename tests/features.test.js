/**
 * Feature Highlights Section Tests
 * Tests for verifying the feature highlights section displays key features
 * in a three-column layout (REQ-2)
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

// Load the HTML file
const htmlPath = path.join(__dirname, '..', 'index.html');
const html = fs.readFileSync(htmlPath, 'utf8');

// Load CSS for styling checks
const cssPath = path.join(__dirname, '..', 'styles.css');
const css = fs.readFileSync(cssPath, 'utf8');

// Parse with JSDOM
const dom = new JSDOM(html, {
    resources: 'usable'
});
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

console.log('\n=== Feature Highlights Section Tests ===\n');

// Test Case 1: Count feature cards in features section
// Type: unit
// Expected: Exactly 3 feature cards are displayed
test('TC1: Count feature cards - Exactly 3 feature cards are displayed', () => {
    const featuresSection = document.querySelector('.features');
    assert(featuresSection, 'Features section should exist');

    const featureCards = featuresSection.querySelectorAll('.feature-card');
    assert(featureCards.length === 3, `Expected 3 feature cards, found ${featureCards.length}`);
});

// Test Case 2: Check for 'Memcached Compatible' feature
// Type: unit
// Expected: Feature card with title 'Memcached Compatible' is present with description about existing client compatibility
test('TC2: Memcached Compatible feature - Title and client compatibility description', () => {
    const featuresSection = document.querySelector('.features');
    assert(featuresSection, 'Features section should exist');

    const featureCards = featuresSection.querySelectorAll('.feature-card');

    let memcachedCard = null;
    featureCards.forEach(card => {
        const title = card.querySelector('h3');
        if (title && title.textContent.includes('Memcached Compatible')) {
            memcachedCard = card;
        }
    });

    assert(memcachedCard, 'Feature card with title "Memcached Compatible" should exist');

    const description = memcachedCard.querySelector('p');
    assert(description, 'Memcached Compatible card should have a description');

    const descText = description.textContent.toLowerCase();
    const hasClientCompatibility = descText.includes('existing memcached clients') ||
                                   descText.includes('memcached client') ||
                                   descText.includes('out of the box');
    assert(hasClientCompatibility, 'Description should mention existing client compatibility');
});

// Test Case 3: Check for 'Persistent Storage' feature
// Type: unit
// Expected: Feature card with title containing 'Persistent' is present with description about SSTable storage
test('TC3: Persistent Storage feature - Title with "Persistent" and SSTable storage description', () => {
    const featuresSection = document.querySelector('.features');
    assert(featuresSection, 'Features section should exist');

    const featureCards = featuresSection.querySelectorAll('.feature-card');

    let persistentCard = null;
    featureCards.forEach(card => {
        const title = card.querySelector('h3');
        if (title && title.textContent.includes('Persistent')) {
            persistentCard = card;
        }
    });

    assert(persistentCard, 'Feature card with title containing "Persistent" should exist');

    const description = persistentCard.querySelector('p');
    assert(description, 'Persistent Storage card should have a description');

    const descText = description.textContent.toLowerCase();
    const hasSStableReference = descText.includes('sstable') ||
                                descText.includes('ss table') ||
                                descText.includes('survives restarts');
    assert(hasSStableReference, 'Description should mention SSTable storage or data persistence');
});

// Test Case 4: Check for 'High Performance' feature
// Type: unit
// Expected: Feature card with title containing 'Performance' is present with description about LSM tree architecture
test('TC4: High Performance feature - Title with "Performance" and LSM tree description', () => {
    const featuresSection = document.querySelector('.features');
    assert(featuresSection, 'Features section should exist');

    const featureCards = featuresSection.querySelectorAll('.feature-card');

    let performanceCard = null;
    featureCards.forEach(card => {
        const title = card.querySelector('h3');
        if (title && title.textContent.includes('Performance')) {
            performanceCard = card;
        }
    });

    assert(performanceCard, 'Feature card with title containing "Performance" should exist');

    const description = performanceCard.querySelector('p');
    assert(description, 'High Performance card should have a description');

    const descText = description.textContent.toLowerCase();
    const hasLSMReference = descText.includes('lsm tree') ||
                            descText.includes('lsm') ||
                            descText.includes('compaction');
    assert(hasLSMReference, 'Description should mention LSM tree architecture or compaction');
});

// Test Case 5: Verify three-column layout on desktop (CSS check)
// Type: e2e (simulated via CSS analysis)
// Expected: Feature cards are arranged in a horizontal three-column layout on desktop viewport
test('TC5: Three-column layout - CSS defines grid with columns for desktop', () => {
    // Verify the feature-grid class uses CSS grid layout
    const hasGridDisplay = css.includes('display: grid') || css.includes('display:grid');
    assert(hasGridDisplay, 'CSS should define grid display for layout');

    // Check for grid-template-columns definition on feature-grid
    const hasGridColumns = css.includes('grid-template-columns');
    assert(hasGridColumns, 'CSS should define grid-template-columns');

    // The CSS uses auto-fit with minmax(300px, 1fr)
    // On 1200px container (max-width), this will fit 3 columns (1200/300 = 4 columns possible, but auto-fit shrinks to content)
    // Actually with 1200px and 300px min, we get floor(1200/300) = 4 but gap reduces it
    // With repeat(auto-fit, minmax(300px, 1fr)), at 1200px - 60px gap (2 gaps * 30px) = 1140px / 3 = 380px per column
    // This should produce 3 columns at desktop widths

    const hasAutoFitMinmax = css.includes('auto-fit') && css.includes('minmax(300px');
    assert(hasAutoFitMinmax, 'CSS should use auto-fit with minmax for responsive columns');

    // Verify feature-grid exists in HTML
    const featureGrid = document.querySelector('.feature-grid');
    assert(featureGrid, 'Feature grid container should exist in HTML');

    // Verify it contains exactly 3 feature cards (which will display in 3 columns on desktop)
    const featureCards = featureGrid.querySelectorAll('.feature-card');
    assert(featureCards.length === 3, 'Feature grid should contain exactly 3 cards for three-column layout');
});

// Summary
console.log(`\n=== Test Summary ===`);
console.log(`Passed: ${passed}`);
console.log(`Failed: ${failed}`);
console.log(`Total: ${passed + failed}`);

// Export results for potential integration
module.exports = { results, passed, failed };

// Exit with appropriate code
process.exit(failed > 0 ? 1 : 0);
