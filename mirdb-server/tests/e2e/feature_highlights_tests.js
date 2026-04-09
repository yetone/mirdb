/**
 * Feature Highlights Section Tests
 *
 * Owner: Scenario 13 - Feature Highlights Section
 *
 * Tests:
 * - Features section exists and contains SSTable, Skip-List, Compaction features
 * - Each feature has an icon/visual element
 * - Feature descriptions are accurate and complete
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
        console.log(`  \u2713 ${name}`);
    } catch (error) {
        failed++;
        results.push({ name, status: 'fail', error: error.message });
        console.log(`  \u2717 ${name}`);
        console.log(`    Error: ${error.message}`);
    }
}

console.log('\n' + '='.repeat(60));
console.log('Scenario 13: Feature Highlights Section Tests');
console.log('='.repeat(60));

// =============================================
// Test Case 1: Features section contains SSTable feature description
// =============================================
console.log('\nTest Case 1: SSTable Feature');

test('features section exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'features'), 'Should have features section with id="features"');
});

test('features section has title', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'features-title'), 'Should have features title');
});

test('SSTable feature card exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'feature-sstable'), 'Should have SSTable feature card with id="feature-sstable"');
});

test('SSTable feature title is present', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'SSTable'), 'Should mention SSTable in feature title');
});

test('SSTable feature mentions storage', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'Storage') || containsText(html, 'storage'),
           'SSTable feature should mention storage');
});

test('SSTable feature describes persistence', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'persistent') || containsText(html, 'durable') || containsText(html, 'disk'),
           'SSTable feature should describe persistence/durability');
});

test('SSTable feature mentions Sorted String Tables', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'Sorted String Table') || containsText(html, 'SSTable'),
           'Should mention Sorted String Tables');
});

// =============================================
// Test Case 2: Features section contains Skip-List Memtable feature description
// =============================================
console.log('\nTest Case 2: Skip-List Memtable Feature');

test('Skip-List feature card exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'feature-skiplist'), 'Should have Skip-List feature card with id="feature-skiplist"');
});

test('Skip-List feature title is present', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'Skip-List') || containsText(html, 'skip-list') || containsText(html, 'Skiplist'),
           'Should mention Skip-List in feature title');
});

test('Skip-List feature mentions Memtable', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'Memtable') || containsText(html, 'memtable'),
           'Skip-List feature should mention Memtable');
});

test('Skip-List feature describes in-memory', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'in-memory') || containsText(html, 'memory'),
           'Skip-List feature should describe in-memory data structure');
});

test('Skip-List feature mentions performance', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'performance') || containsText(html, 'O(log n)') || containsText(html, 'efficient'),
           'Skip-List feature should mention performance characteristics');
});

test('Skip-List feature describes data structure', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'data structure') || containsText(html, 'skip-list'),
           'Skip-List feature should describe the data structure');
});

// =============================================
// Test Case 3: Features section contains Compaction feature description
// =============================================
console.log('\nTest Case 3: Compaction Feature');

test('Compaction feature card exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'feature-compaction'), 'Should have Compaction feature card with id="feature-compaction"');
});

test('Compaction feature title is present', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'Compaction') || containsText(html, 'compaction'),
           'Should mention Compaction in feature title');
});

test('Compaction feature mentions minor compaction', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'minor') || containsText(html, 'Minor'),
           'Compaction feature should mention minor compaction');
});

test('Compaction feature mentions major compaction', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'major') || containsText(html, 'Major'),
           'Compaction feature should mention major compaction');
});

test('Compaction feature describes automatic behavior', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'Auto') || containsText(html, 'automatic') || containsText(html, 'Automatic'),
           'Compaction feature should describe automatic compaction');
});

test('Compaction feature mentions background or levels', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'background') || containsText(html, 'Level') || containsText(html, 'level') || containsText(html, 'merging'),
           'Compaction feature should mention background operation or levels');
});

// =============================================
// Test Case 4: Each feature has accompanying visual element
// =============================================
console.log('\nTest Case 4: Visual Elements (Icons)');

test('features grid exists', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'features-grid'), 'Should have features-grid class');
});

test('feature cards have feature-icon class', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'feature-icon'), 'Feature cards should have feature-icon elements');
});

test('SSTable feature has visual icon', () => {
    const html = loadHomepageHtml();
    // Check that the SSTable feature card section contains an SVG icon
    const sstableSection = html.substring(
        html.indexOf('id="feature-sstable"'),
        html.indexOf('id="feature-skiplist"')
    );
    assert(sstableSection.includes('<svg') || sstableSection.includes('feature-icon'),
           'SSTable feature should have a visual icon (SVG or icon class)');
});

test('Skip-List feature has visual icon', () => {
    const html = loadHomepageHtml();
    // Check that the Skip-List feature card section contains an SVG icon
    const skiplistSection = html.substring(
        html.indexOf('id="feature-skiplist"'),
        html.indexOf('id="feature-compaction"')
    );
    assert(skiplistSection.includes('<svg') || skiplistSection.includes('feature-icon'),
           'Skip-List feature should have a visual icon (SVG or icon class)');
});

test('Compaction feature has visual icon', () => {
    const html = loadHomepageHtml();
    // Check that the Compaction feature card section contains an SVG icon
    const compactionSection = html.substring(
        html.indexOf('id="feature-compaction"'),
        html.indexOf('</div>', html.indexOf('id="feature-compaction"') + 200)
    );
    assert(compactionSection.includes('<svg') || compactionSection.includes('feature-icon'),
           'Compaction feature should have a visual icon (SVG or icon class)');
});

test('SVG icons are present in feature cards', () => {
    const html = loadHomepageHtml();
    const featuresSection = html.substring(
        html.indexOf('id="features"'),
        html.indexOf('id="quickstart"')
    );
    // Count SVG elements in features section
    const svgCount = (featuresSection.match(/<svg/g) || []).length;
    assert(svgCount >= 3, `Should have at least 3 SVG icons in features section (found ${svgCount})`);
});

test('feature icons have aria-hidden for accessibility', () => {
    const html = loadHomepageHtml();
    const featuresSection = html.substring(
        html.indexOf('id="features"'),
        html.indexOf('id="quickstart"')
    );
    assert(featuresSection.includes('aria-hidden="true"'),
           'Feature icons should have aria-hidden="true" for accessibility');
});

// =============================================
// CSS Tests for Feature Section
// =============================================
console.log('\nCSS Tests for Feature Section');

test('CSS has feature-icon styles', () => {
    const css = loadMainCss();
    assert(css.includes('.feature-icon'), 'CSS should have .feature-icon styles');
});

test('CSS defines feature-icon dimensions', () => {
    const css = loadMainCss();
    assert(css.includes('.feature-icon') && (css.includes('width') || css.includes('height')),
           'CSS should define dimensions for feature-icon');
});

test('CSS has feature card hover styles', () => {
    const css = loadMainCss();
    assert(css.includes('.feature-card:hover'), 'CSS should have .feature-card:hover styles');
});

test('CSS has feature-title styles', () => {
    const css = loadMainCss();
    assert(css.includes('.feature-title') || css.includes('.feature-card h3'),
           'CSS should have feature title styles');
});

test('CSS has feature-description styles', () => {
    const css = loadMainCss();
    assert(css.includes('.feature-description') || css.includes('.feature-card p'),
           'CSS should have feature description styles');
});

test('CSS feature-icon has background color', () => {
    const css = loadMainCss();
    const iconSection = css.substring(
        css.indexOf('.feature-icon'),
        css.indexOf('}', css.indexOf('.feature-icon')) + 1
    );
    assert(iconSection.includes('background') || iconSection.includes('color'),
           'Feature icon should have background color defined');
});

// =============================================
// Structure and Accessibility Tests
// =============================================
console.log('\nStructure and Accessibility Tests');

test('features section has region role', () => {
    const html = loadHomepageHtml();
    const featuresSection = html.substring(
        html.indexOf('id="features"'),
        html.indexOf('id="quickstart"')
    );
    assert(featuresSection.includes('role="region"'),
           'Features section should have role="region"');
});

test('features section has aria-labelledby', () => {
    const html = loadHomepageHtml();
    const featuresSection = html.substring(
        html.indexOf('id="features"'),
        html.indexOf('id="quickstart"')
    );
    assert(featuresSection.includes('aria-labelledby="features-title"'),
           'Features section should have aria-labelledby pointing to title');
});

test('features grid has list role', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'features-grid') && containsText(html, 'role="list"'),
           'Features grid should have role="list"');
});

test('feature cards have listitem role', () => {
    const html = loadHomepageHtml();
    const featuresSection = html.substring(
        html.indexOf('id="features"'),
        html.indexOf('id="quickstart"')
    );
    const listItemCount = (featuresSection.match(/role="listitem"/g) || []).length;
    assert(listItemCount >= 3, `Should have at least 3 feature cards with role="listitem" (found ${listItemCount})`);
});

test('all three feature cards have unique IDs', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'feature-sstable'), 'SSTable feature should have unique ID');
    assert(hasElementWithId(html, 'feature-skiplist'), 'Skip-List feature should have unique ID');
    assert(hasElementWithId(html, 'feature-compaction'), 'Compaction feature should have unique ID');
});

// =============================================
// Summary
// =============================================
console.log('\n' + '='.repeat(60));
console.log(`Test Results: ${passed} passed, ${failed} failed`);
console.log('='.repeat(60));

// Exit with appropriate code
if (failed > 0) {
    console.log('\nFailed tests:');
    results.filter(r => r.status === 'fail').forEach(r => {
        console.log(`  - ${r.name}: ${r.error}`);
    });
    process.exit(1);
} else {
    console.log('\nAll Scenario 13 tests passed!');
    process.exit(0);
}
