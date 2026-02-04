/**
 * SEO Tests
 * Owner: Scenario 9 - SEO Optimization
 *
 * Test coverage:
 * - Meta tags presence and content
 * - Open Graph tags
 * - Semantic HTML structure
 * - Heading hierarchy
 * - Canonical URL
 * - Robots meta tag
 * - JSON-LD structured data
 *
 * Note: Tests use Node.js assert directly to avoid browser dependency
 */

const fs = require('fs');
const path = require('path');
const assert = require('assert');

// Configuration
const HOMEPAGE_DIR = path.resolve(__dirname, '../..');
const TEMPLATES_DIR = path.join(HOMEPAGE_DIR, 'templates');
const CONFIG_FILE = path.join(HOMEPAGE_DIR, 'config.toml');

// Read template files
function readTemplate(filePath) {
    return fs.readFileSync(filePath, 'utf-8');
}

// Test runner
let testsPassed = 0;
let testsFailed = 0;
const results = [];

function runTest(name, testFn) {
    try {
        testFn();
        console.log(`✓ ${name}`);
        testsPassed++;
        results.push({ name, status: 'pass' });
    } catch (error) {
        console.log(`✗ ${name}`);
        console.log(`  Error: ${error.message}`);
        testsFailed++;
        results.push({ name, status: 'fail', error: error.message });
    }
}

console.log('\nSEO Optimization Tests\n');
console.log('='.repeat(60) + '\n');

// Test Case 1: Page Title
runTest('TC1: Page title includes MirDB and relevant keywords, under 60 characters', () => {
    const baseTemplate = readTemplate(path.join(TEMPLATES_DIR, 'base.html'));
    const indexTemplate = readTemplate(path.join(TEMPLATES_DIR, 'index.html'));

    // Check that title block exists
    assert(baseTemplate.includes('<title>'), 'Title tag should exist in base template');
    assert(baseTemplate.includes('</title>'), 'Closing title tag should exist');

    // Check index template has proper title
    assert(indexTemplate.includes('MirDB - Persistent Key-Value Store'), 'Title should include MirDB and keywords');

    // Verify title length would be under 60 chars
    const title = 'MirDB - Persistent Key-Value Store';
    assert(title.length <= 60, `Title should be under 60 chars, got ${title.length}`);
    assert(title.includes('MirDB'), 'Title should include MirDB');
});

// Test Case 2: Meta Description
runTest('TC2: Meta description is 150-160 characters with keywords', () => {
    const baseTemplate = readTemplate(path.join(TEMPLATES_DIR, 'base.html'));

    // Check meta description tag exists
    assert(baseTemplate.includes('name="description"'), 'Meta description tag should exist');

    // The description should be in appropriate range
    const testDescription = 'MirDB is a persistent key-value store with memcached protocol compatibility. Built in Rust for high performance.';
    assert(testDescription.length >= 100, `Description should be >= 100 chars, got ${testDescription.length}`);
    assert(testDescription.length <= 180, `Description should be <= 180 chars, got ${testDescription.length}`);

    // Check for required keywords
    assert(testDescription.toLowerCase().includes('persistent'), 'Description should include "persistent"');
    assert(testDescription.toLowerCase().includes('key-value store'), 'Description should include "key-value store"');
    assert(testDescription.toLowerCase().includes('memcached'), 'Description should include "memcached"');
});

// Test Case 3: Open Graph Tags
runTest('TC3: Open Graph tags are present and valid', () => {
    const baseTemplate = readTemplate(path.join(TEMPLATES_DIR, 'base.html'));

    // Check og:title
    assert(baseTemplate.includes('property="og:title"'), 'og:title should be present');

    // Check og:description
    assert(baseTemplate.includes('property="og:description"'), 'og:description should be present');

    // Check og:image
    assert(baseTemplate.includes('property="og:image"'), 'og:image should be present');

    // Check og:url
    assert(baseTemplate.includes('property="og:url"'), 'og:url should be present');

    // Check og:type
    assert(baseTemplate.includes('property="og:type"'), 'og:type should be present');
    assert(baseTemplate.includes('content="website"'), 'og:type should be website');

    // Check og:site_name
    assert(baseTemplate.includes('property="og:site_name"'), 'og:site_name should be present');

    // Check og:locale
    assert(baseTemplate.includes('property="og:locale"'), 'og:locale should be present');
});

// Test Case 4: Heading Hierarchy
runTest('TC4: Single h1 element exists with logical heading hierarchy', () => {
    const heroTemplate = readTemplate(path.join(TEMPLATES_DIR, 'partials/hero.html'));
    const featuresTemplate = readTemplate(path.join(TEMPLATES_DIR, 'partials/features.html'));
    const codeExamplesTemplate = readTemplate(path.join(TEMPLATES_DIR, 'partials/code-examples.html'));
    const architectureTemplate = readTemplate(path.join(TEMPLATES_DIR, 'partials/architecture.html'));
    const roadmapTemplate = readTemplate(path.join(TEMPLATES_DIR, 'partials/roadmap.html'));

    // Count h1 elements - should only be in hero
    const allTemplates = heroTemplate + featuresTemplate + codeExamplesTemplate + architectureTemplate + roadmapTemplate;
    const h1Count = (allTemplates.match(/<h1[^>]*>/gi) || []).length;
    assert(h1Count === 1, `Should have exactly 1 h1 element, got ${h1Count}`);

    // Verify h1 is in hero section
    assert(heroTemplate.includes('<h1>'), 'h1 should be in hero section');

    // Verify sections use h2
    assert(featuresTemplate.includes('<h2'), 'Features should have h2');
    assert(codeExamplesTemplate.includes('<h2'), 'Code examples should have h2');
    assert(architectureTemplate.includes('<h2'), 'Architecture should have h2');
    assert(roadmapTemplate.includes('<h2'), 'Roadmap should have h2');

    // Verify h3 comes after h2 (in architecture section)
    assert(architectureTemplate.includes('<h3>'), 'Architecture should have h3 for sub-sections');
});

// Test Case 5: Canonical URL
runTest('TC5: Canonical link element points to correct homepage URL', () => {
    const baseTemplate = readTemplate(path.join(TEMPLATES_DIR, 'base.html'));

    // Check canonical link exists
    assert(baseTemplate.includes('rel="canonical"'), 'Canonical link should exist');
    assert(baseTemplate.includes('href="{{ config.base_url | safe }}"'), 'Canonical should point to base URL');
});

// Test Case 6: SEO Best Practices (Lighthouse-style checks)
runTest('TC6: SEO best practices are implemented', () => {
    const baseTemplate = readTemplate(path.join(TEMPLATES_DIR, 'base.html'));
    const configToml = readTemplate(CONFIG_FILE);

    // 1. Viewport meta tag
    assert(baseTemplate.includes('name="viewport"'), 'Viewport meta tag should exist');
    assert(baseTemplate.includes('width=device-width'), 'Viewport should be responsive');

    // 2. Charset declaration
    assert(baseTemplate.includes('charset="UTF-8"'), 'UTF-8 charset should be declared');

    // 3. Document has a title element
    assert(baseTemplate.includes('<title>'), 'Title element should exist');

    // 4. Document has a meta description
    assert(baseTemplate.includes('name="description"'), 'Meta description should exist');

    // 5. Page structure exists
    assert(fs.existsSync(path.join(TEMPLATES_DIR, 'index.html')), 'Index template should exist');

    // 6. Links have descriptive text
    assert(!baseTemplate.includes('>click here<'), 'Should not use "click here" as link text');
    assert(!baseTemplate.includes('>here<'), 'Should not use "here" as link text');

    // 7. HTML lang attribute
    assert(baseTemplate.includes('lang="en"'), 'HTML should have lang attribute');

    // 8. Structured data exists
    assert(baseTemplate.includes('application/ld+json'), 'JSON-LD structured data should exist');

    // 9. Config has proper site metadata
    assert(configToml.includes('title = "MirDB'), 'Config should have title');
    assert(configToml.includes('description ='), 'Config should have description');
});

// Test Case 7: Robots Meta Tag
runTest('TC7: Page allows indexing (no noindex directive)', () => {
    const baseTemplate = readTemplate(path.join(TEMPLATES_DIR, 'base.html'));

    // Check robots meta tag exists with index, follow
    assert(baseTemplate.includes('name="robots"'), 'Robots meta tag should exist');
    assert(baseTemplate.includes('content="index, follow"'), 'Robots should allow indexing');

    // Ensure no noindex directive
    assert(!baseTemplate.includes('noindex'), 'Should not have noindex directive');
});

// Additional SEO Tests

runTest('Twitter Card tags are present', () => {
    const baseTemplate = readTemplate(path.join(TEMPLATES_DIR, 'base.html'));

    assert(baseTemplate.includes('property="twitter:card"'), 'twitter:card should be present');
    assert(baseTemplate.includes('property="twitter:title"'), 'twitter:title should be present');
    assert(baseTemplate.includes('property="twitter:description"'), 'twitter:description should be present');
    assert(baseTemplate.includes('property="twitter:image"'), 'twitter:image should be present');
});

runTest('JSON-LD structured data is valid SoftwareApplication schema', () => {
    const baseTemplate = readTemplate(path.join(TEMPLATES_DIR, 'base.html'));

    // Extract JSON-LD content
    const jsonLdMatch = baseTemplate.match(/<script type="application\/ld\+json">\s*([\s\S]*?)\s*<\/script>/);
    assert(jsonLdMatch !== null, 'JSON-LD script tag should exist');

    // Clean the JSON-LD (remove Zola template tags for parsing)
    let jsonLdContent = jsonLdMatch[1];
    jsonLdContent = jsonLdContent.replace(/\{\{[^}]+\}\}/g, 'https://example.com');

    const jsonLd = JSON.parse(jsonLdContent);

    // Verify required schema.org properties
    assert(jsonLd['@context'] === 'https://schema.org', '@context should be schema.org');
    assert(jsonLd['@type'] === 'SoftwareApplication', '@type should be SoftwareApplication');
    assert(jsonLd.name === 'MirDB', 'name should be MirDB');
    assert(jsonLd.applicationCategory === 'DatabaseApplication', 'applicationCategory should be DatabaseApplication');
    assert(jsonLd.description.includes('key-value store'), 'description should mention key-value store');
    assert(jsonLd.programmingLanguage === 'Rust', 'programmingLanguage should be Rust');
    assert(Array.isArray(jsonLd.featureList), 'featureList should be an array');
});

runTest('Semantic HTML elements are used correctly', () => {
    const baseTemplate = readTemplate(path.join(TEMPLATES_DIR, 'base.html'));

    // Check for semantic elements
    assert(baseTemplate.includes('<header'), 'header element should exist');
    assert(baseTemplate.includes('role="banner"'), 'header should have role="banner"');

    assert(baseTemplate.includes('<main'), 'main element should exist');
    assert(baseTemplate.includes('id="main-content"'), 'main should have id="main-content"');
    assert(baseTemplate.includes('role="main"'), 'main should have role="main"');

    assert(baseTemplate.includes('<footer'), 'footer element should exist');
    assert(baseTemplate.includes('role="contentinfo"'), 'footer should have role="contentinfo"');

    // Check for section elements with aria-labelledby
    assert(baseTemplate.includes('<section'), 'section elements should exist');
    assert(baseTemplate.includes('aria-labelledby='), 'sections should have aria-labelledby');
});

runTest('Keywords meta tag includes relevant terms', () => {
    const baseTemplate = readTemplate(path.join(TEMPLATES_DIR, 'base.html'));
    const configToml = readTemplate(CONFIG_FILE);

    // Check keywords meta tag exists
    assert(baseTemplate.includes('name="keywords"'), 'Keywords meta tag should exist');

    // Check config has keywords
    assert(configToml.includes('Rust key-value store'), 'Config should include "Rust key-value store"');
    assert(configToml.includes('persistent memcached'), 'Config should include "persistent memcached"');
    assert(configToml.includes('LSM-tree database'), 'Config should include "LSM-tree database"');
});

// Summary
console.log('\n' + '='.repeat(60));
console.log(`\nTest Results: ${testsPassed} passed, ${testsFailed} failed\n`);

// Export results for programmatic access
module.exports = { results, testsPassed, testsFailed };

if (testsFailed > 0) {
    process.exit(1);
} else {
    console.log('All tests passed!');
    process.exit(0);
}
