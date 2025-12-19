// @ts-check
const { JSDOM } = require('jsdom');
const fs = require('fs');
const path = require('path');
const assert = require('assert');

// Load the HTML file
const htmlPath = path.resolve(__dirname, '../dist/index.html');
const htmlContent = fs.readFileSync(htmlPath, 'utf8');

// Create a DOM from the HTML
const dom = new JSDOM(htmlContent);
const document = dom.window.document;

// Test results collector
const results = {
  passed: 0,
  failed: 0,
  tests: []
};

function test(name, fn) {
  try {
    fn();
    results.passed++;
    results.tests.push({ name, status: 'pass' });
    console.log(`✓ ${name}`);
  } catch (error) {
    results.failed++;
    results.tests.push({ name, status: 'fail', error: error.message, stack: error.stack });
    console.log(`✗ ${name}`);
    console.log(`  Error: ${error.message}`);
  }
}

console.log('Hero Section Display Tests\n');
console.log('='.repeat(50));

// Test Case 1: Product name MirDB is visible in hero section
test('Test Case 1: Product name MirDB is visible in hero section', () => {
  const heroSection = document.querySelector('[data-testid="hero-section"]');
  assert(heroSection, 'Hero section should exist');

  const productName = document.querySelector('[data-testid="product-name"]');
  assert(productName, 'Product name element should exist');
  assert.strictEqual(productName.textContent.trim(), 'MirDB', 'Product name should be "MirDB"');
});

// Test Case 2: Tagline is displayed correctly
test('Test Case 2: Tagline is displayed correctly', () => {
  const tagline = document.querySelector('[data-testid="tagline"]');
  assert(tagline, 'Tagline element should exist');
  assert.strictEqual(
    tagline.textContent.trim(),
    'Persistent Key-Value Store with Memcached Protocol',
    'Tagline should match expected text'
  );
});

// Test Case 3: Get Started button is visible and styled correctly
test('Test Case 3: Get Started button is visible and styled correctly', () => {
  const getStartedBtn = document.querySelector('[data-testid="cta-get-started"]');
  assert(getStartedBtn, 'Get Started button should exist');
  assert(getStartedBtn.textContent.includes('Get Started'), 'Button should contain "Get Started" text');
  assert(getStartedBtn.classList.contains('btn-primary'), 'Button should have btn-primary class');
  assert.strictEqual(getStartedBtn.getAttribute('href'), '#quickstart', 'Button should link to #quickstart');
});

// Test Case 4: View on GitHub button is visible and links correctly
test('Test Case 4: View on GitHub button is visible and links correctly', () => {
  const githubBtn = document.querySelector('[data-testid="cta-github"]');
  assert(githubBtn, 'GitHub button should exist');
  assert(githubBtn.textContent.includes('View on GitHub'), 'Button should contain "View on GitHub" text');
  assert(githubBtn.classList.contains('btn-secondary'), 'Button should have btn-secondary class');

  const href = githubBtn.getAttribute('href');
  assert(href && href.includes('github.com'), 'Button should link to GitHub');
  assert.strictEqual(githubBtn.getAttribute('target'), '_blank', 'Button should open in new tab');
  assert(githubBtn.getAttribute('rel').includes('noopener'), 'Button should have rel="noopener" for security');
});

// Test Case 5: Value proposition description is visible
test('Test Case 5: Value proposition description is visible', () => {
  const valueProposition = document.querySelector('[data-testid="value-proposition"]');
  assert(valueProposition, 'Value proposition element should exist');

  const text = valueProposition.textContent;
  assert(text.length > 100, 'Value proposition should have substantial content (>100 chars)');

  // Check for key value proposition elements
  assert(text.includes('MirDB'), 'Description should mention MirDB');
  assert(text.includes('Memcached'), 'Description should mention Memcached');
  assert(text.includes('Rust'), 'Description should mention Rust');
  assert(text.includes('LSM tree'), 'Description should mention LSM tree architecture');
  assert(text.toLowerCase().includes('persist'), 'Description should mention persistence');
});

console.log('\n' + '='.repeat(50));
console.log(`\nResults: ${results.passed} passed, ${results.failed} failed`);

// Exit with error code if any tests failed
if (results.failed > 0) {
  process.exit(1);
}
