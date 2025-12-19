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

console.log('Key Features Section Tests\n');
console.log('='.repeat(50));

// Test Case 1: Check Memcached Compatible feature
test('Test Case 1: Check Memcached Compatible feature', () => {
  const featuresSection = document.querySelector('[data-testid="features-section"]');
  assert(featuresSection, 'Features section should exist');

  const featureCard = document.querySelector('[data-testid="feature-memcached"]');
  assert(featureCard, 'Memcached Compatible feature card should exist');

  const title = document.querySelector('[data-testid="feature-memcached-title"]');
  assert(title, 'Memcached Compatible title should exist');
  assert.strictEqual(title.textContent.trim(), 'Memcached Compatible', 'Title should be "Memcached Compatible"');

  const description = document.querySelector('[data-testid="feature-memcached-description"]');
  assert(description, 'Memcached Compatible description should exist');
  assert(description.textContent.toLowerCase().includes('drop-in replacement'), 'Description should mention drop-in replacement');
});

// Test Case 2: Check Persistent Storage feature
test('Test Case 2: Check Persistent Storage feature', () => {
  const featureCard = document.querySelector('[data-testid="feature-persistent"]');
  assert(featureCard, 'Persistent Storage feature card should exist');

  const title = document.querySelector('[data-testid="feature-persistent-title"]');
  assert(title, 'Persistent Storage title should exist');
  assert.strictEqual(title.textContent.trim(), 'Persistent Storage', 'Title should be "Persistent Storage"');

  const description = document.querySelector('[data-testid="feature-persistent-description"]');
  assert(description, 'Persistent Storage description should exist');
  assert(description.textContent.includes('SSTable'), 'Description should mention SSTable architecture');
});

// Test Case 3: Check High Performance feature
test('Test Case 3: Check High Performance feature', () => {
  const featureCard = document.querySelector('[data-testid="feature-performance"]');
  assert(featureCard, 'High Performance feature card should exist');

  const title = document.querySelector('[data-testid="feature-performance-title"]');
  assert(title, 'High Performance title should exist');
  assert.strictEqual(title.textContent.trim(), 'High Performance', 'Title should be "High Performance"');

  const description = document.querySelector('[data-testid="feature-performance-description"]');
  assert(description, 'High Performance description should exist');
  assert(description.textContent.includes('LSM tree'), 'Description should mention LSM tree optimization');
});

// Test Case 4: Check Simple Configuration feature
test('Test Case 4: Check Simple Configuration feature', () => {
  const featureCard = document.querySelector('[data-testid="feature-config"]');
  assert(featureCard, 'Simple Configuration feature card should exist');

  const title = document.querySelector('[data-testid="feature-config-title"]');
  assert(title, 'Simple Configuration title should exist');
  assert.strictEqual(title.textContent.trim(), 'Simple Configuration', 'Title should be "Simple Configuration"');

  const description = document.querySelector('[data-testid="feature-config-description"]');
  assert(description, 'Simple Configuration description should exist');
  assert(description.textContent.includes('TOML'), 'Description should mention TOML-based configuration');
});

// Test Case 5: Verify feature icons presence
test('Test Case 5: Verify feature icons presence', () => {
  const icons = [
    { testId: 'feature-memcached-icon', name: 'Memcached Compatible' },
    { testId: 'feature-persistent-icon', name: 'Persistent Storage' },
    { testId: 'feature-performance-icon', name: 'High Performance' },
    { testId: 'feature-config-icon', name: 'Simple Configuration' }
  ];

  icons.forEach(({ testId, name }) => {
    const icon = document.querySelector(`[data-testid="${testId}"]`);
    assert(icon, `${name} feature card should have an icon`);

    // Check that the icon container has proper styling (contains svg)
    const svg = icon.querySelector('svg');
    assert(svg, `${name} icon should contain an SVG element`);
  });
});

console.log('\n' + '='.repeat(50));
console.log(`\nResults: ${results.passed} passed, ${results.failed} failed`);

// Exit with error code if any tests failed
if (results.failed > 0) {
  process.exit(1);
}
