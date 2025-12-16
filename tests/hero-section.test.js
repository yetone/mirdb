/**
 * E2E Tests for Hero Section Value Proposition
 * Tests the MirDB homepage hero section for branding, tagline, and CTA elements
 */
const { JSDOM } = require('jsdom');
const fs = require('fs');
const path = require('path');
const assert = require('assert');

const INDEX_PATH = path.join(__dirname, '..', 'index.html');

/**
 * Load the index.html file and return a JSDOM instance
 */
function loadPage() {
  const html = fs.readFileSync(INDEX_PATH, 'utf-8');
  return new JSDOM(html);
}

/**
 * Test runner to manage test execution
 */
const tests = [];
let passed = 0;
let failed = 0;

function test(name, fn) {
  tests.push({ name, fn });
}

async function runTests() {
  console.log('\nRunning Hero Section E2E Tests\n');
  console.log('='.repeat(60));

  for (const t of tests) {
    try {
      await t.fn();
      passed++;
      console.log(`✓ ${t.name}`);
    } catch (error) {
      failed++;
      console.log(`✗ ${t.name}`);
      console.log(`  Error: ${error.message}`);
    }
  }

  console.log('='.repeat(60));
  console.log(`\nResults: ${passed} passed, ${failed} failed, ${tests.length} total\n`);

  if (failed > 0) {
    process.exit(1);
  }
}

/**
 * Test Case 1: Hero section contains h1 with MirDB name and tagline mentioning 'key-value store' or 'Memcached'
 */
test('hero section contains MirDB branding and value proposition tagline', () => {
  const dom = loadPage();
  const document = dom.window.document;

  // Verify hero section exists
  const heroSection = document.querySelector('[data-testid="hero-section"], .hero, #hero, section.hero');
  assert(heroSection, 'Hero section should exist');

  // Verify h1 contains MirDB name
  const h1 = heroSection.querySelector('h1');
  assert(h1, 'Hero section should contain an h1 element');
  assert(h1.textContent.includes('MirDB'), 'h1 should contain MirDB name');

  // Verify tagline mentions 'key-value store' or 'Memcached'
  const heroText = heroSection.textContent.toLowerCase();
  const hasKeyValueStore = heroText.includes('key-value store');
  const hasMemcached = heroText.includes('memcached');
  assert(hasKeyValueStore || hasMemcached,
    'Hero section should mention "key-value store" or "Memcached"');
});

/**
 * Test Case 2: Primary CTA button is present with valid href
 */
test('primary CTA button (Get Started/GitHub) is present with valid href', () => {
  const dom = loadPage();
  const document = dom.window.document;

  const heroSection = document.querySelector('[data-testid="hero-section"], .hero, #hero, section.hero');
  assert(heroSection, 'Hero section should exist');

  // Look for primary CTA button
  const primaryCta = heroSection.querySelector('a.btn-primary, a.cta-primary, a[data-testid="primary-cta"]');
  assert(primaryCta, 'Primary CTA button should exist in hero section');

  // Verify button text
  const buttonText = primaryCta.textContent.toLowerCase();
  const hasGetStarted = buttonText.includes('get started');
  const hasGitHub = buttonText.includes('github');
  assert(hasGetStarted || hasGitHub,
    'Primary CTA should contain "Get Started" or "GitHub" text');

  // Verify href is present and valid
  const href = primaryCta.getAttribute('href');
  assert(href, 'Primary CTA should have an href attribute');
  assert(href.length > 0, 'Primary CTA href should not be empty');
});

/**
 * Test Case 3: Secondary CTA button (Documentation) is present with valid href
 */
test('secondary CTA button (Documentation) is present with valid href', () => {
  const dom = loadPage();
  const document = dom.window.document;

  const heroSection = document.querySelector('[data-testid="hero-section"], .hero, #hero, section.hero');
  assert(heroSection, 'Hero section should exist');

  // Look for secondary CTA button
  const secondaryCta = heroSection.querySelector('a.btn-secondary, a.cta-secondary, a[data-testid="secondary-cta"]');
  assert(secondaryCta, 'Secondary CTA button should exist in hero section');

  // Verify button text contains documentation-related text
  const buttonText = secondaryCta.textContent.toLowerCase();
  const hasDocumentation = buttonText.includes('documentation') ||
                           buttonText.includes('docs') ||
                           buttonText.includes('read');
  assert(hasDocumentation,
    'Secondary CTA should contain documentation-related text');

  // Verify href is present and valid
  const href = secondaryCta.getAttribute('href');
  assert(href, 'Secondary CTA should have an href attribute');
  assert(href.length > 0, 'Secondary CTA href should not be empty');
});

/**
 * Test Case 4: Hero section value proposition text mentions Memcached compatibility, persistence, and/or Rust performance
 */
test('hero section contains value proposition text about features', () => {
  const dom = loadPage();
  const document = dom.window.document;

  const heroSection = document.querySelector('[data-testid="hero-section"], .hero, #hero, section.hero');
  assert(heroSection, 'Hero section should exist');

  // Get all text content from hero section
  const heroText = heroSection.textContent.toLowerCase();

  // Check for value proposition keywords
  const hasMemcachedCompatibility = heroText.includes('memcached');
  const hasPersistence = heroText.includes('persistent') || heroText.includes('persistence');
  const hasRustPerformance = heroText.includes('rust') || heroText.includes('performance') || heroText.includes('high-performance');

  // At least one of these value propositions should be present
  assert(hasMemcachedCompatibility || hasPersistence || hasRustPerformance,
    'Hero section should mention Memcached compatibility, persistence, or Rust/performance');
});

// Run all tests
runTests();
