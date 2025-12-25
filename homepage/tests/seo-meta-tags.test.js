/**
 * SEO Meta Tags Tests for MirDB Homepage
 * Using JSDOM for DOM testing in Node.js environment
 *
 * Scenario: Verify proper meta tags are present for SEO
 */

import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import assert from 'assert';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Simple test framework
let passCount = 0;
let failCount = 0;
const errors = [];

function describe(name, fn) {
  console.log('\n' + name);
  fn();
}

function it(name, fn) {
  try {
    fn();
    console.log('  ✓ ' + name);
    passCount++;
  } catch (error) {
    console.log('  ✗ ' + name);
    console.log('    Error: ' + error.message);
    errors.push({ name, error: error.message });
    failCount++;
  }
}

// Load the built HTML (Astro builds to dist/)
const htmlPath = path.join(__dirname, '..', 'dist', 'index.html');
if (!fs.existsSync(htmlPath)) {
  console.error('Error: dist/index.html not found. Run "npm run build" first.');
  process.exit(1);
}

const htmlContent = fs.readFileSync(htmlPath, 'utf8');
const dom = new JSDOM(htmlContent);
const document = dom.window.document;

// Run tests
describe('SEO Meta Tags - Title Tag', function() {
  /**
   * Test Case 1: Check for title tag
   * Expected: Page has title tag containing 'MirDB' and descriptive text (50-60 characters)
   */
  it('should have a title tag containing "MirDB" and descriptive text (50-60 characters)', function() {
    const titleElement = document.querySelector('title');
    assert.ok(titleElement, 'Title element should exist');

    const titleText = titleElement.textContent;
    assert.ok(titleText, 'Title should not be empty');
    assert.ok(titleText.includes('MirDB'), 'Title should contain "MirDB"');

    // Check title length (50-60 characters is optimal for SEO)
    const titleLength = titleText.length;
    assert.ok(titleLength >= 50 && titleLength <= 60,
      `Title length should be 50-60 characters, got ${titleLength} characters: "${titleText}"`);
  });
});

describe('SEO Meta Tags - Meta Description', function() {
  /**
   * Test Case 2: Check for meta description
   * Expected: Page has meta description tag with 150-160 characters describing MirDB
   */
  it('should have a meta description tag with 150-160 characters describing MirDB', function() {
    const metaDescription = document.querySelector('meta[name="description"]');
    assert.ok(metaDescription, 'Meta description element should exist');

    const descriptionContent = metaDescription.getAttribute('content');
    assert.ok(descriptionContent, 'Meta description content should not be empty');
    assert.ok(descriptionContent.includes('MirDB') || descriptionContent.toLowerCase().includes('mirdb'),
      'Meta description should mention MirDB');

    // Check description length (150-160 characters is optimal for SEO)
    const descLength = descriptionContent.length;
    assert.ok(descLength >= 150 && descLength <= 160,
      `Meta description length should be 150-160 characters, got ${descLength} characters: "${descriptionContent}"`);
  });
});

describe('SEO Meta Tags - Open Graph Tags', function() {
  /**
   * Test Case 3: Check for Open Graph tags
   * Expected: Page has og:title, og:description, og:type, and og:image meta tags
   */
  it('should have og:title meta tag', function() {
    const ogTitle = document.querySelector('meta[property="og:title"]');
    assert.ok(ogTitle, 'og:title meta tag should exist');

    const content = ogTitle.getAttribute('content');
    assert.ok(content, 'og:title content should not be empty');
    assert.ok(content.includes('MirDB'), 'og:title should contain "MirDB"');
  });

  it('should have og:description meta tag', function() {
    const ogDescription = document.querySelector('meta[property="og:description"]');
    assert.ok(ogDescription, 'og:description meta tag should exist');

    const content = ogDescription.getAttribute('content');
    assert.ok(content, 'og:description content should not be empty');
  });

  it('should have og:type meta tag', function() {
    const ogType = document.querySelector('meta[property="og:type"]');
    assert.ok(ogType, 'og:type meta tag should exist');

    const content = ogType.getAttribute('content');
    assert.ok(content, 'og:type content should not be empty');
    assert.ok(content === 'website', `og:type should be "website", got "${content}"`);
  });

  it('should have og:image meta tag', function() {
    const ogImage = document.querySelector('meta[property="og:image"]');
    assert.ok(ogImage, 'og:image meta tag should exist');

    const content = ogImage.getAttribute('content');
    assert.ok(content, 'og:image content should not be empty');
    // Verify it's a valid URL or path
    assert.ok(content.startsWith('http') || content.startsWith('/'),
      'og:image should be a valid URL or absolute path');
  });
});

describe('SEO Meta Tags - Twitter Card Tags', function() {
  /**
   * Test Case 4: Check for Twitter card meta tags
   * Expected: Page has twitter:card, twitter:title, and twitter:description meta tags
   */
  it('should have twitter:card meta tag', function() {
    const twitterCard = document.querySelector('meta[name="twitter:card"]');
    assert.ok(twitterCard, 'twitter:card meta tag should exist');

    const content = twitterCard.getAttribute('content');
    assert.ok(content, 'twitter:card content should not be empty');
    assert.ok(['summary', 'summary_large_image', 'app', 'player'].includes(content),
      `twitter:card should have a valid value, got "${content}"`);
  });

  it('should have twitter:title meta tag', function() {
    const twitterTitle = document.querySelector('meta[name="twitter:title"]');
    assert.ok(twitterTitle, 'twitter:title meta tag should exist');

    const content = twitterTitle.getAttribute('content');
    assert.ok(content, 'twitter:title content should not be empty');
    assert.ok(content.includes('MirDB'), 'twitter:title should contain "MirDB"');
  });

  it('should have twitter:description meta tag', function() {
    const twitterDescription = document.querySelector('meta[name="twitter:description"]');
    assert.ok(twitterDescription, 'twitter:description meta tag should exist');

    const content = twitterDescription.getAttribute('content');
    assert.ok(content, 'twitter:description content should not be empty');
  });
});

describe('SEO Meta Tags - Canonical URL', function() {
  /**
   * Test Case 5: Check for canonical URL
   * Expected: Page has canonical link tag pointing to the definitive URL
   */
  it('should have a canonical link tag pointing to the definitive URL', function() {
    const canonicalLink = document.querySelector('link[rel="canonical"]');
    assert.ok(canonicalLink, 'Canonical link element should exist');

    const href = canonicalLink.getAttribute('href');
    assert.ok(href, 'Canonical href should not be empty');
    assert.ok(href.startsWith('http'),
      `Canonical URL should be an absolute URL starting with http, got "${href}"`);
  });
});

// Summary
console.log('\n----------------------------------------');
console.log(`Test Results: ${passCount} passed, ${failCount} failed`);

if (failCount > 0) {
  console.log('\nFailed tests:');
  errors.forEach(({ name, error }) => {
    console.log(`  - ${name}: ${error}`);
  });
  process.exit(1);
} else {
  console.log('\nAll tests passed!');
  process.exit(0);
}
