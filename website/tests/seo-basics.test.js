/**
 * SEO Basics E2E Tests
 *
 * These tests verify that basic SEO elements are in place for discoverability.
 * They analyze the built static HTML output to verify meta tags.
 */

import * as fs from 'fs';
import * as path from 'path';
import { fileURLToPath } from 'url';
import assert from 'assert';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const distPath = path.join(__dirname, '..', 'dist', 'index.html');

let htmlContent;
let testResults = [];

// Helper to run a test
function runTest(name, testFn) {
  try {
    testFn();
    testResults.push({ name, status: 'pass' });
    console.log(`✓ ${name}`);
  } catch (error) {
    testResults.push({ name, status: 'fail', error: error.message, stack: error.stack });
    console.log(`✗ ${name}`);
    console.log(`  Error: ${error.message}`);
  }
}

// Load HTML content
try {
  htmlContent = fs.readFileSync(distPath, 'utf-8');
} catch (error) {
  console.error('Failed to read dist/index.html. Run "npm run build" first.');
  process.exit(1);
}

console.log('\nSEO Basics Tests\n');

// Test Case 1: Page title contains MirDB and relevant keywords
runTest('page title contains MirDB and relevant keywords', () => {
  const titleMatch = htmlContent.match(/<title>([^<]+)<\/title>/i);
  assert(titleMatch, 'Title tag should exist');

  const title = titleMatch[1];
  const titleLower = title.toLowerCase();

  // Title should contain 'MirDB'
  assert(titleLower.includes('mirdb'), `Title should contain 'MirDB'. Got: "${title}"`);

  // Title should contain relevant keywords about the product
  const hasRelevantKeywords =
    titleLower.includes('key-value') ||
    titleLower.includes('memcached') ||
    titleLower.includes('persistent') ||
    titleLower.includes('store');

  assert(hasRelevantKeywords, `Title should contain relevant keywords (key-value, memcached, persistent, or store). Got: "${title}"`);
});

// Test Case 2: Meta description is present and describes MirDB
runTest('meta description is present and describes MirDB', () => {
  const metaMatch = htmlContent.match(/<meta\s+name="description"\s+content="([^"]+)"/i);
  assert(metaMatch, 'Meta description tag should exist');

  const metaDescription = metaMatch[1];
  const descLower = metaDescription.toLowerCase();

  // Meta description should mention MirDB or describe the product
  const describesMirDB =
    descLower.includes('mirdb') ||
    descLower.includes('key-value') ||
    descLower.includes('memcached') ||
    descLower.includes('persistent');

  assert(describesMirDB, `Meta description should describe MirDB. Got: "${metaDescription}"`);

  // Meta description should be a reasonable length (50-160 characters is optimal for SEO)
  assert(metaDescription.length > 30, `Meta description should be longer than 30 characters. Got: ${metaDescription.length}`);
  assert(metaDescription.length < 200, `Meta description should be shorter than 200 characters. Got: ${metaDescription.length}`);
});

// Test Case 3: Open Graph meta tags are present
runTest('Open Graph meta tags are present (og:title, og:description, og:type)', () => {
  // Check og:title
  const ogTitleMatch = htmlContent.match(/<meta\s+property="og:title"\s+content="([^"]+)"/i);
  assert(ogTitleMatch, 'og:title meta tag should exist');
  assert(ogTitleMatch[1].length > 0, 'og:title should have content');

  // Check og:description
  const ogDescMatch = htmlContent.match(/<meta\s+property="og:description"\s+content="([^"]+)"/i);
  assert(ogDescMatch, 'og:description meta tag should exist');
  assert(ogDescMatch[1].length > 0, 'og:description should have content');

  // Check og:type
  const ogTypeMatch = htmlContent.match(/<meta\s+property="og:type"\s+content="([^"]+)"/i);
  assert(ogTypeMatch, 'og:type meta tag should exist');
  assert(ogTypeMatch[1].length > 0, 'og:type should have content');
});

// Summary
const passed = testResults.filter(t => t.status === 'pass').length;
const failed = testResults.filter(t => t.status === 'fail').length;

console.log(`\nResults: ${passed} passed, ${failed} failed\n`);

if (failed > 0) {
  process.exit(1);
}
