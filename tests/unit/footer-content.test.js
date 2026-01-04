/**
 * Unit tests for Footer Content scenario
 * Scenario: Verify footer contains required links and information
 *
 * Test Case 1: Check footer presence - Footer element exists with semantic <footer> tag
 */
const { JSDOM } = require('jsdom');
const fs = require('fs');
const path = require('path');

const indexPath = path.join(__dirname, '../../index.html');

function runTests() {
  console.log('Running Footer Content Unit Tests...\n');

  let passed = 0;
  let failed = 0;
  const results = [];

  // Read the HTML file
  let html;
  try {
    html = fs.readFileSync(indexPath, 'utf-8');
  } catch (error) {
    console.error('Failed to read index.html:', error.message);
    process.exit(1);
  }

  // Parse HTML with JSDOM
  const dom = new JSDOM(html);
  const document = dom.window.document;

  // Test 1: Footer element exists with semantic <footer> tag
  function testFooterExists() {
    const footer = document.querySelector('footer');
    if (footer && footer.tagName.toLowerCase() === 'footer') {
      console.log('✓ Footer element exists with semantic <footer> tag');
      passed++;
      results.push({ test: 'footer_exists', status: 'pass' });
    } else {
      console.log('✗ Footer element with semantic <footer> tag not found');
      failed++;
      results.push({ test: 'footer_exists', status: 'fail', error: 'No <footer> element found' });
    }
  }

  // Test 2: Footer is a direct child of body (top-level landmark)
  function testFooterIsTopLevel() {
    const footer = document.querySelector('body > footer');
    if (footer) {
      console.log('✓ Footer is a direct child of body (top-level landmark)');
      passed++;
      results.push({ test: 'footer_top_level', status: 'pass' });
    } else {
      console.log('✗ Footer should be a direct child of body');
      failed++;
      results.push({ test: 'footer_top_level', status: 'fail', error: 'Footer is not a direct child of body' });
    }
  }

  // Test 3: Footer contains content
  function testFooterHasContent() {
    const footer = document.querySelector('footer');
    if (footer && footer.textContent.trim().length > 0) {
      console.log('✓ Footer contains content');
      passed++;
      results.push({ test: 'footer_has_content', status: 'pass' });
    } else {
      console.log('✗ Footer is empty or missing');
      failed++;
      results.push({ test: 'footer_has_content', status: 'fail', error: 'Footer has no content' });
    }
  }

  // Test 4: Footer has footer-content wrapper for styling
  function testFooterContentWrapper() {
    const footerContent = document.querySelector('footer .footer-content');
    if (footerContent) {
      console.log('✓ Footer has footer-content wrapper for styling');
      passed++;
      results.push({ test: 'footer_content_wrapper', status: 'pass' });
    } else {
      console.log('✗ Footer should have a .footer-content wrapper');
      failed++;
      results.push({ test: 'footer_content_wrapper', status: 'fail', error: 'Missing .footer-content wrapper' });
    }
  }

  // Run all tests
  testFooterExists();
  testFooterIsTopLevel();
  testFooterHasContent();
  testFooterContentWrapper();

  // Summary
  console.log(`\n${'='.repeat(50)}`);
  console.log(`Tests: ${passed} passed, ${failed} failed`);
  console.log(`${'='.repeat(50)}\n`);

  if (failed > 0) {
    process.exit(1);
  }
}

runTests();
