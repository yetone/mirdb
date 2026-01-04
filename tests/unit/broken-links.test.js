/**
 * Unit tests for verifying no broken links exist on the page
 * Scenario: Error Handling - Broken Links
 *
 * Test Cases:
 * 1. No empty href attributes
 * 2. Internal anchor links (#section) point to existing elements
 */
const { JSDOM } = require('jsdom');
const fs = require('fs');
const path = require('path');

// Test configuration
const indexPath = path.join(__dirname, '../../index.html');

function runTests() {
  console.log('Running Broken Links Unit Tests...\n');

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

  /**
   * Test 1: No empty href attributes
   * Scans all anchor tags to ensure no href is empty or just whitespace
   */
  function testNoEmptyHrefAttributes() {
    const allLinks = document.querySelectorAll('a[href]');
    let emptyHrefLinks = [];

    allLinks.forEach(link => {
      const href = link.getAttribute('href');
      // Check for empty, whitespace-only, or just "#" href
      if (!href || href.trim() === '' || href === '#') {
        emptyHrefLinks.push({
          text: link.textContent?.trim() || '[no text]',
          href: href || '[empty]'
        });
      }
    });

    if (emptyHrefLinks.length === 0) {
      console.log(`✓ No empty href attributes found (checked ${allLinks.length} links)`);
      passed++;
      results.push({ test: 'no_empty_href', status: 'pass' });
    } else {
      console.log(`✗ Found ${emptyHrefLinks.length} links with empty href:`);
      emptyHrefLinks.forEach(link => {
        console.log(`  - "${link.text}" has href="${link.href}"`);
      });
      failed++;
      results.push({
        test: 'no_empty_href',
        status: 'fail',
        error: `Found ${emptyHrefLinks.length} empty href attributes`
      });
    }
  }

  /**
   * Test 2: Internal anchor links point to existing elements
   * Ensures all #section links have corresponding id elements
   */
  function testInternalAnchorLinksExist() {
    const internalLinks = document.querySelectorAll('a[href^="#"]');
    let brokenAnchors = [];
    let validAnchors = 0;

    internalLinks.forEach(link => {
      const href = link.getAttribute('href');
      // Skip if just "#"
      if (href === '#') {
        return;
      }

      // Extract the id from the href (remove the #)
      const targetId = href.substring(1);

      // Check if element with this id exists
      const targetElement = document.getElementById(targetId);

      if (targetElement) {
        validAnchors++;
      } else {
        brokenAnchors.push({
          text: link.textContent?.trim() || '[no text]',
          href: href,
          targetId: targetId
        });
      }
    });

    if (brokenAnchors.length === 0 && validAnchors > 0) {
      console.log(`✓ All ${validAnchors} internal anchor links point to existing elements`);
      passed++;
      results.push({ test: 'internal_anchors_valid', status: 'pass' });
    } else if (validAnchors === 0 && brokenAnchors.length === 0) {
      console.log('✓ No internal anchor links found (nothing to validate)');
      passed++;
      results.push({ test: 'internal_anchors_valid', status: 'pass' });
    } else {
      console.log(`✗ Found ${brokenAnchors.length} broken internal anchor links:`);
      brokenAnchors.forEach(anchor => {
        console.log(`  - "${anchor.text}" links to #${anchor.targetId} (element not found)`);
      });
      failed++;
      results.push({
        test: 'internal_anchors_valid',
        status: 'fail',
        error: `Found ${brokenAnchors.length} broken anchor links: ${brokenAnchors.map(a => a.href).join(', ')}`
      });
    }
  }

  /**
   * Test 3: All links have href attribute (not missing)
   * Ensures anchor tags have the href attribute defined
   */
  function testAllLinksHaveHref() {
    const allAnchors = document.querySelectorAll('a');
    let missingHref = [];

    allAnchors.forEach(anchor => {
      if (!anchor.hasAttribute('href')) {
        missingHref.push({
          text: anchor.textContent?.trim() || '[no text]'
        });
      }
    });

    if (missingHref.length === 0) {
      console.log(`✓ All ${allAnchors.length} anchor tags have href attribute`);
      passed++;
      results.push({ test: 'all_links_have_href', status: 'pass' });
    } else {
      console.log(`✗ Found ${missingHref.length} anchor tags without href:`);
      missingHref.forEach(link => {
        console.log(`  - "${link.text}"`);
      });
      failed++;
      results.push({
        test: 'all_links_have_href',
        status: 'fail',
        error: `Found ${missingHref.length} anchors without href attribute`
      });
    }
  }

  // Run all tests
  testNoEmptyHrefAttributes();
  testInternalAnchorLinksExist();
  testAllLinksHaveHref();

  // Summary
  console.log(`\n${'='.repeat(50)}`);
  console.log(`Tests: ${passed} passed, ${failed} failed`);
  console.log(`${'='.repeat(50)}\n`);

  if (failed > 0) {
    process.exit(1);
  }
}

runTests();
