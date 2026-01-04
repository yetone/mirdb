/**
 * Unit test for verifying no broken links exist on the page
 * Scenario: Error Handling - Broken Links
 *
 * Test Cases:
 * 1. Scan all href attributes - No empty href attributes
 * 2. Check internal anchor links - Internal anchor links (#section) point to existing elements
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

  // Test 1: No empty href attributes
  function testNoEmptyHref() {
    const allLinks = document.querySelectorAll('a[href]');
    let emptyHrefLinks = [];

    allLinks.forEach(link => {
      const href = link.getAttribute('href');
      // Check for empty, whitespace-only, or just "#" href values
      if (!href || href.trim() === '' || href === '#') {
        emptyHrefLinks.push({
          text: link.textContent?.trim() || '[no text]',
          href: href || '[empty]'
        });
      }
    });

    if (emptyHrefLinks.length === 0 && allLinks.length > 0) {
      console.log(`✓ All ${allLinks.length} links have valid non-empty href attributes`);
      passed++;
      results.push({ test: 'no_empty_href', status: 'pass' });
    } else if (allLinks.length === 0) {
      console.log('✗ No links found on the page');
      failed++;
      results.push({ test: 'no_empty_href', status: 'fail', error: 'No links found on the page' });
    } else {
      const errorDetails = emptyHrefLinks.map(l => `"${l.text}" (href="${l.href}")`).join(', ');
      console.log(`✗ Found ${emptyHrefLinks.length} links with empty href: ${errorDetails}`);
      failed++;
      results.push({ test: 'no_empty_href', status: 'fail', error: `Empty href links: ${errorDetails}` });
    }
  }

  // Test 2: Internal anchor links point to existing elements
  function testInternalAnchorLinks() {
    const anchorLinks = document.querySelectorAll('a[href^="#"]');
    let brokenAnchorLinks = [];
    let validAnchorLinks = [];

    anchorLinks.forEach(link => {
      const href = link.getAttribute('href');
      // Skip lone "#" as it's checked in test 1
      if (href === '#') return;

      // Extract the ID from the href (remove the #)
      const targetId = href.substring(1);

      // Check if an element with that ID exists
      const targetElement = document.getElementById(targetId);

      if (!targetElement) {
        brokenAnchorLinks.push({
          text: link.textContent?.trim() || '[no text]',
          href: href,
          targetId: targetId
        });
      } else {
        validAnchorLinks.push({
          text: link.textContent?.trim() || '[no text]',
          href: href,
          targetId: targetId
        });
      }
    });

    // Include only meaningful anchor links (not lone "#")
    const meaningfulAnchorLinks = Array.from(anchorLinks).filter(l => l.getAttribute('href') !== '#');

    if (brokenAnchorLinks.length === 0 && meaningfulAnchorLinks.length > 0) {
      console.log(`✓ All ${validAnchorLinks.length} internal anchor links point to existing elements`);
      passed++;
      results.push({ test: 'internal_anchor_links', status: 'pass' });
    } else if (meaningfulAnchorLinks.length === 0) {
      console.log('⚠ No internal anchor links found (skipping test)');
      // Not failing here since the page might not have internal anchors
      passed++;
      results.push({ test: 'internal_anchor_links', status: 'pass', note: 'No internal anchor links found' });
    } else {
      const errorDetails = brokenAnchorLinks.map(l => `"${l.text}" (href="${l.href}" -> #${l.targetId})`).join(', ');
      console.log(`✗ Found ${brokenAnchorLinks.length} broken anchor links: ${errorDetails}`);
      failed++;
      results.push({ test: 'internal_anchor_links', status: 'fail', error: `Broken anchor links: ${errorDetails}` });
    }
  }

  // Test 3: Verify all anchor link targets have proper IDs
  function testAnchorTargetsAccessible() {
    // Find all elements that are targets of internal links
    const anchorLinks = document.querySelectorAll('a[href^="#"]');
    const targetIds = new Set();

    anchorLinks.forEach(link => {
      const href = link.getAttribute('href');
      if (href && href !== '#') {
        targetIds.add(href.substring(1));
      }
    });

    let accessibleTargets = [];
    let inaccessibleTargets = [];

    targetIds.forEach(id => {
      const element = document.getElementById(id);
      if (element) {
        // Check if element is visible (not hidden or display:none in inline style)
        const style = element.getAttribute('style') || '';
        const isHidden = style.includes('display: none') || style.includes('visibility: hidden');

        if (isHidden) {
          inaccessibleTargets.push(id);
        } else {
          accessibleTargets.push(id);
        }
      }
    });

    if (inaccessibleTargets.length === 0 && targetIds.size > 0) {
      console.log(`✓ All ${accessibleTargets.length} anchor target elements are accessible`);
      passed++;
      results.push({ test: 'anchor_targets_accessible', status: 'pass' });
    } else if (targetIds.size === 0) {
      console.log('⚠ No anchor targets to validate (skipping test)');
      passed++;
      results.push({ test: 'anchor_targets_accessible', status: 'pass', note: 'No anchor targets found' });
    } else {
      console.log(`✗ Found ${inaccessibleTargets.length} inaccessible anchor targets: ${inaccessibleTargets.join(', ')}`);
      failed++;
      results.push({ test: 'anchor_targets_accessible', status: 'fail', error: `Inaccessible targets: ${inaccessibleTargets.join(', ')}` });
    }
  }

  // Run all tests
  testNoEmptyHref();
  testInternalAnchorLinks();
  testAnchorTargetsAccessible();

  // Summary
  console.log(`\n${'='.repeat(50)}`);
  console.log(`Tests: ${passed} passed, ${failed} failed`);
  console.log(`${'='.repeat(50)}\n`);

  if (failed > 0) {
    process.exit(1);
  }
}

runTests();
