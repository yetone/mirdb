/**
 * Unit test for verifying external links have proper security attributes
 */
const { JSDOM } = require('jsdom');
const fs = require('fs');
const path = require('path');

// Test configuration
const indexPath = path.join(__dirname, '../../index.html');

function runTests() {
  console.log('Running External Links Security Tests...\n');

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

  // Test 1: All external links have target="_blank"
  function testTargetBlank() {
    const externalLinks = document.querySelectorAll('a[href^="https://"], a[href^="http://"]');
    let allHaveTarget = true;
    let missingTargetLinks = [];

    externalLinks.forEach(link => {
      const target = link.getAttribute('target');
      if (target !== '_blank') {
        allHaveTarget = false;
        missingTargetLinks.push(link.getAttribute('href'));
      }
    });

    if (allHaveTarget && externalLinks.length > 0) {
      console.log(`✓ All ${externalLinks.length} external links have target="_blank"`);
      passed++;
      results.push({ test: 'target_blank', status: 'pass' });
    } else if (externalLinks.length === 0) {
      console.log('✗ No external links found');
      failed++;
      results.push({ test: 'target_blank', status: 'fail', error: 'No external links found' });
    } else {
      console.log(`✗ Some external links missing target="_blank": ${missingTargetLinks.join(', ')}`);
      failed++;
      results.push({ test: 'target_blank', status: 'fail', error: `Missing target="_blank": ${missingTargetLinks.join(', ')}` });
    }
  }

  // Test 2: All external links have rel="noopener"
  function testNoopener() {
    const externalLinks = document.querySelectorAll('a[href^="https://"], a[href^="http://"]');
    let allHaveNoopener = true;
    let missingNoopenerLinks = [];

    externalLinks.forEach(link => {
      const rel = link.getAttribute('rel') || '';
      if (!rel.includes('noopener')) {
        allHaveNoopener = false;
        missingNoopenerLinks.push(link.getAttribute('href'));
      }
    });

    if (allHaveNoopener && externalLinks.length > 0) {
      console.log(`✓ All ${externalLinks.length} external links have rel="noopener"`);
      passed++;
      results.push({ test: 'noopener', status: 'pass' });
    } else if (externalLinks.length === 0) {
      console.log('✗ No external links found');
      failed++;
      results.push({ test: 'noopener', status: 'fail', error: 'No external links found' });
    } else {
      console.log(`✗ Some external links missing rel="noopener": ${missingNoopenerLinks.join(', ')}`);
      failed++;
      results.push({ test: 'noopener', status: 'fail', error: `Missing rel="noopener": ${missingNoopenerLinks.join(', ')}` });
    }
  }

  // Test 3: All external links have rel="noreferrer"
  function testNoreferrer() {
    const externalLinks = document.querySelectorAll('a[href^="https://"], a[href^="http://"]');
    let allHaveNoreferrer = true;
    let missingNoreferrerLinks = [];

    externalLinks.forEach(link => {
      const rel = link.getAttribute('rel') || '';
      if (!rel.includes('noreferrer')) {
        allHaveNoreferrer = false;
        missingNoreferrerLinks.push(link.getAttribute('href'));
      }
    });

    if (allHaveNoreferrer && externalLinks.length > 0) {
      console.log(`✓ All ${externalLinks.length} external links have rel="noreferrer"`);
      passed++;
      results.push({ test: 'noreferrer', status: 'pass' });
    } else if (externalLinks.length === 0) {
      console.log('✗ No external links found');
      failed++;
      results.push({ test: 'noreferrer', status: 'fail', error: 'No external links found' });
    } else {
      console.log(`✗ Some external links missing rel="noreferrer": ${missingNoreferrerLinks.join(', ')}`);
      failed++;
      results.push({ test: 'noreferrer', status: 'fail', error: `Missing rel="noreferrer": ${missingNoreferrerLinks.join(', ')}` });
    }
  }

  // Test 4: GitHub repository link is present and valid
  function testGitHubLink() {
    const gitHubLinks = document.querySelectorAll('a[href*="github.com/yetone/mirdb"]');

    if (gitHubLinks.length > 0) {
      console.log(`✓ GitHub repository link is present (${gitHubLinks.length} links)`);
      passed++;
      results.push({ test: 'github_link', status: 'pass' });
    } else {
      console.log('✗ GitHub repository link is missing');
      failed++;
      results.push({ test: 'github_link', status: 'fail', error: 'No GitHub repository link found' });
    }
  }

  // Test 5: Footer has GitHub and license info
  function testFooterLinks() {
    const footer = document.querySelector('footer');
    if (!footer) {
      console.log('✗ Footer element not found');
      failed++;
      results.push({ test: 'footer_links', status: 'fail', error: 'Footer element not found' });
      return;
    }

    const footerLinks = footer.querySelectorAll('a');
    const footerText = footer.textContent.toLowerCase();

    // Check for GitHub link in footer
    const hasGitHubLink = Array.from(footerLinks).some(link =>
      link.getAttribute('href')?.includes('github.com/yetone/mirdb')
    );

    // Check for license info
    const hasLicenseInfo = footerText.includes('license') || footerText.includes('mit');

    if (hasGitHubLink && hasLicenseInfo) {
      console.log('✓ Footer has GitHub link and license information');
      passed++;
      results.push({ test: 'footer_links', status: 'pass' });
    } else {
      const missing = [];
      if (!hasGitHubLink) missing.push('GitHub link');
      if (!hasLicenseInfo) missing.push('license information');
      console.log(`✗ Footer missing: ${missing.join(', ')}`);
      failed++;
      results.push({ test: 'footer_links', status: 'fail', error: `Missing: ${missing.join(', ')}` });
    }
  }

  // Run all tests
  testTargetBlank();
  testNoopener();
  testNoreferrer();
  testGitHubLink();
  testFooterLinks();

  // Summary
  console.log(`\n${'='.repeat(50)}`);
  console.log(`Tests: ${passed} passed, ${failed} failed`);
  console.log(`${'='.repeat(50)}\n`);

  if (failed > 0) {
    process.exit(1);
  }
}

runTests();
