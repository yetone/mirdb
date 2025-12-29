/**
 * Unit tests for GitHub Repository Integration
 * Tests REQ-9 from PRD: Link to GitHub repository for source code access
 *
 * Test Cases:
 * TC3 (unit): Verify link href attribute - GitHub links have valid href pointing to github.com
 * TC4 (unit): Verify link security attributes - External links have rel='noopener noreferrer' or rel='noopener'
 * TC5 (unit): Verify link opens in new tab - External GitHub links have target='_blank'
 */

const { test, describe } = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');

const indexHtmlPath = path.resolve(__dirname, '../../index.html');

describe('GitHub Links Unit Tests', () => {
  let html;

  // Load HTML once before all tests
  test.beforeEach(() => {
    html = fs.readFileSync(indexHtmlPath, 'utf8');
  });

  // Test Case 3: Verify link href attribute
  test('TC3: GitHub links have valid href pointing to github.com', () => {
    // Find all links that contain 'github' in href
    const githubLinkRegex = /<a[^>]*href=["']([^"']*github\.com[^"']*)["'][^>]*>/gi;
    const matches = [...html.matchAll(githubLinkRegex)];

    // Should have at least 2 GitHub links (hero and footer)
    assert.ok(
      matches.length >= 2,
      `Expected at least 2 GitHub links, found ${matches.length}`
    );

    // Verify each link has a valid GitHub URL
    matches.forEach((match, index) => {
      const href = match[1];
      assert.ok(
        href.includes('github.com'),
        `Link ${index + 1} href should contain github.com: ${href}`
      );

      // Verify URL format is valid (starts with https:// or http://)
      assert.ok(
        href.startsWith('https://github.com') || href.startsWith('http://github.com'),
        `Link ${index + 1} should have valid GitHub URL: ${href}`
      );
    });
  });

  // Test Case 4: Verify link security attributes
  test('TC4: External GitHub links have rel="noopener noreferrer" or rel="noopener"', () => {
    // Find all links that point to github.com
    const githubLinkRegex = /<a[^>]*href=["'][^"']*github\.com[^"']*["'][^>]*>/gi;
    const linkMatches = html.match(githubLinkRegex) || [];

    assert.ok(
      linkMatches.length >= 2,
      `Expected at least 2 GitHub links, found ${linkMatches.length}`
    );

    linkMatches.forEach((link, index) => {
      // Check for rel attribute with noopener
      const hasNoopener = /rel=["'][^"']*noopener[^"']*["']/i.test(link);

      assert.ok(
        hasNoopener,
        `GitHub link ${index + 1} should have rel="noopener" or rel="noopener noreferrer": ${link}`
      );
    });
  });

  // Test Case 5: Verify link opens in new tab
  test('TC5: External GitHub links have target="_blank"', () => {
    // Find all links that point to github.com
    const githubLinkRegex = /<a[^>]*href=["'][^"']*github\.com[^"']*["'][^>]*>/gi;
    const linkMatches = html.match(githubLinkRegex) || [];

    assert.ok(
      linkMatches.length >= 2,
      `Expected at least 2 GitHub links, found ${linkMatches.length}`
    );

    linkMatches.forEach((link, index) => {
      // Check for target="_blank"
      const hasTargetBlank = /target=["']_blank["']/i.test(link);

      assert.ok(
        hasTargetBlank,
        `GitHub link ${index + 1} should have target="_blank": ${link}`
      );
    });
  });

  // Additional test: Verify hero section has GitHub link
  test('Hero section contains GitHub link', () => {
    // Find hero section
    const heroSectionRegex = /<header[^>]*class="[^"]*hero[^"]*"[^>]*>[\s\S]*?<\/header>/gi;
    const heroMatch = html.match(heroSectionRegex);

    assert.ok(heroMatch, 'Hero section should exist');

    const heroHtml = heroMatch[0];

    // Check that hero contains a GitHub link
    assert.ok(
      heroHtml.includes('github.com'),
      'Hero section should contain a GitHub link'
    );

    // Check that hero link has proper text
    assert.ok(
      /GitHub/i.test(heroHtml),
      'Hero section GitHub link should have visible GitHub text'
    );
  });

  // Additional test: Verify footer section has GitHub link
  test('Footer section contains GitHub link', () => {
    // Find footer section
    const footerSectionRegex = /<footer[^>]*>[\s\S]*?<\/footer>/gi;
    const footerMatch = html.match(footerSectionRegex);

    assert.ok(footerMatch, 'Footer section should exist');

    const footerHtml = footerMatch[0];

    // Check that footer contains a GitHub link
    assert.ok(
      footerHtml.includes('github.com'),
      'Footer section should contain a GitHub link'
    );
  });
});
