/**
 * Unit tests for Footer Section Content
 * Scenario: Footer Section Content - Verify footer contains required information and links
 *
 * Test Cases:
 * TC1 (unit): Check for footer element - Footer element exists at bottom of page
 * TC3 (unit): Check for license info - Footer mentions license or links to LICENSE file
 * TC4 (unit): Check for version info - Footer may contain version or release information
 */

const { test, describe } = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');

const indexHtmlPath = path.resolve(__dirname, '../../index.html');

describe('Footer Section Content Unit Tests', () => {
  let html;

  // Load HTML once before each test
  test.beforeEach(() => {
    html = fs.readFileSync(indexHtmlPath, 'utf8');
  });

  // Test Case 1: Check for footer element
  test('TC1: Footer element exists at bottom of page', () => {
    // Find footer element
    const footerRegex = /<footer[^>]*class="[^"]*footer[^"]*"[^>]*>[\s\S]*?<\/footer>/gi;
    const footerMatch = html.match(footerRegex);

    assert.ok(footerMatch, 'Footer element should exist');
    assert.strictEqual(footerMatch.length, 1, 'There should be exactly one footer element');

    // Verify footer is at the end of the document (before closing body tag)
    const footerPosition = html.indexOf('<footer');
    const bodyClosePosition = html.indexOf('</body>');

    assert.ok(footerPosition > 0, 'Footer should be found in HTML');
    assert.ok(bodyClosePosition > 0, 'Body closing tag should be found');
    assert.ok(
      footerPosition < bodyClosePosition,
      'Footer should appear before body closing tag'
    );

    // Verify footer is the last major section before scripts/body end
    const afterFooter = html.substring(footerPosition);
    const hasNoSectionsAfter = !/<section[^>]*>/i.test(afterFooter.replace(/<footer[\s\S]*?<\/footer>/gi, ''));
    assert.ok(hasNoSectionsAfter, 'No section elements should appear after footer');
  });

  // Test Case 3: Check for license info
  test('TC3: Footer mentions license or links to LICENSE file', () => {
    // Find footer section
    const footerRegex = /<footer[^>]*>[\s\S]*?<\/footer>/gi;
    const footerMatch = html.match(footerRegex);

    assert.ok(footerMatch, 'Footer section should exist');

    const footerHtml = footerMatch[0];

    // Check for license mention (case insensitive)
    // Should contain "MIT", "Apache", "LICENSE", or other common license references
    const hasLicenseText = /(?:MIT|Apache|GPL|BSD|ISC|license)/i.test(footerHtml);
    const hasLicenseLink = /href=["'][^"']*LICENSE[^"']*["']/i.test(footerHtml) ||
                          /href=["'][^"']*license[^"']*["']/i.test(footerHtml);

    assert.ok(
      hasLicenseText || hasLicenseLink,
      'Footer should mention license type or link to LICENSE file. Found: ' + footerHtml
    );
  });

  // Test Case 4: Check for version info (optional - may contain)
  test('TC4: Footer may contain version or release information', () => {
    // Find footer section
    const footerRegex = /<footer[^>]*>[\s\S]*?<\/footer>/gi;
    const footerMatch = html.match(footerRegex);

    assert.ok(footerMatch, 'Footer section should exist');

    const footerHtml = footerMatch[0];

    // Check for version info - this is optional per the scenario ("may contain")
    // Look for patterns like "v1.0.0", "Version 1.0", "Release", etc.
    const hasVersionText = /(?:v?\d+\.\d+(?:\.\d+)?|version|release)/i.test(footerHtml);

    // Note: This test documents whether version info exists but doesn't fail if missing
    // since the scenario says "may contain" (optional)
    if (hasVersionText) {
      // Version info exists - verify it looks reasonable
      const versionPattern = /v?\d+\.\d+(?:\.\d+)?/i;
      const versionMatch = footerHtml.match(versionPattern);
      if (versionMatch) {
        assert.ok(true, `Version information found: ${versionMatch[0]}`);
      }
    } else {
      // Version info is optional, so this is still a pass
      assert.ok(true, 'Version info is optional and not present - this is acceptable');
    }
  });

  // Additional test: Footer has proper structure
  test('Footer has proper semantic structure', () => {
    // Verify footer uses semantic footer element
    const footerElementExists = /<footer\b/i.test(html);
    assert.ok(footerElementExists, 'Footer should use semantic <footer> element');

    // Verify footer has container for consistent styling
    const footerRegex = /<footer[^>]*>[\s\S]*?<\/footer>/gi;
    const footerMatch = html.match(footerRegex);
    assert.ok(footerMatch, 'Footer should exist');

    const footerHtml = footerMatch[0];
    const hasContainer = /class="[^"]*container[^"]*"/i.test(footerHtml) ||
                        /<div[^>]*class="container"[^>]*>/i.test(footerHtml);
    assert.ok(hasContainer, 'Footer should have a container div for consistent layout');
  });

  // Test: Footer contains GitHub link (additional verification)
  test('Footer contains GitHub repository link', () => {
    const footerRegex = /<footer[^>]*>[\s\S]*?<\/footer>/gi;
    const footerMatch = html.match(footerRegex);

    assert.ok(footerMatch, 'Footer should exist');

    const footerHtml = footerMatch[0];

    // Check for GitHub link
    const hasGitHubLink = /github\.com/i.test(footerHtml);
    assert.ok(hasGitHubLink, 'Footer should contain GitHub repository link');

    // Verify the link has proper attributes
    const linkRegex = /<a[^>]*href=["'][^"']*github\.com[^"']*["'][^>]*>/gi;
    const linkMatch = footerHtml.match(linkRegex);
    assert.ok(linkMatch, 'Footer should have anchor tag linking to GitHub');
  });
});
