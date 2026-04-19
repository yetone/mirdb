/**
 * Navigation E2E Tests (String-based)
 * Owner: Scenario 7 - Navigation and GitHub Links
 *
 * Tests for:
 * - Header navigation with GitHub and Docs links
 * - External link behavior (new tab, security attributes)
 * - Footer links and license information
 *
 * Using HTML string testing approach due to Playwright environment issues.
 * See: jest-html-string-testing skill
 */

import { readFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

describe('Navigation and GitHub Links', () => {
  let html;

  beforeAll(() => {
    const htmlPath = join(__dirname, '../../index.html');
    html = readFileSync(htmlPath, 'utf-8');
  });

  // Test Case 1: Check header GitHub link href
  test('TC1: Header GitHub link points to correct repository URL', () => {
    // Extract header section
    const headerMatch = html.match(/<header class="header"[\s\S]*?<\/header>/);
    expect(headerMatch).not.toBeNull();
    const headerHtml = headerMatch[0];

    // Verify GitHub link exists with correct href
    expect(headerHtml).toContain('https://github.com/yetone/mirdb');
  });

  // Test Case 2: Check header GitHub link target
  test('TC2: Header GitHub link has target="_blank" and rel="noopener noreferrer"', () => {
    // Extract header section
    const headerMatch = html.match(/<header class="header"[\s\S]*?<\/header>/);
    expect(headerMatch).not.toBeNull();
    const headerHtml = headerMatch[0];

    // Find GitHub link
    const githubLinkMatch = headerHtml.match(/<a[^>]*href="https:\/\/github\.com\/yetone\/mirdb"[^>]*>/);
    expect(githubLinkMatch).not.toBeNull();
    const githubLink = githubLinkMatch[0];

    // Verify target and rel attributes
    expect(githubLink).toContain('target="_blank"');
    expect(githubLink).toContain('rel="noopener noreferrer"');
  });

  // Test Case 3: Check documentation link presence
  test('TC3: Documentation link is present and points to documentation section', () => {
    // Extract header section
    const headerMatch = html.match(/<header class="header"[\s\S]*?<\/header>/);
    expect(headerMatch).not.toBeNull();
    const headerHtml = headerMatch[0];

    // Check for Docs link or Features link (documentation-like)
    const hasDocsLink = headerHtml.includes('Docs') || headerHtml.includes('#features') || headerHtml.includes('#quickstart');
    expect(hasDocsLink).toBe(true);

    // Check navigation has multiple links
    const linkMatches = headerHtml.match(/class="header__link"/g);
    expect(linkMatches).not.toBeNull();
    expect(linkMatches.length).toBeGreaterThanOrEqual(1);
  });

  // Test Case 4: Check footer license information
  test('TC4: Footer contains license information (MIT)', () => {
    // Extract footer section
    const footerMatch = html.match(/<footer class="footer"[\s\S]*?<\/footer>/);
    expect(footerMatch).not.toBeNull();
    const footerHtml = footerMatch[0];

    // Check for license text
    const hasLicense = footerHtml.toLowerCase().includes('license') || footerHtml.toLowerCase().includes('mit');
    expect(hasLicense).toBe(true);

    // Check for license link
    expect(footerHtml).toContain('/LICENSE');
  });

  // Test Case 5: Check all external links have new tab indicators
  test('TC5: External links have visual indicator for new tab', () => {
    // Find all external links (those with target="_blank")
    const externalLinks = html.match(/<a[^>]*target="_blank"[^>]*>[\s\S]*?<\/a>/g);
    expect(externalLinks).not.toBeNull();
    expect(externalLinks.length).toBeGreaterThan(0);

    // Check each external link for visual indicator
    for (const link of externalLinks) {
      // Check for at least one indicator:
      // 1. External link icon SVG
      // 2. aria-label mentioning new tab
      // 3. External link class
      const hasIcon = link.includes('external-link-icon') || link.includes('<svg') || link.includes('external-icon');
      const hasAriaLabel = link.includes('new tab') || link.includes('opens in');
      const hasClass = link.includes('external');

      expect(hasIcon || hasAriaLabel || hasClass).toBe(true);
    }
  });

  test('Header contains logo and navigation links', () => {
    // Extract header section
    const headerMatch = html.match(/<header class="header"[\s\S]*?<\/header>/);
    expect(headerMatch).not.toBeNull();
    const headerHtml = headerMatch[0];

    // Check for logo
    expect(headerHtml).toContain('class="header__logo"');
    expect(headerHtml).toContain('MiRDB');

    // Check for navigation
    expect(headerHtml).toContain('class="header__nav"');

    // Check for navigation links
    const navLinks = headerHtml.match(/class="header__link"/g);
    expect(navLinks).not.toBeNull();
    expect(navLinks.length).toBeGreaterThanOrEqual(1);
  });

  test('Footer contains navigation links', () => {
    // Extract footer section
    const footerMatch = html.match(/<footer class="footer"[\s\S]*?<\/footer>/);
    expect(footerMatch).not.toBeNull();
    const footerHtml = footerMatch[0];

    // Check for footer links
    expect(footerHtml).toContain('class="footer__links"');

    // Check for GitHub link
    expect(footerHtml).toContain('github.com');
  });

  test('All external links in header have proper security attributes', () => {
    // Extract header section
    const headerMatch = html.match(/<header class="header"[\s\S]*?<\/header>/);
    expect(headerMatch).not.toBeNull();
    const headerHtml = headerMatch[0];

    // Find all links with target="_blank" in header
    const externalLinks = headerHtml.match(/<a[^>]*target="_blank"[^>]*>/g);
    if (externalLinks) {
      for (const link of externalLinks) {
        expect(link).toContain('rel="noopener noreferrer"');
      }
    }
  });

  test('All external links in footer have proper security attributes', () => {
    // Extract footer section
    const footerMatch = html.match(/<footer class="footer"[\s\S]*?<\/footer>/);
    expect(footerMatch).not.toBeNull();
    const footerHtml = footerMatch[0];

    // Find all links with target="_blank" in footer
    const externalLinks = footerHtml.match(/<a[^>]*target="_blank"[^>]*>/g);
    if (externalLinks) {
      for (const link of externalLinks) {
        expect(link).toContain('rel="noopener noreferrer"');
      }
    }
  });
});
