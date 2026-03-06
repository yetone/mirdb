/**
 * External Links E2E Tests
 * Owner: Scenario 5 - External Links Functionality
 *
 * Test coverage:
 * - GitHub repository link
 * - Link target="_blank" attributes
 * - Memcached protocol docs link
 * - No broken links validation
 */

import { test, expect } from '@playwright/test';
import { waitForPageLoad } from './test-utils';

const GITHUB_REPO_URL = 'https://github.com/yetone/mirdb';
const MEMCACHED_DOCS_URL = 'https://github.com/memcached/memcached/blob/master/doc/protocol.txt';

test.describe('External Links Functionality', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);
  });

  test('TC1: GitHub repository link exists with correct href', async ({ page }) => {
    // Find GitHub link(s) on the page
    const githubLinks = page.locator(`a[href="${GITHUB_REPO_URL}"]`);

    // Expect at least one GitHub link to exist
    await expect(githubLinks.first()).toBeVisible();

    // Verify the href attribute
    const href = await githubLinks.first().getAttribute('href');
    expect(href).toBe(GITHUB_REPO_URL);
  });

  test('TC2: GitHub link opens in new tab with security attributes', async ({ page }) => {
    // Find GitHub link
    const githubLink = page.locator(`a[href="${GITHUB_REPO_URL}"]`).first();

    await expect(githubLink).toBeVisible();

    // Verify target="_blank" attribute
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify rel="noopener noreferrer" for security
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');
  });

  test('TC3: Memcached protocol documentation link exists', async ({ page }) => {
    // Find Memcached docs link on the page
    const memcachedDocsLink = page.locator(`a[href="${MEMCACHED_DOCS_URL}"]`);

    // Expect the link to exist and be visible
    await expect(memcachedDocsLink.first()).toBeVisible();

    // Verify the href attribute
    const href = await memcachedDocsLink.first().getAttribute('href');
    expect(href).toBe(MEMCACHED_DOCS_URL);

    // Verify it opens in new tab with security attributes
    const target = await memcachedDocsLink.first().getAttribute('target');
    expect(target).toBe('_blank');

    const rel = await memcachedDocsLink.first().getAttribute('rel');
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');
  });

  test('TC4: All anchor elements have valid href attributes', async ({ page }) => {
    // Get all anchor elements with href attributes
    const links = page.locator('a[href]');
    const count = await links.count();

    // Ensure there are links on the page
    expect(count).toBeGreaterThan(0);

    // Check each link
    for (let i = 0; i < count; i++) {
      const link = links.nth(i);
      const href = await link.getAttribute('href');

      // href should not be null or empty
      expect(href).not.toBeNull();
      expect(href).not.toBe('');

      // Validate href format
      if (href) {
        // Valid patterns: absolute URLs, relative paths, anchor links, mailto, tel
        const isValidHref =
          href.startsWith('http://') ||
          href.startsWith('https://') ||
          href.startsWith('/') ||
          href.startsWith('#') ||
          href.startsWith('mailto:') ||
          href.startsWith('tel:') ||
          href.startsWith('./') ||
          href.startsWith('../');

        expect(isValidHref).toBe(true);
      }
    }
  });

  test('All external links have target="_blank" and rel="noopener noreferrer"', async ({ page }) => {
    // Find all external links (starting with http:// or https://)
    const externalLinks = page.locator('a[href^="http://"], a[href^="https://"]');
    const count = await externalLinks.count();

    // Ensure there are external links
    expect(count).toBeGreaterThan(0);

    // Check each external link has proper security attributes
    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      const href = await link.getAttribute('href');

      // Verify target="_blank"
      const target = await link.getAttribute('target');
      expect(target, `Link to ${href} should have target="_blank"`).toBe('_blank');

      // Verify rel contains noopener and noreferrer
      const rel = await link.getAttribute('rel');
      expect(rel, `Link to ${href} should have rel="noopener noreferrer"`).toContain('noopener');
      expect(rel, `Link to ${href} should have rel="noopener noreferrer"`).toContain('noreferrer');
    }
  });

  test('Internal anchor links point to existing sections', async ({ page }) => {
    // Find all internal anchor links (starting with #)
    const anchorLinks = page.locator('a[href^="#"]');
    const count = await anchorLinks.count();

    // Check each anchor link points to an existing element
    for (let i = 0; i < count; i++) {
      const link = anchorLinks.nth(i);
      const href = await link.getAttribute('href');

      if (href && href.length > 1) {
        const targetId = href.substring(1); // Remove the # prefix
        const targetElement = page.locator(`#${targetId}`);

        // Verify the target element exists
        await expect(targetElement, `Anchor ${href} should point to existing element`).toHaveCount(1);
      }
    }
  });
});
