/**
 * Link Validation Integration Tests
 * Owner: Scenario 23 (Link Validation)
 *
 * Test groups:
 * - Extract all page links
 * - Validate internal links
 * - HTTP request to external links
 * - Check for broken links
 * - Check for placeholder hrefs
 */

import { test, expect } from '@playwright/test';

test.describe('Link Validation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC1: All anchor elements have valid href attributes', async ({ page }) => {
    // Extract all anchor elements and their href attributes
    const links = await page.locator('a[href]').all();
    expect(links.length).toBeGreaterThan(0);

    const hrefs: string[] = [];
    for (const link of links) {
      const href = await link.getAttribute('href');
      hrefs.push(href || '');
    }

    // Validate each href is a valid URL or anchor
    for (const href of hrefs) {
      // Skip empty checks here (covered in TC3)
      if (!href) continue;

      const isValidUrl = href.startsWith('http://') || href.startsWith('https://');
      const isValidAnchor = href.startsWith('#') && href.length > 1;
      const isRelativeUrl = href.startsWith('/') || href.startsWith('./');

      const isValid = isValidUrl || isValidAnchor || isRelativeUrl;
      expect(
        isValid,
        `Invalid href found: "${href}" - must be a valid URL, anchor (with target), or relative path`
      ).toBe(true);
    }
  });

  test('TC2: External links return valid HTTP responses', async ({ page, request }) => {
    // Extract all external links (http/https)
    const links = await page.locator('a[href^="http"]').all();
    expect(links.length).toBeGreaterThan(0);

    const externalUrls = new Set<string>();
    for (const link of links) {
      const href = await link.getAttribute('href');
      if (href) {
        externalUrls.add(href);
      }
    }

    // Perform HEAD request to each unique external URL
    const results: { url: string; status: number; ok: boolean }[] = [];

    for (const url of externalUrls) {
      try {
        // Use HEAD request for efficiency
        const response = await request.head(url, {
          timeout: 10000,
          ignoreHTTPSErrors: true,
        });

        const status = response.status();
        // Accept 2xx success codes and 3xx redirects
        const isValid = (status >= 200 && status < 400);

        results.push({ url, status, ok: isValid });
      } catch (error) {
        // If HEAD fails, try GET as some servers don't support HEAD
        try {
          const response = await request.get(url, {
            timeout: 10000,
            ignoreHTTPSErrors: true,
          });

          const status = response.status();
          const isValid = (status >= 200 && status < 400);
          results.push({ url, status, ok: isValid });
        } catch {
          // Network error - mark as failed
          results.push({ url, status: 0, ok: false });
        }
      }
    }

    // Check all external links returned valid status
    const failedLinks = results.filter(r => !r.ok);
    expect(
      failedLinks,
      `Failed external links:\n${failedLinks.map(f => `  ${f.url}: status ${f.status}`).join('\n')}`
    ).toHaveLength(0);
  });

  test('TC3: No links have empty or placeholder hrefs', async ({ page }) => {
    // Check for empty href attributes
    const emptyHrefs = await page.locator('a[href=""]').count();
    expect(emptyHrefs, 'Found links with empty href=""').toBe(0);

    // Check for placeholder href="#"
    const placeholderHrefs = await page.locator('a[href="#"]').count();
    expect(placeholderHrefs, 'Found links with placeholder href="#"').toBe(0);

    // Check for javascript:void(0) or javascript: placeholders
    const jsVoidHrefs = await page.locator('a[href^="javascript:"]').count();
    expect(jsVoidHrefs, 'Found links with javascript: href').toBe(0);

    // Additional check: Ensure all links have non-whitespace href
    const allLinks = await page.locator('a[href]').all();
    for (const link of allLinks) {
      const href = await link.getAttribute('href');
      expect(href?.trim().length).toBeGreaterThan(0);
    }
  });

  test('Internal anchor links point to existing elements', async ({ page }) => {
    // Extract all internal anchor links
    const anchorLinks = await page.locator('a[href^="#"]').all();

    for (const link of anchorLinks) {
      const href = await link.getAttribute('href');
      if (href && href.length > 1) {
        const targetId = href.substring(1); // Remove the '#'
        const targetElement = page.locator(`#${targetId}`);
        const count = await targetElement.count();

        expect(
          count,
          `Internal anchor "${href}" points to non-existent element with id="${targetId}"`
        ).toBe(1);
      }
    }
  });

  test('External links have proper security attributes', async ({ page }) => {
    // External links should have rel="noopener noreferrer" for security
    const externalLinks = await page.locator('a[href^="http"][target="_blank"]').all();

    for (const link of externalLinks) {
      const rel = await link.getAttribute('rel');
      const href = await link.getAttribute('href');

      expect(
        rel,
        `External link "${href}" with target="_blank" should have rel attribute`
      ).toBeTruthy();

      expect(
        rel?.includes('noopener'),
        `External link "${href}" should have rel="noopener" for security`
      ).toBe(true);
    }
  });

  test('All links are accessible (have visible text or aria-label)', async ({ page }) => {
    const links = await page.locator('a[href]').all();

    for (const link of links) {
      const href = await link.getAttribute('href');
      const textContent = await link.textContent();
      const ariaLabel = await link.getAttribute('aria-label');
      const title = await link.getAttribute('title');

      // Link should have either visible text, aria-label, or title
      const hasAccessibleName =
        (textContent && textContent.trim().length > 0) ||
        (ariaLabel && ariaLabel.trim().length > 0) ||
        (title && title.trim().length > 0);

      expect(
        hasAccessibleName,
        `Link "${href}" has no accessible name (text content, aria-label, or title)`
      ).toBe(true);
    }
  });

  test('Links have distinguishable text (no generic "click here")', async ({ page }) => {
    const links = await page.locator('a[href]').all();
    const genericPhrases = ['click here', 'read more', 'learn more', 'here', 'link'];

    for (const link of links) {
      const textContent = await link.textContent();
      const href = await link.getAttribute('href');

      if (textContent) {
        const normalizedText = textContent.trim().toLowerCase();
        const isGeneric = genericPhrases.includes(normalizedText);

        expect(
          isGeneric,
          `Link "${href}" uses generic text "${textContent.trim()}" - use descriptive text instead`
        ).toBe(false);
      }
    }
  });
});
