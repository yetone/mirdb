// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Error Handling - Broken Links Test Suite
 * Scenario ID: 14
 * UUID: aad9769a-b187-4de0-92df-f4597948d833
 *
 * Verifies the page handles potential navigation errors gracefully:
 * - Internal anchor links work correctly
 * - External links have appropriate attributes (target, rel)
 */

test.describe('Error Handling - Broken Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1 (E2E): Click all internal navigation links
   * Expected: All internal links scroll to correct section smoothly
   */
  test('TC1: All internal navigation links scroll to correct sections smoothly', async ({ page }) => {
    // Get all internal anchor links (href starts with #)
    const internalLinks = page.locator('a[href^="#"]');
    const linkCount = await internalLinks.count();

    // Ensure there are internal links to test
    expect(linkCount).toBeGreaterThan(0);

    // Collect all internal link hrefs and their targets
    const linkData = [];
    for (let i = 0; i < linkCount; i++) {
      const link = internalLinks.nth(i);
      const href = await link.getAttribute('href');
      if (href && href !== '#') {
        const targetId = href.substring(1); // Remove the '#'
        linkData.push({ href, targetId, index: i });
      }
    }

    // Test each internal link
    for (const { href, targetId, index } of linkData) {
      // Scroll back to top first
      await page.evaluate(() => window.scrollTo(0, 0));
      await page.waitForTimeout(100);

      // Click the internal link
      const link = internalLinks.nth(index);
      const isVisible = await link.isVisible();

      if (isVisible) {
        await link.click();

        // Wait for scroll animation
        await page.waitForTimeout(500);

        // Verify the target section exists and is in viewport
        const targetSection = page.locator(`#${targetId}`);
        const targetExists = await targetSection.count() > 0;

        expect(targetExists, `Target section #${targetId} should exist for link ${href}`).toBe(true);

        if (targetExists) {
          await expect(targetSection, `Section #${targetId} should be in viewport after clicking ${href}`).toBeInViewport({ ratio: 0.1 });
        }
      }
    }
  });

  /**
   * Test Case 2 (Unit): Check external links for target attribute
   * Expected: External links have target='_blank' attribute
   */
  test('TC2: All external links have target="_blank" attribute', async ({ page }) => {
    // Get all external links (href starts with http:// or https://)
    const externalLinks = page.locator('a[href^="http://"], a[href^="https://"]');
    const linkCount = await externalLinks.count();

    // Ensure there are external links to test
    expect(linkCount).toBeGreaterThan(0);

    // Check each external link has target="_blank"
    for (let i = 0; i < linkCount; i++) {
      const link = externalLinks.nth(i);
      const href = await link.getAttribute('href');
      const target = await link.getAttribute('target');
      const linkText = await link.textContent();

      expect(
        target,
        `External link "${linkText}" (${href}) should have target="_blank"`
      ).toBe('_blank');
    }
  });

  /**
   * Test Case 3 (Unit): Check external links for security
   * Expected: External links with target='_blank' have rel='noopener noreferrer'
   */
  test('TC3: External links with target="_blank" have rel="noopener noreferrer"', async ({ page }) => {
    // Get all external links that open in new tab
    const externalLinksWithBlank = page.locator('a[href^="http://"][target="_blank"], a[href^="https://"][target="_blank"]');
    const linkCount = await externalLinksWithBlank.count();

    // Ensure there are external links to test
    expect(linkCount).toBeGreaterThan(0);

    // Check each external link has proper rel attribute
    for (let i = 0; i < linkCount; i++) {
      const link = externalLinksWithBlank.nth(i);
      const href = await link.getAttribute('href');
      const rel = await link.getAttribute('rel');
      const linkText = await link.textContent();

      // rel attribute should exist
      expect(
        rel,
        `External link "${linkText}" (${href}) should have rel attribute`
      ).toBeTruthy();

      // rel attribute should contain 'noopener'
      expect(
        rel,
        `External link "${linkText}" (${href}) should have 'noopener' in rel attribute`
      ).toContain('noopener');

      // rel attribute should contain 'noreferrer'
      expect(
        rel,
        `External link "${linkText}" (${href}) should have 'noreferrer' in rel attribute`
      ).toContain('noreferrer');
    }
  });

  /**
   * Additional test: Verify no broken internal anchor links
   * Ensures all internal links point to existing sections
   */
  test('All internal anchor links point to existing sections', async ({ page }) => {
    const internalLinks = page.locator('a[href^="#"]');
    const linkCount = await internalLinks.count();

    for (let i = 0; i < linkCount; i++) {
      const link = internalLinks.nth(i);
      const href = await link.getAttribute('href');

      if (href && href !== '#') {
        const targetId = href.substring(1);
        const targetSection = page.locator(`#${targetId}`);
        const exists = await targetSection.count() > 0;

        expect(
          exists,
          `Internal link ${href} should point to an existing section`
        ).toBe(true);
      }
    }
  });

  /**
   * Additional test: Verify external links are accessible (not empty href)
   */
  test('All external links have valid non-empty hrefs', async ({ page }) => {
    const externalLinks = page.locator('a[href^="http://"], a[href^="https://"]');
    const linkCount = await externalLinks.count();

    for (let i = 0; i < linkCount; i++) {
      const link = externalLinks.nth(i);
      const href = await link.getAttribute('href');
      const linkText = await link.textContent();

      expect(
        href,
        `External link "${linkText}" should have a valid href`
      ).toBeTruthy();

      expect(
        href?.length,
        `External link "${linkText}" href should not be empty`
      ).toBeGreaterThan(0);

      // Verify it's a proper URL
      expect(
        href,
        `External link "${linkText}" should have a valid URL format`
      ).toMatch(/^https?:\/\/.+/);
    }
  });
});
