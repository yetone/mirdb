// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Unit Tests for Navigation Links - Security and Accessibility
 * Scenario: Verify that external links have proper security attributes as per REQ-6
 */

test.describe('Navigation Links - Security and Accessibility', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage before each test
    await page.goto('/');
  });

  /**
   * Test Case 4: Verify external links have target='_blank' and rel='noopener'
   * Input: Verify external links have target='_blank' and rel='noopener'
   * Expected: External links open in new tab with security attributes
   */
  test('TC4: All external links have target="_blank" and rel="noopener" attributes', async ({ page }) => {
    // Get all external links (links that start with http or https)
    const externalLinks = page.locator('a[href^="http"]');
    const count = await externalLinks.count();

    // Verify we have external links to test
    expect(count).toBeGreaterThan(0);

    // Check each external link for security attributes
    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      const href = await link.getAttribute('href');
      const target = await link.getAttribute('target');
      const rel = await link.getAttribute('rel');

      // Verify target="_blank" for new tab opening
      expect(target, `External link ${href} should have target="_blank"`).toBe('_blank');

      // Verify rel contains "noopener" for security
      expect(rel, `External link ${href} should have rel containing "noopener"`).toContain('noopener');
    }
  });

  /**
   * Test Case 4b: Verify specific GitHub button has security attributes
   */
  test('TC4b: Hero GitHub button has correct security attributes', async ({ page }) => {
    const githubBtn = page.locator('[data-testid="github-btn"]');
    await expect(githubBtn).toBeVisible();

    // Check security attributes
    const target = await githubBtn.getAttribute('target');
    const rel = await githubBtn.getAttribute('rel');

    expect(target).toBe('_blank');
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');
  });

  /**
   * Test Case 4c: Verify documentation links have security attributes
   */
  test('TC4c: Documentation links have correct security attributes', async ({ page }) => {
    // Check docs link
    const docsLink = page.locator('[data-testid="docs-link"]');
    await expect(docsLink).toBeVisible();

    const docsTarget = await docsLink.getAttribute('target');
    const docsRel = await docsLink.getAttribute('rel');

    expect(docsTarget).toBe('_blank');
    expect(docsRel).toContain('noopener');
    expect(docsRel).toContain('noreferrer');

    // Check wiki link
    const wikiLink = page.locator('[data-testid="wiki-link"]');
    await expect(wikiLink).toBeVisible();

    const wikiTarget = await wikiLink.getAttribute('target');
    const wikiRel = await wikiLink.getAttribute('rel');

    expect(wikiTarget).toBe('_blank');
    expect(wikiRel).toContain('noopener');
    expect(wikiRel).toContain('noreferrer');
  });

  /**
   * Test Case 4d: Verify footer external links have security attributes
   */
  test('TC4d: Footer external links have correct security attributes', async ({ page }) => {
    // Find all external links in footer
    const footerExternalLinks = page.locator('.footer a[href^="http"]');
    const count = await footerExternalLinks.count();

    // Footer should have at least one external link (GitHub)
    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const link = footerExternalLinks.nth(i);
      const href = await link.getAttribute('href');
      const target = await link.getAttribute('target');
      const rel = await link.getAttribute('rel');

      expect(target, `Footer external link ${href} should have target="_blank"`).toBe('_blank');
      expect(rel, `Footer external link ${href} should have rel containing "noopener"`).toContain('noopener');
    }
  });

  /**
   * Test: Verify internal links do NOT have target="_blank"
   */
  test('TC-Internal: Internal anchor links do not open in new tab', async ({ page }) => {
    // Get all internal anchor links (href starting with #)
    const internalLinks = page.locator('a[href^="#"]');
    const count = await internalLinks.count();

    // Verify we have internal links to test
    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const link = internalLinks.nth(i);
      const href = await link.getAttribute('href');
      const target = await link.getAttribute('target');

      // Internal links should not have target="_blank"
      expect(target, `Internal link ${href} should not have target="_blank"`).toBeNull();
    }
  });

  /**
   * Test: Verify links have proper href values (no empty hrefs)
   */
  test('TC-Href: All links have non-empty href values', async ({ page }) => {
    const allLinks = page.locator('a[href]');
    const count = await allLinks.count();

    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const link = allLinks.nth(i);
      const href = await link.getAttribute('href');

      // Verify href is not empty
      expect(href, `Link should have non-empty href`).toBeTruthy();
      expect(href?.trim().length, `Link href should not be empty string`).toBeGreaterThan(0);
    }
  });

  /**
   * Test: Verify links are keyboard accessible
   */
  test('TC-Accessibility: Links are keyboard accessible', async ({ page }) => {
    // Test that Get Started button is focusable
    const getStartedBtn = page.locator('[data-testid="get-started-btn"]');
    await getStartedBtn.focus();
    await expect(getStartedBtn).toBeFocused();

    // Test that GitHub button is focusable
    const githubBtn = page.locator('[data-testid="github-btn"]');
    await githubBtn.focus();
    await expect(githubBtn).toBeFocused();

    // Test keyboard navigation with Tab
    await page.keyboard.press('Tab');
    // The next focusable element should be focused
    const focusedElement = page.locator(':focus');
    await expect(focusedElement).not.toHaveCount(0);
  });
});
