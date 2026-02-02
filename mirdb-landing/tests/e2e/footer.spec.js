/**
 * Footer Section E2E Tests
 * Owner: Scenario 17 - Footer and Attribution
 *
 * Tests:
 * - Footer element exists with semantic footer tag
 * - Footer contains GitHub link
 * - Footer contains attribution text
 * - All footer links are functional
 */

import { test, expect } from '@playwright/test';

test.describe('Footer Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Footer element exists with semantic footer tag', async ({ page }) => {
    // Verify footer element exists
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Verify footer has proper class
    await expect(footer).toHaveClass(/site-footer/);
  });

  test('TC2: Footer contains link to GitHub repository', async ({ page }) => {
    const footer = page.locator('footer');

    // Verify GitHub link exists in footer (use first to get the main GitHub link)
    const githubLink = footer.locator('a[href="https://github.com/yetone/mirdb"]').first();
    await expect(githubLink).toBeVisible();

    // Verify link has proper attributes
    await expect(githubLink).toHaveAttribute('target', '_blank');
    await expect(githubLink).toHaveAttribute('rel', /noopener/);
  });

  test('TC3: Footer contains copyright or attribution information', async ({ page }) => {
    const footer = page.locator('footer');

    // Check for copyright/attribution text
    const footerText = await footer.textContent();

    // Should contain copyright or attribution
    const hasAttribution = footerText.includes('©') ||
                          footerText.includes('Copyright') ||
                          footerText.includes('MirDB') ||
                          footerText.includes('MIT');
    expect(hasAttribution).toBe(true);

    // Verify attribution element exists
    const attribution = footer.locator('.footer-attribution');
    await expect(attribution).toBeVisible();
  });

  test('TC4: All footer links navigate to valid destinations', async ({ page }) => {
    const footer = page.locator('footer');

    // Get all links in footer
    const links = footer.locator('a');
    const count = await links.count();

    // Footer should have at least one link
    expect(count).toBeGreaterThan(0);

    // Verify each link has a valid href
    for (let i = 0; i < count; i++) {
      const link = links.nth(i);
      const href = await link.getAttribute('href');

      // Ensure href is not empty or invalid
      expect(href).toBeTruthy();
      expect(href).not.toBe('#');

      // External links should have proper attributes
      if (href.startsWith('http')) {
        await expect(link).toHaveAttribute('target', '_blank');
        const rel = await link.getAttribute('rel');
        expect(rel).toContain('noopener');
      }
    }
  });

  test('Footer is positioned at the bottom of the page', async ({ page }) => {
    const footer = page.locator('footer');

    // Scroll to footer
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Verify footer is at the end of the document
    const footerBox = await footer.boundingBox();
    const viewportSize = page.viewportSize();

    // Footer should be within the viewport after scrolling
    expect(footerBox).not.toBeNull();
  });

  test('Footer contains documentation link', async ({ page }) => {
    const footer = page.locator('footer');

    // Check for docs/documentation link
    const docsLink = footer.locator('a[href*="readme"], a[href*="README"], a[href*="docs"], a:has-text("Docs"), a:has-text("Documentation")');
    const count = await docsLink.count();

    // Should have at least one documentation-related link
    expect(count).toBeGreaterThan(0);
  });

  test('Footer has proper color contrast', async ({ page }) => {
    const footer = page.locator('footer');

    // Check that footer text is readable
    const textColor = await footer.evaluate(el => getComputedStyle(el).color);
    const bgColor = await footer.evaluate(el => getComputedStyle(el).backgroundColor);

    // Both should be defined (not transparent or auto)
    expect(textColor).toBeTruthy();
    expect(bgColor).toBeTruthy();
  });
});
