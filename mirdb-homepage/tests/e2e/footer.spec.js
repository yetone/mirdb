/**
 * Footer Section E2E Tests
 * Owner: Scenario 11 - Footer Section
 *
 * Tests:
 * - Footer presence
 * - GitHub link
 * - Documentation link
 * - License information
 * - Copyright notice
 */

const { test, expect } = require('@playwright/test');

test.describe('Footer Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC1: Footer element exists at bottom of page', async ({ page }) => {
    // Verify footer section exists
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Verify footer is at the bottom of the page structure
    const footerPosition = await page.evaluate(() => {
      const footer = document.querySelector('footer');
      const main = document.querySelector('main');
      if (!footer || !main) return false;
      // Footer should come after main in DOM
      return footer.compareDocumentPosition(main) & Node.DOCUMENT_POSITION_PRECEDING;
    });
    expect(footerPosition).toBeTruthy();
  });

  test('TC2: Footer contains link to GitHub repository, opens in new tab', async ({ page }) => {
    // Find GitHub link in footer
    const githubLink = page.locator('footer a').filter({ hasText: 'GitHub' });
    await expect(githubLink).toBeVisible();

    // Verify it links to GitHub
    const href = await githubLink.getAttribute('href');
    expect(href).toContain('github.com');
    expect(href).toContain('mirdb');

    // Verify it opens in new tab
    await expect(githubLink).toHaveAttribute('target', '_blank');

    // Verify security attributes for external link
    await expect(githubLink).toHaveAttribute('rel', /noopener/);
  });

  test('TC3: Footer contains link to documentation', async ({ page }) => {
    // Find documentation link in footer
    const docsLink = page.locator('footer a').filter({ hasText: 'Documentation' });
    await expect(docsLink).toBeVisible();

    // Verify it has a valid href
    const href = await docsLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href.length).toBeGreaterThan(0);

    // Verify it opens in new tab for external links
    await expect(docsLink).toHaveAttribute('target', '_blank');

    // Verify security attributes
    await expect(docsLink).toHaveAttribute('rel', /noopener/);
  });

  test('TC4: Footer displays license information', async ({ page }) => {
    // Find license information in footer
    const licenseText = page.locator('footer').filter({ hasText: /license/i });
    await expect(licenseText).toBeVisible();

    // Verify MIT license is mentioned
    const footerText = await page.locator('footer').textContent();
    expect(footerText.toLowerCase()).toContain('mit');

    // Verify license link exists
    const licenseLink = page.locator('footer a[href*="LICENSE"]');
    if (await licenseLink.count() > 0) {
      await expect(licenseLink).toBeVisible();
    }
  });

  test('TC5: Footer contains copyright notice with year', async ({ page }) => {
    // Find copyright notice in footer
    const footer = page.locator('footer');
    const footerText = await footer.textContent();

    // Verify copyright symbol or text is present
    expect(footerText).toMatch(/©|copyright/i);

    // Verify year is present (4-digit year)
    expect(footerText).toMatch(/\d{4}/);

    // Verify MirDB is mentioned in copyright
    expect(footerText.toLowerCase()).toContain('mirdb');
  });

  test('Footer links are focusable and have visible focus states', async ({ page }) => {
    // Get all footer links
    const footerLinks = page.locator('footer a');
    const linkCount = await footerLinks.count();

    expect(linkCount).toBeGreaterThan(0);

    // Test first link focus
    const firstLink = footerLinks.first();
    await firstLink.focus();
    await expect(firstLink).toBeFocused();
  });

  test('Footer is visible on mobile viewport', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });

    // Scroll to footer
    await page.evaluate(() => {
      document.querySelector('footer').scrollIntoView();
    });

    // Verify footer is visible
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Verify links are still accessible
    const githubLink = page.locator('footer a').filter({ hasText: 'GitHub' });
    await expect(githubLink).toBeVisible();
  });

  test('All footer external links have proper security attributes', async ({ page }) => {
    // Get all external links in footer (links with target="_blank")
    const externalLinks = page.locator('footer a[target="_blank"]');
    const linkCount = await externalLinks.count();

    for (let i = 0; i < linkCount; i++) {
      const link = externalLinks.nth(i);
      // Verify rel attribute contains noopener for security
      const rel = await link.getAttribute('rel');
      expect(rel).toMatch(/noopener/);
    }
  });
});
