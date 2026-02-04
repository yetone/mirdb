/**
 * Footer E2E Tests
 * Owner: Scenario 10 - Footer & Navigation
 *
 * Test coverage:
 * - Footer section visibility and content
 * - GitHub link behavior (opens in new tab)
 * - External link security attributes (noopener noreferrer)
 * - License information display
 * - Mobile responsive layout
 */

const { test, expect } = require('@playwright/test');

test.describe('Footer and External Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Footer displays with GitHub link, license info, and copyright notice', async ({ page }) => {
    // Verify footer section exists and is visible
    const footer = page.locator('[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Verify GitHub link is present
    const githubLink = page.locator('[data-testid="github-link"]');
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toContainText('GitHub');

    // Verify license link is present
    const licenseLink = page.locator('[data-testid="license-link"]');
    await expect(licenseLink).toBeVisible();
    await expect(licenseLink).toContainText('License');

    // Verify license info text is present
    const licenseInfo = page.locator('[data-testid="license-info"]');
    await expect(licenseInfo).toBeVisible();
    await expect(licenseInfo).toContainText('MIT License');

    // Verify copyright notice is present
    const copyright = page.locator('[data-testid="copyright"]');
    await expect(copyright).toBeVisible();
    await expect(copyright).toContainText('MirDB');
    await expect(copyright).toContainText('Persistent Key-Value Store');
  });

  test('TC2: GitHub repository link opens github.com/yetone/mirdb in new tab', async ({ page, context }) => {
    // Find the GitHub link
    const githubLink = page.locator('[data-testid="github-link"]');
    await expect(githubLink).toBeVisible();

    // Verify href points to the correct GitHub repository
    const href = await githubLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');

    // Verify it opens in new tab
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify rel attribute for security
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');

    // Test that clicking opens a new page
    const pagePromise = context.waitForEvent('page');
    await githubLink.click();
    const newPage = await pagePromise;

    // Verify the new page URL
    await newPage.waitForLoadState('domcontentloaded');
    expect(newPage.url()).toContain('github.com/yetone/mirdb');

    await newPage.close();
  });

  test('TC3: External links have rel=noopener noreferrer attribute', async ({ page }) => {
    // Get all external links in the footer
    const footerLinks = page.locator('.footer__link');
    const linkCount = await footerLinks.count();

    expect(linkCount).toBeGreaterThan(0);

    // Check each link has proper security attributes
    for (let i = 0; i < linkCount; i++) {
      const link = footerLinks.nth(i);
      const href = await link.getAttribute('href');

      // Only check external links (starting with http)
      if (href && href.startsWith('http')) {
        const rel = await link.getAttribute('rel');
        const target = await link.getAttribute('target');

        // Verify security attributes
        expect(rel, `Link to ${href} should have noopener`).toContain('noopener');
        expect(rel, `Link to ${href} should have noreferrer`).toContain('noreferrer');
        expect(target, `Link to ${href} should open in new tab`).toBe('_blank');
      }
    }
  });

  test('TC4: License information is displayed and links to license file', async ({ page, context }) => {
    // Verify license link is present with correct text
    const licenseLink = page.locator('[data-testid="license-link"]');
    await expect(licenseLink).toBeVisible();
    await expect(licenseLink).toContainText('MIT License');

    // Verify license link points to the correct URL
    const href = await licenseLink.getAttribute('href');
    expect(href).toContain('github.com/yetone/mirdb');
    expect(href).toContain('LICENSE');

    // Verify the license info text is displayed
    const licenseInfo = page.locator('[data-testid="license-info"]');
    await expect(licenseInfo).toBeVisible();
    await expect(licenseInfo).toContainText('MIT License');

    // Verify it opens in new tab with security attributes
    const target = await licenseLink.getAttribute('target');
    const rel = await licenseLink.getAttribute('rel');
    expect(target).toBe('_blank');
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');

    // Test clicking the license link
    const pagePromise = context.waitForEvent('page');
    await licenseLink.click();
    const newPage = await pagePromise;

    await newPage.waitForLoadState('domcontentloaded');
    expect(newPage.url()).toContain('LICENSE');

    await newPage.close();
  });

  test('TC5: Footer content stacks appropriately on mobile viewport', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    // Verify footer is visible on mobile
    const footer = page.locator('[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Verify all links are visible
    const githubLink = page.locator('[data-testid="github-link"]');
    const licenseLink = page.locator('[data-testid="license-link"]');
    const issuesLink = page.locator('[data-testid="issues-link"]');

    await expect(githubLink).toBeVisible();
    await expect(licenseLink).toBeVisible();
    await expect(issuesLink).toBeVisible();

    // Verify copyright is readable
    const copyright = page.locator('[data-testid="copyright"]');
    await expect(copyright).toBeVisible();

    // Get link positions to verify stacking
    const footerLinks = page.locator('.footer__links');
    const linksStyle = await footerLinks.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        flexDirection: style.flexDirection,
        display: style.display
      };
    });

    // On mobile, links should be stacked vertically (flex-direction: column)
    expect(linksStyle.display).toBe('flex');
    expect(linksStyle.flexDirection).toBe('column');

    // Verify links don't overflow the viewport
    const linksBBox = await footerLinks.boundingBox();
    if (linksBBox) {
      expect(linksBBox.width).toBeLessThanOrEqual(375);
    }
  });

  test('Footer has proper accessibility structure', async ({ page }) => {
    // Verify footer has role="contentinfo"
    const footer = page.locator('footer[role="contentinfo"]');
    await expect(footer).toBeVisible();

    // Verify footer has aria-label
    const ariaLabel = await footer.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel).toContain('footer');

    // Verify nav has aria-label
    const nav = footer.locator('nav');
    const navAriaLabel = await nav.getAttribute('aria-label');
    expect(navAriaLabel).toBeTruthy();

    // Verify external links have descriptive aria-labels
    const githubLink = page.locator('[data-testid="github-link"]');
    const githubAriaLabel = await githubLink.getAttribute('aria-label');
    expect(githubAriaLabel).toContain('GitHub');
    expect(githubAriaLabel).toContain('new tab');
  });

  test('Footer links are keyboard accessible', async ({ page }) => {
    // Navigate to the footer using keyboard
    await page.goto('/');

    // Tab through the page to reach footer links
    const githubLink = page.locator('[data-testid="github-link"]');
    const licenseLink = page.locator('[data-testid="license-link"]');
    const issuesLink = page.locator('[data-testid="issues-link"]');

    // Focus on first footer link
    await githubLink.focus();
    await expect(githubLink).toBeFocused();

    // Tab to next link
    await page.keyboard.press('Tab');
    await expect(licenseLink).toBeFocused();

    // Tab to issues link
    await page.keyboard.press('Tab');
    await expect(issuesLink).toBeFocused();
  });

  test('Footer links have minimum touch target size', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    const footerLinks = page.locator('.footer__link');
    const linkCount = await footerLinks.count();

    for (let i = 0; i < linkCount; i++) {
      const link = footerLinks.nth(i);
      const boundingBox = await link.boundingBox();

      if (boundingBox) {
        // WCAG 2.1 AA requires minimum touch target of 44x44 pixels
        expect(boundingBox.height, `Link ${i} should have min height of 44px`).toBeGreaterThanOrEqual(44);
      }
    }
  });
});
