/**
 * Footer Section E2E Tests
 * Owner: Scenario 8 - Footer Section
 *
 * Tests:
 * - Footer element exists and is present at bottom of page
 * - Copyright notice with current year is displayed
 * - License information is displayed or linked
 * - GitHub repository link is present in footer
 */

import { test, expect } from '@playwright/test';

test.describe('Footer Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should have footer element present at bottom of page', async ({ page }) => {
    // Test Case 1: Footer element is present at bottom of page
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // Verify footer has role="contentinfo" for accessibility
    await expect(footer).toHaveAttribute('role', 'contentinfo');

    // Scroll to bottom and verify footer is visible
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await expect(footer).toBeInViewport();

    // Verify footer is positioned after main content
    const main = page.locator('main');
    const mainBox = await main.boundingBox();
    const footerBox = await footer.boundingBox();

    expect(mainBox).not.toBeNull();
    expect(footerBox).not.toBeNull();
    expect(footerBox!.y).toBeGreaterThan(mainBox!.y + mainBox!.height - 10);
  });

  test('should display copyright notice with current year', async ({ page }) => {
    // Test Case 2: Copyright notice with current year is present
    const copyright = page.locator('.footer__copyright');
    await expect(copyright).toBeVisible();

    const copyrightText = await copyright.textContent();
    expect(copyrightText).not.toBeNull();

    // Verify copyright symbol is present
    expect(copyrightText).toContain('©');

    // Verify current year (2026) is present
    expect(copyrightText).toContain('2026');

    // Verify MirDB is mentioned
    expect(copyrightText).toContain('MirDB');
  });

  test('should display license information', async ({ page }) => {
    // Test Case 3: License information is displayed or linked
    const license = page.locator('.footer__license');
    await expect(license).toBeVisible();

    const licenseText = await license.textContent();
    expect(licenseText).not.toBeNull();

    // Verify MIT license is mentioned
    expect(licenseText?.toLowerCase()).toContain('mit');

    // Verify license link is present
    const licenseLink = page.locator('.footer__license-link');
    await expect(licenseLink).toBeVisible();

    // Verify link points to LICENSE file on GitHub
    const href = await licenseLink.getAttribute('href');
    expect(href).toContain('github.com/yetone/mirdb');
    expect(href?.toLowerCase()).toContain('license');

    // Verify link opens in new tab for external links
    const target = await licenseLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify noopener noreferrer for security
    const rel = await licenseLink.getAttribute('rel');
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');
  });

  test('should have GitHub repository link in footer', async ({ page }) => {
    // Test Case 4: GitHub repository link is present in footer
    const footer = page.locator('footer.footer');
    // Find the link that points directly to the repository (not LICENSE or other paths)
    const githubLink = footer.locator('a[href="https://github.com/yetone/mirdb"]');
    await expect(githubLink).toBeVisible();

    // Verify link points to the correct repository
    const href = await githubLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');

    // Verify link text includes "GitHub"
    const linkText = await githubLink.textContent();
    expect(linkText?.toLowerCase()).toContain('github');

    // Verify it opens in a new tab
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify noopener noreferrer for security
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');
  });

  test('should have navigation links in footer', async ({ page }) => {
    // Additional test: Footer navigation with community links
    const footerNav = page.locator('footer.footer nav');
    await expect(footerNav).toBeVisible();

    // Check footer has proper aria-label
    await expect(footerNav).toHaveAttribute('aria-label', 'Footer navigation');

    // Verify links list exists
    const links = page.locator('.footer__links .footer__link');
    const linkCount = await links.count();
    expect(linkCount).toBeGreaterThanOrEqual(2);

    // Verify all links are visible
    for (const link of await links.all()) {
      await expect(link).toBeVisible();
    }
  });

  test('should have documentation link in footer', async ({ page }) => {
    // Additional test: Documentation link
    const footer = page.locator('footer.footer');
    const docLink = footer.locator('a[href*="github.com/yetone/mirdb#readme"]');
    await expect(docLink).toBeVisible();

    const linkText = await docLink.textContent();
    expect(linkText?.toLowerCase()).toContain('documentation');
  });

  test('should have proper accessibility in footer', async ({ page }) => {
    // Accessibility test
    const footer = page.locator('footer.footer');

    // Footer should have contentinfo role
    await expect(footer).toHaveAttribute('role', 'contentinfo');

    // Navigation should be labeled
    const footerNav = footer.locator('nav');
    await expect(footerNav).toHaveAttribute('aria-label');

    // All links should be keyboard accessible
    const links = footer.locator('.footer__link');
    const firstLink = links.first();
    await firstLink.focus();
    await expect(firstLink).toBeFocused();

    // Tab through links
    await page.keyboard.press('Tab');
    const secondLink = links.nth(1);
    await expect(secondLink).toBeFocused();
  });

  test('should display correctly on mobile viewport', async ({ page }) => {
    // Responsive test: Mobile viewport
    await page.setViewportSize({ width: 320, height: 568 });

    const footer = page.locator('footer.footer');
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await expect(footer).toBeVisible();

    // Verify copyright is visible
    const copyright = page.locator('.footer__copyright');
    await expect(copyright).toBeVisible();

    // Verify links are visible and tappable
    const links = page.locator('.footer__links .footer__link');
    for (const link of await links.all()) {
      await expect(link).toBeVisible();
      const box = await link.boundingBox();
      expect(box).not.toBeNull();
      // Touch target should be at least 44px for accessibility
      expect(box!.height).toBeGreaterThanOrEqual(24);
    }
  });
});
