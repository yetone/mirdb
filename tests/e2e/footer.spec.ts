/**
 * Footer Tests
 * Owner: Scenario 8 - Footer with Credits and Links
 *
 * Test cases:
 * - Footer element exists
 * - License information present
 * - Author credits (yetone)
 * - Copyright year
 * - GitHub Issues link
 */

import { test, expect } from '@playwright/test';

test.describe('Footer with Credits and Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC1: Footer element or section with appropriate semantic HTML exists', async ({ page }) => {
    // Verify footer element exists using semantic HTML
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // Verify footer has content (not empty)
    const footerContent = await footer.textContent();
    expect(footerContent).toBeTruthy();
    expect(footerContent!.trim().length).toBeGreaterThan(0);
  });

  test('TC2: License text (MIT, Apache, etc.) or link to LICENSE is present', async ({ page }) => {
    const footer = page.locator('footer.footer');

    // Check for license text or link
    const licenseText = await footer.textContent();
    const hasLicenseText = licenseText!.toLowerCase().includes('mit') ||
                           licenseText!.toLowerCase().includes('apache') ||
                           licenseText!.toLowerCase().includes('license');

    const licenseLink = footer.locator('a[href*="LICENSE"], a[href*="license"], a:has-text("License"), a:has-text("MIT")');
    const hasLicenseLink = await licenseLink.count() > 0;

    expect(hasLicenseText || hasLicenseLink).toBeTruthy();
  });

  test('TC3: Author name yetone or email reference is present', async ({ page }) => {
    const footer = page.locator('footer.footer');

    // Check for author credits
    const footerText = await footer.textContent();
    const hasAuthorName = footerText!.toLowerCase().includes('yetone');
    const hasAuthorEmail = footerText!.includes('yetoneful@gmail.com');

    expect(hasAuthorName || hasAuthorEmail).toBeTruthy();
  });

  test('TC4: Copyright notice with year (2024 or 2025) is present', async ({ page }) => {
    const footer = page.locator('footer.footer');

    // Check for copyright notice with year
    const footerText = await footer.textContent();
    const hasCopyright = footerText!.includes('2024') ||
                         footerText!.includes('2025') ||
                         footerText!.includes('2026');
    const hasCopyrightSymbol = footerText!.includes('©') ||
                                footerText!.toLowerCase().includes('copyright');

    expect(hasCopyright).toBeTruthy();
    expect(hasCopyrightSymbol).toBeTruthy();
  });

  test('TC5: Link to GitHub Issues for community support is present', async ({ page }) => {
    const footer = page.locator('footer.footer');

    // Check for GitHub Issues link
    const issuesLink = footer.locator('a[href*="github.com/yetone/mirdb/issues"]');
    await expect(issuesLink).toBeVisible();

    // Verify the link is clickable
    const href = await issuesLink.getAttribute('href');
    expect(href).toContain('github.com/yetone/mirdb/issues');
  });

  test('Footer is properly styled and visible at page bottom', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator('footer.footer');
    await footer.scrollIntoViewIfNeeded();

    // Verify footer is visible
    await expect(footer).toBeVisible();

    // Verify footer has proper background styling
    const footerBox = await footer.boundingBox();
    expect(footerBox).toBeTruthy();
    expect(footerBox!.height).toBeGreaterThan(50);
  });
});
