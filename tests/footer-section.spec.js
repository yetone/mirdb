const { test, expect } = require('@playwright/test');

/**
 * Footer Section Tests
 * Scenario: Validate the footer contains project links, license information, and attribution
 */

test.describe('Footer Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Page contains a footer element', async ({ page }) => {
    // Check that a footer element exists on the page
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Verify footer is at the bottom of the page (after main content)
    const footerBoundingBox = await footer.boundingBox();
    expect(footerBoundingBox).not.toBeNull();
    expect(footerBoundingBox.y).toBeGreaterThan(0);
  });

  test('Test Case 2: Footer contains link to https://github.com/yetone/mirdb', async ({ page }) => {
    // Check that footer contains a GitHub link
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Find the GitHub link within footer
    const githubLink = footer.locator('a[href="https://github.com/yetone/mirdb"]');
    await expect(githubLink).toBeVisible();

    // Verify the link href
    const href = await githubLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');
  });

  test('Test Case 3: Footer displays license information or link to license', async ({ page }) => {
    // Check that footer contains license information
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Get footer text content
    const footerText = await footer.textContent();

    // Check for license mention (MIT, LICENSE, etc.)
    const hasLicenseInfo = footerText.toLowerCase().includes('license') ||
                          footerText.toLowerCase().includes('mit') ||
                          footerText.toLowerCase().includes('apache') ||
                          footerText.toLowerCase().includes('bsd') ||
                          footerText.toLowerCase().includes('gpl');

    // Alternatively, check for a link to license
    const licenseLink = footer.locator('a').filter({ hasText: /license/i });
    const hasLicenseLink = await licenseLink.count() > 0;

    // Either license info text or license link should be present
    expect(hasLicenseInfo || hasLicenseLink).toBe(true);
  });
});
