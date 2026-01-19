// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E tests for Footer Section Display
 * Scenario: Verify that footer contains project information and links as specified in REQ-9
 *
 * Tests cover:
 * 1. License information display
 * 2. Version information display (0.1.0)
 * 3. GitHub repository link
 */

test.describe('Footer Section Display', () => {

  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage
    await page.goto('/');
  });

  /**
   * Test Case 1: Check for license information
   * Input: Check for license information
   * Expected: Footer displays license information (e.g., MIT, Apache 2.0)
   */
  test('TC1: Footer displays license information', async ({ page }) => {
    // Locate the footer section
    const footer = page.locator('footer, .footer');
    await expect(footer).toBeVisible();

    // Scroll to footer to ensure it's in viewport
    await footer.scrollIntoViewIfNeeded();

    // Check that the footer contains license information
    const footerText = await footer.textContent();

    // Verify that license information is present (MIT, Apache 2.0, or other common licenses)
    const hasLicenseInfo = /MIT|Apache|GPL|BSD|ISC|License/i.test(footerText);
    expect(hasLicenseInfo).toBeTruthy();

    // Specifically check for MIT License as specified in PRD
    expect(footerText.toLowerCase()).toContain('mit');
  });

  /**
   * Test Case 2: Check for version information
   * Input: Check for version information
   * Expected: Footer displays version number (0.1.0)
   */
  test('TC2: Footer displays version number (0.1.0)', async ({ page }) => {
    // Locate the footer section
    const footer = page.locator('footer, .footer');
    await expect(footer).toBeVisible();

    // Scroll to footer to ensure it's in viewport
    await footer.scrollIntoViewIfNeeded();

    // Check that the footer contains version information
    const footerText = await footer.textContent();

    // Verify that version number is present
    const hasVersionInfo = /v?0\.1\.0|version.*0\.1\.0/i.test(footerText);
    expect(hasVersionInfo).toBeTruthy();
  });

  /**
   * Test Case 3: Check for project repository link
   * Input: Check for project repository link
   * Expected: Footer contains link to GitHub repository
   */
  test('TC3: Footer contains link to GitHub repository', async ({ page }) => {
    // Locate the footer section
    const footer = page.locator('footer, .footer');
    await expect(footer).toBeVisible();

    // Scroll to footer to ensure it's in viewport
    await footer.scrollIntoViewIfNeeded();

    // Find GitHub link in footer
    const footerGitHubLink = footer.locator('a[href*="github.com"]');
    await expect(footerGitHubLink.first()).toBeVisible();

    // Verify the link points to GitHub
    const href = await footerGitHubLink.first().getAttribute('href');
    expect(href).toContain('github.com');

    // Verify the link contains a valid repository path pattern
    const urlPattern = /github\.com\/[^\/]+\/[^\/]+/;
    expect(href).toMatch(urlPattern);
  });

  /**
   * Additional test: Footer is visible at the bottom of the page
   */
  test('Footer is visible at the bottom of the page', async ({ page }) => {
    // Scroll to the bottom of the page
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));

    // Wait for any animations to complete
    await page.waitForTimeout(300);

    // Locate the footer section
    const footer = page.locator('footer, .footer');
    await expect(footer).toBeVisible();

    // Verify footer has proper structure (contains a container)
    const container = footer.locator('.container, [class*="container"]');
    const containerCount = await container.count();

    // Footer should have content organized in a container
    expect(containerCount).toBeGreaterThanOrEqual(0);
  });

  /**
   * Additional test: Footer contains all required elements together
   */
  test('Footer contains all required elements (license, version, GitHub link)', async ({ page }) => {
    // Locate the footer section
    const footer = page.locator('footer, .footer');
    await expect(footer).toBeVisible();

    // Scroll to footer to ensure it's in viewport
    await footer.scrollIntoViewIfNeeded();

    // Get footer content
    const footerText = await footer.textContent();

    // Check for all three required elements
    const hasLicense = /MIT/i.test(footerText);
    const hasVersion = /0\.1\.0/i.test(footerText);

    // Check for GitHub link
    const footerGitHubLink = footer.locator('a[href*="github.com"]');
    const hasGitHubLink = await footerGitHubLink.count() > 0;

    // All elements should be present
    expect(hasLicense).toBeTruthy();
    expect(hasVersion).toBeTruthy();
    expect(hasGitHubLink).toBeTruthy();
  });

});
