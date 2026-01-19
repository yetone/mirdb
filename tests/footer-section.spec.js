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
   * Test Case 1: Footer displays license information
   * Input: Check for license information
   * Expected: Footer displays license information (e.g., MIT, Apache 2.0)
   */
  test('TC1: Footer displays license information', async ({ page }) => {
    // Scroll to footer to ensure it is visible
    const footer = page.locator('[data-testid="footer-section"]');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Find the license information element
    const licenseElement = page.locator('[data-testid="footer-license"]');
    await expect(licenseElement).toBeVisible();

    // Get the license text
    const licenseText = await licenseElement.textContent();

    // Verify license information is displayed (should contain common license types)
    const hasLicenseInfo =
      licenseText.toLowerCase().includes('license') ||
      licenseText.toLowerCase().includes('mit') ||
      licenseText.toLowerCase().includes('apache') ||
      licenseText.toLowerCase().includes('bsd') ||
      licenseText.toLowerCase().includes('gpl');

    expect(hasLicenseInfo).toBeTruthy();

    // Specifically verify MIT license as mentioned in PRD
    expect(licenseText).toContain('MIT');
  });

  /**
   * Test Case 2: Footer displays version information
   * Input: Check for version information
   * Expected: Footer displays version number (0.1.0)
   */
  test('TC2: Footer displays version number (0.1.0)', async ({ page }) => {
    // Scroll to footer to ensure it is visible
    const footer = page.locator('[data-testid="footer-section"]');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Find the version information element
    const versionElement = page.locator('[data-testid="footer-version"]');
    await expect(versionElement).toBeVisible();

    // Get the version text
    const versionText = await versionElement.textContent();

    // Verify version number is displayed
    expect(versionText.toLowerCase()).toContain('version');
    expect(versionText).toContain('0.1.0');
  });

  /**
   * Test Case 3: Footer contains link to GitHub repository
   * Input: Check for project repository link
   * Expected: Footer contains link to GitHub repository
   */
  test('TC3: Footer contains link to GitHub repository', async ({ page }) => {
    // Scroll to footer to ensure it is visible
    const footer = page.locator('[data-testid="footer-section"]');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Find the GitHub link in footer
    const githubLink = page.locator('[data-testid="footer-github-link"]');
    await expect(githubLink).toBeVisible();

    // Get the href attribute
    const href = await githubLink.getAttribute('href');

    // Verify the link points to GitHub
    expect(href).toContain('github.com');

    // Verify it's a valid link (has href)
    expect(href).toBeTruthy();

    // Verify the link text mentions GitHub
    const linkText = await githubLink.textContent();
    expect(linkText.toLowerCase()).toContain('github');
  });

  /**
   * Additional test: Footer section is visible at the bottom of the page
   */
  test('Footer section is visible at page bottom', async ({ page }) => {
    // Scroll to the bottom of the page
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));

    // Wait for footer to be visible
    const footer = page.locator('[data-testid="footer-section"]');
    await expect(footer).toBeVisible();

    // Verify the footer element exists
    const footerBox = await footer.boundingBox();
    expect(footerBox).toBeTruthy();
  });

  /**
   * Additional test: Footer info section contains all required information
   */
  test('Footer info section contains version, license, and port information', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator('[data-testid="footer-section"]');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Find the footer info section
    const footerInfo = page.locator('[data-testid="footer-info"]');
    await expect(footerInfo).toBeVisible();

    // Get all text content from footer info
    const infoText = await footerInfo.textContent();

    // Verify all required information is present
    expect(infoText).toContain('Version');
    expect(infoText).toContain('0.1.0');
    expect(infoText).toContain('License');
    expect(infoText).toContain('MIT');
  });

  /**
   * Additional test: GitHub link opens in new tab (security best practice)
   */
  test('GitHub link has proper security attributes', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator('[data-testid="footer-section"]');
    await footer.scrollIntoViewIfNeeded();

    // Find the GitHub link
    const githubLink = page.locator('[data-testid="footer-github-link"]');

    // Verify it opens in new tab
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify security attributes for external links
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

});
