// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: Footer Content
 * Scenario: Verify footer contains GitHub link, licensing information, and version info
 * Per REQ-9: Footer must contain project links and licensing
 */

test.describe('Footer Content', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage before each test
    await page.goto('/');
  });

  /**
   * Test Case 1: Check footer for GitHub link
   * Input: Check footer for GitHub link
   * Expected: GitHub repository link is present in footer
   */
  test('TC1: GitHub repository link is present in footer', async ({ page }) => {
    // Verify the footer exists
    const footer = page.locator('[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Verify the GitHub link exists in the footer
    const githubLink = page.locator('[data-testid="footer-github-link"]');
    await expect(githubLink).toBeVisible();

    // Verify the link text contains "GitHub"
    const linkText = await githubLink.textContent();
    expect(linkText.toLowerCase()).toContain('github');

    // Verify the href points to GitHub
    const href = await githubLink.getAttribute('href');
    expect(href).toContain('github.com');
    expect(href).toContain('mirdb');
  });

  /**
   * Test Case 2: Check footer for license information
   * Input: Check footer for license information
   * Expected: MIT/Apache license information is displayed
   */
  test('TC2: MIT/Apache license information is displayed', async ({ page }) => {
    // Verify the footer exists
    const footer = page.locator('[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Verify the license element exists
    const licenseElement = page.locator('[data-testid="footer-license"]');
    await expect(licenseElement).toBeVisible();

    // Get the license text
    const licenseText = await licenseElement.textContent();

    // Verify the license mentions MIT and/or Apache
    const hasMIT = licenseText.toLowerCase().includes('mit');
    const hasApache = licenseText.toLowerCase().includes('apache');
    expect(hasMIT || hasApache).toBe(true);

    // Verify specific content "MIT / Apache 2.0"
    expect(licenseText).toContain('MIT');
    expect(licenseText).toContain('Apache');
  });

  /**
   * Test Case 3: Check footer for version info
   * Input: Check footer for version info
   * Expected: Version information is displayed in footer
   */
  test('TC3: Version information is displayed in footer', async ({ page }) => {
    // Verify the footer exists
    const footer = page.locator('[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Verify the version element exists
    const versionElement = page.locator('[data-testid="footer-version"]');
    await expect(versionElement).toBeVisible();

    // Get the version text
    const versionText = await versionElement.textContent();

    // Verify it contains version information (word "version" or version pattern like x.x.x)
    const hasVersionWord = versionText.toLowerCase().includes('version');
    const hasVersionPattern = /\d+\.\d+\.\d+/.test(versionText);
    expect(hasVersionWord || hasVersionPattern).toBe(true);
  });

  /**
   * Test Case 4: Test footer GitHub link
   * Input: Test footer GitHub link
   * Expected: Clicking GitHub link opens correct repository
   */
  test('TC4: Clicking GitHub link opens correct repository', async ({ page, context }) => {
    // Verify the footer GitHub link exists
    const githubLink = page.locator('[data-testid="footer-github-link"]');
    await expect(githubLink).toBeVisible();

    // Verify it opens in a new tab (target="_blank")
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify it has rel="noopener noreferrer" for security
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');

    // Verify the href points to the correct repository
    const href = await githubLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');

    // Test that clicking opens a new page
    const [newPage] = await Promise.all([
      context.waitForEvent('page'),
      githubLink.click()
    ]);

    // Verify the new page URL contains the correct GitHub repository
    const newPageUrl = newPage.url();
    expect(newPageUrl).toContain('github.com');
    expect(newPageUrl).toContain('mirdb');
  });
});
