// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Scenario 7: Footer and External Links
 * Tests for verifying the footer displays GitHub link, license info, and credits.
 */

test.describe('Footer and External Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Footer presence at the bottom of the page
   * Input: Check footer presence
   * Expected: Footer is present at the bottom of the page
   */
  test('TC1: Footer is present at the bottom of the page', async ({ page }) => {
    // Check footer element exists
    const footer = page.locator('footer[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Verify footer is a semantic footer element
    await expect(page.locator('footer')).toBeVisible();

    // Scroll to bottom and verify footer is visible
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await expect(footer).toBeInViewport();
  });

  /**
   * Test Case 2: GitHub link presence
   * Input: Check GitHub link
   * Expected: Link to GitHub repository (https://github.com/yetone/mirdb) is present
   */
  test('TC2: GitHub link is present with correct URL', async ({ page }) => {
    const footer = page.locator('footer[data-testid="footer"]');
    const githubLink = footer.locator('[data-testid="github-link"]');

    // Verify GitHub link exists and is visible
    await expect(githubLink).toBeVisible();

    // Verify the href points to the correct repository
    const href = await githubLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');

    // Verify the link text contains "GitHub"
    await expect(githubLink).toContainText('GitHub');
  });

  /**
   * Test Case 3: GitHub link opens in new tab
   * Input: Click GitHub link
   * Expected: Link opens in new tab and leads to correct repository
   */
  test('TC3: GitHub link opens in new tab with security attributes', async ({ page }) => {
    const footer = page.locator('footer[data-testid="footer"]');
    const githubLink = footer.locator('[data-testid="github-link"]');

    // Verify target="_blank" for opening in new tab
    await expect(githubLink).toHaveAttribute('target', '_blank');

    // Verify rel attribute contains noopener for security
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');

    // Verify the href is correct
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
  });

  /**
   * Test Case 4: MIT license is mentioned
   * Input: Check license info
   * Expected: MIT license is mentioned in footer
   */
  test('TC4: MIT license is mentioned in footer', async ({ page }) => {
    const footer = page.locator('footer[data-testid="footer"]');
    const licenseInfo = footer.locator('[data-testid="license-info"]');

    // Verify license element exists
    await expect(licenseInfo).toBeVisible();

    // Verify MIT license is mentioned
    await expect(licenseInfo).toContainText('MIT');
    await expect(licenseInfo).toContainText('License');
  });

  /**
   * Test Case 5: Credits/attribution section is present
   * Input: Check credits presence
   * Expected: Credits or attribution section is present
   */
  test('TC5: Credits or attribution section is present', async ({ page }) => {
    const footer = page.locator('footer[data-testid="footer"]');
    const credits = footer.locator('[data-testid="credits"]');

    // Verify credits section exists
    await expect(credits).toBeVisible();

    // Verify it contains attribution text (e.g., "Created by" or author name)
    const creditsText = await credits.textContent();
    expect(creditsText).toBeTruthy();
    expect(creditsText.length).toBeGreaterThan(0);

    // Check for author attribution
    await expect(credits).toContainText('yetone');
  });

  /**
   * Additional test: Footer has proper styling
   */
  test('Footer has proper background and text styling', async ({ page }) => {
    const footer = page.locator('footer[data-testid="footer"]');

    // Verify footer has background color class
    await expect(footer).toHaveClass(/bg-gray-800/);

    // Verify footer has text color class
    await expect(footer).toHaveClass(/text-white/);
  });

  /**
   * Additional test: Copyright information is present
   */
  test('Copyright information is present', async ({ page }) => {
    const footer = page.locator('footer[data-testid="footer"]');
    const copyright = footer.locator('[data-testid="copyright"]');

    // Verify copyright section exists
    await expect(copyright).toBeVisible();

    // Verify it contains copyright symbol or text
    const copyrightText = await copyright.textContent();
    expect(copyrightText).toMatch(/\u00A9|copyright|MirDB/i);
  });
});
