// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

// Base URL for the static HTML file
const BASE_URL = 'file://' + path.resolve(__dirname, '../dist/index.html');

test.describe('Footer Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(BASE_URL);
  });

  test('Test Case 1: GitHub repository link is present and opens in new tab', async ({ page }) => {
    // Navigate to footer section
    const footer = page.locator('[data-testid="footer-section"]');
    await expect(footer).toBeVisible();

    // Check GitHub repository link
    const githubLink = page.locator('[data-testid="footer-github-link"]');
    await expect(githubLink).toBeVisible();

    // Verify it contains GitHub text
    await expect(githubLink).toContainText('GitHub');

    // Verify it links to the MirDB repository
    await expect(githubLink).toHaveAttribute('href', /github\.com.*mirdb/);

    // Verify it opens in a new tab
    await expect(githubLink).toHaveAttribute('target', '_blank');
    await expect(githubLink).toHaveAttribute('rel', /noopener/);
  });

  test('Test Case 2: Documentation link is present and opens in new tab', async ({ page }) => {
    // Navigate to footer section
    const footer = page.locator('[data-testid="footer-section"]');
    await expect(footer).toBeVisible();

    // Check documentation link
    const docsLink = page.locator('[data-testid="footer-docs-link"]');
    await expect(docsLink).toBeVisible();

    // Verify it contains Documentation text
    await expect(docsLink).toContainText('Documentation');

    // Verify it has an href attribute (links to docs)
    const href = await docsLink.getAttribute('href');
    expect(href).toBeTruthy();

    // Verify it opens in a new tab
    await expect(docsLink).toHaveAttribute('target', '_blank');
    await expect(docsLink).toHaveAttribute('rel', /noopener/);
  });

  test('Test Case 3: License information (MIT/Apache) is displayed', async ({ page }) => {
    // Navigate to footer section
    const footer = page.locator('[data-testid="footer-section"]');
    await expect(footer).toBeVisible();

    // Check license information
    const licenseInfo = page.locator('[data-testid="footer-license"]');
    await expect(licenseInfo).toBeVisible();

    // Get the license text
    const licenseText = await licenseInfo.textContent();

    // Verify it mentions MIT or Apache license
    expect(licenseText).toBeTruthy();
    const hasMIT = licenseText.toLowerCase().includes('mit');
    const hasApache = licenseText.toLowerCase().includes('apache');
    expect(hasMIT || hasApache).toBeTruthy();
  });

  test('Test Case 4: Project status indicator is visible in footer', async ({ page }) => {
    // Navigate to footer section
    const footer = page.locator('[data-testid="footer-section"]');
    await expect(footer).toBeVisible();

    // Check project status indicator
    const statusIndicator = page.locator('[data-testid="footer-status"]');
    await expect(statusIndicator).toBeVisible();

    // Get the status text
    const statusText = await statusIndicator.textContent();

    // Verify it contains status information (e.g., "Active Development", "Stable", etc.)
    expect(statusText).toBeTruthy();
    expect(statusText.length).toBeGreaterThan(0);
  });
});
