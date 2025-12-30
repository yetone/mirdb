// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Navigation Links Functionality', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: At least one GitHub link exists', async ({ page }) => {
    // Test Case 1: Query for anchor elements with href containing 'github'
    // Expected: At least one GitHub link exists
    const githubLinks = page.locator('a[href*="github"]');
    const count = await githubLinks.count();

    expect(count).toBeGreaterThanOrEqual(1);

    // Verify at least one GitHub link is visible
    const firstGithubLink = githubLinks.first();
    await expect(firstGithubLink).toBeVisible();
  });

  test('TC2: GitHub link navigates to valid GitHub repository URL', async ({ page }) => {
    // Test Case 2: Click GitHub link and verify destination
    // Expected: Link navigates to valid GitHub repository URL
    const githubLink = page.locator('a[href*="github.com"]').first();
    await expect(githubLink).toBeVisible();

    const href = await githubLink.getAttribute('href');

    // Verify the URL is a valid GitHub repository URL
    expect(href).toMatch(/^https:\/\/github\.com\/[a-zA-Z0-9_-]+\/[a-zA-Z0-9_-]+/);

    // Verify it points to the MirDB repository
    expect(href).toContain('mirdb');
  });

  test('TC3: Documentation link exists and is accessible', async ({ page }) => {
    // Test Case 3: Query for documentation link in navigation
    // Expected: Documentation link exists and is accessible
    const docLink = page.locator('a').filter({
      hasText: /documentation|docs|readme/i
    }).first();

    await expect(docLink).toBeVisible();

    const href = await docLink.getAttribute('href');

    // Verify the documentation link points to a valid URL
    expect(href).toBeTruthy();
    // Documentation should link to either GitHub README or a docs site
    expect(href).toMatch(/github\.com.*readme|docs\./i);
  });

  test('TC4: License information is present in footer', async ({ page }) => {
    // Test Case 4: Query footer for license information
    // Expected: License information is present in footer
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Check for license link or text in footer
    const licenseElement = footer.locator('a').filter({
      hasText: /license/i
    });

    const licenseCount = await licenseElement.count();

    if (licenseCount > 0) {
      // License link exists
      await expect(licenseElement.first()).toBeVisible();

      const href = await licenseElement.first().getAttribute('href');
      // License link should point to LICENSE file or repository root (if no LICENSE file exists)
      expect(href).toMatch(/license|github\.com/i);
    } else {
      // Check if license text is present in footer
      const footerText = await footer.textContent();
      expect(footerText?.toLowerCase()).toMatch(/license|mit|apache|gpl/i);
    }
  });

  test('TC5: External links have target="_blank" and rel="noopener"', async ({ page }) => {
    // Test Case 5: Verify all external links have target='_blank' and rel='noopener'
    // Expected: External links open in new tab with security attributes

    // Get all external links (links starting with http that point to external domains)
    const externalLinks = page.locator('a[href^="http"]');
    const count = await externalLinks.count();

    expect(count).toBeGreaterThan(0);

    // Check each external link for proper attributes
    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      const href = await link.getAttribute('href');

      // Skip if it's an internal link (same origin)
      if (href && !href.includes('localhost') && !href.includes('127.0.0.1')) {
        const target = await link.getAttribute('target');
        const rel = await link.getAttribute('rel');

        // External links should open in new tab
        expect(target).toBe('_blank');

        // External links should have noopener for security
        expect(rel).toContain('noopener');
      }
    }
  });
});
