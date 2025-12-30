// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Footer Information - Required Links and Project Information', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Footer element exists with semantic <footer> tag', async ({ page }) => {
    // Test Case 1: Query for footer element
    // Expected: Footer element exists with semantic <footer> tag

    // Verify footer element exists
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Verify there is exactly one footer element
    const footerCount = await footer.count();
    expect(footerCount).toBe(1);

    // Verify the footer has content
    const footerText = await footer.textContent();
    expect(footerText).toBeTruthy();
    expect(footerText.trim().length).toBeGreaterThan(0);

    // Verify footer has the expected class for styling
    const footerClass = await footer.getAttribute('class');
    expect(footerClass).toContain('footer');
  });

  test('TC2: Footer contains link to GitHub repository', async ({ page }) => {
    // Test Case 2: Query footer for GitHub link
    // Expected: Footer contains link to GitHub repository

    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Find GitHub link within footer
    const githubLink = footer.locator('a[href*="github.com"]').first();
    await expect(githubLink).toBeVisible();

    // Verify the link text indicates GitHub
    const linkText = await githubLink.textContent();
    expect(linkText?.toLowerCase()).toMatch(/github/i);

    // Verify the link points to the MirDB repository
    const href = await githubLink.getAttribute('href');
    expect(href).toContain('github.com');
    expect(href).toContain('mirdb');

    // Verify the link opens in a new tab with security attributes
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  test('TC3: License information is present in footer', async ({ page }) => {
    // Test Case 3: Query footer for license text or link
    // Expected: License information is present in footer

    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Check for license link in footer
    const licenseLink = footer.locator('a').filter({
      hasText: /license/i
    });

    const licenseCount = await licenseLink.count();

    if (licenseCount > 0) {
      // License link exists
      await expect(licenseLink.first()).toBeVisible();

      const href = await licenseLink.first().getAttribute('href');
      // License link should point to LICENSE file, repository, or crates.io
      expect(href).toMatch(/license|github\.com|crates\.io/i);
    } else {
      // Check if license text is present in footer content
      const footerText = await footer.textContent();
      const hasLicenseInfo = footerText?.toLowerCase().match(/license|mit|apache|gpl|bsd|isc/i);
      expect(hasLicenseInfo).toBeTruthy();
    }
  });

  test('TC4: Version number or status indicator is present in footer', async ({ page }) => {
    // Test Case 4: Query footer for version or project status
    // Expected: Version number or status indicator is present

    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    const footerText = await footer.textContent();

    // Check for version number pattern (e.g., v1.0.0, 1.0, version 1.0)
    const hasVersionNumber = footerText?.match(/v?\d+\.\d+(\.\d+)?/i);

    // Check for status indicators (e.g., "Alpha", "Beta", "Stable", "Active Development")
    const hasStatusIndicator = footerText?.toLowerCase().match(
      /alpha|beta|stable|released?|development|active|production|preview|early access/i
    );

    // Check for explicit version or status labels
    const hasVersionLabel = footerText?.toLowerCase().match(/version|status|release/i);

    // At least one of these should be present
    const hasVersionOrStatus = hasVersionNumber || hasStatusIndicator || hasVersionLabel;

    expect(
      hasVersionOrStatus,
      `Footer should contain version number or status indicator. Footer text: "${footerText}"`
    ).toBeTruthy();
  });

  test('TC5: Footer contains documentation link', async ({ page }) => {
    // Additional test: Verify documentation link in footer
    // Expected: Documentation link exists and is accessible

    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Find documentation link within footer
    const docLink = footer.locator('a').filter({
      hasText: /documentation|docs|readme/i
    });

    await expect(docLink.first()).toBeVisible();

    const href = await docLink.first().getAttribute('href');
    expect(href).toBeTruthy();

    // Documentation should link to either GitHub README or a docs site
    expect(href).toMatch(/github\.com.*readme|docs\.|documentation/i);

    // Verify the link opens in a new tab with security attributes
    const target = await docLink.first().getAttribute('target');
    expect(target).toBe('_blank');

    const rel = await docLink.first().getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  test('Footer links are within a nav element for accessibility', async ({ page }) => {
    // Accessibility check: Footer links should be within a nav element
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Check if there's a nav element within footer
    const footerNav = footer.locator('nav');
    const navCount = await footerNav.count();

    expect(navCount).toBeGreaterThanOrEqual(1);

    // Verify the nav contains the footer links
    const navLinks = await footerNav.locator('a').count();
    expect(navLinks).toBeGreaterThan(0);
  });

  test('Footer contains copyright notice', async ({ page }) => {
    // Verify copyright information is present
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    const footerText = await footer.textContent();

    // Check for copyright symbol or text
    const hasCopyright = footerText?.match(/©|\(c\)|copyright/i);
    expect(hasCopyright, 'Footer should contain copyright notice').toBeTruthy();

    // Check for year (either current year or a range)
    const hasYear = footerText?.match(/20\d{2}/);
    expect(hasYear, 'Footer should contain a year').toBeTruthy();
  });
});
