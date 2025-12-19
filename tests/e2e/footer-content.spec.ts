import { test, expect } from '@playwright/test';

test.describe('Footer Content', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Footer element exists with appropriate content
  test('TC1: Footer element exists with appropriate content', async ({ page }) => {
    // Query for footer element
    const footer = page.locator('footer');

    // Verify footer element exists and is visible
    await expect(footer).toBeVisible();

    // Verify footer has content
    const footerContent = await footer.textContent();
    expect(footerContent).toBeTruthy();
    expect(footerContent!.length).toBeGreaterThan(0);

    // Verify footer has role="contentinfo" for accessibility
    const footerRole = await footer.getAttribute('role');
    expect(footerRole).toBe('contentinfo');
  });

  // Test Case 2: Footer contains link to GitHub/source repository
  test('TC2: Footer contains link to source repository', async ({ page }) => {
    // Check footer for repository link
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Look for GitHub link within footer
    const githubLink = footer.locator('a[href*="github.com"]');
    await expect(githubLink.first()).toBeVisible();

    // Verify link points to valid GitHub repository URL
    const href = await githubLink.first().getAttribute('href');
    expect(href).toMatch(/^https?:\/\/(www\.)?github\.com\/[\w-]+\/[\w-]+/);

    // Verify link has appropriate text
    const linkText = await githubLink.first().textContent();
    expect(linkText).toBeTruthy();
  });

  // Test Case 3: Footer contains license information or link
  test('TC3: Footer contains license info or link', async ({ page }) => {
    // Check footer for license info
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    const footerContent = await footer.textContent();

    // Check for license mention or link
    const hasLicenseLink = await footer.locator('a[href*="LICENSE"], a[href*="license"]').count() > 0;
    const hasLicenseText = /license|MIT|Apache|GPL|BSD/i.test(footerContent || '');

    // At least one should be present
    expect(hasLicenseLink || hasLicenseText).toBeTruthy();

    // If license link exists, verify it's clickable
    if (hasLicenseLink) {
      const licenseLink = footer.locator('a[href*="LICENSE"], a[href*="license"]').first();
      await expect(licenseLink).toBeVisible();
      const href = await licenseLink.getAttribute('href');
      expect(href).toBeTruthy();
    }
  });

  // Test Case 4: Footer contains link to documentation or README
  test('TC4: Footer contains documentation or README link', async ({ page }) => {
    // Check footer for documentation link
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    const footerContent = await footer.textContent();

    // Check for documentation links - could be README, docs, documentation, or internal page links
    const hasReadmeLink = await footer.locator('a[href*="README"], a[href*="readme"]').count() > 0;
    const hasDocsLink = await footer.locator('a[href*="docs"], a[href*="documentation"]').count() > 0;
    const hasGettingStartedLink = await footer.locator('a[href*="getting-started"], a[href="#getting-started"]').count() > 0;
    const hasConfigLink = await footer.locator('a[href*="configuration"], a[href="#configuration"]').count() > 0;
    const hasDocText = /documentation|docs|readme|getting started|configuration/i.test(footerContent || '');

    // At least one documentation-related link should be present
    expect(hasReadmeLink || hasDocsLink || hasGettingStartedLink || hasConfigLink || hasDocText).toBeTruthy();
  });

  // Additional: Verify footer has proper structure
  test('Footer has proper structural elements', async ({ page }) => {
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Footer should have at least one navigation section or link group
    const hasNav = await footer.locator('nav').count() > 0;
    const hasLinks = await footer.locator('a').count() > 0;

    expect(hasNav || hasLinks).toBeTruthy();

    // Should have multiple links
    const linkCount = await footer.locator('a').count();
    expect(linkCount).toBeGreaterThanOrEqual(2);
  });

  // Verify all footer links are functional
  test('All footer links have valid href attributes', async ({ page }) => {
    const footer = page.locator('footer');
    const links = footer.locator('a');
    const linkCount = await links.count();

    expect(linkCount).toBeGreaterThan(0);

    for (let i = 0; i < linkCount; i++) {
      const link = links.nth(i);
      const href = await link.getAttribute('href');
      expect(href).toBeTruthy();
      expect(href!.length).toBeGreaterThan(0);
    }
  });
});
