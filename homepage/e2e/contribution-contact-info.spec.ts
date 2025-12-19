import { test, expect } from '@playwright/test';

const GITHUB_REPO_URL = 'https://github.com/mirdb/mirdb';
const GITHUB_ISSUES_URL = 'https://github.com/mirdb/mirdb/issues';
const GITHUB_CONTRIBUTING_URL = 'https://github.com/mirdb/mirdb/blob/main/CONTRIBUTING.md';

test.describe('Contribution and Contact Information', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('footer contains contribution link to GitHub issues or contributing guidelines', async ({ page }) => {
    // Test Case 1: Check footer for contribution information
    // Look for a link that points to either GitHub issues or contributing guidelines
    const contributionLink = page.locator('footer a[href*="github.com/mirdb/mirdb"]').filter({
      has: page.locator('text=/contribute|issues|contributing/i')
    });

    // Alternative: check for any link with contribution-related text or href
    const anyContributionLink = page.locator('footer').locator('a').filter({
      hasText: /contribute|contributing|issues/i
    });

    // At least one contribution-related link should exist in the footer
    const contributionLinkCount = await anyContributionLink.count();
    expect(contributionLinkCount).toBeGreaterThan(0);

    // Verify the link has a valid GitHub URL
    const link = anyContributionLink.first();
    await expect(link).toBeVisible();
    const href = await link.getAttribute('href');
    expect(href).toContain('github.com/mirdb/mirdb');
  });

  test('footer displays license information', async ({ page }) => {
    // Test Case 2: Check for license information
    // License type should be displayed or linked in footer
    const footer = page.locator('footer');

    // Check for license text or link
    const licenseText = footer.locator('text=/MIT|license/i');
    const licenseLink = footer.locator('a[href*="LICENSE"], a[href*="license"]');

    // Either license text or a license link should be present
    const hasLicenseText = await licenseText.count() > 0;
    const hasLicenseLink = await licenseLink.count() > 0;

    expect(hasLicenseText || hasLicenseLink).toBeTruthy();

    if (hasLicenseText) {
      await expect(licenseText.first()).toBeVisible();
    }
  });

  test('contribution link is functional and navigates correctly', async ({ page, context }) => {
    // Test Case 3: Verify contribution link is functional
    const footer = page.locator('footer');
    const contributionLink = footer.locator('a').filter({
      hasText: /contribute|contributing|issues/i
    }).first();

    await expect(contributionLink).toBeVisible();

    // Verify the link has correct attributes for external navigation
    await expect(contributionLink).toHaveAttribute('target', '_blank');
    await expect(contributionLink).toHaveAttribute('rel', /noopener/);

    // Verify the href is a valid GitHub URL
    const href = await contributionLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toMatch(/github\.com\/mirdb\/mirdb\/(issues|blob\/main\/CONTRIBUTING)/);
  });

  test('footer contribution section is accessible', async ({ page }) => {
    // Accessibility: contribution links should have meaningful text
    const footer = page.locator('footer');
    const contributionLink = footer.locator('a').filter({
      hasText: /contribute|contributing|issues/i
    }).first();

    await expect(contributionLink).toBeVisible();

    // Link should have accessible name
    const linkText = await contributionLink.textContent();
    expect(linkText).toBeTruthy();
    expect(linkText!.trim().length).toBeGreaterThan(0);
  });

  test('footer contains all required contributor information elements', async ({ page }) => {
    // Comprehensive check for all REQ-9 requirements
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // 1. GitHub repository link
    const githubLink = footer.locator('a[href*="github.com/mirdb/mirdb"]');
    expect(await githubLink.count()).toBeGreaterThan(0);

    // 2. Contribution link (issues or contributing)
    const contributionText = footer.locator('text=/contribute|contributing|issues/i');
    expect(await contributionText.count()).toBeGreaterThan(0);

    // 3. License information
    const licenseInfo = footer.locator('text=/MIT|license/i');
    expect(await licenseInfo.count()).toBeGreaterThan(0);
  });
});
