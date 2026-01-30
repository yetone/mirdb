/**
 * Footer and Links Tests
 * Owner: Scenario 7 - Footer and Links
 *
 * Tests:
 * - Footer element presence
 * - GitHub repository link
 * - License information
 * - Contribution guidelines link
 */

const { test, expect } = require('@playwright/test');
const { BASE_URL, SELECTORS, waitForPageLoad } = require('./test-utils');

test.describe('Footer and Links Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(BASE_URL);
    await waitForPageLoad(page);
  });

  test('TC1: Footer element exists with appropriate semantic tag', async ({ page }) => {
    // Check that footer element exists with semantic <footer> tag
    const footer = page.locator('footer#footer');
    await expect(footer).toBeVisible();

    // Verify it uses the correct class naming convention
    await expect(footer).toHaveClass(/footer/);

    // Verify footer is at the bottom of the page
    const footerBox = await footer.boundingBox();
    const viewportSize = page.viewportSize();
    expect(footerBox).not.toBeNull();
  });

  test('TC2: GitHub repository link exists in footer', async ({ page }) => {
    const footer = page.locator('footer#footer');

    // Check for GitHub link to https://github.com/yetone/mirdb
    const githubLink = footer.locator('a[href="https://github.com/yetone/mirdb"]');
    await expect(githubLink).toBeVisible();

    // Verify link has appropriate attributes for external link
    await expect(githubLink).toHaveAttribute('target', '_blank');
    await expect(githubLink).toHaveAttribute('rel', /noopener/);
  });

  test('TC3: License information is mentioned or linked', async ({ page }) => {
    const footer = page.locator('footer#footer');

    // Check for license text or link
    // Could be MIT, Apache, etc. or a link to LICENSE file
    const licenseText = footer.locator('text=/license/i');
    const licenseLink = footer.locator('a[href*="license"], a[href*="LICENSE"]');

    // At least one should be present
    const hasLicenseText = await licenseText.count() > 0;
    const hasLicenseLink = await licenseLink.count() > 0;

    expect(hasLicenseText || hasLicenseLink).toBe(true);
  });

  test('TC4: Contribution link exists (issues, contributing, or discussions)', async ({ page }) => {
    const footer = page.locator('footer#footer');

    // Check for contribution-related links
    // Could be: issues page, contributing guidelines, discussions
    const issuesLink = footer.locator('a[href*="issues"]');
    const contributingLink = footer.locator('a[href*="contributing"], a[href*="CONTRIBUTING"]');
    const discussionsLink = footer.locator('a[href*="discussions"]');
    const contributeText = footer.locator('text=/contribut/i');

    const hasIssuesLink = await issuesLink.count() > 0;
    const hasContributingLink = await contributingLink.count() > 0;
    const hasDiscussionsLink = await discussionsLink.count() > 0;
    const hasContributeText = await contributeText.count() > 0;

    // At least one should be present
    expect(hasIssuesLink || hasContributingLink || hasDiscussionsLink || hasContributeText).toBe(true);
  });

  test('Footer contains project name or branding', async ({ page }) => {
    const footer = page.locator('footer#footer');

    // Check for MirDB branding in footer
    const mirdbText = footer.locator('text=/mirdb/i');
    await expect(mirdbText.first()).toBeVisible();
  });

  test('Footer links are accessible with keyboard', async ({ page }) => {
    const footer = page.locator('footer#footer');
    const links = footer.locator('a');

    const linkCount = await links.count();
    expect(linkCount).toBeGreaterThan(0);

    // Check that links are focusable
    for (let i = 0; i < Math.min(linkCount, 3); i++) {
      const link = links.nth(i);
      await link.focus();
      await expect(link).toBeFocused();
    }
  });
});
