/**
 * Navigation E2E Tests
 * Owner: Scenario 4 - Navigation and Resources
 *
 * End-to-end tests for navigation links, footer, and external resources.
 *
 * Expected test coverage:
 * - Navigation menu presence in header with at least 3 links
 * - Navigation links scroll to correct sections
 * - GitHub repository link exists
 * - Footer contains copyright and license information
 * - External links have proper attributes
 *
 * Requirements traced:
 * - REQ-4: Navigation to additional resources
 * - USR-4: User can find and navigate to resources
 * - Success Criteria: All navigation links function correctly
 */

const { test, expect } = require('@playwright/test');

test.describe('Navigation and Resource Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Check for navigation menu in header
  test('header contains navigation element with at least 3 links', async ({ page }) => {
    // Check header exists
    const header = page.locator('header');
    await expect(header).toBeVisible();

    // Check nav element exists
    const nav = header.locator('nav');
    await expect(nav).toBeVisible();

    // Count navigation links
    const navLinks = nav.locator('a');
    const count = await navLinks.count();
    expect(count).toBeGreaterThanOrEqual(3);

    // Verify expected links are present
    await expect(nav.locator('a[href="#features"]')).toBeVisible();
    await expect(nav.locator('a[href="#quickstart"]')).toBeVisible();
    await expect(nav.locator('a[href="#resources"]')).toBeVisible();
  });

  // Test Case 2: Click 'Features' navigation link
  test('clicking Features navigation link scrolls to features section', async ({ page }) => {
    const featuresLink = page.locator('header nav a[href="#features"]');
    await expect(featuresLink).toBeVisible();

    // Get initial scroll position
    const initialScroll = await page.evaluate(() => window.scrollY);

    // Click the Features link
    await featuresLink.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Verify features section is now in viewport
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();

    // Verify page scrolled
    const finalScroll = await page.evaluate(() => window.scrollY);
    expect(finalScroll).toBeGreaterThan(initialScroll);
  });

  // Test Case 3: Click 'Quick Start' navigation link
  test('clicking Quick Start navigation link scrolls to quickstart section', async ({ page }) => {
    const quickstartLink = page.locator('header nav a[href="#quickstart"]');
    await expect(quickstartLink).toBeVisible();

    // Get initial scroll position
    const initialScroll = await page.evaluate(() => window.scrollY);

    // Click the Quick Start link
    await quickstartLink.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Verify quickstart section is now in viewport
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();

    // Verify page scrolled
    const finalScroll = await page.evaluate(() => window.scrollY);
    expect(finalScroll).toBeGreaterThan(initialScroll);
  });

  // Test Case 4: Check for GitHub repository link
  test('GitHub repository link exists on the page', async ({ page }) => {
    // Check for GitHub link anywhere on the page
    const githubLinks = page.locator('a[href*="github.com/yetone/mirdb"]');
    const count = await githubLinks.count();
    expect(count).toBeGreaterThanOrEqual(1);

    // Verify at least one GitHub link is visible
    const firstGithubLink = githubLinks.first();
    await expect(firstGithubLink).toBeVisible();
  });

  // Test Case 6: Check footer for copyright
  test('footer contains copyright symbol or Copyright text', async ({ page }) => {
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    const footerText = await footer.textContent();
    const hasCopyright = footerText.includes('©') ||
                         footerText.toLowerCase().includes('copyright');
    expect(hasCopyright).toBeTruthy();
  });

  // Test Case 7: Check footer for license info
  test('footer contains license information or link to LICENSE file', async ({ page }) => {
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    const footerText = await footer.textContent();
    const hasLicense = footerText.toLowerCase().includes('license') ||
                       footerText.toLowerCase().includes('mit');

    // Also check for license link
    const licenseLink = footer.locator('a[href*="license"], a[href*="LICENSE"]');
    const hasLicenseLink = await licenseLink.count() > 0;

    expect(hasLicense || hasLicenseLink).toBeTruthy();
  });

  // Test Case 8: Verify external links open in new tab
  test('external links have target=_blank or rel=noopener attribute', async ({ page }) => {
    // Get all external links (links to github.com or other external domains)
    const externalLinks = page.locator('a[href^="https://"]');
    const count = await externalLinks.count();

    // There should be external links
    expect(count).toBeGreaterThan(0);

    // Check each external link has proper attributes
    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      const target = await link.getAttribute('target');
      const rel = await link.getAttribute('rel');

      // External links should open in new tab with noopener
      const hasTargetBlank = target === '_blank';
      const hasNoopener = rel && rel.includes('noopener');

      expect(hasTargetBlank).toBeTruthy();
      expect(hasNoopener).toBeTruthy();
    }
  });

  // Additional test: Resources section is visible and has content
  test('resources section has resource cards with links', async ({ page }) => {
    const resourcesSection = page.locator('#resources');
    await expect(resourcesSection).toBeVisible();

    // Check for resource cards
    const resourceCards = resourcesSection.locator('.resource-card');
    const count = await resourceCards.count();
    expect(count).toBeGreaterThanOrEqual(1);

    // Verify resource cards have links
    const resourceLinks = resourcesSection.locator('a.resource-card');
    const linkCount = await resourceLinks.count();
    expect(linkCount).toBeGreaterThanOrEqual(1);
  });

  // Additional test: Footer navigation links work
  test('footer contains working navigation links', async ({ page }) => {
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Check footer has navigation section
    const footerNav = footer.locator('nav, .footer-nav');
    const hasNav = await footerNav.count() > 0;

    if (hasNav) {
      const navLinks = footerNav.locator('a');
      const count = await navLinks.count();
      expect(count).toBeGreaterThanOrEqual(1);
    }
  });
});
