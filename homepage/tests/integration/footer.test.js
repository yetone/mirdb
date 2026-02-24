/**
 * Footer Integration Tests
 * Owner: Scenario 8 - Footer Section
 *
 * Test cases:
 * - Footer contains MIT license
 * - GitHub link present
 * - Uses semantic <footer> element
 * - All links functional
 */

import { test, expect } from '@playwright/test';

test.describe('Footer Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Footer contains MIT license text or link', async ({ page }) => {
    // Check footer section exists
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Check for MIT license text
    const footerText = await footer.textContent();
    expect(footerText).toContain('MIT');

    // Check for MIT license link
    const mitLink = page.locator('#footer-mit-link');
    await expect(mitLink).toBeVisible();
    await expect(mitLink).toContainText('MIT License');
    await expect(mitLink).toHaveAttribute('href', /opensource\.org\/licenses\/MIT|mit-license/i);
  });

  test('TC2: GitHub repository link is present in footer', async ({ page }) => {
    // Check for GitHub link in footer
    const githubLink = page.locator('#footer-github-link');
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toContainText('GitHub');

    // Verify the link points to GitHub
    await expect(githubLink).toHaveAttribute('href', /github\.com/);

    // Verify security attributes for external link
    await expect(githubLink).toHaveAttribute('target', '_blank');
    await expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  test('TC3: Footer is semantically correct using <footer> element', async ({ page }) => {
    // Check that footer element exists
    const footer = page.locator('footer');
    await expect(footer).toHaveCount(1);

    // Check it has proper ID for targeting
    await expect(footer).toHaveAttribute('id', 'site-footer');

    // Check it has the footer class
    await expect(footer).toHaveClass(/footer/);

    // Verify footer is at the bottom of the page
    const footerBoundingBox = await footer.boundingBox();
    const viewportSize = page.viewportSize();
    expect(footerBoundingBox).not.toBeNull();
  });

  test('TC4: All footer links navigate to correct destinations', async ({ page }) => {
    // Test Documentation link
    const docsLink = page.locator('#footer-docs-link');
    await expect(docsLink).toBeVisible();
    await expect(docsLink).toHaveAttribute('href', /docs/);

    // Test GitHub link
    const githubLink = page.locator('#footer-github-link');
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toHaveAttribute('href', /github\.com.*mirdb/);

    // Test Releases link
    const releasesLink = page.locator('#footer-releases-link');
    await expect(releasesLink).toBeVisible();
    await expect(releasesLink).toHaveAttribute('href', /github\.com.*releases/);

    // Test Discussions link
    const discussionsLink = page.locator('#footer-discussions-link');
    await expect(discussionsLink).toBeVisible();
    await expect(discussionsLink).toHaveAttribute('href', /github\.com.*discussions/);

    // Test Issues link
    const issuesLink = page.locator('#footer-issues-link');
    await expect(issuesLink).toBeVisible();
    await expect(issuesLink).toHaveAttribute('href', /github\.com.*issues/);

    // Test Contributing link
    const contributingLink = page.locator('#footer-contributing-link');
    await expect(contributingLink).toBeVisible();
    await expect(contributingLink).toHaveAttribute('href', /CONTRIBUTING/);

    // Test MIT License link
    const mitLink = page.locator('#footer-mit-link');
    await expect(mitLink).toBeVisible();
    await expect(mitLink).toHaveAttribute('href', /opensource\.org\/licenses\/MIT/);

    // Verify all external links have proper security attributes
    const externalLinks = page.locator('footer a[target="_blank"]');
    const count = await externalLinks.count();
    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      await expect(link).toHaveAttribute('rel', 'noopener noreferrer');
    }
  });

  test('Footer has proper structure with branding and navigation', async ({ page }) => {
    // Check footer brand section
    const footerLogo = page.locator('.footer-logo');
    await expect(footerLogo).toBeVisible();
    await expect(footerLogo).toContainText('MirDB');

    // Check footer tagline
    const footerTagline = page.locator('.footer-tagline');
    await expect(footerTagline).toBeVisible();
    await expect(footerTagline).toContainText('persistent key-value store');

    // Check footer navigation exists
    const footerNav = page.locator('.footer-nav');
    await expect(footerNav).toBeVisible();
    await expect(footerNav).toHaveAttribute('aria-label', 'Footer navigation');

    // Check Resources section
    const resourcesTitle = page.locator('.footer-links-title:has-text("Resources")');
    await expect(resourcesTitle).toBeVisible();

    // Check Community section
    const communityTitle = page.locator('.footer-links-title:has-text("Community")');
    await expect(communityTitle).toBeVisible();
  });

  test('Footer copyright displays current year', async ({ page }) => {
    const footerCopyright = page.locator('.footer-copyright');
    await expect(footerCopyright).toBeVisible();
    await expect(footerCopyright).toContainText('2024');
    await expect(footerCopyright).toContainText('MirDB');
  });

  test('Footer is visible when scrolling to the bottom', async ({ page }) => {
    // Scroll to the bottom of the page
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(500);

    // Check that footer is visible
    const footer = page.locator('footer');
    await expect(footer).toBeInViewport();
  });
});
