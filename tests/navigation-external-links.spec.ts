import { test, expect } from '@playwright/test';

/**
 * E2E Tests for Navigation and External Links
 *
 * This test suite verifies that all navigation elements and external links
 * (GitHub, documentation, issue tracker) are present and functional.
 *
 * Requirements: REQ-7 - Include links to GitHub repository, documentation, and issue tracker
 */

test.describe('Navigation and External Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: GitHub repository link is present and points to correct repository', async ({ page }) => {
    // Check hero section GitHub link
    const heroGithubLink = page.locator('[data-link="github-hero"]');
    await expect(heroGithubLink).toBeVisible();
    await expect(heroGithubLink).toHaveAttribute('href', 'https://github.com/example/mirdb');
    await expect(heroGithubLink).toHaveText('View on GitHub');

    // Verify the link opens in a new tab
    await expect(heroGithubLink).toHaveAttribute('target', '_blank');
    await expect(heroGithubLink).toHaveAttribute('rel', 'noopener');
  });

  test('Test Case 2: Documentation link is present and functional', async ({ page }) => {
    // Check footer documentation link
    const docsLink = page.locator('[data-link="documentation"]');
    await expect(docsLink).toBeVisible();
    await expect(docsLink).toHaveAttribute('href', '#getting-started');
    await expect(docsLink).toHaveText('Documentation');

    // Click the documentation link and verify it navigates to the getting started section
    await docsLink.click();
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeInViewport();
  });

  test('Test Case 3: Issue tracker link is present and functional', async ({ page }) => {
    // Check footer issues link
    const issuesLink = page.locator('[data-link="issues"]');
    await expect(issuesLink).toBeVisible();
    await expect(issuesLink).toHaveAttribute('href', 'https://github.com/example/mirdb/issues');
    await expect(issuesLink).toHaveText('Issues');

    // Verify the link opens in a new tab
    await expect(issuesLink).toHaveAttribute('target', '_blank');
    await expect(issuesLink).toHaveAttribute('rel', 'noopener');
  });

  test('Test Case 4: Footer contains GitHub repository link', async ({ page }) => {
    // Scroll to footer to ensure visibility
    const footer = page.locator('[data-section="footer"]');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Check footer GitHub link
    const footerGithubLink = page.locator('[data-link="github-footer"]');
    await expect(footerGithubLink).toBeVisible();
    await expect(footerGithubLink).toHaveAttribute('href', 'https://github.com/example/mirdb');
    await expect(footerGithubLink).toHaveText('GitHub');

    // Verify the link opens in a new tab
    await expect(footerGithubLink).toHaveAttribute('target', '_blank');
    await expect(footerGithubLink).toHaveAttribute('rel', 'noopener');
  });

  test('Test Case 5: License information is displayed in footer', async ({ page }) => {
    // Scroll to footer to ensure visibility
    const footer = page.locator('[data-section="footer"]');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Check license information
    const licenseInfo = page.locator('[data-info="license"]');
    await expect(licenseInfo).toBeVisible();
    await expect(licenseInfo).toContainText('MIT License');
  });

  test('Test Case 6: Version information is displayed', async ({ page }) => {
    // Scroll to footer to ensure visibility
    const footer = page.locator('[data-section="footer"]');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Check version information
    const versionInfo = page.locator('[data-info="version"]');
    await expect(versionInfo).toBeVisible();
    await expect(versionInfo).toContainText('Version');
    // Verify version matches expected format (Version X.X.X)
    await expect(versionInfo).toHaveText(/Version \d+\.\d+\.\d+/);
  });

  test('Footer navigation links are properly organized', async ({ page }) => {
    // Verify footer structure
    const footer = page.locator('[data-section="footer"]');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Check all footer links are present
    const footerLinks = footer.locator('.footer-links a');
    await expect(footerLinks).toHaveCount(3);

    // Verify each link type is present
    await expect(page.locator('[data-link="github-footer"]')).toBeVisible();
    await expect(page.locator('[data-link="issues"]')).toBeVisible();
    await expect(page.locator('[data-link="documentation"]')).toBeVisible();
  });

  test('External links have proper security attributes', async ({ page }) => {
    // All external links should have target="_blank" and rel="noopener" for security
    const externalLinks = page.locator('a[target="_blank"]');
    const count = await externalLinks.count();

    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      await expect(link).toHaveAttribute('rel', /noopener/);
    }
  });

  test('Hero CTA buttons are visible and properly styled', async ({ page }) => {
    // Check the CTA buttons container
    const ctaButtons = page.locator('.cta-buttons');
    await expect(ctaButtons).toBeVisible();

    // Verify Get Started button
    const getStartedBtn = page.locator('[data-link="get-started"]');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toHaveClass(/btn-primary/);
    await expect(getStartedBtn).toHaveText('Get Started');

    // Verify View on GitHub button
    const githubBtn = page.locator('[data-link="github-hero"]');
    await expect(githubBtn).toBeVisible();
    await expect(githubBtn).toHaveClass(/btn-secondary/);
    await expect(githubBtn).toHaveText('View on GitHub');
  });
});
