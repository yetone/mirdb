/**
 * Navigation Integration Tests
 * Owner: Scenario 4 - Navigation and Resources
 *
 * Integration tests for navigation and links.
 *
 * Expected test coverage:
 * - All navigation links function correctly
 * - External links open in new tab
 * - Anchor links scroll to correct section
 * - GitHub repository link is valid
 *
 * Requirements traced:
 * - REQ-4: Navigation to additional resources
 * - Success Criteria: All navigation links function correctly
 */

const { test, expect } = require('@playwright/test');

test.describe('Navigation Integration Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 5: Verify GitHub link is valid (returns HTTP 200)
  test('GitHub link returns HTTP 200 when accessed', async ({ page, request }) => {
    // Get the GitHub link from the page
    const githubLink = page.locator('a[href*="github.com/yetone/mirdb"]').first();
    await expect(githubLink).toBeVisible();

    const href = await githubLink.getAttribute('href');
    expect(href).toBeTruthy();

    // Make HTTP request to verify the link is valid
    const response = await request.get(href);
    expect(response.status()).toBe(200);
  });

  // Test: Verify all anchor links point to existing sections
  test('all anchor navigation links point to existing sections', async ({ page }) => {
    const anchorLinks = page.locator('header nav a[href^="#"]');
    const count = await anchorLinks.count();

    expect(count).toBeGreaterThanOrEqual(3);

    for (let i = 0; i < count; i++) {
      const link = anchorLinks.nth(i);
      const href = await link.getAttribute('href');

      if (href && href.startsWith('#')) {
        const sectionId = href.substring(1);
        const section = page.locator(`#${sectionId}`);
        await expect(section).toBeAttached();
      }
    }
  });

  // Test: Navigation links in footer also work
  test('footer navigation links point to existing sections', async ({ page }) => {
    const footerAnchorLinks = page.locator('footer a[href^="#"]');
    const count = await footerAnchorLinks.count();

    for (let i = 0; i < count; i++) {
      const link = footerAnchorLinks.nth(i);
      const href = await link.getAttribute('href');

      if (href && href.startsWith('#')) {
        const sectionId = href.substring(1);
        const section = page.locator(`#${sectionId}`);
        await expect(section).toBeAttached();
      }
    }
  });

  // Test: Resources section contains at least one external link
  test('resources section contains external documentation links', async ({ page }) => {
    const resourcesSection = page.locator('#resources');
    await expect(resourcesSection).toBeVisible();

    const externalLinks = resourcesSection.locator('a[href^="https://"]');
    const count = await externalLinks.count();
    expect(count).toBeGreaterThanOrEqual(1);
  });

  // Test: Hero section GitHub button is properly configured
  test('hero section View on GitHub button links to repository', async ({ page }) => {
    const heroSection = page.locator('#hero, .hero');
    const githubButton = heroSection.locator('a[href*="github.com"]');

    await expect(githubButton).toBeVisible();

    const href = await githubButton.getAttribute('href');
    expect(href).toContain('github.com/yetone/mirdb');

    const target = await githubButton.getAttribute('target');
    expect(target).toBe('_blank');
  });
});
