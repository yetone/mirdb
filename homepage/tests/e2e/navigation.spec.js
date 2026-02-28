/**
 * E2E tests for navigation
 * Owner: Scenario 4 - Navigation and External Links
 *
 * Test suites:
 * - Header navigation links
 * - Footer links
 * - External link attributes (target, rel)
 * - Mobile menu functionality
 */

const { test, expect } = require('@playwright/test');

const BASE_URL = 'http://localhost:3000';

test.describe('Navigation and External Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(BASE_URL);
  });

  test.describe('Header Navigation', () => {
    test('TC1: Header element exists with navigation content', async ({ page }) => {
      const header = page.locator('header');
      await expect(header).toBeVisible();

      // Header should have navigation content
      const nav = header.locator('nav');
      await expect(nav).toBeVisible();
    });

    test('TC2: GitHub link exists in header', async ({ page }) => {
      const header = page.locator('header');
      const githubLink = header.locator('a[href*="github.com"]');
      await expect(githubLink).toBeVisible();
    });

    test('TC3: GitHub link URL is correct', async ({ page }) => {
      const header = page.locator('header');
      const githubLink = header.locator('a[href*="github.com"]');
      await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    });

    test('TC4: GitHub link opens in new tab', async ({ page }) => {
      const header = page.locator('header');
      const githubLink = header.locator('a[href*="github.com"]');
      await expect(githubLink).toHaveAttribute('target', '_blank');
    });

    test('TC5: GitHub link has security attributes', async ({ page }) => {
      const header = page.locator('header');
      const githubLink = header.locator('a[href*="github.com"]');
      const rel = await githubLink.getAttribute('rel');
      expect(rel).toMatch(/noopener/);
    });

    test('Header contains navigation links to page sections', async ({ page }) => {
      const nav = page.locator('header nav');

      // Check for internal navigation links
      await expect(nav.locator('a[href="#features"]')).toBeVisible();
      await expect(nav.locator('a[href="#status"]')).toBeVisible();
      await expect(nav.locator('a[href="#usage"]')).toBeVisible();
      await expect(nav.locator('a[href="#quickstart"]')).toBeVisible();
    });

    test('Header logo links to home', async ({ page }) => {
      const logo = page.locator('header .header-logo');
      await expect(logo).toBeVisible();
      await expect(logo).toHaveAttribute('href', '/');
    });
  });

  test.describe('Footer Navigation', () => {
    test('TC6: Footer element exists', async ({ page }) => {
      const footer = page.locator('footer');
      await expect(footer).toBeVisible();
    });

    test('TC7: Documentation link exists', async ({ page }) => {
      const footer = page.locator('footer');
      // Look for documentation or README links
      const docLink = footer.locator('a[href*="readme"], a[href*="README"], a[href*="docs"], a[href*="documentation"]').first();
      await expect(docLink).toBeVisible();
    });

    test('TC8: Issues/discussions link exists', async ({ page }) => {
      const footer = page.locator('footer');
      // Look for issues or discussions link
      const issuesLink = footer.locator('a[href*="issues"], a[href*="discussions"]').first();
      await expect(issuesLink).toBeVisible();
    });

    test('TC9: License information is present', async ({ page }) => {
      const footer = page.locator('footer');
      // Check for license text or link
      const hasLicenseText = await footer.locator(':text("MIT"), :text("License")').count() > 0;
      const hasLicenseLink = await footer.locator('a[href*="license"], a[href*="LICENSE"]').count() > 0;
      expect(hasLicenseText || hasLicenseLink).toBeTruthy();
    });

    test('Footer contains project links section', async ({ page }) => {
      const footer = page.locator('footer');
      // Footer should contain organized links
      const footerLinks = footer.locator('a');
      const linksCount = await footerLinks.count();
      expect(linksCount).toBeGreaterThanOrEqual(3);
    });
  });

  test.describe('External Link Security', () => {
    test('TC10: All external links have proper security attributes', async ({ page }) => {
      // Get all links with external URLs (not starting with # or /)
      const externalLinks = page.locator('a[href^="http"]');
      const count = await externalLinks.count();

      expect(count).toBeGreaterThan(0);

      for (let i = 0; i < count; i++) {
        const link = externalLinks.nth(i);
        const href = await link.getAttribute('href');

        // Skip same-origin links if any
        if (href.startsWith(BASE_URL)) continue;

        // Check target attribute
        await expect(link).toHaveAttribute('target', '_blank');

        // Check rel contains noopener
        const rel = await link.getAttribute('rel');
        expect(rel, `Link ${href} should have rel="noopener"`).toMatch(/noopener/);
      }
    });

    test('External links have both target and rel attributes', async ({ page }) => {
      const githubLinks = page.locator('a[href*="github.com"]');
      const count = await githubLinks.count();

      for (let i = 0; i < count; i++) {
        const link = githubLinks.nth(i);
        await expect(link).toHaveAttribute('target', '_blank');
        const rel = await link.getAttribute('rel');
        expect(rel).toMatch(/noopener/);
      }
    });
  });

  test.describe('Mobile Menu', () => {
    // Note: Mobile menu responsive visibility is handled by Scenario 6 (Responsive Design)
    // These tests verify the mobile menu functionality exists in the DOM
    test('Mobile menu toggle button exists in DOM', async ({ page }) => {
      const menuButton = page.locator('.mobile-menu-toggle');
      // Menu button should exist in the DOM (even if hidden on desktop)
      await expect(menuButton).toHaveCount(1);
      // Verify it has proper aria attributes
      await expect(menuButton).toHaveAttribute('aria-label', 'Toggle menu');
    });

    test('Mobile menu toggle has proper accessibility attributes', async ({ page }) => {
      const menuButton = page.locator('.mobile-menu-toggle');
      await expect(menuButton).toHaveAttribute('aria-expanded', 'false');
    });
  });

  test.describe('Smooth Scrolling', () => {
    test('Internal links scroll to target sections', async ({ page }) => {
      // Click on Features link
      const featuresLink = page.locator('header nav a[href="#features"]');
      await featuresLink.click();

      // Wait for scroll animation
      await page.waitForTimeout(500);

      // Check if features section is in viewport
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();
    });
  });
});
