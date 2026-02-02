/**
 * Navigation E2E Tests
 * Owner: Scenario 6 - Navigation and CTAs
 *
 * Tests:
 * - Header presence and fixed position
 * - All navigation links present
 * - GitHub link correct URL
 * - Smooth scroll behavior
 * - External link security attributes
 * - Getting Started and Configuration navigation
 */

import { test, expect } from '@playwright/test';

test.describe('Navigation and CTAs', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Fixed navigation header with logo exists', async ({ page }) => {
    // Check header exists
    const header = page.locator('header');
    await expect(header).toBeVisible();

    // Check header has fixed position
    const position = await header.evaluate((el) => {
      return window.getComputedStyle(el).position;
    });
    expect(position).toBe('fixed');

    // Check logo exists
    const logo = page.locator('header .nav-logo');
    await expect(logo).toBeVisible();
  });

  test('TC2: GitHub link points to correct URL', async ({ page }) => {
    // Find GitHub link in navigation
    const githubLink = page.locator('a[href="https://github.com/yetone/mirdb"]').first();
    await expect(githubLink).toBeVisible();

    // Verify href attribute
    const href = await githubLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');
  });

  test('TC3: External links have security attributes', async ({ page }) => {
    // Find all external links (GitHub, etc.)
    const externalLinks = page.locator('a[href^="https://"]');
    const count = await externalLinks.count();

    // Verify at least one external link exists
    expect(count).toBeGreaterThan(0);

    // Check each external link has rel="noopener noreferrer"
    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      const rel = await link.getAttribute('rel');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    }
  });

  test('TC4: Getting Started navigation item exists', async ({ page }) => {
    // Check for Getting Started link/button in navigation or as CTA
    const getStartedLink = page.locator('a:has-text("Get Started"), a:has-text("Getting Started"), button:has-text("Get Started")');
    await expect(getStartedLink.first()).toBeVisible();
  });

  test('TC5: Configuration reference navigation exists', async ({ page }) => {
    // Check for Configuration or Specs link in navigation
    const configLink = page.locator('a:has-text("Configuration"), a:has-text("Specs"), a:has-text("Specifications"), a[href="#specifications"]');
    await expect(configLink.first()).toBeVisible();
  });

  test('TC6: Smooth scroll navigation works', async ({ page }) => {
    // Wait for the page to be fully loaded
    await page.waitForLoadState('domcontentloaded');

    // Click on a navigation link that should scroll to a section
    const featuresLink = page.locator('nav a[href="#features"]');
    await expect(featuresLink).toBeVisible();

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the features link
    await featuresLink.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(500);

    // Verify page has scrolled
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);

    // Verify the features section is now in view
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  test('Documentation link exists', async ({ page }) => {
    // Check for Documentation link
    const docsLink = page.locator('a:has-text("Docs"), a:has-text("Documentation")');
    await expect(docsLink.first()).toBeVisible();
  });

  test('CTA buttons are prominent and visible', async ({ page }) => {
    // Check for primary CTA buttons
    const ctaButtons = page.locator('.cta-btn, .btn-primary, a.cta');

    // Should have at least one CTA
    const count = await ctaButtons.count();
    expect(count).toBeGreaterThan(0);

    // First CTA should be visible
    await expect(ctaButtons.first()).toBeVisible();
  });

  test('Navigation contains all required menu items', async ({ page }) => {
    const nav = page.locator('nav');
    await expect(nav).toBeVisible();

    // Check for required navigation items
    await expect(page.locator('nav a[href="#features"]')).toBeVisible();
    await expect(page.locator('nav a[href^="https://github.com"]').first()).toBeVisible();
  });

  test('View on GitHub CTA exists', async ({ page }) => {
    // Look for GitHub CTA button
    const githubCTA = page.locator('a:has-text("View on GitHub"), a:has-text("GitHub")').first();
    await expect(githubCTA).toBeVisible();

    // Verify it links to the correct repository
    const href = await githubCTA.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');
  });
});
