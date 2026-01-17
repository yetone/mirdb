import { test, expect } from '@playwright/test';

/**
 * E2E Tests for Broken Image Error Handling
 *
 * This test suite verifies graceful handling of broken or missing images.
 * It tests that appropriate fallbacks are in place and alt text is displayed.
 *
 * Related Requirements: NFR-3 (Accessibility), REQ-7 (Logo display)
 */

test.describe('Error Handling - Broken Images', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Alt text is displayed for all images', async ({ page }) => {
    // Get all images on the page
    const images = page.locator('img');
    const count = await images.count();

    // Verify there are images on the page
    expect(count).toBeGreaterThan(0);

    // Check that every image has alt text defined
    for (let i = 0; i < count; i++) {
      const img = images.nth(i);
      const altText = await img.getAttribute('alt');

      // Every image should have alt text for accessibility
      expect(altText).toBeTruthy();
      expect(altText!.length).toBeGreaterThan(0);
    }
  });

  test('Test Case 2: No 404 errors for image resources', async ({ page }) => {
    const imageErrors: string[] = [];

    // Listen for response events to catch 404 errors on images
    page.on('response', (response) => {
      const url = response.url();
      const status = response.status();

      // Check if this is an image request
      if (
        url.match(/\.(gif|png|jpg|jpeg|svg|webp|ico)$/i) ||
        url.includes('img.shields.io')
      ) {
        if (status === 404) {
          imageErrors.push(`404 Error: ${url}`);
        }
      }
    });

    // Navigate and wait for all resources to load
    await page.goto('/', { waitUntil: 'networkidle' });

    // Verify no 404 errors occurred for image resources
    expect(imageErrors).toEqual([]);
  });

  test('Test Case 3: Logo fallback - MirDB text is visible when logo fails', async ({ page }) => {
    // Block the logo image from loading
    await page.route('**/logo.gif', (route) => route.abort());

    // Navigate to the page
    await page.goto('/');

    // The h1 with "MirDB" text should always be visible
    const heading = page.locator('h1');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('MirDB');

    // The logo img element should still have proper alt text even if blocked
    const logo = page.locator('img.logo');
    const altText = await logo.getAttribute('alt');
    expect(altText).toBe('MirDB Logo');
  });

  test('All images have meaningful alt text descriptions', async ({ page }) => {
    const images = page.locator('img');
    const count = await images.count();

    for (let i = 0; i < count; i++) {
      const img = images.nth(i);
      const altText = await img.getAttribute('alt');

      // Alt text should be descriptive (more than just a single word for most images)
      expect(altText).toBeTruthy();

      // Alt text should not be just empty or placeholder
      expect(altText).not.toMatch(/^(image|img|photo|picture)$/i);
    }
  });

  test('Logo image has correct fallback behavior', async ({ page }) => {
    // Block the logo image
    await page.route('**/logo.gif', (route) => route.abort());

    await page.goto('/');

    // The hero section should still be functional
    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    // The MirDB heading should be prominently visible
    const heading = page.locator('.hero h1');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('MirDB');

    // The tagline should also be visible
    const tagline = page.locator('.tagline');
    await expect(tagline).toBeVisible();

    // CTA buttons should remain functional
    const getStartedBtn = page.locator('[data-link="get-started"]');
    await expect(getStartedBtn).toBeVisible();
  });

  test('Badge images have fallback alt text', async ({ page }) => {
    // Check CI badge
    const ciBadge = page.locator('[data-badge="ci"] img');
    await expect(ciBadge).toHaveAttribute('alt');
    const ciAlt = await ciBadge.getAttribute('alt');
    expect(ciAlt).toContain('CircleCI');

    // Check version badge
    const versionBadge = page.locator('[data-badge="version"] img');
    await expect(versionBadge).toHaveAttribute('alt');
    const versionAlt = await versionBadge.getAttribute('alt');
    expect(versionAlt).toContain('Version');
  });

  test('Page remains functional when external badge images fail', async ({ page }) => {
    // Block shields.io badge images
    await page.route('**/img.shields.io/**', (route) => route.abort());

    await page.goto('/');

    // The page should still load and be functional
    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    // The badges container should exist even if images fail
    const badges = page.locator('[data-section="badges"]');
    await expect(badges).toBeVisible();

    // The MirDB branding should still be clear
    await expect(page.locator('h1')).toHaveText('MirDB');

    // Navigation should still work
    const getStartedBtn = page.locator('[data-link="get-started"]');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toHaveAttribute('href', '#getting-started');
  });

  test('Images load with correct natural dimensions', async ({ page }) => {
    await page.goto('/', { waitUntil: 'networkidle' });

    // Get the logo image
    const logo = page.locator('img.logo');

    // Wait for image to potentially load
    await page.waitForTimeout(1000);

    // Check if image exists and has dimensions
    const isVisible = await logo.isVisible();

    if (isVisible) {
      // If the logo is visible, verify it has valid dimensions
      const boundingBox = await logo.boundingBox();
      expect(boundingBox).not.toBeNull();

      if (boundingBox) {
        expect(boundingBox.width).toBeGreaterThan(0);
        expect(boundingBox.height).toBeGreaterThan(0);
      }
    }
  });
});
