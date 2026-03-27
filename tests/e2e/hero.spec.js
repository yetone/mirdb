/**
 * Hero Section E2E Tests
 * Owner: Scenario 1 - Hero Section and Branding
 *
 * Tests:
 * - Logo visibility and alt text
 * - Value proposition text content
 * - Project name in title
 * - Hero section layout
 */
import { test, expect } from '@playwright/test';

test.describe('Hero Section and Branding', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/index.html');
  });

  test('TC1: Logo image is visible with correct alt text', async ({ page }) => {
    // Navigate to homepage and inspect hero section
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Logo image should be visible with alt text 'MirDB Logo'
    const logo = heroSection.locator('img[alt="MirDB Logo"]');
    await expect(logo).toBeVisible();
    await expect(logo).toHaveAttribute('alt', 'MirDB Logo');
  });

  test('TC2: Hero heading contains MirDB, persistent, and key-value store', async ({ page }) => {
    // Check hero heading text content
    const heroHeading = page.locator('#hero h1');
    await expect(heroHeading).toBeVisible();

    const headingText = await heroHeading.textContent();
    expect(headingText).toContain('MirDB');
    expect(headingText.toLowerCase()).toContain('persistent');
    expect(headingText.toLowerCase()).toContain('key-value store');
  });

  test('TC3: Hero description mentions memcached protocol compatibility', async ({ page }) => {
    // Check hero description text
    const heroDescription = page.locator('#hero p');
    await expect(heroDescription).toBeVisible();

    const descriptionText = await heroDescription.textContent();
    expect(descriptionText.toLowerCase()).toContain('memcached protocol');
  });

  test('TC4: Page title contains MirDB', async ({ page }) => {
    // Verify project name in page title
    const title = await page.title();
    expect(title).toContain('MirDB');
  });
});
