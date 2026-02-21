/**
 * Unit tests for HTML Structure and Accessibility
 * Owner: Scenario 9
 *
 * Test case 4 from Scenario 2:
 * - Hero uses proper h1 tag for main heading (only one h1 on page)
 */

const { test, expect } = require('@playwright/test');

test.describe('HTML Structure - Hero Section Semantic Validation', () => {
  test.beforeEach(async ({ page, baseURL }) => {
    await page.goto(baseURL);
  });

  test('should have proper heading hierarchy starting with h1 in hero', async ({ page }) => {
    // Test case 4: Check semantic HTML structure
    const h1Elements = page.locator('h1');
    const h1Count = await h1Elements.count();

    // There should be exactly one h1 on the page
    expect(h1Count).toBe(1);

    // The h1 should be in the hero section
    const heroH1 = page.locator('#hero h1');
    await expect(heroH1).toBeVisible();
  });

  test('should have h2 elements following h1 in heading hierarchy', async ({ page }) => {
    // Verify proper heading hierarchy (h1 > h2)
    const h1 = page.locator('h1');
    const h2Elements = page.locator('h2');

    // h1 should exist
    await expect(h1).toBeVisible();

    // h2 elements should exist for sections
    const h2Count = await h2Elements.count();
    expect(h2Count).toBeGreaterThanOrEqual(1);

    // No h3 should appear without h2 preceding it in the DOM
    // This is a basic check - more complex hierarchy validation would require parsing the DOM
  });

  test('should have semantic section elements with proper IDs', async ({ page }) => {
    // Verify sections have proper IDs for navigation
    const heroSection = page.locator('section#hero');
    const gettingStartedSection = page.locator('section#getting-started');

    await expect(heroSection).toBeVisible();
    await expect(gettingStartedSection).toBeVisible();
  });

  test('should have proper main content area', async ({ page }) => {
    // Verify main element exists and contains hero
    const main = page.locator('main');
    await expect(main).toBeVisible();

    // Hero should be inside main
    const heroInMain = main.locator('#hero');
    await expect(heroInMain).toBeVisible();
  });

  test('should have skip-to-content link for accessibility', async ({ page }) => {
    // Verify skip link exists
    const skipLink = page.locator('a[href="#main-content"]');
    await expect(skipLink).toHaveCount(1);
  });
});
