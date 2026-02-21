/**
 * Hero Section E2E Tests
 * Owner: Scenario 1 - Hero Section Display
 *
 * Tests:
 * - Get Started button scrolls to quick start section
 */

const { test, expect } = require('@playwright/test');

test.describe('Hero Section E2E', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Click Get Started button scrolls to quick start section', async ({ page }) => {
    // Test case 4: Clicking Get Started button scrolls to quickstart section

    // Find the Get Started button
    const getStartedBtn = page.locator('#get-started-btn');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toHaveText('Get Started');

    // Get the quickstart section
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the Get Started button
    await getStartedBtn.click();

    // Wait for scroll animation/navigation
    await page.waitForTimeout(500);

    // Verify the page scrolled or the quickstart section is in view
    const quickstartBoundingBox = await quickstartSection.boundingBox();
    const viewportHeight = page.viewportSize().height;

    // Check that quickstart section is visible in viewport (top is within viewport)
    expect(quickstartBoundingBox.y).toBeLessThan(viewportHeight);
  });

  test('Hero section displays all required elements', async ({ page }) => {
    // Verify logo is visible
    const logo = page.locator('#mirdb-logo');
    await expect(logo).toBeVisible();
    await expect(logo).toHaveAttribute('src', /logo\.gif$/);

    // Verify tagline is visible
    const tagline = page.locator('.hero-tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toHaveText('A Persistent Key-Value Store with Memcached Protocol');

    // Verify CTA button
    const ctaButton = page.locator('.cta-button');
    await expect(ctaButton).toBeVisible();
    await expect(ctaButton).toHaveAttribute('href', '#quickstart');
  });
});
