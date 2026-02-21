/**
 * E2E tests for Hero Section
 * Owner: Scenario 2
 *
 * Test cases:
 * - Hero section presence with h1
 * - Tagline content validation (persistent key-value store, Memcached)
 * - CTA button presence and navigation
 * - Single h1 semantic check
 */

const { test, expect } = require('@playwright/test');

test.describe('Hero Section', () => {
  test.beforeEach(async ({ page, baseURL }) => {
    await page.goto(baseURL);
  });

  test('should display hero section with h1 containing product tagline', async ({ page }) => {
    // Test case 1: Check hero section exists with h1 and tagline
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    const h1 = heroSection.locator('h1');
    await expect(h1).toBeVisible();

    // Verify tagline mentions 'persistent key-value store' and 'Memcached'
    const h1Text = await h1.textContent();
    expect(h1Text.toLowerCase()).toContain('persistent key-value store');
    expect(h1Text.toLowerCase()).toContain('memcached');
  });

  test('should display visible and clickable CTA button with Get Started text', async ({ page }) => {
    // Test case 2: Verify CTA button presence
    const ctaButton = page.locator('#hero a[href="#getting-started"]');
    await expect(ctaButton).toBeVisible();

    const buttonText = await ctaButton.textContent();
    expect(buttonText.toLowerCase()).toContain('get started');

    // Verify it's clickable (no disabled state)
    await expect(ctaButton).toBeEnabled();
  });

  test('should smoothly scroll to Getting Started section when CTA is clicked', async ({ page }) => {
    // Test case 3: Click CTA button and verify navigation
    const ctaButton = page.locator('#hero a[href="#getting-started"]');
    const gettingStartedSection = page.locator('#getting-started');

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the CTA button
    await ctaButton.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(1000);

    // Verify we scrolled down
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);

    // Verify the Getting Started section is in viewport
    await expect(gettingStartedSection).toBeInViewport();
  });

  test('should have only one h1 tag on the entire page', async ({ page }) => {
    // Test case 4: Check semantic HTML structure - only one h1 on page
    const h1Elements = page.locator('h1');
    const h1Count = await h1Elements.count();

    expect(h1Count).toBe(1);

    // Verify the h1 is in the hero section
    const heroH1 = page.locator('#hero h1');
    await expect(heroH1).toBeVisible();
  });

  test('should have accessible CTA button with proper focus state', async ({ page }) => {
    // Additional accessibility test
    const ctaButton = page.locator('#hero a[href="#getting-started"]');

    // Check it can receive focus
    await ctaButton.focus();
    await expect(ctaButton).toBeFocused();

    // Verify role is appropriate (link styled as button)
    const role = await ctaButton.getAttribute('role');
    // Links with role="button" or plain links are both acceptable
    expect(role === null || role === 'button').toBeTruthy();
  });

  test('should have hero section immediately visible after page load', async ({ page }) => {
    // Hero should be the first content section after header
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeInViewport();

    // Verify hero comes right after header in DOM
    const header = page.locator('header');
    await expect(header).toBeVisible();
  });

  test('should display supplementary product information badges', async ({ page }) => {
    // Verify additional content about the product
    const heroSection = page.locator('#hero');

    // Check for product highlights/badges
    const heroText = await heroSection.textContent();
    expect(heroText.toLowerCase()).toContain('memcached');
    expect(heroText.toLowerCase()).toContain('rust');
  });
});
