// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Hero Section Display (Scenario 1)
 *
 * These tests verify that the hero section displays the product name,
 * tagline, and value proposition correctly as per REQ-1.
 */

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    // Load landing page with fresh cache
    await page.goto('/');
  });

  /**
   * Test Case 1: Product name 'MirDB' is displayed prominently
   * Input: Load landing page and query for hero section elements
   * Expected: Product name 'MirDB' is displayed prominently
   */
  test('should display product name MirDB prominently', async ({ page }) => {
    // Verify hero section is visible
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify product name is displayed
    const productName = page.locator('.hero-product-name');
    await expect(productName).toBeVisible();
    await expect(productName).toHaveText('MirDB');

    // Verify it's displayed prominently (h1 element)
    const h1Element = page.locator('h1#hero-title');
    await expect(h1Element).toBeVisible();
    await expect(h1Element).toHaveText('MirDB');
  });

  /**
   * Test Case 2: Tagline is displayed in hero section
   * Input: Query for tagline element in hero section
   * Expected: Tagline 'Persistent Key-Value Store with Memcached Protocol' is displayed
   */
  test('should display tagline in hero section', async ({ page }) => {
    // Verify tagline element exists and is visible
    const tagline = page.locator('.hero-tagline');
    await expect(tagline).toBeVisible();

    // Verify exact tagline text
    await expect(tagline).toHaveText('Persistent Key-Value Store with Memcached Protocol');
  });

  /**
   * Test Case 3: Primary CTA button is visible and clickable
   * Input: Query for primary CTA button
   * Expected: Primary CTA button (Get Started or View on GitHub) is visible and clickable
   */
  test('should display primary CTA button that is visible and clickable', async ({ page }) => {
    // Query for primary CTA button
    const primaryCTA = page.locator('[data-testid="primary-cta"]');
    await expect(primaryCTA).toBeVisible();

    // Verify button text is "Get Started" (matches PRD spec)
    await expect(primaryCTA).toHaveText('Get Started');

    // Verify button is clickable (has proper role and is enabled)
    await expect(primaryCTA).toBeEnabled();

    // Verify it links to GitHub
    const href = await primaryCTA.getAttribute('href');
    expect(href).toContain('github.com/yetone/mirdb');
  });

  /**
   * Test Case 4: Secondary CTA button is visible and clickable
   * Input: Query for secondary CTA button
   * Expected: Secondary CTA button (Learn More) is visible and clickable
   */
  test('should display secondary CTA button that is visible and clickable', async ({ page }) => {
    // Query for secondary CTA button
    const secondaryCTA = page.locator('[data-testid="secondary-cta"]');
    await expect(secondaryCTA).toBeVisible();

    // Verify button text is "Learn More"
    await expect(secondaryCTA).toHaveText('Learn More');

    // Verify button is clickable (has proper role and is enabled)
    await expect(secondaryCTA).toBeEnabled();

    // Verify it links to features section
    const href = await secondaryCTA.getAttribute('href');
    expect(href).toBe('#features');
  });

  /**
   * Additional test: Hero section is visible above the fold (desktop viewport)
   * Context: Desktop viewport (1920x1080)
   */
  test('should display hero section above the fold on desktop', async ({ page }) => {
    // Set viewport to desktop size (already configured in playwright.config.js)
    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.goto('/');

    // Verify hero section is in viewport without scrolling
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeInViewport();

    // Verify all critical elements are visible
    const productName = page.locator('.hero-product-name');
    const tagline = page.locator('.hero-tagline');
    const primaryCTA = page.locator('[data-testid="primary-cta"]');

    await expect(productName).toBeInViewport();
    await expect(tagline).toBeInViewport();
    await expect(primaryCTA).toBeInViewport();
  });

  /**
   * Additional test: Value proposition text is displayed
   * Verifies the hero section includes a value proposition as per PRD
   */
  test('should display value proposition text', async ({ page }) => {
    const valueProposition = page.locator('.hero-value-proposition');
    await expect(valueProposition).toBeVisible();

    // Verify it contains key differentiators mentioned in PRD
    const text = await valueProposition.textContent();
    expect(text).toContain('memcached');
    expect(text).toContain('persist');
    expect(text).toContain('Rust');
  });
});
