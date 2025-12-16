// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Product name 'MirDB' is visible in the hero section
   */
  test('should display product name MirDB in hero section', async ({ page }) => {
    // Verify hero section exists
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify product name is displayed
    const productName = page.locator('[data-testid="product-name"]');
    await expect(productName).toBeVisible();
    await expect(productName).toHaveText('MirDB');
  });

  /**
   * Test Case 2: Tagline contains 'persistent key-value store', 'Memcached protocol compatibility', and 'Rust'
   */
  test('should display tagline with key terms', async ({ page }) => {
    const tagline = page.locator('[data-testid="tagline"]');
    await expect(tagline).toBeVisible();

    const taglineText = await tagline.textContent();

    // Verify tagline contains required terms
    expect(taglineText).toContain('persistent key-value store');
    expect(taglineText).toContain('Memcached protocol compatibility');
    expect(taglineText).toContain('Rust');
  });

  /**
   * Test Case 3: Primary CTA button navigates to GitHub repository or getting started section
   */
  test('should have primary CTA button linking to GitHub repository', async ({ page }) => {
    const primaryCTA = page.locator('[data-testid="primary-cta"]');
    await expect(primaryCTA).toBeVisible();

    // Verify button text
    const buttonText = await primaryCTA.textContent();
    expect(buttonText?.trim()).toMatch(/View on GitHub|Get Started/);

    // Verify href points to GitHub
    const href = await primaryCTA.getAttribute('href');
    expect(href).toContain('github.com');
  });

  /**
   * Test Case 4: Secondary CTA (Learn More) scrolls to features section
   */
  test('should have secondary CTA that scrolls to features section', async ({ page }) => {
    const secondaryCTA = page.locator('[data-testid="secondary-cta"]');
    await expect(secondaryCTA).toBeVisible();

    // Verify button text
    await expect(secondaryCTA).toHaveText('Learn More');

    // Verify href points to features section
    const href = await secondaryCTA.getAttribute('href');
    expect(href).toBe('#features');

    // Click and verify scroll to features section
    await secondaryCTA.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Verify features section is now visible in viewport
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeInViewport();
  });

  /**
   * Additional test: Hero section is above the fold (visible on page load)
   */
  test('should display hero section above the fold', async ({ page }) => {
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();
    await expect(heroSection).toBeInViewport();
  });

  /**
   * Additional test: CTA buttons container exists with both buttons
   */
  test('should display both CTA buttons', async ({ page }) => {
    const primaryCTA = page.locator('[data-testid="primary-cta"]');
    const secondaryCTA = page.locator('[data-testid="secondary-cta"]');

    await expect(primaryCTA).toBeVisible();
    await expect(secondaryCTA).toBeVisible();

    // Both buttons should be in the hero section
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection.locator('[data-testid="primary-cta"]')).toBeVisible();
    await expect(heroSection.locator('[data-testid="secondary-cta"]')).toBeVisible();
  });
});
