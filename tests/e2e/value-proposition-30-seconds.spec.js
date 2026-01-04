// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: 30-Second Value Proposition
 *
 * Scenario: User Journey - 30-Second Value Proposition
 * Description: Verify users can understand MirDB's value proposition within 30 seconds of landing
 *
 * This suite validates that critical information is visible above the fold
 * and clearly communicates MirDB's unique value proposition.
 */

test.describe('30-Second Value Proposition', () => {
  // Desktop viewport for above-the-fold testing
  const DESKTOP_VIEWPORT = { width: 1280, height: 720 };

  test.beforeEach(async ({ page }) => {
    // Set desktop viewport
    await page.setViewportSize(DESKTOP_VIEWPORT);
    // Navigate to the landing page
    await page.goto('/');
    // Wait for page to be fully loaded
    await page.waitForLoadState('domcontentloaded');
  });

  /**
   * Test Case 1: Above-the-fold content
   * Input: Check above-the-fold content
   * Expected: Product name, tagline, and key value proposition visible without scrolling on desktop
   */
  test('should display product name, tagline, and value proposition above the fold without scrolling', async ({ page }) => {
    // Verify the hero section exists and is visible
    const heroSection = page.locator('[data-testid="hero"]');
    await expect(heroSection).toBeVisible();

    // Check that the product name "MirDB" is visible
    const productName = page.locator('h1');
    await expect(productName).toBeVisible();
    await expect(productName).toContainText('MirDB');

    // Verify product name is above the fold (within viewport)
    const productNameBox = await productName.boundingBox();
    expect(productNameBox).toBeTruthy();
    expect(productNameBox.y).toBeLessThan(DESKTOP_VIEWPORT.height);
    expect(productNameBox.y + productNameBox.height).toBeLessThan(DESKTOP_VIEWPORT.height);

    // Check that the tagline is visible
    const tagline = page.locator('.tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Persistent Key-Value Store');
    await expect(tagline).toContainText('Memcached Protocol');

    // Verify tagline is above the fold
    const taglineBox = await tagline.boundingBox();
    expect(taglineBox).toBeTruthy();
    expect(taglineBox.y).toBeLessThan(DESKTOP_VIEWPORT.height);
    expect(taglineBox.y + taglineBox.height).toBeLessThan(DESKTOP_VIEWPORT.height);

    // Check that the value proposition description is visible
    const description = page.locator('.hero .description');
    await expect(description).toBeVisible();

    // Verify description is above the fold
    const descriptionBox = await description.boundingBox();
    expect(descriptionBox).toBeTruthy();
    expect(descriptionBox.y).toBeLessThan(DESKTOP_VIEWPORT.height);
  });

  /**
   * Test Case 2: Clear messaging
   * Input: Verify clear messaging
   * Expected: Hero section clearly communicates: persistent storage + Memcached compatibility
   * Type: manual - but we automate the verification of content presence
   */
  test('should clearly communicate persistent storage and Memcached compatibility in hero section', async ({ page }) => {
    // Get the hero section
    const heroSection = page.locator('[data-testid="hero"]');
    await expect(heroSection).toBeVisible();

    // Get all text content from the hero section
    const heroText = await heroSection.textContent();

    // Verify persistent storage is mentioned
    expect(heroText.toLowerCase()).toContain('persistent');

    // Verify Memcached compatibility is mentioned
    expect(heroText.toLowerCase()).toContain('memcached');

    // Verify the specific value proposition elements
    // The tagline should clearly state the core value
    const tagline = page.locator('.tagline');
    const taglineText = await tagline.textContent();
    expect(taglineText).toContain('Persistent');
    expect(taglineText).toContain('Memcached');

    // The description should expand on this
    const description = page.locator('.hero .description');
    const descriptionText = await description.textContent();

    // Should mention persistence
    expect(descriptionText.toLowerCase()).toContain('persist');

    // Should mention it's a drop-in replacement or compatible with memcached
    expect(descriptionText.toLowerCase()).toMatch(/drop-in|replacement|memcached|compatible/);

    // Verify key differentiators are communicated
    // LSM tree architecture should be mentioned
    expect(descriptionText.toLowerCase()).toContain('lsm');

    // Built with Rust should be mentioned
    expect(descriptionText.toLowerCase()).toContain('rust');
  });

  /**
   * Test Case 3: CTA visibility
   * Input: Check CTA visibility
   * Expected: Primary call-to-action is visible above the fold
   */
  test('should display primary CTA button visible above the fold', async ({ page }) => {
    // Find the primary CTA button
    const ctaButton = page.locator('[data-testid="cta"]');
    await expect(ctaButton).toBeVisible();

    // Verify CTA has appropriate text
    const ctaText = await ctaButton.textContent();
    expect(ctaText.toLowerCase()).toMatch(/get started|learn more|try now|documentation|github/);

    // Verify CTA is above the fold
    const ctaBox = await ctaButton.boundingBox();
    expect(ctaBox).toBeTruthy();
    expect(ctaBox.y).toBeLessThan(DESKTOP_VIEWPORT.height);
    expect(ctaBox.y + ctaBox.height).toBeLessThan(DESKTOP_VIEWPORT.height);

    // Verify CTA is clickable (has href for links)
    const href = await ctaButton.getAttribute('href');
    expect(href).toBeTruthy();

    // Verify there are CTA buttons in the hero section
    const heroCtaButtons = page.locator('.hero .cta-buttons a, .hero .cta-buttons button');
    const ctaCount = await heroCtaButtons.count();
    expect(ctaCount).toBeGreaterThanOrEqual(1);

    // Verify all CTA buttons are above the fold
    for (let i = 0; i < ctaCount; i++) {
      const button = heroCtaButtons.nth(i);
      await expect(button).toBeVisible();
      const buttonBox = await button.boundingBox();
      expect(buttonBox).toBeTruthy();
      expect(buttonBox.y + buttonBox.height).toBeLessThan(DESKTOP_VIEWPORT.height);
    }
  });

  /**
   * Additional test: Verify the 30-second comprehension goal
   * This test ensures all critical information is immediately visible and readable
   */
  test('should have all critical value proposition elements visible immediately', async ({ page }) => {
    // All these elements should be visible without any user interaction
    const criticalElements = {
      'Product Name': page.locator('h1'),
      'Tagline': page.locator('.tagline'),
      'Description': page.locator('.hero .description'),
      'Primary CTA': page.locator('[data-testid="cta"]'),
      'Hero Section': page.locator('[data-testid="hero"]')
    };

    for (const [name, locator] of Object.entries(criticalElements)) {
      await expect(locator, `${name} should be visible`).toBeVisible();

      // Verify element is in viewport
      const box = await locator.boundingBox();
      expect(box, `${name} should have a bounding box`).toBeTruthy();
      expect(box.y, `${name} should start within viewport`).toBeLessThan(DESKTOP_VIEWPORT.height);
    }

    // Verify the hero section contains all the key messaging
    const heroSection = page.locator('[data-testid="hero"]');
    const heroText = await heroSection.textContent();

    // Key value proposition elements that should be communicated
    const valuePropositionKeywords = [
      'MirDB',
      'Persistent',
      'Key-Value',
      'Memcached',
      'Rust',
      'LSM'
    ];

    for (const keyword of valuePropositionKeywords) {
      expect(
        heroText.toLowerCase().includes(keyword.toLowerCase()),
        `Hero section should contain "${keyword}"`
      ).toBeTruthy();
    }
  });

  /**
   * Test: Verify responsive above-the-fold on common desktop resolutions
   */
  test('should maintain above-the-fold content visibility on various desktop resolutions', async ({ page }) => {
    const desktopResolutions = [
      { width: 1920, height: 1080, name: 'Full HD' },
      { width: 1366, height: 768, name: 'HD' },
      { width: 1280, height: 720, name: '720p' },
      { width: 1440, height: 900, name: 'WXGA+' }
    ];

    for (const resolution of desktopResolutions) {
      await page.setViewportSize({ width: resolution.width, height: resolution.height });

      // Re-navigate to ensure proper layout
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Check product name visibility
      const productName = page.locator('h1');
      await expect(productName, `Product name should be visible at ${resolution.name}`).toBeVisible();

      const productNameBox = await productName.boundingBox();
      expect(
        productNameBox.y + productNameBox.height,
        `Product name should be above fold at ${resolution.name}`
      ).toBeLessThan(resolution.height);

      // Check CTA visibility
      const ctaButton = page.locator('[data-testid="cta"]');
      await expect(ctaButton, `CTA should be visible at ${resolution.name}`).toBeVisible();

      const ctaBox = await ctaButton.boundingBox();
      expect(
        ctaBox.y + ctaBox.height,
        `CTA should be above fold at ${resolution.name}`
      ).toBeLessThan(resolution.height);
    }
  });
});
