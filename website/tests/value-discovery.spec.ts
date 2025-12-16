import { test, expect } from '@playwright/test';

/**
 * User Story Validation - Value Discovery (US-1)
 * Validates that users can immediately understand MirDB's value proposition.
 *
 * This test suite covers:
 * - Test Case 2: Product name, tagline, and CTA visible without scrolling (above-the-fold)
 * - Test Case 3: Primary CTA button is prominently visible in hero
 * - Test Case 4: Tagline clearly communicates Memcached compatibility and Rust
 *
 * Note: Test Case 1 is a manual user testing scenario.
 */
test.describe('Value Discovery - User Story Validation', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage with fresh state
    await page.goto('/');
    // Wait for hero section to be fully loaded
    await page.waitForSelector('.hero-section');
  });

  test('TC2: Hero section above-the-fold - Product name, tagline, and CTA visible without scrolling', async ({ page }) => {
    // Get viewport dimensions
    const viewportSize = page.viewportSize();
    expect(viewportSize).not.toBeNull();

    // Check product name (h1) is visible within viewport
    const productName = page.locator('.hero-section h1#hero-title');
    await expect(productName).toBeVisible();
    await expect(productName).toContainText('MirDB');

    // Verify the product name is within viewport bounds (above the fold)
    const productNameBox = await productName.boundingBox();
    expect(productNameBox).not.toBeNull();
    expect(productNameBox!.y + productNameBox!.height).toBeLessThan(viewportSize!.height);

    // Check tagline is visible within viewport
    const tagline = page.locator('.hero-section .tagline');
    await expect(tagline).toBeVisible();

    // Verify the tagline is within viewport bounds
    const taglineBox = await tagline.boundingBox();
    expect(taglineBox).not.toBeNull();
    expect(taglineBox!.y + taglineBox!.height).toBeLessThan(viewportSize!.height);

    // Check CTA buttons are visible within viewport
    const ctaButtons = page.locator('.hero-section .cta-buttons');
    await expect(ctaButtons).toBeVisible();

    // Verify CTA buttons are within viewport bounds
    const ctaBox = await ctaButtons.boundingBox();
    expect(ctaBox).not.toBeNull();
    expect(ctaBox!.y + ctaBox!.height).toBeLessThan(viewportSize!.height);

    // Verify both buttons exist
    const getStartedBtn = page.locator('.hero-section .cta-primary');
    const githubBtn = page.locator('.hero-section .cta-secondary');
    await expect(getStartedBtn).toBeVisible();
    await expect(githubBtn).toBeVisible();
  });

  test('TC3: Primary CTA button prominently visible in hero', async ({ page }) => {
    // Check that the primary CTA ("Get Started") is prominently visible
    const primaryCTA = page.locator('.hero-section .cta-primary');
    await expect(primaryCTA).toBeVisible();
    await expect(primaryCTA).toContainText('Get Started');

    // Verify the button is within the viewport (above the fold)
    const viewportSize = page.viewportSize();
    const ctaBox = await primaryCTA.boundingBox();
    expect(ctaBox).not.toBeNull();
    expect(viewportSize).not.toBeNull();
    expect(ctaBox!.y + ctaBox!.height).toBeLessThan(viewportSize!.height);

    // Verify the button has reasonable size (is prominent)
    expect(ctaBox!.width).toBeGreaterThan(80);
    expect(ctaBox!.height).toBeGreaterThan(30);

    // Verify the button links to quickstart section
    const href = await primaryCTA.getAttribute('href');
    expect(href).toBe('#quickstart');

    // Verify button is clickable/interactive
    await expect(primaryCTA).toBeEnabled();
  });

  test('TC4: Tagline clearly communicates Memcached compatibility and Rust', async ({ page }) => {
    // Check that the tagline is present and contains the required keywords
    const tagline = page.locator('.hero-section .tagline');
    await expect(tagline).toBeVisible();

    // Get the tagline text
    const taglineText = await tagline.textContent();
    expect(taglineText).not.toBeNull();

    // Verify the tagline mentions key value propositions
    // Per PRD: "A persistent key-value store with Memcached protocol compatibility, written in Rust"
    expect(taglineText!.toLowerCase()).toContain('memcached');
    expect(taglineText!.toLowerCase()).toContain('rust');
    expect(taglineText!.toLowerCase()).toContain('key-value');

    // Verify it communicates protocol compatibility
    expect(taglineText!.toLowerCase()).toContain('compatibility');

    // Verify it indicates persistence
    expect(taglineText!.toLowerCase()).toContain('persistent');
  });

  test('Value proposition is understood quickly - hero section loads fast', async ({ page }) => {
    // Measure time for hero section to become visible
    const startTime = Date.now();

    // Wait for the hero section to be fully visible
    await page.waitForSelector('.hero-section', { state: 'visible' });

    // Also wait for key content to be rendered
    await page.waitForSelector('.hero-section h1');
    await page.waitForSelector('.hero-section .tagline');
    await page.waitForSelector('.hero-section .cta-primary');

    const loadTime = Date.now() - startTime;

    // Content should be visible almost instantly (under 2 seconds for local test)
    // This supports the "within 5 seconds" requirement for understanding the value proposition
    expect(loadTime).toBeLessThan(2000);

    // Verify all critical elements are present
    const h1 = page.locator('.hero-section h1');
    await expect(h1).toBeVisible();
    await expect(h1).toContainText('MirDB');
  });

  test('Hero section maintains visibility on different viewport sizes', async ({ page }) => {
    // Test common viewport sizes to ensure above-the-fold visibility
    const viewports = [
      { width: 1920, height: 1080, name: 'Desktop Full HD' },
      { width: 1366, height: 768, name: 'Desktop HD' },
      { width: 1280, height: 720, name: 'Desktop 720p' },
    ];

    for (const viewport of viewports) {
      await page.setViewportSize({ width: viewport.width, height: viewport.height });
      await page.goto('/');

      // Verify all hero elements are visible within viewport
      const productName = page.locator('.hero-section h1#hero-title');
      const tagline = page.locator('.hero-section .tagline');
      const ctaButtons = page.locator('.hero-section .cta-buttons');

      await expect(productName).toBeVisible();
      await expect(tagline).toBeVisible();
      await expect(ctaButtons).toBeVisible();

      // Verify elements are within viewport bounds
      const productNameBox = await productName.boundingBox();
      const taglineBox = await tagline.boundingBox();
      const ctaBox = await ctaButtons.boundingBox();

      expect(productNameBox).not.toBeNull();
      expect(taglineBox).not.toBeNull();
      expect(ctaBox).not.toBeNull();

      // All elements should be above the fold
      expect(productNameBox!.y + productNameBox!.height).toBeLessThan(viewport.height);
      expect(taglineBox!.y + taglineBox!.height).toBeLessThan(viewport.height);
      expect(ctaBox!.y + ctaBox!.height).toBeLessThan(viewport.height);
    }
  });
});
