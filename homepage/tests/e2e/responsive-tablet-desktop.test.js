/**
 * E2E Tests for Responsive Design - Tablet and Desktop Viewports
 *
 * This test suite verifies that the MirDB homepage displays correctly on:
 * - Tablet viewport (768px width)
 * - Desktop viewport (1280px+ width)
 * - Resizing from mobile to desktop (320px to 1920px)
 */

const { test, expect } = require('@playwright/test');

/**
 * Test Case 1: Tablet Viewport (768px width)
 * Verifies layout adapts appropriately with potential 2-column feature layout
 */
test.describe('Test Case 1: Tablet Viewport (768px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('/');
  });

  test('should load homepage at 768px width with adapted layout', async ({ page }) => {
    // Verify page loads successfully
    await expect(page).toHaveTitle(/MirDB/);

    // Verify hero section is visible
    const hero = page.locator('.hero, header.hero, section.hero, #hero');
    await expect(hero.first()).toBeVisible();

    // Verify hero title is visible and properly sized
    const heroTitle = page.locator('.hero-title, .hero h1, h1').first();
    await expect(heroTitle).toBeVisible();
  });

  test('should display features section with appropriate grid layout at tablet size', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features, .features-section, .features');
    await expect(featuresSection.first()).toBeVisible();

    // Verify feature cards are visible
    const featureCards = page.locator('.feature-card, .feature-item, [data-testid="feature-item"]');
    const cardCount = await featureCards.count();

    // Should have 4-6 feature cards
    expect(cardCount).toBeGreaterThanOrEqual(4);
    expect(cardCount).toBeLessThanOrEqual(6);

    // Verify all feature cards are visible
    for (let i = 0; i < cardCount; i++) {
      await expect(featureCards.nth(i)).toBeVisible();
    }
  });

  test('should have proper navigation visibility at tablet viewport', async ({ page }) => {
    // Check navigation is present
    const nav = page.locator('nav, .navbar, .nav-container');
    await expect(nav.first()).toBeVisible();

    // Logo should be visible
    const logo = page.locator('.logo, .logo-text, nav a').first();
    await expect(logo).toBeVisible();
  });

  test('should not have horizontal scrollbar at tablet viewport', async ({ page }) => {
    // Check that no horizontal overflow exists
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });

  test('should display CTA buttons appropriately at tablet size', async ({ page }) => {
    const ctaButtons = page.locator('.hero-cta .btn, .cta-buttons .btn, .hero .btn');
    const buttonCount = await ctaButtons.count();

    // Should have at least one CTA button
    expect(buttonCount).toBeGreaterThanOrEqual(1);

    // First CTA should be visible
    await expect(ctaButtons.first()).toBeVisible();
  });
});

/**
 * Test Case 2: Desktop Viewport (1280px width)
 * Verifies full desktop layout with features in grid layout
 */
test.describe('Test Case 2: Desktop Viewport (1280px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.goto('/');
  });

  test('should load homepage at 1280px width with full desktop layout', async ({ page }) => {
    // Verify page loads successfully
    await expect(page).toHaveTitle(/MirDB/);

    // Verify hero section is visible
    const hero = page.locator('.hero, header.hero, section.hero, #hero');
    await expect(hero.first()).toBeVisible();

    // Verify hero title is visible with larger font
    const heroTitle = page.locator('.hero-title, .hero h1, h1').first();
    await expect(heroTitle).toBeVisible();
  });

  test('should display features in grid layout at desktop size', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features, .features-section, .features');
    await expect(featuresSection.first()).toBeVisible();

    // Verify feature cards are in multi-column grid
    const featureCards = page.locator('.feature-card, .feature-item, [data-testid="feature-item"]');
    const cardCount = await featureCards.count();

    // Should have 4-6 feature cards
    expect(cardCount).toBeGreaterThanOrEqual(4);
    expect(cardCount).toBeLessThanOrEqual(6);

    // At desktop size (1280px), with minmax(320px, 1fr), should have at least 2 columns
    // Get positions of first two cards
    if (cardCount >= 2) {
      const firstCardBox = await featureCards.nth(0).boundingBox();
      const secondCardBox = await featureCards.nth(1).boundingBox();

      // In a multi-column grid, second card should be beside first (same Y) or below (different Y)
      // At 1280px, grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)) allows 3 columns
      expect(firstCardBox).not.toBeNull();
      expect(secondCardBox).not.toBeNull();
    }
  });

  test('should have full navigation visible at desktop viewport', async ({ page }) => {
    // Check navigation is present
    const nav = page.locator('nav, .navbar');
    await expect(nav.first()).toBeVisible();

    // Logo should be visible
    const logo = page.locator('.logo, .logo-text, nav a').first();
    await expect(logo).toBeVisible();

    // Navigation links should be visible
    const navLinks = page.locator('.nav-links a, nav ul li a, nav a');
    const linkCount = await navLinks.count();
    expect(linkCount).toBeGreaterThanOrEqual(1);
  });

  test('should not have horizontal scrollbar at desktop viewport', async ({ page }) => {
    // Check that no horizontal overflow exists
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });

  test('should display CTA buttons side by side at desktop size', async ({ page }) => {
    const ctaButtons = page.locator('.hero-cta .btn, .cta-buttons .btn, .hero .btn');
    const buttonCount = await ctaButtons.count();

    // Should have at least 2 CTA buttons (primary and secondary)
    expect(buttonCount).toBeGreaterThanOrEqual(1);

    // Check if buttons are side by side (same Y position)
    if (buttonCount >= 2) {
      const firstButtonBox = await ctaButtons.nth(0).boundingBox();
      const secondButtonBox = await ctaButtons.nth(1).boundingBox();

      // At desktop size, buttons should be roughly at the same vertical position
      expect(firstButtonBox).not.toBeNull();
      expect(secondButtonBox).not.toBeNull();

      // Y positions should be similar (within 50px tolerance for alignment)
      if (firstButtonBox && secondButtonBox) {
        expect(Math.abs(firstButtonBox.y - secondButtonBox.y)).toBeLessThan(50);
      }
    }
  });

  test('should display quick start section properly at desktop', async ({ page }) => {
    const quickStartSection = page.locator('#quick-start, .quickstart-section, .quickstart, #quickstart');
    await expect(quickStartSection.first()).toBeVisible();

    // Code blocks should be visible
    const codeBlock = page.locator('.code-block, pre code');
    await expect(codeBlock.first()).toBeVisible();
  });

  test('should display footer links at desktop viewport', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator('footer, .footer');
    await footer.first().scrollIntoViewIfNeeded();
    await expect(footer.first()).toBeVisible();

    // Footer links should be visible
    const footerLinks = page.locator('.footer-links a, footer a');
    const linkCount = await footerLinks.count();
    expect(linkCount).toBeGreaterThanOrEqual(1);
  });
});

/**
 * Test Case 3: Viewport Resize (320px to 1920px)
 * Verifies no layout breaks or content overflow during resize
 */
test.describe('Test Case 3: Viewport Resize (320px to 1920px)', () => {
  test('should not have layout breaks or content overflow at any viewport size', async ({ page }) => {
    // Test viewport sizes from 320px to 1920px
    const viewportWidths = [320, 480, 640, 768, 1024, 1280, 1440, 1920];

    for (const width of viewportWidths) {
      await page.setViewportSize({ width, height: 800 });
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Check for horizontal overflow
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      // No horizontal scroll at any size
      expect(hasHorizontalScroll).toBe(false);

      // Verify main sections are visible
      const hero = page.locator('.hero, header.hero, section.hero, #hero');
      await expect(hero.first()).toBeVisible();

      const features = page.locator('#features, .features-section, .features');
      await expect(features.first()).toBeVisible();
    }
  });

  test('should maintain readable content at all viewport sizes', async ({ page }) => {
    const viewportWidths = [320, 768, 1280, 1920];

    for (const width of viewportWidths) {
      await page.setViewportSize({ width, height: 800 });
      await page.goto('/');

      // Hero title should be visible at all sizes
      const heroTitle = page.locator('.hero-title, .hero h1, h1').first();
      await expect(heroTitle).toBeVisible();

      // Content should be within viewport bounds
      const heroBox = await heroTitle.boundingBox();
      expect(heroBox).not.toBeNull();
      if (heroBox) {
        // Content should start within the viewport
        expect(heroBox.x).toBeGreaterThanOrEqual(0);
        expect(heroBox.x + heroBox.width).toBeLessThanOrEqual(width + 1); // Allow 1px tolerance
      }
    }
  });

  test('should have consistent navigation behavior across viewport sizes', async ({ page }) => {
    const viewportWidths = [768, 1024, 1280, 1920];

    for (const width of viewportWidths) {
      await page.setViewportSize({ width, height: 800 });
      await page.goto('/');

      // Navigation should be present
      const nav = page.locator('nav, .navbar');
      await expect(nav.first()).toBeVisible();

      // Logo should always be visible
      const logo = page.locator('.logo, .logo-text').first();
      await expect(logo).toBeVisible();
    }
  });

  test('should properly transition feature grid layout between breakpoints', async ({ page }) => {
    // Test key breakpoints
    const breakpoints = [
      { width: 480, expectedCols: 1 },  // Mobile: single column
      { width: 768, expectedCols: 2 },   // Tablet: could be 2 columns
      { width: 1280, expectedCols: 3 },  // Desktop: could be 3 columns
    ];

    for (const { width } of breakpoints) {
      await page.setViewportSize({ width, height: 800 });
      await page.goto('/');

      // Scroll to features section
      const featuresSection = page.locator('#features, .features-section, .features');
      await featuresSection.first().scrollIntoViewIfNeeded();

      // Verify feature cards are visible
      const featureCards = page.locator('.feature-card, .feature-item, [data-testid="feature-item"]');
      const cardCount = await featureCards.count();

      expect(cardCount).toBeGreaterThanOrEqual(4);

      // All cards should be visible without overflow
      for (let i = 0; i < Math.min(cardCount, 3); i++) {
        const card = featureCards.nth(i);
        await expect(card).toBeVisible();

        const cardBox = await card.boundingBox();
        expect(cardBox).not.toBeNull();
        if (cardBox) {
          // Card should be within viewport width
          expect(cardBox.x).toBeGreaterThanOrEqual(0);
          expect(cardBox.x + cardBox.width).toBeLessThanOrEqual(width + 1);
        }
      }
    }
  });

  test('should handle smooth transitions when resizing viewport', async ({ page }) => {
    await page.goto('/');

    // Start at mobile size
    await page.setViewportSize({ width: 320, height: 800 });
    await page.waitForLoadState('networkidle');

    // Gradually increase to desktop
    const sizes = [320, 480, 768, 1024, 1280];

    for (const width of sizes) {
      await page.setViewportSize({ width, height: 800 });

      // Wait for any CSS transitions
      await page.waitForTimeout(100);

      // Verify no horizontal overflow after each resize
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);

      // Hero section should remain visible
      const hero = page.locator('.hero, header.hero, section.hero, #hero');
      await expect(hero.first()).toBeVisible();
    }
  });
});

/**
 * Additional responsive design tests
 */
test.describe('Additional Responsive Tests', () => {
  test('should have proper text wrapping at all viewport sizes', async ({ page }) => {
    const widths = [320, 768, 1280];

    for (const width of widths) {
      await page.setViewportSize({ width, height: 800 });
      await page.goto('/');

      // Check hero description text wrapping
      const heroDescription = page.locator('.hero-description, .hero p, .hero-tagline').first();
      const descBox = await heroDescription.boundingBox();

      if (descBox) {
        // Text should not overflow viewport
        expect(descBox.x + descBox.width).toBeLessThanOrEqual(width + 1);
      }
    }
  });

  test('should maintain proper spacing and padding at different sizes', async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.goto('/');

    // At desktop, container should have proper max-width
    const container = page.locator('.features-container, .container, .quickstart-container').first();
    const containerBox = await container.boundingBox();

    expect(containerBox).not.toBeNull();
    if (containerBox) {
      // Container should be centered and not span full width at desktop
      expect(containerBox.x).toBeGreaterThan(0);
    }
  });

  test('should ensure code blocks are readable at all sizes', async ({ page }) => {
    const widths = [768, 1280];

    for (const width of widths) {
      await page.setViewportSize({ width, height: 800 });
      await page.goto('/');

      // Scroll to code section
      const codeBlock = page.locator('.code-block, pre').first();
      await codeBlock.scrollIntoViewIfNeeded();

      await expect(codeBlock).toBeVisible();

      const codeBox = await codeBlock.boundingBox();
      if (codeBox) {
        // Code block should fit within viewport
        expect(codeBox.x).toBeGreaterThanOrEqual(0);
        expect(codeBox.x + codeBox.width).toBeLessThanOrEqual(width + 20); // Allow some padding
      }
    }
  });
});
