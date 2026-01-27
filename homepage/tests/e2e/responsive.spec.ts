/**
 * Responsive design e2e tests.
 * Owner: Scenario 6 (mobile tests) & Scenario 7 (tablet tests)
 *
 * Tests viewport-specific layouts and responsive behavior.
 */

import { test, expect } from '@playwright/test';

/**
 * ======================================
 * TABLET VIEWPORT TESTS (768px)
 * Owner: Scenario 7 - Responsive Design Tablet
 * ======================================
 */

test.describe('Tablet Viewport (768px)', () => {
  test.beforeEach(async ({ page }) => {
    // Set tablet viewport (iPad size)
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('/');
  });

  test('homepage renders correctly at tablet viewport', async ({ page }) => {
    // Verify page loads
    await expect(page).toHaveTitle(/MirDB/);

    // Check that main sections are visible
    await expect(page.locator('.hero')).toBeVisible();
    await expect(page.locator('.features')).toBeVisible();
    await expect(page.locator('.quick-start')).toBeVisible();
    await expect(page.locator('.footer')).toBeVisible();

    // Verify viewport width is applied
    const viewport = page.viewportSize();
    expect(viewport?.width).toBe(768);
  });

  test('feature cards display in 2-column grid layout on tablet', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('.features');
    await expect(featuresSection).toBeVisible();

    // Get the features grid
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Get computed styles to verify grid layout
    const gridStyles = await featuresGrid.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        display: styles.display,
        gridTemplateColumns: styles.gridTemplateColumns
      };
    });

    // Verify grid display
    expect(gridStyles.display).toBe('grid');

    // At 768px, should have 2 columns
    // gridTemplateColumns will show computed pixel values like "340px 340px"
    const columnCount = gridStyles.gridTemplateColumns.split(' ').filter(col => col.trim() !== '').length;
    expect(columnCount).toBe(2);

    // Verify feature cards are present
    const featureCards = page.locator('.feature-card');
    await expect(featureCards.first()).toBeVisible();
  });

  test('hero section adapts to tablet viewport', async ({ page }) => {
    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    // Check hero content is visible
    const heroTitle = page.locator('.hero-title');
    await expect(heroTitle).toBeVisible();

    const heroTagline = page.locator('.hero-tagline');
    await expect(heroTagline).toBeVisible();

    const heroDescription = page.locator('.hero-description');
    await expect(heroDescription).toBeVisible();

    // Check CTA buttons are visible and properly sized
    const ctaButtons = page.locator('.hero-cta .btn');
    await expect(ctaButtons.first()).toBeVisible();

    // Verify hero section fits within viewport without horizontal overflow
    const heroBox = await hero.boundingBox();
    expect(heroBox).not.toBeNull();
    if (heroBox) {
      // Hero should not exceed viewport width
      expect(heroBox.width).toBeLessThanOrEqual(768);
    }

    // Verify hero content is centered
    const heroContent = page.locator('.hero-content');
    const contentBox = await heroContent.boundingBox();
    expect(contentBox).not.toBeNull();
    if (contentBox) {
      // Content should be centered (roughly equal margins on each side)
      const leftMargin = contentBox.x;
      const rightMargin = 768 - (contentBox.x + contentBox.width);
      // Allow some tolerance for centering (within 50px)
      expect(Math.abs(leftMargin - rightMargin)).toBeLessThan(50);
    }
  });

  test('navigation is visible and properly sized on tablet', async ({ page }) => {
    const nav = page.locator('.nav');
    await expect(nav).toBeVisible();

    // Verify nav links are visible at tablet size (not hidden in hamburger)
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    // Navigation should fit within viewport
    const navBox = await nav.boundingBox();
    expect(navBox).not.toBeNull();
    if (navBox) {
      expect(navBox.width).toBeLessThanOrEqual(768);
    }
  });

  test('commands grid shows 2 columns on tablet', async ({ page }) => {
    // Scroll to commands section
    const commandsSection = page.locator('.commands');
    await commandsSection.scrollIntoViewIfNeeded();
    await expect(commandsSection).toBeVisible();

    // Get the commands grid
    const commandsGrid = page.locator('.commands-grid');
    await expect(commandsGrid).toBeVisible();

    // Verify grid columns
    const gridStyles = await commandsGrid.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        display: styles.display,
        gridTemplateColumns: styles.gridTemplateColumns
      };
    });

    expect(gridStyles.display).toBe('grid');

    // Should have 2 columns at tablet viewport
    const columnCount = gridStyles.gridTemplateColumns.split(' ').filter(col => col.trim() !== '').length;
    expect(columnCount).toBe(2);
  });

  test('footer adapts to tablet layout', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator('.footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Footer content should be visible
    const footerContent = page.locator('.footer-content');
    await expect(footerContent).toBeVisible();

    // Footer links should be accessible
    const footerLinks = page.locator('.footer-links a');
    await expect(footerLinks.first()).toBeVisible();

    // Footer should not overflow
    const footerBox = await footer.boundingBox();
    expect(footerBox).not.toBeNull();
    if (footerBox) {
      expect(footerBox.width).toBeLessThanOrEqual(768);
    }
  });

  test('no horizontal scrollbar on tablet viewport', async ({ page }) => {
    // Check that page doesn't have horizontal overflow
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });

    expect(hasHorizontalScroll).toBe(false);
  });

  test('touch targets meet minimum size requirements on tablet', async ({ page }) => {
    // Check CTA buttons have minimum touch target size (44px)
    const buttons = page.locator('.btn');
    const buttonCount = await buttons.count();

    for (let i = 0; i < buttonCount; i++) {
      const button = buttons.nth(i);
      if (await button.isVisible()) {
        const box = await button.boundingBox();
        if (box) {
          // Minimum touch target should be 44x44
          expect(box.height).toBeGreaterThanOrEqual(44);
        }
      }
    }
  });
});

/**
 * ======================================
 * MOBILE VIEWPORT TESTS (375px)
 * Owner: Scenario 6 - Responsive Design Mobile
 * ======================================
 */

test.describe('Mobile Viewport (375px)', () => {
  test.beforeEach(async ({ page }) => {
    // Set mobile viewport (iPhone SE size)
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');
  });

  // Placeholder for Scenario 6 to implement mobile-specific tests
  test('placeholder for mobile tests', async ({ page }) => {
    // This test is a placeholder for Scenario 6
    await expect(page).toHaveTitle(/MirDB/);
  });
});
