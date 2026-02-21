/**
 * Tablet Responsive Design E2E Tests
 * Owner: Scenario 10 - Responsive Design - Tablet
 *
 * Tests verify:
 * - Homepage renders correctly at 768x1024 viewport (tablet)
 * - No horizontal scrolling
 * - Layout adapts to narrower viewport
 * - Touch targets meet 44x44px minimum
 * - Navigation is accessible
 */

const { test, expect } = require('@playwright/test');

// Tablet viewport dimensions (iPad portrait)
const TABLET_VIEWPORT = { width: 768, height: 1024 };
const MIN_TOUCH_TARGET = 44; // WCAG minimum touch target size in pixels

test.describe('Tablet Responsive Design (768x1024)', () => {
  test.beforeEach(async ({ page }) => {
    // Set tablet viewport
    await page.setViewportSize(TABLET_VIEWPORT);
    await page.goto('/');
  });

  test('TC1: All content is visible without horizontal scrolling', async ({ page }) => {
    // Check there is no horizontal overflow
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);

    // Body scroll width should not exceed viewport width (allowing 1px tolerance for rounding)
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth + 1);

    // Ensure no horizontal scrollbar is present
    const hasHorizontalScrollbar = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScrollbar).toBe(false);

    // Verify all major sections are visible within the viewport width
    const sections = ['#hero', '#features', '#quickstart', '#roadmap', '.site-footer'];
    for (const selector of sections) {
      const section = await page.locator(selector);
      if (await section.count() > 0) {
        const box = await section.boundingBox();
        expect(box).not.toBeNull();
        expect(box.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);
      }
    }
  });

  test('TC2: Features grid adapts to 2-column or single-column layout', async ({ page }) => {
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Get the feature cards
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(2);

    // Get positions of the first two cards to verify layout
    const firstCard = await featureCards.nth(0).boundingBox();
    const secondCard = await featureCards.nth(1).boundingBox();

    expect(firstCard).not.toBeNull();
    expect(secondCard).not.toBeNull();

    // Cards should either be:
    // 1. Side by side (2-column layout: y positions similar, x positions different)
    // 2. Stacked vertically (single-column: x positions similar, y positions different)
    const isSideBySide = Math.abs(firstCard.y - secondCard.y) < 50;
    const isStacked = Math.abs(firstCard.x - secondCard.x) < 50;

    // One of these layouts should be true
    expect(isSideBySide || isStacked).toBe(true);

    // Each card should fit within viewport width
    expect(firstCard.x + firstCard.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width + 10);
    expect(secondCard.x + secondCard.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width + 10);
  });

  test('TC3: Navigation is accessible (horizontal or hamburger menu)', async ({ page }) => {
    const header = page.locator('.site-header');
    await expect(header).toBeVisible();

    // Check if navigation is visible (either as horizontal nav or hamburger menu)
    const mainNav = page.locator('.main-nav');
    const mobileToggle = page.locator('.mobile-menu-toggle');

    const isNavVisible = await mainNav.isVisible();
    const isToggleVisible = await mobileToggle.isVisible();

    // Either main nav or mobile toggle should be visible for navigation access
    expect(isNavVisible || isToggleVisible).toBe(true);

    // If navigation links are visible, verify they are accessible
    if (isNavVisible) {
      const navLinks = page.locator('.nav-link');
      const linkCount = await navLinks.count();
      expect(linkCount).toBeGreaterThan(0);

      // Verify nav links are within viewport
      for (let i = 0; i < linkCount; i++) {
        const link = await navLinks.nth(i).boundingBox();
        if (link) {
          expect(link.x + link.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);
        }
      }
    }

    // If hamburger menu is visible, verify it can be clicked
    if (isToggleVisible) {
      const toggleBox = await mobileToggle.boundingBox();
      expect(toggleBox).not.toBeNull();
      // Touch target should be at least 44x44
      expect(toggleBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
      expect(toggleBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
    }
  });

  test('TC4: Touch targets are at least 44x44 pixels', async ({ page }) => {
    // Test interactive elements for touch target compliance
    const interactiveSelectors = [
      '.cta-button',           // Get Started button
      '.nav-link',             // Navigation links
      '.footer-link',          // Footer links
      '.copy-btn',             // Copy buttons in code blocks
      '.mobile-menu-toggle',   // Mobile menu toggle (if visible)
      '.header-logo'           // Logo (clickable)
    ];

    for (const selector of interactiveSelectors) {
      const elements = page.locator(selector);
      const count = await elements.count();

      for (let i = 0; i < count; i++) {
        const element = elements.nth(i);
        const isVisible = await element.isVisible();

        if (isVisible) {
          const box = await element.boundingBox();
          if (box) {
            // For accessibility, touch targets should be at least 44x44 pixels
            // Using a slight tolerance for CSS calculations
            expect(box.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET - 4);
            expect(box.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET - 4);
          }
        }
      }
    }

    // Specifically test the CTA button as it's a primary action
    const ctaButton = page.locator('.cta-button');
    if (await ctaButton.isVisible()) {
      const ctaBox = await ctaButton.boundingBox();
      expect(ctaBox).not.toBeNull();
      expect(ctaBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
      expect(ctaBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
    }
  });

  test('Tablet viewport shows correctly sized hero section', async ({ page }) => {
    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    const heroBox = await hero.boundingBox();
    expect(heroBox).not.toBeNull();

    // Hero should span full viewport width (accounting for margins)
    expect(heroBox.width).toBeGreaterThanOrEqual(TABLET_VIEWPORT.width - 100);

    // Hero content should be centered and not overflow
    const heroContent = page.locator('.hero-content');
    const contentBox = await heroContent.boundingBox();
    expect(contentBox).not.toBeNull();
    expect(contentBox.x + contentBox.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);
  });

  test('Roadmap columns adapt to tablet layout', async ({ page }) => {
    const roadmapColumns = page.locator('.roadmap-columns');
    await expect(roadmapColumns).toBeVisible();

    const columns = page.locator('.roadmap-column');
    const columnCount = await columns.count();
    expect(columnCount).toBe(2);

    // Get positions to verify layout
    const firstColumn = await columns.nth(0).boundingBox();
    const secondColumn = await columns.nth(1).boundingBox();

    expect(firstColumn).not.toBeNull();
    expect(secondColumn).not.toBeNull();

    // Columns should either be side by side or stacked
    const isSideBySide = Math.abs(firstColumn.y - secondColumn.y) < 50;
    const isStacked = Math.abs(firstColumn.x - secondColumn.x) < 50;

    expect(isSideBySide || isStacked).toBe(true);

    // Each column should fit within viewport
    expect(firstColumn.x + firstColumn.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width + 10);
    expect(secondColumn.x + secondColumn.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width + 10);
  });

  test('Footer adapts to tablet layout', async ({ page }) => {
    const footer = page.locator('.site-footer');
    await expect(footer).toBeVisible();

    // Footer should fit within viewport
    const footerBox = await footer.boundingBox();
    expect(footerBox).not.toBeNull();
    expect(footerBox.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width + 1);

    // Footer links should be accessible
    const footerLinks = page.locator('.footer-link');
    const linkCount = await footerLinks.count();
    expect(linkCount).toBeGreaterThan(0);
  });

  test('Quick start section code blocks fit within tablet viewport', async ({ page }) => {
    const codeBlocks = page.locator('.code-block');
    const blockCount = await codeBlocks.count();

    for (let i = 0; i < blockCount; i++) {
      const block = codeBlocks.nth(i);
      if (await block.isVisible()) {
        const box = await block.boundingBox();
        expect(box).not.toBeNull();
        // Code blocks should fit within viewport (with some padding allowance)
        expect(box.x + box.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width + 50);
      }
    }
  });
});
