// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

/**
 * Test Suite: Responsive Design - Tablet
 *
 * This test suite validates that the MirDB homepage displays correctly
 * on tablet devices with proper responsive layout.
 *
 * Test Cases:
 * 1. Content displays in tablet-optimized layout at 768px viewport width
 * 2. Feature cards display in 2-column grid or appropriate tablet layout
 * 3. Navigation is fully accessible and usable on tablet
 */

// Tablet viewport configuration (768px width - standard tablet breakpoint)
const TABLET_VIEWPORT = { width: 768, height: 1024 };
// Additional tablet viewport for testing range (1024px - upper tablet bound)
const TABLET_LARGE_VIEWPORT = { width: 1024, height: 768 };

test.describe('Responsive Design - Tablet', () => {
  test.beforeEach(async ({ page }) => {
    // Set tablet viewport before navigating
    await page.setViewportSize(TABLET_VIEWPORT);

    // Navigate to the homepage
    await page.goto('file://' + path.join(__dirname, '..', 'index.html'));

    // Wait for page to fully load
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC1: Content displays in tablet-optimized layout at 768px viewport width', async ({ page }) => {
    // Verify viewport is set correctly
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(viewportWidth).toBe(768);

    // Get the document body dimensions - ensure no horizontal overflow
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth);

    // Verify all main sections are visible
    const header = page.locator('.header');
    await expect(header).toBeVisible();

    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    const features = page.locator('#features');
    await expect(features).toBeVisible();

    const quickStart = page.locator('#quick-start');
    await expect(quickStart).toBeVisible();

    const footer = page.locator('.footer');
    await expect(footer).toBeVisible();

    // Verify hero content is properly sized for tablet
    const heroTitle = page.locator('.hero h1');
    await expect(heroTitle).toBeVisible();
    const titleFontSize = await heroTitle.evaluate((el) =>
      parseFloat(window.getComputedStyle(el).fontSize)
    );
    // On tablet, title should be readable but not as large as desktop (3.5rem = 56px)
    expect(titleFontSize).toBeGreaterThan(30); // Still prominent
    expect(titleFontSize).toBeLessThanOrEqual(60); // But not too large

    // Verify navigation is in horizontal layout on tablet (not stacked like mobile)
    const nav = page.locator('.nav');
    const navStyle = await nav.evaluate((el) => ({
      flexDirection: window.getComputedStyle(el).flexDirection
    }));
    // At 768px (above mobile breakpoint), nav should be row layout
    expect(navStyle.flexDirection).toBe('row');

    // Verify main content containers don't overflow the viewport
    const contentElements = await page.locator('.hero-content, .features-grid, .quick-start-content, .footer-content').all();
    for (const element of contentElements) {
      const box = await element.boundingBox();
      if (box) {
        expect(box.x).toBeGreaterThanOrEqual(0);
        expect(box.x + box.width).toBeLessThanOrEqual(viewportWidth + 1);
      }
    }
  });

  test('TC2: Feature cards display in 2-column grid or appropriate tablet layout', async ({ page }) => {
    // Navigate to features section
    const featuresGrid = page.locator('[data-testid="features-grid"]');
    await expect(featuresGrid).toBeVisible();

    // Get all feature cards
    const featureCards = featuresGrid.locator('.feature-card');
    const cardCount = await featureCards.count();

    // Ensure there are feature cards to test
    expect(cardCount).toBeGreaterThanOrEqual(3);

    // Get bounding boxes for all cards
    const cardBoundingBoxes = [];
    for (let i = 0; i < cardCount; i++) {
      const box = await featureCards.nth(i).boundingBox();
      expect(box).not.toBeNull();
      cardBoundingBoxes.push(box);
    }

    // Verify grid shows 2-column layout on tablet
    // At 768px with minmax(280px, 1fr), we should fit 2 columns
    const gridComputedStyle = await featuresGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns
      };
    });

    // Should be grid display
    expect(gridComputedStyle.display).toBe('grid');

    // Parse the grid-template-columns to count columns
    const columnValues = gridComputedStyle.gridTemplateColumns.trim().split(/\s+/);
    // On tablet (768px), with minmax(280px, 1fr), we expect 2 columns
    expect(columnValues.length).toBeGreaterThanOrEqual(2);

    // Verify cards are arranged in rows (some cards should be side by side)
    // Check if first two cards have similar Y positions (same row)
    if (cardCount >= 2) {
      const firstCardY = cardBoundingBoxes[0].y;
      const secondCardY = cardBoundingBoxes[1].y;
      // Cards in the same row should have similar Y positions (within tolerance)
      expect(Math.abs(firstCardY - secondCardY)).toBeLessThan(10);
    }

    // Verify cards don't overflow viewport
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    for (const box of cardBoundingBoxes) {
      expect(box.x + box.width).toBeLessThanOrEqual(viewportWidth + 1);
    }
  });

  test('TC3: Navigation is fully accessible and usable on tablet', async ({ page }) => {
    // Check navigation elements are visible
    const nav = page.locator('.nav');
    await expect(nav).toBeVisible();

    // Check logo is visible
    const logo = page.locator('.logo');
    await expect(logo).toBeVisible();

    // Check nav links are visible and accessible
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    // Verify nav links are displayed horizontally
    const navLinksStyle = await navLinks.evaluate((el) => ({
      display: window.getComputedStyle(el).display,
      flexDirection: window.getComputedStyle(el).flexDirection
    }));
    expect(navLinksStyle.display).toBe('flex');

    // Verify all nav links are clickable and have proper size
    const navLinkItems = navLinks.locator('a');
    const linkCount = await navLinkItems.count();
    expect(linkCount).toBeGreaterThanOrEqual(2); // At least Features and GitHub links

    // Check each link is visible and has proper touch target size
    const MIN_TOUCH_TARGET = 44;
    for (let i = 0; i < linkCount; i++) {
      const link = navLinkItems.nth(i);
      await expect(link).toBeVisible();

      const box = await link.boundingBox();
      expect(box).not.toBeNull();

      // At least one dimension should meet minimum touch target
      const meetsTarget = box.width >= MIN_TOUCH_TARGET || box.height >= MIN_TOUCH_TARGET;
      expect(meetsTarget).toBe(true);
    }

    // Verify navigation is keyboard accessible
    // Focus on first nav link
    await navLinkItems.first().focus();
    const firstLinkFocused = await navLinkItems.first().evaluate((el) => el === document.activeElement);
    expect(firstLinkFocused).toBe(true);

    // Tab to next link
    await page.keyboard.press('Tab');
    const secondLinkFocused = await navLinkItems.nth(1).evaluate((el) => el === document.activeElement);
    expect(secondLinkFocused).toBe(true);

    // Verify CTA buttons in hero are accessible on tablet
    const primaryCta = page.locator('[data-testid="primary-cta"]');
    await expect(primaryCta).toBeVisible();
    const ctaBox = await primaryCta.boundingBox();
    expect(ctaBox).not.toBeNull();
    expect(ctaBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
    expect(ctaBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);

    const secondaryCta = page.locator('[data-testid="secondary-cta"]');
    await expect(secondaryCta).toBeVisible();
  });

  test('Footer is properly laid out on tablet viewport', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator('.footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Check footer content layout on tablet
    const footerContent = page.locator('.footer-content');
    const footerStyle = await footerContent.evaluate((el) => ({
      flexDirection: window.getComputedStyle(el).flexDirection,
      display: window.getComputedStyle(el).display
    }));

    // On tablet, footer should use flex layout
    expect(footerStyle.display).toBe('flex');
    // At 768px (above mobile breakpoint), should be row layout
    expect(footerStyle.flexDirection).toBe('row');

    // Verify footer nav links are accessible
    const footerNav = page.locator('.footer-nav');
    await expect(footerNav).toBeVisible();

    // Check all footer links are visible
    const footerLinks = footerNav.locator('a');
    const linkCount = await footerLinks.count();
    expect(linkCount).toBeGreaterThanOrEqual(2);

    for (let i = 0; i < linkCount; i++) {
      await expect(footerLinks.nth(i)).toBeVisible();
    }
  });

  test('Quick start section is properly displayed on tablet', async ({ page }) => {
    // Navigate to quick start section
    const quickStart = page.locator('#quick-start');
    await quickStart.scrollIntoViewIfNeeded();
    await expect(quickStart).toBeVisible();

    // Verify section title is visible
    const title = quickStart.locator('h2');
    await expect(title).toBeVisible();

    // Verify code blocks are contained within viewport
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    const codeBlocks = quickStart.locator('pre');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      await expect(codeBlock).toBeVisible();

      const box = await codeBlock.boundingBox();
      expect(box).not.toBeNull();
      // Code block should be contained within viewport
      expect(box.x + box.width).toBeLessThanOrEqual(viewportWidth + 1);
    }
  });

  test('CTA buttons are properly displayed and accessible on tablet', async ({ page }) => {
    // Check CTA buttons in hero section
    const ctaButtons = page.locator('.cta-buttons');
    await expect(ctaButtons).toBeVisible();

    // Verify CTA buttons are displayed in a flex container
    const ctaStyle = await ctaButtons.evaluate((el) => ({
      display: window.getComputedStyle(el).display,
      flexWrap: window.getComputedStyle(el).flexWrap
    }));
    expect(ctaStyle.display).toBe('flex');

    // Get both CTA buttons
    const primaryCta = page.locator('[data-testid="primary-cta"]');
    const secondaryCta = page.locator('[data-testid="secondary-cta"]');

    await expect(primaryCta).toBeVisible();
    await expect(secondaryCta).toBeVisible();

    // Verify both buttons are on the same line on tablet (enough width)
    const primaryBox = await primaryCta.boundingBox();
    const secondaryBox = await secondaryCta.boundingBox();

    expect(primaryBox).not.toBeNull();
    expect(secondaryBox).not.toBeNull();

    // Both buttons should be on same row (similar Y position)
    expect(Math.abs(primaryBox.y - secondaryBox.y)).toBeLessThan(10);
  });
});

test.describe('Responsive Design - Tablet (Large Viewport)', () => {
  test.beforeEach(async ({ page }) => {
    // Set larger tablet viewport
    await page.setViewportSize(TABLET_LARGE_VIEWPORT);

    // Navigate to the homepage
    await page.goto('file://' + path.join(__dirname, '..', 'index.html'));

    // Wait for page to fully load
    await page.waitForLoadState('domcontentloaded');
  });

  test('Layout adapts properly at 1024px tablet width', async ({ page }) => {
    // Verify viewport is set correctly
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(viewportWidth).toBe(1024);

    // Verify no horizontal overflow
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth);

    // Verify all main sections are visible
    await expect(page.locator('.header')).toBeVisible();
    await expect(page.locator('.hero')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('#quick-start')).toBeVisible();
    await expect(page.locator('.footer')).toBeVisible();

    // At 1024px, feature grid should show multiple columns
    const featuresGrid = page.locator('[data-testid="features-grid"]');
    const gridStyle = await featuresGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns
      };
    });

    expect(gridStyle.display).toBe('grid');
    const columnCount = gridStyle.gridTemplateColumns.trim().split(/\s+/).length;
    expect(columnCount).toBeGreaterThanOrEqual(2);
  });
});
