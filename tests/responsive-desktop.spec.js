// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

/**
 * Test Suite: Responsive Design - Desktop
 *
 * This test suite validates that the MirDB homepage displays correctly
 * on desktop viewports with proper full layout and optimal content width.
 *
 * Test Cases:
 * 1. Full desktop layout is displayed with optimal content width at 1440px
 * 2. Feature cards display in 3-4 column grid at desktop width
 * 3. Content does not stretch beyond readable width on ultra-wide displays
 */

// Desktop viewport configurations
const DESKTOP_VIEWPORT = { width: 1440, height: 900 };
const ULTRA_WIDE_VIEWPORT = { width: 2560, height: 1440 };
const MINIMUM_DESKTOP_VIEWPORT = { width: 1280, height: 800 };

test.describe('Responsive Design - Desktop', () => {
  test.beforeEach(async ({ page }) => {
    // Set desktop viewport before navigating
    await page.setViewportSize(DESKTOP_VIEWPORT);

    // Navigate to the homepage
    await page.goto('file://' + path.join(__dirname, '..', 'index.html'));

    // Wait for page to fully load
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC1: Full desktop layout is displayed with optimal content width at 1440px viewport', async ({ page }) => {
    // Verify viewport is set correctly
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(viewportWidth).toBe(DESKTOP_VIEWPORT.width);

    // Check that all main sections are visible
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

    // Verify navigation is in horizontal layout (not stacked like mobile)
    const nav = page.locator('.nav');
    const navStyle = await nav.evaluate((el) => ({
      display: window.getComputedStyle(el).display,
      flexDirection: window.getComputedStyle(el).flexDirection
    }));
    expect(navStyle.display).toBe('flex');
    expect(navStyle.flexDirection).toBe('row');

    // Verify hero section has optimal content width
    const heroContent = page.locator('.hero-content');
    const heroBox = await heroContent.boundingBox();
    expect(heroBox).not.toBeNull();

    // Hero content should be centered and have reasonable max-width
    const heroStyle = await heroContent.evaluate((el) => ({
      maxWidth: window.getComputedStyle(el).maxWidth
    }));
    expect(heroStyle.maxWidth).not.toBe('none');

    // Verify hero title uses full desktop font size (not reduced mobile size)
    const heroTitle = page.locator('.hero h1');
    const titleFontSize = await heroTitle.evaluate((el) =>
      parseFloat(window.getComputedStyle(el).fontSize)
    );
    // Desktop h1 should be 3.5rem (56px at 16px base)
    expect(titleFontSize).toBeGreaterThanOrEqual(50);

    // Verify footer is in horizontal layout (not stacked)
    const footerContent = page.locator('.footer-content');
    const footerStyle = await footerContent.evaluate((el) => ({
      flexDirection: window.getComputedStyle(el).flexDirection
    }));
    expect(footerStyle.flexDirection).toBe('row');

    // Verify no horizontal scrolling needed
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth);
  });

  test('TC2: Feature cards display in 3-4 column grid at desktop width', async ({ page }) => {
    // Navigate to features section
    const featuresGrid = page.locator('[data-testid="features-grid"]');
    await expect(featuresGrid).toBeVisible();

    // Get all feature cards
    const featureCards = featuresGrid.locator('.feature-card');
    const cardCount = await featureCards.count();

    // Ensure there are feature cards to test (should be 4 cards)
    expect(cardCount).toBe(4);

    // Get bounding boxes for all cards
    const cardBoundingBoxes = [];
    for (let i = 0; i < cardCount; i++) {
      const box = await featureCards.nth(i).boundingBox();
      expect(box).not.toBeNull();
      cardBoundingBoxes.push(box);
    }

    // Get the grid computed style to verify multi-column layout
    const gridComputedStyle = await featuresGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns
      };
    });

    // Should be grid display
    expect(gridComputedStyle.display).toBe('grid');

    // Count actual columns by analyzing card positions
    // Cards in the same row should have similar Y positions
    const rowGroups = new Map();
    for (const box of cardBoundingBoxes) {
      // Round Y position to group cards in the same row
      const rowY = Math.round(box.y / 10) * 10;
      if (!rowGroups.has(rowY)) {
        rowGroups.set(rowY, []);
      }
      rowGroups.get(rowY).push(box);
    }

    // Get the number of cards in the first row (determines column count)
    const firstRowCards = Array.from(rowGroups.values())[0];
    const columnCount = firstRowCards.length;

    // At 1440px, with 4 cards and minmax(280px, 1fr), should display 3-4 columns
    expect(columnCount).toBeGreaterThanOrEqual(3);
    expect(columnCount).toBeLessThanOrEqual(4);

    // Verify cards are side by side (have different X positions)
    const xPositions = firstRowCards.map(box => box.x);
    const uniqueXPositions = new Set(xPositions);
    expect(uniqueXPositions.size).toBe(columnCount);

    // Verify cards have reasonable widths for multi-column layout
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    for (const box of cardBoundingBoxes) {
      // Each card should be a fraction of the viewport width (with padding/gaps)
      expect(box.width).toBeLessThan(viewportWidth / 2);
      expect(box.width).toBeGreaterThan(200);
    }
  });

  test('TC3: Content does not stretch beyond readable width on ultra-wide displays', async ({ page }) => {
    // Set ultra-wide viewport (2560px width)
    await page.setViewportSize(ULTRA_WIDE_VIEWPORT);
    await page.waitForLoadState('domcontentloaded');

    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(viewportWidth).toBe(ULTRA_WIDE_VIEWPORT.width);

    // Verify max-width constraint on main content areas
    const maxWidthConstraint = 1200; // --max-width CSS variable value

    // Check navigation content max-width
    const nav = page.locator('.nav');
    const navBox = await nav.boundingBox();
    expect(navBox).not.toBeNull();
    expect(navBox.width).toBeLessThanOrEqual(maxWidthConstraint + 100); // Some padding tolerance

    // Check features grid max-width
    const featuresGrid = page.locator('[data-testid="features-grid"]');
    const gridBox = await featuresGrid.boundingBox();
    expect(gridBox).not.toBeNull();
    expect(gridBox.width).toBeLessThanOrEqual(maxWidthConstraint + 100);

    // Check quick-start content max-width
    const quickStartContent = page.locator('.quick-start-content');
    const quickStartBox = await quickStartContent.boundingBox();
    expect(quickStartBox).not.toBeNull();
    // Quick start has 800px max-width
    expect(quickStartBox.width).toBeLessThanOrEqual(850);

    // Check footer content max-width
    const footerContent = page.locator('.footer-content');
    const footerBox = await footerContent.boundingBox();
    expect(footerBox).not.toBeNull();
    expect(footerBox.width).toBeLessThanOrEqual(maxWidthConstraint + 100);

    // Verify content is centered, not left-aligned on ultra-wide
    // Content should have equal margins on both sides
    const heroContent = page.locator('.hero-content');
    const heroBox = await heroContent.boundingBox();
    expect(heroBox).not.toBeNull();

    // Hero content should be approximately centered
    const leftMargin = heroBox.x;
    const rightMargin = viewportWidth - (heroBox.x + heroBox.width);
    // Margins should be roughly equal (centered content)
    expect(Math.abs(leftMargin - rightMargin)).toBeLessThan(100);
  });

  test('Navigation displays horizontally at desktop viewport', async ({ page }) => {
    // Verify navigation is in horizontal layout
    const nav = page.locator('.nav');
    await expect(nav).toBeVisible();

    const navStyle = await nav.evaluate((el) => ({
      display: window.getComputedStyle(el).display,
      flexDirection: window.getComputedStyle(el).flexDirection,
      justifyContent: window.getComputedStyle(el).justifyContent,
      alignItems: window.getComputedStyle(el).alignItems
    }));

    expect(navStyle.display).toBe('flex');
    expect(navStyle.flexDirection).toBe('row');
    expect(navStyle.justifyContent).toBe('space-between');
    expect(navStyle.alignItems).toBe('center');

    // Logo and nav links should be on the same row
    const logo = page.locator('.logo');
    const navLinks = page.locator('.nav-links');

    const logoBox = await logo.boundingBox();
    const navLinksBox = await navLinks.boundingBox();

    expect(logoBox).not.toBeNull();
    expect(navLinksBox).not.toBeNull();

    // They should have overlapping Y ranges (same row)
    expect(navLinksBox.y).toBeLessThan(logoBox.y + logoBox.height);
    expect(navLinksBox.y + navLinksBox.height).toBeGreaterThan(logoBox.y);

    // Nav links should display inline
    const navLinksStyle = await navLinks.evaluate((el) => ({
      display: window.getComputedStyle(el).display,
      gap: window.getComputedStyle(el).gap
    }));
    expect(navLinksStyle.display).toBe('flex');
  });

  test('Hero section displays with full desktop styling', async ({ page }) => {
    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    // Verify hero has appropriate padding for desktop
    const heroStyle = await hero.evaluate((el) => ({
      padding: window.getComputedStyle(el).padding
    }));
    expect(heroStyle.padding).not.toBe('0px');

    // Verify hero content is properly constrained
    const heroContent = page.locator('.hero-content');
    const heroContentStyle = await heroContent.evaluate((el) => ({
      maxWidth: window.getComputedStyle(el).maxWidth,
      margin: window.getComputedStyle(el).margin
    }));
    expect(heroContentStyle.maxWidth).not.toBe('none');

    // Verify tagline font size is larger on desktop
    const tagline = page.locator('.tagline');
    const taglineSize = await tagline.evaluate((el) =>
      parseFloat(window.getComputedStyle(el).fontSize)
    );
    // Desktop tagline should be 1.5rem (24px)
    expect(taglineSize).toBeGreaterThanOrEqual(24);

    // Verify CTA buttons display inline
    const ctaButtons = page.locator('.cta-buttons');
    const ctaStyle = await ctaButtons.evaluate((el) => ({
      display: window.getComputedStyle(el).display,
      flexWrap: window.getComputedStyle(el).flexWrap,
      justifyContent: window.getComputedStyle(el).justifyContent
    }));
    expect(ctaStyle.display).toBe('flex');
    expect(ctaStyle.justifyContent).toBe('center');

    // Get button boxes to verify they're side by side
    const primaryBtn = page.locator('[data-testid="primary-cta"]');
    const secondaryBtn = page.locator('[data-testid="secondary-cta"]');

    const primaryBox = await primaryBtn.boundingBox();
    const secondaryBox = await secondaryBtn.boundingBox();

    expect(primaryBox).not.toBeNull();
    expect(secondaryBox).not.toBeNull();

    // Buttons should be on the same row (similar Y position)
    expect(Math.abs(primaryBox.y - secondaryBox.y)).toBeLessThan(10);
  });

  test('Footer displays horizontally with proper spacing at desktop viewport', async ({ page }) => {
    const footer = page.locator('.footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Footer content should be in row layout on desktop
    const footerContent = page.locator('.footer-content');
    const footerStyle = await footerContent.evaluate((el) => ({
      display: window.getComputedStyle(el).display,
      flexDirection: window.getComputedStyle(el).flexDirection,
      justifyContent: window.getComputedStyle(el).justifyContent
    }));

    expect(footerStyle.display).toBe('flex');
    expect(footerStyle.flexDirection).toBe('row');
    expect(footerStyle.justifyContent).toBe('space-between');

    // Footer info and nav should be on opposite sides
    const footerInfo = page.locator('.footer-info');
    const footerNav = page.locator('.footer-nav');

    const infoBox = await footerInfo.boundingBox();
    const navBox = await footerNav.boundingBox();

    expect(infoBox).not.toBeNull();
    expect(navBox).not.toBeNull();

    // Footer nav should be to the right of footer info
    expect(navBox.x).toBeGreaterThan(infoBox.x);

    // They should be on the same row
    expect(Math.abs(infoBox.y - navBox.y)).toBeLessThan(50);
  });

  test('Quick start section displays properly at desktop width', async ({ page }) => {
    const quickStart = page.locator('#quick-start');
    await quickStart.scrollIntoViewIfNeeded();
    await expect(quickStart).toBeVisible();

    // Verify quick start content has max-width constraint
    const quickStartContent = page.locator('.quick-start-content');
    const contentStyle = await quickStartContent.evaluate((el) => ({
      maxWidth: window.getComputedStyle(el).maxWidth,
      margin: window.getComputedStyle(el).margin
    }));
    expect(contentStyle.maxWidth).toBe('800px');

    // Verify code blocks have proper width
    const codeBlocks = quickStart.locator('pre');
    const codeCount = await codeBlocks.count();
    expect(codeCount).toBeGreaterThan(0);

    for (let i = 0; i < codeCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      const box = await codeBlock.boundingBox();
      expect(box).not.toBeNull();
      // Code blocks should fit within the content area
      expect(box.width).toBeLessThanOrEqual(850);
    }
  });

  test('Layout adapts properly at minimum desktop breakpoint (1280px)', async ({ page }) => {
    await page.setViewportSize(MINIMUM_DESKTOP_VIEWPORT);
    await page.waitForLoadState('domcontentloaded');

    // Navigation should still be horizontal
    const nav = page.locator('.nav');
    const navStyle = await nav.evaluate((el) => ({
      flexDirection: window.getComputedStyle(el).flexDirection
    }));
    expect(navStyle.flexDirection).toBe('row');

    // Features should still display in multi-column grid
    const featuresGrid = page.locator('[data-testid="features-grid"]');
    const gridStyle = await featuresGrid.evaluate((el) => ({
      display: window.getComputedStyle(el).display,
      gridTemplateColumns: window.getComputedStyle(el).gridTemplateColumns
    }));
    expect(gridStyle.display).toBe('grid');

    // Get feature cards and count columns
    const featureCards = featuresGrid.locator('.feature-card');
    const cardCount = await featureCards.count();

    const cardBoundingBoxes = [];
    for (let i = 0; i < cardCount; i++) {
      const box = await featureCards.nth(i).boundingBox();
      cardBoundingBoxes.push(box);
    }

    // Group cards by row
    const rowGroups = new Map();
    for (const box of cardBoundingBoxes) {
      const rowY = Math.round(box.y / 10) * 10;
      if (!rowGroups.has(rowY)) {
        rowGroups.set(rowY, []);
      }
      rowGroups.get(rowY).push(box);
    }

    const firstRowCards = Array.from(rowGroups.values())[0];
    // At 1280px, should still have at least 3 columns
    expect(firstRowCards.length).toBeGreaterThanOrEqual(3);

    // Footer should be horizontal
    const footerContent = page.locator('.footer-content');
    const footerStyle = await footerContent.evaluate((el) => ({
      flexDirection: window.getComputedStyle(el).flexDirection
    }));
    expect(footerStyle.flexDirection).toBe('row');
  });
});
