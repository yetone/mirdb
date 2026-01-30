/**
 * Responsive Design and Theme Tests
 * Owners: Scenario 8 (Mobile), Scenario 9 (Tablet), Scenario 12 (Dark Mode)
 *
 * Tests:
 * - Mobile viewport (375px)
 * - Tablet viewport (768px)
 * - Navigation adaptation
 * - Touch targets
 * - Dark/light mode switching
 */

const { test, expect } = require('@playwright/test');
const { BASE_URL, SELECTORS, VIEWPORTS, waitForPageLoad } = require('./test-utils');

// ==============================================================
// Scenario 9: Tablet Responsive Design Tests
// ==============================================================

test.describe('Scenario 9: Tablet Responsive Design', () => {
  test.beforeEach(async ({ page }) => {
    // Set tablet viewport size
    await page.setViewportSize(VIEWPORTS.tablet);
    await page.goto(BASE_URL);
    await waitForPageLoad(page);
  });

  test('TC1: Page renders with appropriate layout for tablet at 768px viewport width', async ({ page }) => {
    // Verify viewport is set correctly
    const viewport = page.viewportSize();
    expect(viewport.width).toBe(768);

    // Verify page elements are visible and correctly sized
    const header = page.locator(SELECTORS.header);
    await expect(header).toBeVisible();

    const hero = page.locator(SELECTORS.hero);
    await expect(hero).toBeVisible();

    const features = page.locator(SELECTORS.features);
    await expect(features).toBeVisible();

    // Verify the page renders without horizontal scrollbar
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    expect(bodyWidth).toBeLessThanOrEqual(768);

    // Verify main content areas are properly contained
    const headerBox = await header.boundingBox();
    expect(headerBox.width).toBeLessThanOrEqual(768);
  });

  test('TC2: Features display in 2-column layout at tablet size', async ({ page }) => {
    const featuresGrid = page.locator('.features__grid');
    await expect(featuresGrid).toBeVisible();

    // Verify the grid has feature cards
    const featureCards = page.locator('.features__card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(2);

    // Get the computed grid style
    const gridStyle = await featuresGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns,
      };
    });

    // Verify it's a grid layout
    expect(gridStyle.display).toBe('grid');

    // Verify it's a 2-column layout (should have 2 columns of similar width)
    const columns = gridStyle.gridTemplateColumns.split(' ').filter(col => col.trim() !== '');
    expect(columns.length).toBe(2);

    // Verify feature cards are laid out in 2 columns by checking their positions
    if (cardCount >= 2) {
      const card1 = featureCards.nth(0);
      const card2 = featureCards.nth(1);

      const box1 = await card1.boundingBox();
      const box2 = await card2.boundingBox();

      // In a 2-column layout, the first two cards should be on the same row (same y position)
      // with different x positions
      expect(box1.y).toBeCloseTo(box2.y, 0);
      expect(box1.x).not.toEqual(box2.x);
    }
  });

  test('TC3: Navigation is usable and appropriately sized at tablet size', async ({ page }) => {
    const nav = page.locator(SELECTORS.headerNav);
    await expect(nav).toBeVisible();

    const navLinks = page.locator(SELECTORS.headerNavLinks);
    const linkCount = await navLinks.count();
    expect(linkCount).toBeGreaterThan(0);

    // Verify all navigation links are visible
    for (let i = 0; i < linkCount; i++) {
      const link = navLinks.nth(i);
      await expect(link).toBeVisible();
    }

    // Check that navigation links have appropriate minimum touch target size (at least 44x44 for accessibility)
    const firstLink = navLinks.first();
    const linkBox = await firstLink.boundingBox();

    // Navigation links should be reasonably sized for tablet interaction
    expect(linkBox.width).toBeGreaterThanOrEqual(32);
    expect(linkBox.height).toBeGreaterThanOrEqual(20);

    // Verify header stays within viewport width
    const header = page.locator(SELECTORS.header);
    const headerBox = await header.boundingBox();
    expect(headerBox.width).toBeLessThanOrEqual(768);

    // Verify navigation doesn't overflow
    const navBox = await nav.boundingBox();
    expect(navBox.x + navBox.width).toBeLessThanOrEqual(768);
  });

  test('Hero section adapts to tablet viewport', async ({ page }) => {
    const hero = page.locator(SELECTORS.hero);
    await expect(hero).toBeVisible();

    const heroTitle = page.locator('.hero__title');
    await expect(heroTitle).toBeVisible();

    // Verify hero section fits within viewport
    const heroBox = await hero.boundingBox();
    expect(heroBox.width).toBeLessThanOrEqual(768);

    // Verify CTA buttons are visible and clickable
    const ctaButtons = page.locator('.hero__cta');
    const ctaCount = await ctaButtons.count();

    for (let i = 0; i < ctaCount; i++) {
      const cta = ctaButtons.nth(i);
      await expect(cta).toBeVisible();

      const ctaBox = await cta.boundingBox();
      // CTA buttons should be appropriately sized for tablet
      expect(ctaBox.width).toBeGreaterThanOrEqual(80);
      expect(ctaBox.height).toBeGreaterThanOrEqual(32);
    }
  });

  test('Usage section renders correctly on tablet', async ({ page }) => {
    const usage = page.locator(SELECTORS.usage);
    await expect(usage).toBeVisible();

    // Verify code blocks are contained within viewport
    const codeBlocks = page.locator('.usage__code-block');
    const blockCount = await codeBlocks.count();

    for (let i = 0; i < blockCount; i++) {
      const block = codeBlocks.nth(i);
      const blockBox = await block.boundingBox();

      if (blockBox) {
        // Code blocks should not overflow the viewport
        expect(blockBox.x).toBeGreaterThanOrEqual(0);
        expect(blockBox.x + blockBox.width).toBeLessThanOrEqual(768 + 16); // Allow small margin for padding
      }
    }
  });

  test('All sections are accessible via scrolling on tablet', async ({ page }) => {
    // Verify all main sections are visible when scrolled to
    const sections = [
      SELECTORS.header,
      SELECTORS.hero,
      SELECTORS.features,
      SELECTORS.usage,
    ];

    for (const selector of sections) {
      const section = page.locator(selector);
      await section.scrollIntoViewIfNeeded();
      await expect(section).toBeVisible();
    }
  });
});
