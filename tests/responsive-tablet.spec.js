// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Responsive Design - Tablet E2E Tests (NFR-1)
 * Verifies the page displays correctly on tablet devices
 */

test.describe('Responsive Design - Tablet', () => {
  // Test Case 1: Load page at 768px viewport width
  test('TC1: Page displays with tablet-optimized layout at 768px viewport', async ({ page }) => {
    // Set tablet viewport (768px - common tablet width)
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('/');

    // Verify page loads correctly
    await expect(page).toHaveTitle(/MirDB/);

    // Verify main sections are visible
    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();

    // Verify navigation is visible
    const nav = page.locator('nav');
    await expect(nav).toBeVisible();

    // Verify hero heading adjusts for tablet (should be smaller than desktop 4rem)
    const heroH1 = page.locator('#hero h1');
    await expect(heroH1).toBeVisible();
    const fontSize = await heroH1.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // At 768px, font size should be around 2.5rem (40px) based on CSS media query
    expect(fontSize).toBeLessThanOrEqual(42);

    // Verify CTA buttons adapt to column layout at tablet width
    const ctaButtons = page.locator('.cta-buttons');
    await expect(ctaButtons).toBeVisible();
    const flexDirection = await ctaButtons.evaluate((el) => {
      return window.getComputedStyle(el).flexDirection;
    });
    // At 768px, buttons should be in column layout
    expect(flexDirection).toBe('column');

    // Verify features section is visible
    const features = page.locator('#features');
    await expect(features).toBeVisible();

    // Verify content is readable without horizontal scrolling
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = 768;
    expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth + 10); // Allow small tolerance
  });

  // Test Case 2: Check feature cards at tablet viewport
  test('TC2: Feature cards display in 2-column grid at tablet viewport', async ({ page }) => {
    // Set tablet viewport
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('/');

    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Verify features grid container exists
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Verify feature cards are displayed
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(2);

    // Check grid layout using CSS grid properties
    const gridStyle = await featuresGrid.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        display: styles.display,
        gridTemplateColumns: styles.gridTemplateColumns,
      };
    });

    // Verify grid display is used
    expect(gridStyle.display).toBe('grid');

    // At 768px with minmax(300px, 1fr), we should have 2 columns
    // The grid-template-columns computed value should show 2 column values
    const columnWidths = gridStyle.gridTemplateColumns.split(' ').filter(v => v && v !== 'none');
    expect(columnWidths.length).toBeGreaterThanOrEqual(2);

    // Verify first two cards are positioned side by side (same row)
    const firstCard = featureCards.nth(0);
    const secondCard = featureCards.nth(1);

    const firstCardBox = await firstCard.boundingBox();
    const secondCardBox = await secondCard.boundingBox();

    // Both cards should be visible and have bounding boxes
    expect(firstCardBox).not.toBeNull();
    expect(secondCardBox).not.toBeNull();

    if (firstCardBox && secondCardBox) {
      // If 2-column layout, cards should be on the same row (similar Y position)
      // Allow 10px tolerance for slight alignment differences
      expect(Math.abs(firstCardBox.y - secondCardBox.y)).toBeLessThan(10);

      // Cards should be side by side (second card is to the right of first)
      expect(secondCardBox.x).toBeGreaterThan(firstCardBox.x);
    }
  });

  // Test Case 3: Load page at 1024px viewport width (large tablet)
  test('TC3: Page displays correctly at large tablet width (1024px)', async ({ page }) => {
    // Set large tablet viewport (1024px - iPad landscape/large tablet)
    await page.setViewportSize({ width: 1024, height: 768 });
    await page.goto('/');

    // Verify page loads correctly
    await expect(page).toHaveTitle(/MirDB/);

    // Verify all main sections are visible and accessible
    const sections = ['#hero', '#features', '#quick-start', '#commands', '#configuration'];

    for (const selector of sections) {
      const section = page.locator(selector);
      await section.scrollIntoViewIfNeeded();
      await expect(section).toBeVisible();
    }

    // Verify navigation remains functional
    const nav = page.locator('nav');
    await expect(nav).toBeVisible();

    // Verify navigation links are visible at this width
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    // Verify features grid displays correctly
    const featuresGrid = page.locator('.features-grid');
    await featuresGrid.scrollIntoViewIfNeeded();
    await expect(featuresGrid).toBeVisible();

    // At 1024px with padding and max-width constraints, grid shows multi-column layout
    const gridStyle = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });
    const columnWidths = gridStyle.split(' ').filter(v => v && v !== 'none');
    // At 1024px with section padding and max-width: 1200px, expect at least 2 columns
    // (actual available width accounting for padding is less than 1024px)
    expect(columnWidths.length).toBeGreaterThanOrEqual(2);

    // Verify content is readable without horizontal scrolling
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    expect(bodyScrollWidth).toBeLessThanOrEqual(1024 + 10);

    // Verify footer is visible at the bottom
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Verify code blocks are scrollable (not overflowing page)
    const preElements = page.locator('pre');
    const preCount = await preElements.count();

    for (let i = 0; i < Math.min(preCount, 3); i++) {
      const pre = preElements.nth(i);
      await pre.scrollIntoViewIfNeeded();
      const preBox = await pre.boundingBox();
      if (preBox) {
        // Pre elements should not exceed viewport width
        expect(preBox.width).toBeLessThanOrEqual(1024);
      }
    }
  });
});
