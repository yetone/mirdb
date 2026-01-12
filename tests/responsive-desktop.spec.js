// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '../index.html');

// Configure tests to use desktop viewport (1920x1080)
test.describe('Responsive Design - Desktop Viewport', () => {
  test.use({
    viewport: { width: 1920, height: 1080 }
  });

  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  // Test Case 1: No horizontal scrollbar, all content visible at 1920x1080
  test('TC1: should have no horizontal scrollbar at 1920x1080 viewport', async ({ page }) => {
    // Get the document's scroll width and client width
    const scrollInfo = await page.evaluate(() => {
      return {
        scrollWidth: document.documentElement.scrollWidth,
        clientWidth: document.documentElement.clientWidth,
        bodyScrollWidth: document.body.scrollWidth,
        bodyClientWidth: document.body.clientWidth,
        hasHorizontalScrollbar: document.documentElement.scrollWidth > document.documentElement.clientWidth
      };
    });

    // Verify no horizontal scrollbar (scroll width should not exceed client width)
    expect(scrollInfo.hasHorizontalScrollbar).toBe(false);
    expect(scrollInfo.scrollWidth).toBeLessThanOrEqual(scrollInfo.clientWidth + 1); // +1 for rounding

    // Verify hero section is visible
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Verify features section is visible
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify footer is present
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Verify the viewport dimensions are correct
    const viewportSize = page.viewportSize();
    expect(viewportSize?.width).toBe(1920);
    expect(viewportSize?.height).toBe(1080);
  });

  // Test Case 2: Feature cards displayed in multi-column grid layout
  test('TC2: should display feature cards in multi-column grid layout at desktop', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Get the features grid
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Get all feature cards
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(3);

    // Check grid layout - verify cards are in multiple columns
    const gridStyles = await featuresGrid.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        display: computed.display,
        gridTemplateColumns: computed.gridTemplateColumns
      };
    });

    // Verify it's using grid display
    expect(gridStyles.display).toBe('grid');

    // Verify multi-column layout by checking if cards are positioned in different columns
    // Get positions of first few cards
    const cardPositions = [];
    for (let i = 0; i < Math.min(cardCount, 3); i++) {
      const card = featureCards.nth(i);
      const boundingBox = await card.boundingBox();
      if (boundingBox) {
        cardPositions.push({
          x: boundingBox.x,
          y: boundingBox.y,
          width: boundingBox.width
        });
      }
    }

    // At desktop viewport, cards should be side by side (different x positions on same row)
    // or at least not all stacked vertically
    if (cardPositions.length >= 2) {
      // Check that not all cards have the same x position (would mean single column)
      const uniqueXPositions = new Set(cardPositions.map(pos => Math.round(pos.x)));

      // At 1920px, we should have multiple columns
      expect(uniqueXPositions.size).toBeGreaterThan(1);
    }

    // Verify cards have reasonable width at desktop (not full width like mobile)
    for (const pos of cardPositions) {
      // Cards should not be extremely narrow or take up the entire viewport
      expect(pos.width).toBeGreaterThan(200);
      expect(pos.width).toBeLessThan(1500);
    }
  });

  // Test Case 3: Code blocks have appropriate width, no overflow
  test('TC3: should display code blocks with appropriate width and no overflow at desktop', async ({ page }) => {
    // Check if there are code blocks on the page
    const codeBlocks = page.locator('.code-block, pre code, .quick-start pre');
    const codeBlockCount = await codeBlocks.count();

    if (codeBlockCount === 0) {
      // If using the root index.html which may not have code blocks,
      // this test passes as N/A
      test.skip();
      return;
    }

    // Scroll to ensure code blocks are in view
    const firstCodeBlock = codeBlocks.first();
    await firstCodeBlock.scrollIntoViewIfNeeded();

    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);

      // Skip invisible elements
      const isVisible = await codeBlock.isVisible().catch(() => false);
      if (!isVisible) continue;

      const boundingBox = await codeBlock.boundingBox();
      if (!boundingBox) continue;

      // Verify code block width is within viewport
      expect(boundingBox.width).toBeLessThanOrEqual(1920);
      expect(boundingBox.width).toBeGreaterThan(0);

      // Verify code block is not causing horizontal overflow
      const overflowInfo = await codeBlock.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        const parent = el.parentElement;
        return {
          overflow: computed.overflow,
          overflowX: computed.overflowX,
          scrollWidth: el.scrollWidth,
          clientWidth: el.clientWidth,
          parentWidth: parent ? parent.clientWidth : 0
        };
      });

      // Code blocks should handle overflow gracefully (either with scroll or hidden overflow)
      const hasOverflowHandling =
        overflowInfo.overflowX === 'auto' ||
        overflowInfo.overflowX === 'scroll' ||
        overflowInfo.overflowX === 'hidden' ||
        overflowInfo.overflow === 'auto' ||
        overflowInfo.overflow === 'scroll' ||
        overflowInfo.overflow === 'hidden' ||
        overflowInfo.scrollWidth <= overflowInfo.clientWidth + 5; // Allow small tolerance

      expect(hasOverflowHandling).toBe(true);
    }
  });

  // Additional test: Verify navigation is fully visible at desktop
  test('should display navigation fully visible at desktop viewport', async ({ page }) => {
    // Check for navigation elements
    const nav = page.locator('nav, .nav, .navigation, header nav');
    const navExists = await nav.count() > 0;

    // If there's a navigation element, verify it's fully visible
    if (navExists) {
      const firstNav = nav.first();
      await expect(firstNav).toBeVisible();

      const boundingBox = await firstNav.boundingBox();
      if (boundingBox) {
        // Navigation should be within viewport
        expect(boundingBox.x).toBeGreaterThanOrEqual(0);
        expect(boundingBox.x + boundingBox.width).toBeLessThanOrEqual(1920);
      }
    }

    // Verify hero CTA buttons are visible and properly laid out
    const ctaButtons = page.locator('.cta-buttons a, .hero-ctas a');
    const ctaCount = await ctaButtons.count();

    if (ctaCount >= 2) {
      const firstButton = ctaButtons.first();
      const secondButton = ctaButtons.nth(1);

      await expect(firstButton).toBeVisible();
      await expect(secondButton).toBeVisible();

      // At desktop viewport, buttons should be side by side (same y position)
      const firstBox = await firstButton.boundingBox();
      const secondBox = await secondButton.boundingBox();

      if (firstBox && secondBox) {
        // Y positions should be similar (same row) at desktop
        const yDifference = Math.abs(firstBox.y - secondBox.y);
        expect(yDifference).toBeLessThan(50); // Allow some tolerance for alignment
      }
    }
  });

  // Verify proper spacing at desktop viewport
  test('should have proper spacing between sections at desktop viewport', async ({ page }) => {
    // Check that sections are properly spaced
    const sections = page.locator('section, .section');
    const sectionCount = await sections.count();

    if (sectionCount >= 2) {
      for (let i = 0; i < sectionCount - 1; i++) {
        const currentSection = sections.nth(i);
        const nextSection = sections.nth(i + 1);

        const isCurrentVisible = await currentSection.isVisible().catch(() => false);
        const isNextVisible = await nextSection.isVisible().catch(() => false);

        if (isCurrentVisible && isNextVisible) {
          const currentBox = await currentSection.boundingBox();
          const nextBox = await nextSection.boundingBox();

          if (currentBox && nextBox) {
            // Sections should not overlap
            const currentBottom = currentBox.y + currentBox.height;
            expect(nextBox.y).toBeGreaterThanOrEqual(currentBottom - 1); // -1 for rounding tolerance
          }
        }
      }
    }
  });
});
