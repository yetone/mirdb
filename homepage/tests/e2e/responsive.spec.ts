/**
 * Responsive Design E2E Tests
 * Owner: Scenarios 8, 9, 10 - Responsive Design
 *
 * Tests across viewports:
 * - Desktop (1920x1080): Multi-column layout
 * - Tablet (768x1024): Adapted layout
 * - Mobile (375x667): Single-column layout
 * - Touch target sizes
 * - No horizontal overflow
 */

import { test, expect } from '@playwright/test';

test.describe('Responsive Design - Desktop (1920x1080)', () => {
  test.beforeEach(async ({ page }) => {
    // Set viewport to desktop size
    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.goto('/');
  });

  test('TC1: Page renders without horizontal scrollbar at 1920x1080', async ({ page }) => {
    // Check that page width doesn't exceed viewport
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);

    // Body scroll width should not exceed viewport width
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth);

    // Also check for horizontal scrollbar presence
    const hasHorizontalScrollbar = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScrollbar).toBe(false);
  });

  test('TC2: Hero section is properly aligned at desktop size', async ({ page }) => {
    const heroSection = page.locator('#hero');

    // Hero section should be visible
    await expect(heroSection).toBeVisible();

    // Check hero container has proper centering
    const heroContainer = heroSection.locator('.container').first();
    const containerBox = await heroContainer.boundingBox();

    // Container should be centered (not start at x=0)
    expect(containerBox).not.toBeNull();
    if (containerBox) {
      // Container should be centered with margins on both sides at 1920px
      expect(containerBox.x).toBeGreaterThan(0);

      // Check adequate spacing (padding)
      const heroSectionBox = await heroSection.boundingBox();
      if (heroSectionBox) {
        expect(heroSectionBox.height).toBeGreaterThan(200); // Adequate vertical spacing
      }
    }

    // Check h1 is present and centered (text-align: center)
    const h1 = heroSection.locator('h1');
    await expect(h1).toBeVisible();
    const h1Style = await h1.evaluate((el) => window.getComputedStyle(el).textAlign);
    expect(h1Style).toBe('center');
  });

  test('TC3: Features section uses multi-column layout on desktop', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('.features-section');
    await expect(featuresSection).toBeVisible();

    // Find the features grid
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Check grid display style
    const displayStyle = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(displayStyle).toBe('grid');

    // Check grid-template-columns for multi-column layout (should be 3 columns on desktop)
    const gridTemplateColumns = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });

    // Should have 3 columns on desktop (values like "300px 300px 300px" or similar)
    const columns = gridTemplateColumns.split(' ').filter(col => col.trim() !== '');
    expect(columns.length).toBeGreaterThanOrEqual(2); // At least 2 columns for multi-column layout
    expect(columns.length).toBeLessThanOrEqual(4); // But not too many
  });

  test('TC4: All content containers are within viewport width', async ({ page }) => {
    // Get viewport width
    const viewportWidth = 1920;

    // Check main content areas don't overflow
    const containers = page.locator('.container');
    const containerCount = await containers.count();

    for (let i = 0; i < containerCount; i++) {
      const container = containers.nth(i);
      const isVisible = await container.isVisible();

      if (isVisible) {
        const box = await container.boundingBox();
        if (box) {
          // Container should fit within viewport
          expect(box.x).toBeGreaterThanOrEqual(0);
          expect(box.x + box.width).toBeLessThanOrEqual(viewportWidth);
        }
      }
    }

    // Check sections don't overflow
    const sections = page.locator('section, .section');
    const sectionCount = await sections.count();

    for (let i = 0; i < sectionCount; i++) {
      const section = sections.nth(i);
      const isVisible = await section.isVisible();

      if (isVisible) {
        const box = await section.boundingBox();
        if (box) {
          expect(box.width).toBeLessThanOrEqual(viewportWidth);
        }
      }
    }

    // Check feature cards don't overflow
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();

    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      const isVisible = await card.isVisible();

      if (isVisible) {
        const box = await card.boundingBox();
        if (box) {
          expect(box.x).toBeGreaterThanOrEqual(0);
          expect(box.x + box.width).toBeLessThanOrEqual(viewportWidth);
        }
      }
    }
  });

  test('Quick Start section uses row layout on desktop', async ({ page }) => {
    const quickStartContent = page.locator('.quick-start-content');
    await expect(quickStartContent).toBeVisible();

    // Check flex-direction is row on desktop
    const flexDirection = await quickStartContent.evaluate((el) => {
      return window.getComputedStyle(el).flexDirection;
    });
    expect(flexDirection).toBe('row');
  });

  test('All sections have adequate spacing at desktop size', async ({ page }) => {
    // Check that sections have proper padding
    const sections = page.locator('.section');
    const sectionCount = await sections.count();

    for (let i = 0; i < sectionCount; i++) {
      const section = sections.nth(i);
      const isVisible = await section.isVisible();

      if (isVisible) {
        const paddingTop = await section.evaluate((el) => {
          return parseInt(window.getComputedStyle(el).paddingTop);
        });
        const paddingBottom = await section.evaluate((el) => {
          return parseInt(window.getComputedStyle(el).paddingBottom);
        });

        // Sections should have reasonable vertical padding (at least 32px)
        expect(paddingTop).toBeGreaterThanOrEqual(32);
        expect(paddingBottom).toBeGreaterThanOrEqual(32);
      }
    }
  });

  test('Text is readable at desktop size', async ({ page }) => {
    // Check body font size is reasonable for desktop
    const bodyFontSize = await page.evaluate(() => {
      return parseInt(window.getComputedStyle(document.body).fontSize);
    });
    expect(bodyFontSize).toBeGreaterThanOrEqual(14);
    expect(bodyFontSize).toBeLessThanOrEqual(20);

    // Check h1 is larger for desktop readability
    const h1 = page.locator('h1').first();
    const h1FontSize = await h1.evaluate((el) => {
      return parseInt(window.getComputedStyle(el).fontSize);
    });
    expect(h1FontSize).toBeGreaterThanOrEqual(32); // Should be large for hero

    // Check line-height for readability
    const lineHeight = await page.evaluate(() => {
      return parseFloat(window.getComputedStyle(document.body).lineHeight);
    });
    // Line-height should be at least 1.4 for readability (or a pixel value > font-size)
    expect(lineHeight).toBeGreaterThanOrEqual(bodyFontSize * 1.4);
  });
});
