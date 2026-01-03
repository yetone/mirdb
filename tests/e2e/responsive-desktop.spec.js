// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Responsive Design - Desktop Tests
 *
 * Verifies the homepage displays correctly on desktop viewport sizes:
 * - 1920px (large desktop/full HD)
 * - 1280px (standard desktop)
 */

test.describe('Responsive Design - Desktop', () => {
  test.describe('1920px Viewport (Large Desktop)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 1920, height: 1080 });
      await page.goto('/');
    });

    test('no horizontal scrollbar appears at 1920px viewport', async ({ page }) => {
      // Check document width doesn't exceed viewport
      const documentWidth = await page.evaluate(() => document.documentElement.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);

      expect(documentWidth).toBeLessThanOrEqual(viewportWidth);

      // Alternative check: verify no horizontal overflow on body
      const bodyOverflowX = await page.evaluate(() => {
        const body = document.body;
        return body.scrollWidth > body.clientWidth;
      });

      expect(bodyOverflowX).toBe(false);
    });

    test('content is centered and has max-width constraint at 1920px', async ({ page }) => {
      // Check container has max-width set
      const container = page.locator('.container').first();
      await expect(container).toBeVisible();

      const containerBox = await container.boundingBox();
      expect(containerBox).not.toBeNull();

      // Container should have max-width (1200px from CSS) and be centered
      // At 1920px viewport, container should be less than full width
      expect(containerBox.width).toBeLessThan(1920);
      expect(containerBox.width).toBeLessThanOrEqual(1200 + 64); // max-width + padding

      // Container should be centered (left margin should be roughly equal to right margin)
      const leftMargin = containerBox.x;
      const rightMargin = 1920 - containerBox.x - containerBox.width;

      // Allow 5px tolerance for centering
      expect(Math.abs(leftMargin - rightMargin)).toBeLessThan(5);
    });

    test('feature cards display in multi-column grid at 1920px', async ({ page }) => {
      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();

      // Get all feature cards
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();
      expect(cardCount).toBe(4);

      // Get positions of all cards
      const cardPositions = [];
      for (let i = 0; i < cardCount; i++) {
        const box = await featureCards.nth(i).boundingBox();
        cardPositions.push(box);
      }

      // At 1920px, cards should be in multiple columns (not stacked vertically)
      // Check that not all cards have the same x position (would indicate single column)
      const uniqueXPositions = new Set(cardPositions.map(box => Math.round(box.x)));
      expect(uniqueXPositions.size).toBeGreaterThan(1);

      // Verify cards are arranged in a grid layout (at least 2 columns)
      // First two cards should have different x positions or be in the same row
      const firstCardY = Math.round(cardPositions[0].y);
      const cardsInFirstRow = cardPositions.filter(box => Math.round(box.y) === firstCardY);
      expect(cardsInFirstRow.length).toBeGreaterThanOrEqual(2);
    });
  });

  test.describe('1280px Viewport (Standard Desktop)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 800 });
      await page.goto('/');
    });

    test('page displays correctly without issues at 1280px', async ({ page }) => {
      // Verify no horizontal scroll
      const documentWidth = await page.evaluate(() => document.documentElement.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);
      expect(documentWidth).toBeLessThanOrEqual(viewportWidth);

      // Verify all major sections are visible
      await expect(page.locator('.hero')).toBeVisible();
      await expect(page.locator('#features')).toBeVisible();
      await expect(page.locator('#quickstart')).toBeVisible();
      await expect(page.locator('#commands')).toBeVisible();
      await expect(page.locator('#roadmap')).toBeVisible();
      await expect(page.locator('#config')).toBeVisible();
      await expect(page.locator('.footer')).toBeVisible();

      // Verify main content elements are properly sized
      const container = page.locator('.container').first();
      const containerBox = await container.boundingBox();
      expect(containerBox).not.toBeNull();

      // Container should fit within viewport
      expect(containerBox.width).toBeLessThanOrEqual(1280);

      // Feature cards should still display in grid at 1280px
      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();

      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();
      expect(cardCount).toBe(4);

      // Verify grid layout is maintained
      const cardPositions = [];
      for (let i = 0; i < cardCount; i++) {
        const box = await featureCards.nth(i).boundingBox();
        cardPositions.push(box);
      }

      const uniqueXPositions = new Set(cardPositions.map(box => Math.round(box.x)));
      expect(uniqueXPositions.size).toBeGreaterThan(1);
    });

    test('text readability - line length is comfortable at 1280px', async ({ page }) => {
      // Description text should have reasonable line width (not too wide)
      const description = page.locator('.hero .description');
      await expect(description).toBeVisible();

      const descBox = await description.boundingBox();
      expect(descBox).not.toBeNull();

      // Line length should not exceed ~75 characters (around 600-800px depending on font)
      // The description has max-width: 600px in CSS
      expect(descBox.width).toBeLessThanOrEqual(650);

      // Quick start steps have max-width for readability
      const quickStartStep = page.locator('.quick-start-step').first();
      await expect(quickStartStep).toBeVisible();

      const stepBox = await quickStartStep.boundingBox();
      expect(stepBox).not.toBeNull();

      // Quick start step should have constrained width (max-width: 800px in CSS)
      expect(stepBox.width).toBeLessThanOrEqual(850);
    });
  });

  test.describe('Layout Verification', () => {
    test('all sections render without horizontal overflow at 1920px', async ({ page }) => {
      await page.setViewportSize({ width: 1920, height: 1080 });
      await page.goto('/');

      const sections = [
        '.hero',
        '#features',
        '#quickstart',
        '#commands',
        '#roadmap',
        '#config',
        '.footer'
      ];

      for (const selector of sections) {
        const section = page.locator(selector);
        await expect(section).toBeVisible();

        const box = await section.boundingBox();
        expect(box).not.toBeNull();

        // Section width should not exceed viewport width
        expect(box.width).toBeLessThanOrEqual(1920);

        // Section should start at x=0 (no negative margin issues)
        expect(box.x).toBeGreaterThanOrEqual(0);
      }
    });

    test('grids display properly at desktop width', async ({ page }) => {
      await page.setViewportSize({ width: 1920, height: 1080 });
      await page.goto('/');

      // Check features grid
      const featuresGrid = page.locator('.features-grid');
      const featuresBox = await featuresGrid.boundingBox();
      expect(featuresBox).not.toBeNull();
      expect(featuresBox.width).toBeLessThanOrEqual(1200 + 64); // max-width + container padding

      // Check commands grid
      const commandsGrid = page.locator('.commands-grid');
      const commandsBox = await commandsGrid.boundingBox();
      expect(commandsBox).not.toBeNull();
      expect(commandsBox.width).toBeLessThanOrEqual(1200 + 64);

      // Check roadmap grid
      const roadmapGrid = page.locator('.roadmap-grid');
      const roadmapBox = await roadmapGrid.boundingBox();
      expect(roadmapBox).not.toBeNull();
      expect(roadmapBox.width).toBeLessThanOrEqual(1200 + 64);
    });
  });
});
