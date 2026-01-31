/**
 * Tablet Responsive E2E Tests
 * Owner: Scenario 10 - Responsive Design Tablet
 *
 * Viewport: 768px - 1023px
 *
 * Tests:
 * - Two column feature grid
 * - Condensed navigation
 * - Layout adaptation
 *
 * Requirements: NFR-2
 */

const { test, expect } = require('@playwright/test');

// Tablet viewport configuration (iPad portrait)
const TABLET_VIEWPORT = {
  width: 768,
  height: 1024
};

// Upper bound of tablet viewport
const TABLET_VIEWPORT_MAX = {
  width: 1023,
  height: 768
};

test.describe('Responsive Design - Tablet (768px - 1023px)', () => {
  test.beforeEach(async ({ page }) => {
    // Set viewport to tablet width (768px - iPad portrait)
    await page.setViewportSize(TABLET_VIEWPORT);
    await page.goto('/');
    // Wait for page to fully load
    await page.waitForLoadState('domcontentloaded');
  });

  test.describe('Test Case 1: Page renders correctly at 768px viewport', () => {
    test('page renders with tablet-optimized layout at minimum tablet width', async ({ page }) => {
      // Verify page loads correctly
      await expect(page).toHaveTitle(/MirDB/);

      // Verify main content sections are visible
      const hero = page.locator('#hero');
      const features = page.locator('#features');
      const architecture = page.locator('#architecture');

      await expect(hero).toBeVisible();
      await expect(features).toBeVisible();
      await expect(architecture).toBeVisible();
    });

    test('page renders correctly at upper tablet bound (1023px)', async ({ page }) => {
      await page.setViewportSize(TABLET_VIEWPORT_MAX);

      // Verify page loads correctly
      await expect(page).toHaveTitle(/MirDB/);

      // Verify main content sections are visible
      const hero = page.locator('#hero');
      const features = page.locator('#features');

      await expect(hero).toBeVisible();
      await expect(features).toBeVisible();
    });

    test('no horizontal scrollbar present at tablet width', async ({ page }) => {
      // Check that document does not require horizontal scrolling
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      expect(hasHorizontalScroll).toBe(false);
    });

    test('content is properly contained within viewport', async ({ page }) => {
      // Verify body width does not exceed viewport
      const bodyWidth = await page.evaluate(() => {
        return document.body.scrollWidth;
      });

      expect(bodyWidth).toBeLessThanOrEqual(TABLET_VIEWPORT.width);
    });
  });

  test.describe('Test Case 2: Feature cards display in two-column grid', () => {
    test('features grid displays exactly 2 columns at tablet viewport', async ({ page }) => {
      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();

      // Get computed grid styles
      const gridColumns = await featuresGrid.evaluate((el) => {
        return window.getComputedStyle(el).gridTemplateColumns;
      });

      // Should have 2 columns (gridTemplateColumns will show pixel values for 2 cols)
      const columnCount = gridColumns.split(' ').length;
      expect(columnCount).toBe(2);
    });

    test('all 6 feature cards are visible', async ({ page }) => {
      const featureCards = page.locator('.feature-card');
      await expect(featureCards).toHaveCount(6);

      // Verify each card is visible
      for (let i = 0; i < 6; i++) {
        await expect(featureCards.nth(i)).toBeVisible();
      }
    });

    test('feature cards maintain proper spacing in two-column layout', async ({ page }) => {
      const featureCards = page.locator('.feature-card');

      // Get positions of first two cards to verify they are in same row
      const firstCard = await featureCards.nth(0).boundingBox();
      const secondCard = await featureCards.nth(1).boundingBox();

      // First two cards should be on the same row (same Y position approximately)
      expect(Math.abs(firstCard.y - secondCard.y)).toBeLessThan(10);

      // Cards should be side by side (different X positions)
      expect(secondCard.x).toBeGreaterThan(firstCard.x);
    });

    test('feature cards span approximately half the grid width each', async ({ page }) => {
      const featuresGrid = page.locator('.features-grid');
      const featureCards = page.locator('.feature-card');

      const gridBox = await featuresGrid.boundingBox();
      const firstCardBox = await featureCards.nth(0).boundingBox();
      const secondCardBox = await featureCards.nth(1).boundingBox();

      // Each card should take approximately half the grid width (with gap)
      // Allow for padding and gap adjustments
      const totalCardWidth = firstCardBox.width + secondCardBox.width;
      expect(totalCardWidth).toBeLessThan(gridBox.width);

      // Each card should be roughly similar in width
      expect(Math.abs(firstCardBox.width - secondCardBox.width)).toBeLessThan(50);
    });

    test('feature cards maintain 2-column layout at upper tablet bound (1023px)', async ({ page }) => {
      await page.setViewportSize(TABLET_VIEWPORT_MAX);

      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();

      // Get computed grid styles
      const gridColumns = await featuresGrid.evaluate((el) => {
        return window.getComputedStyle(el).gridTemplateColumns;
      });

      // Should still have 2 columns
      const columnCount = gridColumns.split(' ').length;
      expect(columnCount).toBe(2);
    });
  });

  test.describe('Test Case 3: Architecture section adapts appropriately', () => {
    test('architecture diagram is visible at tablet viewport', async ({ page }) => {
      const architectureSection = page.locator('#architecture');
      await architectureSection.scrollIntoViewIfNeeded();

      await expect(architectureSection).toBeVisible();

      const diagram = page.locator('.architecture-diagram');
      await expect(diagram).toBeVisible();
    });

    test('architecture content stacks or adapts for tablet layout', async ({ page }) => {
      const architectureSection = page.locator('#architecture');
      await architectureSection.scrollIntoViewIfNeeded();

      // Check that architecture content is properly displayed
      const architectureContent = page.locator('.architecture-content');
      await expect(architectureContent).toBeVisible();

      // Verify path explanations are visible
      const writePath = page.locator('.write-path');
      const readPath = page.locator('.read-path');

      await expect(writePath).toBeVisible();
      await expect(readPath).toBeVisible();
    });

    test('architecture diagram does not overflow container', async ({ page }) => {
      const diagram = page.locator('.architecture-diagram');
      await diagram.scrollIntoViewIfNeeded();

      const diagramBox = await diagram.boundingBox();

      // Diagram should fit within viewport width
      expect(diagramBox.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);
    });

    test('compaction types display appropriately at tablet width', async ({ page }) => {
      const compactionSection = page.locator('.compaction');
      await compactionSection.scrollIntoViewIfNeeded();

      await expect(compactionSection).toBeVisible();

      const compactionTypes = page.locator('.compaction-type');
      await expect(compactionTypes).toHaveCount(2);

      // Both compaction types should be visible
      await expect(compactionTypes.nth(0)).toBeVisible();
      await expect(compactionTypes.nth(1)).toBeVisible();
    });
  });

  test.describe('Navigation behavior at tablet viewport', () => {
    test('navigation adapts appropriately for tablet', async ({ page }) => {
      const header = page.locator('.header');
      await expect(header).toBeVisible();

      // Navigation links should still be visible on tablet
      // (tablet may show full nav or condensed nav depending on design)
      const navLinks = page.locator('.nav-links');
      const hamburger = page.locator('.hamburger-menu');

      // At least one navigation method should be accessible
      const navVisible = await navLinks.isVisible();
      const hamburgerVisible = await hamburger.isVisible();

      // Either full navigation or hamburger menu should be present
      expect(navVisible || hamburgerVisible).toBe(true);
    });

    test('logo is visible at tablet viewport', async ({ page }) => {
      const logo = page.locator('.logo');
      await expect(logo).toBeVisible();
    });
  });

  test.describe('Additional tablet layout tests', () => {
    test('hero section displays correctly at tablet width', async ({ page }) => {
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();

      const heroTitle = page.locator('.hero h1');
      const heroTagline = page.locator('.hero-tagline');
      const heroCta = page.locator('.hero-cta');

      await expect(heroTitle).toBeVisible();
      await expect(heroTagline).toBeVisible();
      await expect(heroCta).toBeVisible();
    });

    test('CTA buttons are properly sized for tablet touch', async ({ page }) => {
      const ctaButtons = page.locator('.hero-cta .btn');

      for (let i = 0; i < await ctaButtons.count(); i++) {
        const buttonBox = await ctaButtons.nth(i).boundingBox();
        // Buttons should have minimum touch target of 44px
        expect(buttonBox.height).toBeGreaterThanOrEqual(44);
      }
    });

    test('footer displays correctly at tablet viewport', async ({ page }) => {
      const footer = page.locator('.footer');
      await footer.scrollIntoViewIfNeeded();

      await expect(footer).toBeVisible();

      // Footer content should be accessible
      const footerContent = page.locator('.footer-content');
      await expect(footerContent).toBeVisible();
    });

    test('comparison table is readable at tablet width', async ({ page }) => {
      const comparison = page.locator('#comparison');
      await comparison.scrollIntoViewIfNeeded();

      await expect(comparison).toBeVisible();

      const comparisonTable = page.locator('.comparison-table');
      await expect(comparisonTable).toBeVisible();

      // Table should be contained within viewport
      const tableBox = await comparisonTable.boundingBox();
      expect(tableBox.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);
    });

    test('quick start section displays correctly at tablet width', async ({ page }) => {
      const quickstart = page.locator('#quickstart');
      await quickstart.scrollIntoViewIfNeeded();

      await expect(quickstart).toBeVisible();

      // Code blocks should be visible and contained
      const codeBlocks = page.locator('.quickstart-step pre');
      const count = await codeBlocks.count();
      expect(count).toBeGreaterThan(0);
    });

    test('configuration table displays correctly at tablet width', async ({ page }) => {
      const configuration = page.locator('#configuration');
      await configuration.scrollIntoViewIfNeeded();

      await expect(configuration).toBeVisible();

      const configTable = page.locator('.config-table');
      await expect(configTable).toBeVisible();
    });
  });
});
