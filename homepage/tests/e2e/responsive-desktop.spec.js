/**
 * Desktop Responsive E2E Tests
 * Owner: Scenario 11 - Responsive Design Desktop
 *
 * Viewport: >= 1024px (1440px)
 *
 * Tests:
 * - Three column feature grid
 * - Full horizontal navigation
 * - Side-by-side layouts
 *
 * Requirements: NFR-2
 */

const { test, expect } = require('@playwright/test');

// Desktop viewport configuration (1440px width as specified in scenario)
const DESKTOP_VIEWPORT = { width: 1440, height: 900 };

// Desktop breakpoint minimum
const DESKTOP_MIN_WIDTH = 1024;

test.describe('Desktop Responsive Design (1024px+)', () => {
  test.beforeEach(async ({ page }) => {
    // Set desktop viewport (1440px as per test case)
    await page.setViewportSize(DESKTOP_VIEWPORT);
    await page.goto('/');
    // Wait for page to be fully loaded
    await page.waitForLoadState('networkidle');
  });

  test.describe('Test Case 1: Page renders with full desktop layout at 1440px', () => {
    test('page renders without horizontal scrolling', async ({ page }) => {
      // Verify no horizontal scrollbar (page width equals viewport width)
      const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);

      expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth);
    });

    test('page renders with correct viewport width', async ({ page }) => {
      const viewportWidth = await page.evaluate(() => window.innerWidth);
      expect(viewportWidth).toBe(DESKTOP_VIEWPORT.width);
    });

    test('main content is visible and properly laid out', async ({ page }) => {
      // Verify all main sections are visible
      const header = page.locator('.header');
      const hero = page.locator('.hero');
      const features = page.locator('#features');
      const architecture = page.locator('#architecture');
      const quickstart = page.locator('#quickstart');
      const comparison = page.locator('#comparison');
      const footer = page.locator('.footer');

      await expect(header).toBeVisible();
      await expect(hero).toBeVisible();
      await expect(features).toBeVisible();
      await expect(architecture).toBeVisible();
      await expect(quickstart).toBeVisible();
      await expect(comparison).toBeVisible();
      await expect(footer).toBeVisible();
    });

    test('container has appropriate max-width for desktop', async ({ page }) => {
      const container = page.locator('.container').first();
      const containerBox = await container.boundingBox();

      // Container should have a max-width around 1200px (as per styles.css)
      expect(containerBox.width).toBeLessThanOrEqual(1200);
    });

    test('no layout overflow issues at 1440px viewport', async ({ page }) => {
      // Check that body doesn't have overflow-x: hidden being triggered
      const htmlOverflow = await page.evaluate(() => {
        const html = document.documentElement;
        return getComputedStyle(html).overflowX;
      });

      // Desktop should not need overflow hidden
      expect(htmlOverflow).not.toBe('hidden');
    });
  });

  test.describe('Test Case 2: Full horizontal navigation bar visible', () => {
    test('navigation links are visible on desktop', async ({ page }) => {
      const navLinks = page.locator('.nav-links');
      await expect(navLinks).toBeVisible();
    });

    test('navigation links display in horizontal row (flex)', async ({ page }) => {
      const navLinks = page.locator('.nav-links');
      const displayStyle = await navLinks.evaluate(el => getComputedStyle(el).display);

      expect(displayStyle).toBe('flex');
    });

    test('all navigation links are visible and accessible', async ({ page }) => {
      const navLinkItems = page.locator('.nav-links a');
      const count = await navLinkItems.count();

      // Should have at least 4 navigation links (Features, Architecture, Quick Start, Comparison, GitHub)
      expect(count).toBeGreaterThanOrEqual(4);

      // Verify each link is visible
      for (let i = 0; i < count; i++) {
        await expect(navLinkItems.nth(i)).toBeVisible();
      }
    });

    test('hamburger menu is hidden on desktop', async ({ page }) => {
      const hamburgerMenu = page.locator('.hamburger-menu');
      await expect(hamburgerMenu).toBeHidden();
    });

    test('mobile navigation menu is hidden on desktop', async ({ page }) => {
      const mobileNav = page.locator('.mobile-nav');

      // Mobile nav should be hidden or not visible at all on desktop
      const isHidden = await mobileNav.evaluate(el => {
        const style = getComputedStyle(el);
        return style.display === 'none' || style.visibility === 'hidden';
      });

      expect(isHidden).toBe(true);
    });

    test('logo is visible in header', async ({ page }) => {
      const logo = page.locator('.header .logo');
      await expect(logo).toBeVisible();
    });

    test('header spans full width', async ({ page }) => {
      const header = page.locator('.header');
      const headerBox = await header.boundingBox();

      expect(headerBox.width).toBe(DESKTOP_VIEWPORT.width);
    });
  });

  test.describe('Test Case 3: Feature cards display in three-column grid', () => {
    test('features grid exists and is visible', async ({ page }) => {
      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();
    });

    test('features grid uses CSS Grid with 3 columns on desktop', async ({ page }) => {
      const featuresGrid = page.locator('.features-grid');

      const gridTemplateColumns = await featuresGrid.evaluate(el =>
        getComputedStyle(el).gridTemplateColumns
      );

      // Should have 3 columns - gridTemplateColumns will show actual computed widths
      const columnCount = gridTemplateColumns.split(' ').length;
      expect(columnCount).toBe(3);
    });

    test('feature cards are arranged in 3 columns', async ({ page }) => {
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();

      // Should have 6 feature cards as per the design
      expect(cardCount).toBe(6);

      // Get positions of first 3 cards to verify they're in a row
      const firstRowCards = [];
      for (let i = 0; i < 3; i++) {
        const box = await featureCards.nth(i).boundingBox();
        firstRowCards.push(box);
      }

      // All first row cards should have the same Y position (approximately)
      const tolerance = 5; // 5px tolerance for rendering differences
      expect(Math.abs(firstRowCards[0].y - firstRowCards[1].y)).toBeLessThan(tolerance);
      expect(Math.abs(firstRowCards[1].y - firstRowCards[2].y)).toBeLessThan(tolerance);

      // Cards should be side by side (different X positions)
      expect(firstRowCards[0].x).toBeLessThan(firstRowCards[1].x);
      expect(firstRowCards[1].x).toBeLessThan(firstRowCards[2].x);
    });

    test('second row of feature cards also displays correctly', async ({ page }) => {
      const featureCards = page.locator('.feature-card');

      // Get positions of cards 4-6 (second row)
      const secondRowCards = [];
      for (let i = 3; i < 6; i++) {
        const box = await featureCards.nth(i).boundingBox();
        secondRowCards.push(box);
      }

      // All second row cards should have the same Y position (approximately)
      const tolerance = 5;
      expect(Math.abs(secondRowCards[0].y - secondRowCards[1].y)).toBeLessThan(tolerance);
      expect(Math.abs(secondRowCards[1].y - secondRowCards[2].y)).toBeLessThan(tolerance);

      // Second row should be below first row
      const firstRowFirstCard = await featureCards.nth(0).boundingBox();
      expect(secondRowCards[0].y).toBeGreaterThan(firstRowFirstCard.y);
    });

    test('feature cards have consistent width in grid', async ({ page }) => {
      const featureCards = page.locator('.feature-card');
      const firstCard = await featureCards.nth(0).boundingBox();
      const secondCard = await featureCards.nth(1).boundingBox();
      const thirdCard = await featureCards.nth(2).boundingBox();

      // Cards in same row should have approximately equal width
      const tolerance = 10; // 10px tolerance
      expect(Math.abs(firstCard.width - secondCard.width)).toBeLessThan(tolerance);
      expect(Math.abs(secondCard.width - thirdCard.width)).toBeLessThan(tolerance);
    });
  });

  test.describe('Test Case 4: Architecture section displays side-by-side layout', () => {
    test('architecture section is visible', async ({ page }) => {
      const architecture = page.locator('#architecture');
      await expect(architecture).toBeVisible();
    });

    test('architecture diagram is visible', async ({ page }) => {
      const diagram = page.locator('.architecture-diagram');
      await expect(diagram).toBeVisible();
    });

    test('architecture content has appropriate desktop width', async ({ page }) => {
      const architectureContent = page.locator('.architecture-content');
      const box = await architectureContent.boundingBox();

      // Content should have a reasonable desktop width (max-width: 900px per styles)
      expect(box.width).toBeGreaterThan(600);
      expect(box.width).toBeLessThanOrEqual(900);
    });

    test('write path and read path explanations are visible', async ({ page }) => {
      const writePath = page.locator('.write-path');
      const readPath = page.locator('.read-path');

      await expect(writePath).toBeVisible();
      await expect(readPath).toBeVisible();
    });

    test('compaction types display in multi-column layout on desktop', async ({ page }) => {
      const compactionTypes = page.locator('.compaction-types');

      const gridTemplateColumns = await compactionTypes.evaluate(el =>
        getComputedStyle(el).gridTemplateColumns
      );

      // Should have at least 2 columns for compaction types on desktop
      const columnCount = gridTemplateColumns.split(' ').length;
      expect(columnCount).toBeGreaterThanOrEqual(2);
    });

    test('compaction type cards are side by side on desktop', async ({ page }) => {
      const compactionTypeCards = page.locator('.compaction-type');
      const count = await compactionTypeCards.count();

      if (count >= 2) {
        const firstCard = await compactionTypeCards.nth(0).boundingBox();
        const secondCard = await compactionTypeCards.nth(1).boundingBox();

        // Cards should be side by side (different X positions, similar Y)
        const tolerance = 20;
        expect(Math.abs(firstCard.y - secondCard.y)).toBeLessThan(tolerance);
        expect(firstCard.x).toBeLessThan(secondCard.x);
      }
    });
  });

  test.describe('Additional Desktop Layout Verifications', () => {
    test('hero section displays properly on desktop', async ({ page }) => {
      const hero = page.locator('.hero');
      const heroBox = await hero.boundingBox();

      // Hero should have substantial height on desktop
      expect(heroBox.height).toBeGreaterThan(300);
    });

    test('hero CTA buttons are side by side on desktop', async ({ page }) => {
      const ctaContainer = page.locator('.hero-cta');
      const buttons = ctaContainer.locator('.btn');

      const count = await buttons.count();
      expect(count).toBeGreaterThanOrEqual(2);

      if (count >= 2) {
        const firstButton = await buttons.nth(0).boundingBox();
        const secondButton = await buttons.nth(1).boundingBox();

        // Buttons should be side by side (different X positions)
        expect(firstButton.x).not.toBe(secondButton.x);

        // Buttons should be on same row (similar Y position)
        const tolerance = 10;
        expect(Math.abs(firstButton.y - secondButton.y)).toBeLessThan(tolerance);
      }
    });

    test('footer content displays in multi-column layout', async ({ page }) => {
      const footerContent = page.locator('.footer-content');

      const gridTemplateColumns = await footerContent.evaluate(el =>
        getComputedStyle(el).gridTemplateColumns
      );

      // Footer should have multiple columns on desktop
      const columnCount = gridTemplateColumns.split(' ').length;
      expect(columnCount).toBeGreaterThanOrEqual(2);
    });

    test('comparison table displays fully on desktop', async ({ page }) => {
      const comparisonTable = page.locator('.comparison-table');
      await expect(comparisonTable).toBeVisible();

      // Table should not overflow its container
      const tableBox = await comparisonTable.boundingBox();
      const parentBox = await comparisonTable.locator('..').boundingBox();

      expect(tableBox.width).toBeLessThanOrEqual(parentBox.width + 50); // Small tolerance for padding
    });

    test('configuration table displays properly on desktop', async ({ page }) => {
      const configTable = page.locator('.config-table');
      await expect(configTable).toBeVisible();

      // All table headers should be visible
      const headers = configTable.locator('th');
      const headerCount = await headers.count();
      expect(headerCount).toBeGreaterThanOrEqual(3);
    });
  });

  test.describe('Desktop Breakpoint Boundary Tests', () => {
    test('layout is desktop-appropriate at minimum desktop width (1024px)', async ({ page }) => {
      // Test at the minimum desktop breakpoint
      await page.setViewportSize({ width: DESKTOP_MIN_WIDTH, height: 768 });
      await page.waitForLoadState('networkidle');

      // Navigation should still be visible
      const navLinks = page.locator('.nav-links');
      await expect(navLinks).toBeVisible();

      // Hamburger should be hidden
      const hamburger = page.locator('.hamburger-menu');
      await expect(hamburger).toBeHidden();

      // Features grid should still have 3 columns
      const featuresGrid = page.locator('.features-grid');
      const gridTemplateColumns = await featuresGrid.evaluate(el =>
        getComputedStyle(el).gridTemplateColumns
      );
      const columnCount = gridTemplateColumns.split(' ').length;
      expect(columnCount).toBe(3);
    });

    test('layout maintains desktop styles at large viewport (1920px)', async ({ page }) => {
      await page.setViewportSize({ width: 1920, height: 1080 });
      await page.waitForLoadState('networkidle');

      // Navigation should still be visible
      const navLinks = page.locator('.nav-links');
      await expect(navLinks).toBeVisible();

      // Features grid should still have 3 columns
      const featuresGrid = page.locator('.features-grid');
      const gridTemplateColumns = await featuresGrid.evaluate(el =>
        getComputedStyle(el).gridTemplateColumns
      );
      const columnCount = gridTemplateColumns.split(' ').length;
      expect(columnCount).toBe(3);

      // Container should be centered with max-width
      const container = page.locator('.container').first();
      const containerBox = await container.boundingBox();
      expect(containerBox.width).toBeLessThanOrEqual(1200);
    });
  });
});
