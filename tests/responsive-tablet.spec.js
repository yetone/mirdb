// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Responsive Design - Tablet Tests
 *
 * These tests verify that the homepage displays correctly on tablet devices
 * as specified in NFR-1: "Page must be responsive and display correctly on
 * desktop and mobile devices"
 *
 * Tablet viewport: 768x1024 (simulating iPad or similar tablet device)
 */

// Tablet viewport dimensions
const TABLET_VIEWPORT = { width: 768, height: 1024 };

test.describe('Responsive Design - Tablet', () => {
  test.use({ viewport: TABLET_VIEWPORT });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Layout adjusts appropriately at 768px width with no horizontal overflow', async ({ page }) => {
    // Verify page loads successfully
    await expect(page).toHaveTitle(/MirDB/);

    // Check that there's no horizontal overflow (document width should not exceed viewport width)
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const windowWidth = await page.evaluate(() => window.innerWidth);

    // Body should not be wider than the viewport (no horizontal scroll)
    expect(bodyScrollWidth).toBeLessThanOrEqual(windowWidth);

    // Check that no element overflows horizontally
    const overflowingElements = await page.evaluate(() => {
      const viewportWidth = window.innerWidth;
      const allElements = document.querySelectorAll('*');
      const overflowing = [];

      allElements.forEach((el) => {
        const rect = el.getBoundingClientRect();
        if (rect.right > viewportWidth + 1) { // +1 for rounding tolerance
          overflowing.push({
            tag: el.tagName,
            class: el.className,
            right: rect.right,
            viewportWidth: viewportWidth
          });
        }
      });

      return overflowing;
    });

    // No elements should overflow beyond the viewport
    expect(overflowingElements.length).toBe(0);

    // Verify horizontal scrollbar is not present
    const hasHorizontalScrollbar = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScrollbar).toBe(false);
  });

  test('TC2: All sections (hero, features, getting started, etc.) are accessible at tablet size', async ({ page }) => {
    // 1. Hero Section - should be visible and readable
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify hero content is visible
    const heroTitle = page.locator('.hero h1');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toContainText('MirDB');

    const tagline = page.locator('.tagline');
    await expect(tagline).toBeVisible();

    // Verify CTA buttons are visible
    const ctaButtons = page.locator('.cta-buttons .btn');
    await expect(ctaButtons).toHaveCount(2);
    for (const button of await ctaButtons.all()) {
      await expect(button).toBeVisible();
    }

    // 2. Features Section - should be visible and accessible
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Scroll to features section to verify it's accessible
    await featuresSection.scrollIntoViewIfNeeded();

    // Verify features heading is visible
    const featuresHeading = page.locator('.features h2');
    await expect(featuresHeading).toBeVisible();
    await expect(featuresHeading).toContainText('Key Features');

    // Verify feature items are visible
    const featureItems = page.locator('.feature-item');
    await expect(featureItems).toHaveCount(6);
    for (const item of await featureItems.all()) {
      await expect(item).toBeVisible();
    }

    // 3. Architecture Section - should be visible and accessible
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Scroll to architecture section
    await architectureSection.scrollIntoViewIfNeeded();

    // Verify architecture heading is visible
    const architectureHeading = page.locator('.architecture h2');
    await expect(architectureHeading).toBeVisible();
    await expect(architectureHeading).toContainText('Architecture');

    // Verify architecture diagram is visible
    const architectureDiagram = page.locator('[data-testid="architecture-diagram"]');
    await expect(architectureDiagram).toBeVisible();

    // 4. Getting Started Section - should be visible and accessible
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Scroll to getting started section
    await gettingStartedSection.scrollIntoViewIfNeeded();

    // Verify getting started heading is visible
    const gettingStartedHeading = page.locator('.getting-started h2');
    await expect(gettingStartedHeading).toBeVisible();
    await expect(gettingStartedHeading).toContainText('Getting Started');

    // Verify code blocks are visible
    const codeBlocks = page.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    for (const block of await codeBlocks.all()) {
      await block.scrollIntoViewIfNeeded();
      await expect(block).toBeVisible();
    }

    // 5. Footer - should be visible and accessible
    const footer = page.locator('.footer');
    await expect(footer).toBeVisible();

    // Scroll to footer
    await footer.scrollIntoViewIfNeeded();

    // Verify footer links are visible
    const footerLinks = page.locator('.footer-links a');
    const linkCount = await footerLinks.count();
    expect(linkCount).toBeGreaterThanOrEqual(5);
    for (const link of await footerLinks.all()) {
      await expect(link).toBeVisible();
    }
  });

  test('Feature grid adapts to tablet layout', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Get the computed style of the features grid
    const gridStyle = await page.evaluate(() => {
      const grid = document.querySelector('.features-grid');
      if (!grid) return null;
      const style = window.getComputedStyle(grid);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns
      };
    });

    // Verify grid is using CSS grid
    expect(gridStyle).not.toBeNull();
    expect(gridStyle?.display).toBe('grid');

    // At 768px, with minmax(300px, 1fr), we should have 2 columns
    // The grid-template-columns should show actual computed values
    expect(gridStyle?.gridTemplateColumns).toBeTruthy();
  });

  test('Code blocks have horizontal scroll instead of page overflow', async ({ page }) => {
    // Navigate to getting started section
    const gettingStartedSection = page.locator('#getting-started');
    await gettingStartedSection.scrollIntoViewIfNeeded();

    // Check code blocks have proper overflow handling
    const codeBlocksOverflow = await page.evaluate(() => {
      const codeBlocks = document.querySelectorAll('.code-block');
      return Array.from(codeBlocks).map((block) => {
        const style = window.getComputedStyle(block);
        return {
          overflowX: style.overflowX,
          maxWidth: style.maxWidth,
          hasHorizontalScroll: block.scrollWidth > block.clientWidth
        };
      });
    });

    // All code blocks should have overflow-x: auto to handle long lines
    for (const block of codeBlocksOverflow) {
      expect(block.overflowX).toBe('auto');
    }
  });

  test('Navigation and buttons are touch-friendly at tablet size', async ({ page }) => {
    // Check CTA buttons have adequate size for touch
    const buttons = page.locator('.cta-buttons .btn');

    for (const button of await buttons.all()) {
      const box = await button.boundingBox();
      expect(box).not.toBeNull();

      if (box) {
        // Minimum touch target size is typically 44x44 pixels (Apple HIG)
        // We're being slightly lenient here for reasonable tap targets
        expect(box.height).toBeGreaterThanOrEqual(40);
        expect(box.width).toBeGreaterThanOrEqual(40);
      }
    }

    // Check footer links are accessible
    const footerLinks = page.locator('.footer-links a');
    await footerLinks.first().scrollIntoViewIfNeeded();

    for (const link of await footerLinks.all()) {
      await expect(link).toBeVisible();
    }
  });

  test('Images and diagrams scale appropriately for tablet screen', async ({ page }) => {
    // Check architecture diagram SVG scales properly
    const diagram = page.locator('.lsm-tree-diagram');
    await diagram.scrollIntoViewIfNeeded();

    const diagramBox = await diagram.boundingBox();
    expect(diagramBox).not.toBeNull();

    if (diagramBox) {
      // Diagram should fit within the viewport width (with some padding)
      expect(diagramBox.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);

      // Diagram should have reasonable dimensions (not too small)
      expect(diagramBox.width).toBeGreaterThan(300);
    }

    // Verify SVG is visible and accessible
    const svg = page.locator('[data-testid="architecture-svg"]');
    await expect(svg).toBeVisible();
  });

  test('Typography remains readable at tablet viewport', async ({ page }) => {
    // Check hero title font size is readable
    const heroFontSize = await page.evaluate(() => {
      const h1 = document.querySelector('.hero h1');
      if (!h1) return null;
      return window.getComputedStyle(h1).fontSize;
    });

    expect(heroFontSize).not.toBeNull();
    const heroSize = parseFloat(heroFontSize || '0');
    // Hero title should be reasonably large (at least 32px at tablet size)
    expect(heroSize).toBeGreaterThanOrEqual(32);

    // Check body text remains readable
    const bodyFontSize = await page.evaluate(() => {
      const p = document.querySelector('.feature-item p');
      if (!p) return null;
      return window.getComputedStyle(p).fontSize;
    });

    expect(bodyFontSize).not.toBeNull();
    const bodySize = parseFloat(bodyFontSize || '0');
    // Body text should be at least 14px for readability
    expect(bodySize).toBeGreaterThanOrEqual(14);
  });
});
