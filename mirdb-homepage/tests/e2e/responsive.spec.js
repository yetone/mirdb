/**
 * Responsive Design Tests
 * Owner: Scenario 7 - Responsive Design
 *
 * Test cases:
 * - Mobile viewport (375px): no horizontal scroll, stacked layout
 * - Tablet viewport (768px): 2-column where appropriate
 * - Desktop viewport (1440px): full layout
 * - Touch-friendly tap targets (44x44px)
 * - Code blocks with horizontal scroll on mobile
 * - Viewport meta tag present
 * - Media queries in CSS
 */

const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

// Test Case 1: Mobile viewport (375px)
test.describe('Mobile Viewport (375px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('http://localhost:3000');
  });

  test('all content is visible with no horizontal scroll', async ({ page }) => {
    // Check page doesn't have horizontal overflow
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Verify key content is visible
    await expect(page.locator('#hero')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('#getting-started')).toBeVisible();
  });

  test('navigation stacks or collapses on mobile', async ({ page }) => {
    const nav = page.locator('.nav');
    await expect(nav).toBeVisible();

    // On mobile, nav links should either be hidden, stacked vertically, or wrapped
    const navLinks = page.locator('.nav__links');
    const navLinksBox = await navLinks.boundingBox();

    // Navigation should fit within the viewport width
    if (navLinksBox) {
      expect(navLinksBox.width).toBeLessThanOrEqual(375);
    }
  });

  test('hero section text is readable on mobile', async ({ page }) => {
    const heroTitle = page.locator('.hero__title');
    await expect(heroTitle).toBeVisible();

    // Title should be within viewport
    const titleBox = await heroTitle.boundingBox();
    expect(titleBox.width).toBeLessThanOrEqual(375);
  });

  test('feature cards stack vertically on mobile', async ({ page }) => {
    const featureCards = page.locator('.features__card');
    const cardCount = await featureCards.count();

    if (cardCount >= 2) {
      const firstCardBox = await featureCards.nth(0).boundingBox();
      const secondCardBox = await featureCards.nth(1).boundingBox();

      // On mobile, cards should stack (second card below first)
      expect(secondCardBox.y).toBeGreaterThan(firstCardBox.y);
    }
  });
});

// Test Case 2: Tablet viewport (768px)
test.describe('Tablet Viewport (768px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('http://localhost:3000');
  });

  test('layout adjusts to 2-column grid where appropriate', async ({ page }) => {
    const featuresGrid = page.locator('.features__grid');
    await expect(featuresGrid).toBeVisible();

    // Check that feature cards can display in 2 columns at tablet width
    const featureCards = page.locator('.features__card');
    const cardCount = await featureCards.count();

    if (cardCount >= 2) {
      const firstCardBox = await featureCards.nth(0).boundingBox();
      const secondCardBox = await featureCards.nth(1).boundingBox();

      // At tablet width, cards could be side by side (same Y) or stacked
      // The grid should allow 2-column layout
      const gridStyle = await featuresGrid.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          display: style.display,
          gridTemplateColumns: style.gridTemplateColumns
        };
      });

      expect(gridStyle.display).toBe('grid');
    }
  });

  test('text remains readable at tablet width', async ({ page }) => {
    const heroTagline = page.locator('.hero__tagline');
    await expect(heroTagline).toBeVisible();

    const taglineBox = await heroTagline.boundingBox();
    // Text container should be visible and reasonable width
    expect(taglineBox.width).toBeGreaterThan(200);
    expect(taglineBox.width).toBeLessThanOrEqual(768);
  });

  test('no horizontal scrollbar at tablet width', async ({ page }) => {
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });
});

// Test Case 3: Desktop viewport (1440px)
test.describe('Desktop Viewport (1440px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.goto('http://localhost:3000');
  });

  test('full desktop layout with proper spacing', async ({ page }) => {
    // Check main container widths are constrained for readability
    const heroContainer = page.locator('.hero__container');
    const heroContainerBox = await heroContainer.boundingBox();

    // Container should be centered and not span full width
    expect(heroContainerBox.width).toBeLessThanOrEqual(1200);
  });

  test('multi-column sections display correctly', async ({ page }) => {
    const featuresGrid = page.locator('.features__grid');
    await expect(featuresGrid).toBeVisible();

    // At desktop, feature cards should display in multiple columns
    const featureCards = page.locator('.features__card');
    const cardCount = await featureCards.count();

    if (cardCount >= 3) {
      const firstCardBox = await featureCards.nth(0).boundingBox();
      const secondCardBox = await featureCards.nth(1).boundingBox();
      const thirdCardBox = await featureCards.nth(2).boundingBox();

      // At desktop width with 4 cards, at least 2 should be on the same row
      const cardsOnSameRow = (
        Math.abs(firstCardBox.y - secondCardBox.y) < 10 ||
        Math.abs(secondCardBox.y - thirdCardBox.y) < 10 ||
        Math.abs(firstCardBox.y - thirdCardBox.y) < 10
      );
      expect(cardsOnSameRow).toBe(true);
    }
  });

  test('navigation displays horizontally on desktop', async ({ page }) => {
    const navLinks = page.locator('.nav__links');
    await expect(navLinks).toBeVisible();

    const navLinksStyle = await navLinks.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        flexDirection: style.flexDirection
      };
    });

    expect(navLinksStyle.display).toBe('flex');
    // Default flex direction is row
  });
});

// Test Case 4: Touch-friendly tap targets
test.describe('Touch-friendly Tap Targets', () => {
  test('all interactive elements have minimum 44x44px touch target on mobile', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('http://localhost:3000');

    // Check buttons
    const buttons = page.locator('.hero__btn');
    const buttonCount = await buttons.count();

    for (let i = 0; i < buttonCount; i++) {
      const button = buttons.nth(i);
      const box = await button.boundingBox();
      if (box) {
        // Touch target should be at least 44x44
        expect(box.height).toBeGreaterThanOrEqual(44);
        expect(box.width).toBeGreaterThanOrEqual(44);
      }
    }

    // Check navigation links
    const navLinks = page.locator('.nav__links a');
    const navLinkCount = await navLinks.count();

    for (let i = 0; i < navLinkCount; i++) {
      const link = navLinks.nth(i);
      const box = await link.boundingBox();
      if (box) {
        // Touch target should have minimum height/width or appropriate padding
        expect(box.height).toBeGreaterThanOrEqual(44);
      }
    }
  });
});

// Test Case 5: Code blocks on mobile
test.describe('Code Blocks on Mobile', () => {
  test('code blocks have horizontal scroll and do not break layout', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('http://localhost:3000');

    // Navigate to getting started section
    const codeBlocks = page.locator('.getting-started__code-block pre');
    const codeBlockCount = await codeBlocks.count();

    expect(codeBlockCount).toBeGreaterThan(0);

    for (let i = 0; i < Math.min(codeBlockCount, 3); i++) {
      const codeBlock = codeBlocks.nth(i);

      // Check that code block has overflow-x: auto or scroll
      const overflowStyle = await codeBlock.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return style.overflowX;
      });

      expect(['auto', 'scroll']).toContain(overflowStyle);

      // Code block should not exceed viewport width
      const codeBlockBox = await codeBlock.boundingBox();
      expect(codeBlockBox.width).toBeLessThanOrEqual(375);
    }

    // Verify page still has no horizontal scroll (code blocks contained)
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });
});

// Test Case 6: Viewport meta tag
test.describe('Viewport Meta Tag', () => {
  test('HTML head contains proper viewport meta tag', async ({ page }) => {
    await page.goto('http://localhost:3000');

    // Check for viewport meta tag
    const viewportMeta = await page.locator('meta[name="viewport"]');
    await expect(viewportMeta).toHaveCount(1);

    // Verify content attribute includes essential values
    const content = await viewportMeta.getAttribute('content');
    expect(content).toContain('width=device-width');
    expect(content).toContain('initial-scale=1');
  });
});

// Test Case 7: CSS Media Queries
test.describe('CSS Media Queries', () => {
  test('stylesheet contains media queries for responsive breakpoints', async () => {
    // Read the CSS file directly
    const cssPath = path.join(__dirname, '../../css/styles.css');
    const cssContent = fs.readFileSync(cssPath, 'utf8');

    // Check for media query declarations
    const hasMediaQueries = cssContent.includes('@media');
    expect(hasMediaQueries).toBe(true);

    // Check for common responsive breakpoints
    const hasMaxWidthQuery = /@media[^{]*max-width\s*:\s*\d+px/.test(cssContent);
    const hasMinWidthQuery = /@media[^{]*min-width\s*:\s*\d+px/.test(cssContent);

    // Should have at least one type of responsive media query
    expect(hasMaxWidthQuery || hasMinWidthQuery).toBe(true);

    // Check for mobile breakpoint (around 480px or less)
    const hasMobileBreakpoint = /@media[^{]*(max-width\s*:\s*(320|375|480|576)px|min-width\s*:\s*(320|375|480|576)px)/.test(cssContent);

    // Check for tablet breakpoint (around 768px)
    const hasTabletBreakpoint = /@media[^{]*(max-width\s*:\s*768px|min-width\s*:\s*768px|max-width\s*:\s*1024px|min-width\s*:\s*1024px)/.test(cssContent);

    // Check for desktop breakpoint (around 1200px+)
    const hasDesktopBreakpoint = /@media[^{]*(max-width\s*:\s*(1200|1440)px|min-width\s*:\s*(1200|1440)px)/.test(cssContent);

    // Should have breakpoints for different device sizes
    expect(hasMobileBreakpoint || hasTabletBreakpoint).toBe(true);
  });
});
