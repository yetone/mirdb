/**
 * Responsive Design E2E Tests
 * Owner: Scenario 7 - Responsive Design and Accessibility
 *
 * Tests viewport breakpoints: 320px (mobile), 768px (tablet), 1024px (desktop), 1920px (wide)
 * Verifies responsive layouts, touch targets, and content reflow
 */

const { test, expect } = require('@playwright/test');

// Viewport configurations
const VIEWPORTS = {
  mobile: { width: 320, height: 568 },
  mobileLandscape: { width: 568, height: 320 },
  tablet: { width: 768, height: 1024 },
  desktop: { width: 1024, height: 768 },
  wide: { width: 1920, height: 1080 },
};

// Minimum touch target size (WCAG 2.5.5)
const MIN_TOUCH_TARGET = 44;

test.describe('Responsive Design - Viewport Tests', () => {
  test.beforeEach(async ({ page }) => {
    // Start server if needed - tests assume server is running
    await page.goto('/');
  });

  test('Test Case 1: 320px viewport - no horizontal scrolling', async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.mobile);
    await page.waitForLoadState('networkidle');

    // Test requirement: "All content is visible without horizontal scrolling (except code blocks)"
    // Code blocks and architecture diagrams may cause overflow within their containers - that's acceptable
    // We verify that the main layout sections fit within viewport

    // Verify main content areas are visible
    await expect(page.locator('.hero')).toBeVisible();
    await expect(page.locator('.features')).toBeVisible();
    await expect(page.locator('.getting-started')).toBeVisible();
    await expect(page.locator('.footer')).toBeVisible();

    // Hero section should not exceed viewport width
    const hero = page.locator('.hero');
    const heroBox = await hero.boundingBox();
    expect(heroBox.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width);

    // Features section should not exceed viewport width
    const features = page.locator('.features');
    const featuresBox = await features.boundingBox();
    expect(featuresBox.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width);

    // Footer should not exceed viewport width
    const footer = page.locator('.footer');
    const footerBox = await footer.boundingBox();
    expect(footerBox.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width);

    // Navigation should not exceed viewport width
    const nav = page.locator('.nav');
    const navBox = await nav.boundingBox();
    expect(navBox.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width);

    // Verify code blocks have proper overflow handling (horizontal scroll within)
    const codeBlocks = page.locator('.getting-started__code pre, .code-examples__code pre');
    const codeBlockCount = await codeBlocks.count();

    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      if (await codeBlock.isVisible()) {
        const overflow = await codeBlock.evaluate((el) => window.getComputedStyle(el).overflowX);
        expect(overflow).toBe('auto');
      }
    }
  });

  test('Test Case 2: 768px tablet viewport - appropriate layout', async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.tablet);
    await page.waitForLoadState('networkidle');

    // Navigation should be inline (not stacked)
    const navLinks = page.locator('.nav__links');
    await expect(navLinks).toBeVisible();

    // Feature grid should show 2 columns on tablet
    const featuresGrid = page.locator('.features__grid');
    const gridStyle = await featuresGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.gridTemplateColumns;
    });

    // Should have 2 columns (2 fr values)
    const columnCount = gridStyle.split(' ').filter(v => v.includes('px') || v.includes('fr')).length;
    expect(columnCount).toBeGreaterThanOrEqual(2);

    // Hero CTAs should be inline
    const heroCtas = page.locator('.hero__ctas');
    const ctaStyle = await heroCtas.evaluate((el) => {
      return window.getComputedStyle(el).flexDirection;
    });
    expect(ctaStyle).toBe('row');
  });

  test('Test Case 3: 1024px desktop viewport - full layout', async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.desktop);
    await page.waitForLoadState('networkidle');

    // Feature grid should show 3 columns on desktop
    const featuresGrid = page.locator('.features__grid');
    const gridStyle = await featuresGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.gridTemplateColumns;
    });

    // Should have 3 columns
    const columnCount = gridStyle.split(' ').filter(v => v.includes('px') || v.includes('fr')).length;
    expect(columnCount).toBe(3);

    // All sections should be visible
    await expect(page.locator('#hero')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('#getting-started')).toBeVisible();
    await expect(page.locator('#architecture')).toBeVisible();
    await expect(page.locator('#code-examples')).toBeVisible();
    await expect(page.locator('.footer')).toBeVisible();
  });

  test('Test Case 4: 1920px wide viewport - content centered and readable', async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.wide);
    await page.waitForLoadState('networkidle');

    // Container should have max-width and be centered
    const container = page.locator('.container').first();
    const containerBox = await container.boundingBox();
    const viewportWidth = VIEWPORTS.wide.width;

    // Container should not span full width on wide screens
    expect(containerBox.width).toBeLessThan(viewportWidth);

    // Container should be centered (left margin ≈ right margin)
    const leftMargin = containerBox.x;
    const rightMargin = viewportWidth - (containerBox.x + containerBox.width);
    const marginDiff = Math.abs(leftMargin - rightMargin);
    expect(marginDiff).toBeLessThan(50); // Allow some tolerance

    // Hero tagline should have max-width to maintain readability
    const heroTagline = page.locator('.hero__tagline');
    if (await heroTagline.count() > 0) {
      const taglineBox = await heroTagline.boundingBox();
      expect(taglineBox.width).toBeLessThanOrEqual(900); // Should not be too wide
    }
  });

  test('Test Case 5: Touch target sizes on mobile', async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.mobile);
    await page.waitForLoadState('networkidle');

    // Check all buttons have minimum touch target size
    const buttons = page.locator('button, .btn');
    const buttonCount = await buttons.count();

    for (let i = 0; i < buttonCount; i++) {
      const button = buttons.nth(i);
      if (await button.isVisible()) {
        const box = await button.boundingBox();
        if (box) {
          expect(box.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
          expect(box.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
        }
      }
    }

    // Check navigation links have minimum touch target
    const navLinks = page.locator('.nav__links a');
    const navLinkCount = await navLinks.count();

    for (let i = 0; i < navLinkCount; i++) {
      const link = navLinks.nth(i);
      if (await link.isVisible()) {
        const box = await link.boundingBox();
        if (box) {
          expect(box.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
        }
      }
    }
  });

  test('Test Case 14: 200% zoom - content remains readable', async ({ page }) => {
    // Set viewport to simulate 200% zoom
    await page.setViewportSize({ width: 960, height: 540 }); // Half of 1920x1080
    await page.waitForLoadState('networkidle');

    // Verify content reflows properly
    const documentWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);

    // No horizontal scrolling at zoomed viewport
    expect(documentWidth).toBeLessThanOrEqual(viewportWidth + 50);

    // All main sections should still be visible
    await expect(page.locator('.hero')).toBeVisible();
    await expect(page.locator('.features')).toBeVisible();

    // Text should still be readable (check font-size is reasonable)
    const bodyFontSize = await page.evaluate(() => {
      return parseFloat(window.getComputedStyle(document.body).fontSize);
    });
    expect(bodyFontSize).toBeGreaterThanOrEqual(12);
  });
});

test.describe('Responsive Design - Layout Reflow', () => {
  test('Feature cards reflow from 3 to 2 to 1 column', async ({ page }) => {
    await page.goto('/');

    // Desktop: 3 columns
    await page.setViewportSize(VIEWPORTS.desktop);
    let grid = page.locator('.features__grid');
    let gridStyle = await grid.evaluate((el) => window.getComputedStyle(el).gridTemplateColumns);
    expect(gridStyle.split(' ').filter(v => v !== '').length).toBe(3);

    // Tablet: 2 columns
    await page.setViewportSize(VIEWPORTS.tablet);
    await page.waitForTimeout(100); // Wait for CSS to apply
    gridStyle = await grid.evaluate((el) => window.getComputedStyle(el).gridTemplateColumns);
    expect(gridStyle.split(' ').filter(v => v !== '').length).toBe(2);

    // Mobile: 1 column
    await page.setViewportSize(VIEWPORTS.mobile);
    await page.waitForTimeout(100);
    gridStyle = await grid.evaluate((el) => window.getComputedStyle(el).gridTemplateColumns);
    expect(gridStyle.split(' ').filter(v => v !== '').length).toBe(1);
  });

  test('Hero CTAs stack vertically on mobile', async ({ page }) => {
    await page.goto('/');
    await page.setViewportSize(VIEWPORTS.mobile);
    await page.waitForLoadState('networkidle');

    const heroCtas = page.locator('.hero__ctas');
    const flexDirection = await heroCtas.evaluate((el) => {
      return window.getComputedStyle(el).flexDirection;
    });

    expect(flexDirection).toBe('column');
  });

  test('Navigation wraps properly on small screens', async ({ page }) => {
    await page.goto('/');
    await page.setViewportSize(VIEWPORTS.mobile);
    await page.waitForLoadState('networkidle');

    // Navigation links should be visible and accessible
    const navLinks = page.locator('.nav__links');
    await expect(navLinks).toBeVisible();

    // Links should wrap to fit screen
    const navBox = await navLinks.boundingBox();
    expect(navBox.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width);
  });

  test('Footer layout adjusts for mobile', async ({ page }) => {
    await page.goto('/');
    await page.setViewportSize(VIEWPORTS.mobile);
    await page.waitForLoadState('networkidle');

    const footer = page.locator('.footer .container');
    const flexDirection = await footer.evaluate((el) => {
      return window.getComputedStyle(el).flexDirection;
    });

    // Footer should stack on mobile
    expect(flexDirection).toBe('column');
  });
});

test.describe('Responsive Design - Code Blocks', () => {
  test('Code blocks have horizontal scroll on mobile', async ({ page }) => {
    await page.goto('/');
    await page.setViewportSize(VIEWPORTS.mobile);
    await page.waitForLoadState('networkidle');

    // Navigate to getting started section
    const codeBlock = page.locator('.getting-started__code pre').first();

    if (await codeBlock.count() > 0) {
      const overflow = await codeBlock.evaluate((el) => {
        return window.getComputedStyle(el).overflowX;
      });
      expect(overflow).toBe('auto');
    }
  });

  test('Code tabs are scrollable on mobile', async ({ page }) => {
    await page.goto('/');
    await page.setViewportSize(VIEWPORTS.mobile);
    await page.waitForLoadState('networkidle');

    // Check tabs wrapper has overflow scroll
    const tabsWrapper = page.locator('.code-examples__tabs-wrapper');
    if (await tabsWrapper.count() > 0) {
      const overflow = await tabsWrapper.evaluate((el) => {
        return window.getComputedStyle(el).overflowX;
      });
      expect(overflow).toBe('auto');
    }
  });
});

test.describe('Responsive Design - Images and Media', () => {
  test('Images scale properly on mobile', async ({ page }) => {
    await page.goto('/');
    await page.setViewportSize(VIEWPORTS.mobile);
    await page.waitForLoadState('networkidle');

    // Check all images fit within viewport
    const images = page.locator('img');
    const imageCount = await images.count();

    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i);
      if (await img.isVisible()) {
        const box = await img.boundingBox();
        if (box) {
          expect(box.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width);
        }
      }
    }
  });

  test('Architecture diagrams are scrollable on mobile', async ({ page }) => {
    await page.goto('/');
    await page.setViewportSize(VIEWPORTS.mobile);
    await page.waitForLoadState('networkidle');

    // Diagram containers should have overflow handling
    const diagrams = page.locator('.architecture__diagram');
    const diagramCount = await diagrams.count();

    for (let i = 0; i < diagramCount; i++) {
      const diagram = diagrams.nth(i);
      if (await diagram.isVisible()) {
        const overflow = await diagram.evaluate((el) => {
          return window.getComputedStyle(el).overflowX;
        });
        expect(overflow).toBe('auto');
      }
    }
  });
});
