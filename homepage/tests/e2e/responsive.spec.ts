/**
 * Responsive Design E2E Tests
 * Owner: Scenario 7 - Responsive Design
 *
 * Tests verify the homepage displays correctly across:
 * - Mobile (320px)
 * - Tablet (768px)
 * - Desktop (1024px+)
 */

import { test, expect } from '@playwright/test';

// Define viewport sizes for testing
const VIEWPORTS = {
  mobile: { width: 320, height: 568 },
  tablet: { width: 768, height: 1024 },
  desktop: { width: 1024, height: 768 },
  largeDesktop: { width: 1440, height: 900 },
};

// Test Case 1: Load homepage at 320px viewport width
test.describe('Mobile viewport (320px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.mobile);
    await page.goto('/');
    // Wait for content to be fully loaded
    await page.waitForLoadState('networkidle');
  });

  test('all content visible without horizontal scroll, layout is single column', async ({ page }) => {
    // Verify no horizontal scrollbar
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const windowWidth = await page.evaluate(() => window.innerWidth);
    expect(bodyScrollWidth).toBeLessThanOrEqual(windowWidth + 1); // +1 for rounding

    // Verify content is visible
    await expect(page.locator('.hero')).toBeVisible();
    await expect(page.locator('h1, .hero-title')).toBeVisible();

    // Verify single column layout - hero container should be flex-direction: column
    const heroContainer = page.locator('.hero-container');
    if (await heroContainer.count() > 0) {
      const flexDirection = await heroContainer.evaluate((el) =>
        window.getComputedStyle(el).flexDirection
      );
      expect(flexDirection).toBe('column');
    }

    // Verify features grid is single column if it exists
    const featuresGrid = page.locator('.features-grid');
    if (await featuresGrid.count() > 0) {
      const gridCols = await featuresGrid.evaluate((el) =>
        window.getComputedStyle(el).gridTemplateColumns
      );
      // Should be a single column (one value, not multiple)
      const columnCount = gridCols.split(' ').filter(c => c.trim() !== '').length;
      expect(columnCount).toBe(1);
    }
  });

  test('text is readable without zooming', async ({ page }) => {
    // Check that main text elements have readable font sizes (at least 14px)
    const minFontSize = 14;

    // Check hero tagline
    const heroTagline = page.locator('.hero-tagline');
    if (await heroTagline.count() > 0) {
      const taglineFontSize = await heroTagline.evaluate((el) =>
        parseFloat(window.getComputedStyle(el).fontSize)
      );
      expect(taglineFontSize).toBeGreaterThanOrEqual(minFontSize);
    }

    // Check feature descriptions if present
    const featureDesc = page.locator('.feature-description').first();
    if (await featureDesc.count() > 0) {
      const featureFontSize = await featureDesc.evaluate((el) =>
        parseFloat(window.getComputedStyle(el).fontSize)
      );
      expect(featureFontSize).toBeGreaterThanOrEqual(12); // Slightly smaller is ok for secondary text
    }
  });
});

// Test Case 2: Load homepage at 768px viewport width
test.describe('Tablet viewport (768px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.tablet);
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('layout adapts to tablet view, may show 2-column layout', async ({ page }) => {
    // Verify no horizontal scrollbar
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const windowWidth = await page.evaluate(() => window.innerWidth);
    expect(bodyScrollWidth).toBeLessThanOrEqual(windowWidth + 1);

    // Verify content is visible
    await expect(page.locator('.hero')).toBeVisible();

    // At tablet, hero might be row layout
    const heroContainer = page.locator('.hero-container');
    if (await heroContainer.count() > 0) {
      const flexDirection = await heroContainer.evaluate((el) =>
        window.getComputedStyle(el).flexDirection
      );
      // Either row or column is acceptable at tablet
      expect(['row', 'column']).toContain(flexDirection);
    }

    // Features grid should have 2 columns at tablet if present
    const featuresGrid = page.locator('.features-grid');
    if (await featuresGrid.count() > 0) {
      const gridCols = await featuresGrid.evaluate((el) =>
        window.getComputedStyle(el).gridTemplateColumns
      );
      const columnCount = gridCols.split(' ').filter(c => c.trim() !== '').length;
      // Should be 2 columns at tablet
      expect(columnCount).toBeGreaterThanOrEqual(2);
    }
  });
});

// Test Case 3: Load homepage at 1024px viewport width
test.describe('Desktop viewport (1024px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.desktop);
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('full desktop layout displayed with optimal use of space', async ({ page }) => {
    // Verify no horizontal scrollbar
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const windowWidth = await page.evaluate(() => window.innerWidth);
    expect(bodyScrollWidth).toBeLessThanOrEqual(windowWidth + 1);

    // Verify content is visible
    await expect(page.locator('.hero')).toBeVisible();

    // At desktop, hero should be row layout
    const heroContainer = page.locator('.hero-container');
    if (await heroContainer.count() > 0) {
      const flexDirection = await heroContainer.evaluate((el) =>
        window.getComputedStyle(el).flexDirection
      );
      expect(flexDirection).toBe('row');
    }

    // Container should have reasonable max-width for desktop
    const container = page.locator('.container').first();
    if (await container.count() > 0) {
      const maxWidth = await container.evaluate((el) =>
        window.getComputedStyle(el).maxWidth
      );
      // Should have a max-width set
      expect(maxWidth).not.toBe('none');
    }
  });
});

// Test Case 4: Check navigation menu on mobile
test.describe('Navigation on mobile', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.mobile);
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('navigation collapses to hamburger menu or stacked layout', async ({ page }) => {
    // Check that navigation exists
    const nav = page.locator('.header-nav, nav');
    await expect(nav.first()).toBeVisible();

    // The navigation list should wrap or stack on mobile
    const navList = page.locator('.header-nav-list');
    if (await navList.count() > 0) {
      const flexWrap = await navList.evaluate((el) =>
        window.getComputedStyle(el).flexWrap
      );
      // Should allow wrapping on mobile
      expect(flexWrap).toBe('wrap');
    }

    // All nav links should still be accessible
    const navLinks = page.locator('.header-nav-link, .header-nav a');
    const linkCount = await navLinks.count();
    expect(linkCount).toBeGreaterThan(0);

    // Verify each link is visible
    for (let i = 0; i < linkCount; i++) {
      await expect(navLinks.nth(i)).toBeVisible();
    }
  });
});

// Test Case 5: Verify images scale appropriately on mobile
test.describe('Image scaling on mobile', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.mobile);
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('images (logo.gif, usage.gif) scale to fit viewport without overflow', async ({ page }) => {
    // Check hero logo image
    const logoImage = page.locator('.hero-logo-image, img[src*="logo"]');
    if (await logoImage.count() > 0) {
      const logoBox = await logoImage.first().boundingBox();
      if (logoBox) {
        // Image should not exceed viewport width
        expect(logoBox.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width);
        // Image should have max-width: 100%
        const maxWidth = await logoImage.first().evaluate((el) =>
          window.getComputedStyle(el).maxWidth
        );
        expect(maxWidth).toMatch(/100%|.*px/);
      }
    }

    // Check usage gif image
    const usageImage = page.locator('.usage-gif, img[src*="usage"]');
    if (await usageImage.count() > 0) {
      const usageBox = await usageImage.first().boundingBox();
      if (usageBox) {
        // Image width should not exceed viewport width (accounting for some margin)
        expect(usageBox.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width + 32);
      }
    }
  });
});

// Test Case 6: Test button tap targets on mobile
test.describe('Touch targets on mobile', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.mobile);
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('all buttons have minimum 44x44 pixel tap targets', async ({ page }) => {
    const minTouchSize = 44;

    // Check CTA buttons
    const ctaButtons = page.locator('.hero-cta, .usage-docs-link, button');
    const buttonCount = await ctaButtons.count();

    for (let i = 0; i < Math.min(buttonCount, 5); i++) {
      const button = ctaButtons.nth(i);
      if (await button.isVisible()) {
        const box = await button.boundingBox();
        if (box) {
          // Either width or height should meet minimum (accounting for padding)
          expect(box.height).toBeGreaterThanOrEqual(minTouchSize - 4); // Small tolerance
          expect(box.width).toBeGreaterThanOrEqual(minTouchSize - 4);
        }
      }
    }

    // Check navigation links
    const navLinks = page.locator('.header-nav-link');
    const navLinkCount = await navLinks.count();

    for (let i = 0; i < navLinkCount; i++) {
      const link = navLinks.nth(i);
      if (await link.isVisible()) {
        const box = await link.boundingBox();
        if (box) {
          // Height should meet minimum touch target
          expect(box.height).toBeGreaterThanOrEqual(minTouchSize - 4);
        }
      }
    }
  });
});

// Test Case 7: Verify code blocks are readable on mobile
test.describe('Code blocks on mobile', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.mobile);
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('code blocks are horizontally scrollable or wrap appropriately', async ({ page }) => {
    // Check code blocks in installation section
    const codeBlocks = page.locator('.code-block, pre');
    const codeBlockCount = await codeBlocks.count();

    if (codeBlockCount > 0) {
      for (let i = 0; i < codeBlockCount; i++) {
        const codeBlock = codeBlocks.nth(i);
        if (await codeBlock.isVisible()) {
          // Code blocks should have overflow-x: auto for scrolling
          const overflowX = await codeBlock.evaluate((el) => {
            const style = window.getComputedStyle(el);
            // Check the pre element inside if it's a wrapper
            const pre = el.querySelector('pre') || el;
            return window.getComputedStyle(pre).overflowX;
          });
          expect(['auto', 'scroll', 'hidden']).toContain(overflowX);

          // Code block should not overflow the viewport
          const box = await codeBlock.boundingBox();
          if (box) {
            // Account for padding/margin
            expect(box.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width + 32);
          }
        }
      }
    }
  });
});

// Additional test: No horizontal scrolling at any viewport
test.describe('No horizontal scrolling', () => {
  for (const [viewportName, viewport] of Object.entries(VIEWPORTS)) {
    test(`no horizontal scrolling at ${viewportName} (${viewport.width}px)`, async ({ page }) => {
      await page.setViewportSize(viewport);
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Check document doesn't have horizontal overflow
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      expect(hasHorizontalScroll).toBe(false);
    });
  }
});

// Cross-viewport visual consistency test
test.describe('Responsive breakpoint transitions', () => {
  test('content remains accessible across all breakpoints', async ({ page }) => {
    const viewportsToTest = [
      VIEWPORTS.mobile,
      { width: 480, height: 800 }, // Larger mobile
      VIEWPORTS.tablet,
      VIEWPORTS.desktop,
      VIEWPORTS.largeDesktop,
    ];

    for (const viewport of viewportsToTest) {
      await page.setViewportSize(viewport);
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Core content should always be visible
      await expect(page.locator('.hero')).toBeVisible();

      // No horizontal overflow
      const scrollWidth = await page.evaluate(() => document.body.scrollWidth);
      expect(scrollWidth).toBeLessThanOrEqual(viewport.width + 1);
    }
  });
});
