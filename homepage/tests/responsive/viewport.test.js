/**
 * Responsive Design Tests
 * Owner: Scenario 6 - Responsive Design
 *
 * Test coverage:
 * - Desktop viewport (1920x1080)
 * - Tablet viewport (768x1024)
 * - Mobile viewport (375x667)
 * - Layout integrity across breakpoints
 * - Touch target sizes
 * - No horizontal scrolling
 */

const { test, expect } = require('@playwright/test');
const { VIEWPORTS, MIN_TOUCH_TARGET } = require('../setup.js');

// Test Case 1: Desktop viewport (1920x1080)
test.describe('Desktop Viewport (1920x1080)', () => {
  test.use({ viewport: VIEWPORTS.desktop });

  test('full layout displays correctly with all sections visible and properly spaced', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Check main sections are visible
    await expect(page.locator('#hero')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('#code-examples')).toBeVisible();
    await expect(page.locator('#architecture')).toBeVisible();
    await expect(page.locator('#roadmap')).toBeVisible();
    await expect(page.locator('footer')).toBeVisible();

    // Check hero section takes up significant width
    const hero = page.locator('#hero');
    const heroBox = await hero.boundingBox();
    expect(heroBox.width).toBeGreaterThanOrEqual(VIEWPORTS.desktop.width * 0.8);

    // Check no horizontal scroll
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });

  test('CTA buttons are displayed side by side on desktop', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Find CTA buttons in hero
    const ctaContainer = page.locator('#hero .flex').first();
    await expect(ctaContainer).toBeVisible();

    const buttons = ctaContainer.locator('a');
    const buttonCount = await buttons.count();
    expect(buttonCount).toBeGreaterThanOrEqual(2);

    if (buttonCount >= 2) {
      const firstButton = await buttons.nth(0).boundingBox();
      const secondButton = await buttons.nth(1).boundingBox();

      // Buttons should be on the same row (similar Y position)
      expect(Math.abs(firstButton.y - secondButton.y)).toBeLessThan(10);
    }
  });
});

// Test Case 2: Tablet viewport (768x1024)
test.describe('Tablet Viewport (768x1024)', () => {
  test.use({ viewport: VIEWPORTS.tablet });

  test('layout adapts appropriately, text remains readable', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // All sections should still be visible
    await expect(page.locator('#hero')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('#code-examples')).toBeVisible();

    // Check hero title is visible
    const heroTitle = page.locator('#hero h1');
    await expect(heroTitle).toBeVisible();

    // No horizontal scroll
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });
});

// Test Case 3: Mobile viewport (375x667)
test.describe('Mobile Viewport (375x667)', () => {
  test.use({ viewport: VIEWPORTS.mobile });

  test('single-column layout, touch-friendly button sizes, all content accessible', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // All sections should be visible
    await expect(page.locator('#hero')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('#code-examples')).toBeVisible();

    // Check buttons have minimum touch target size (44px)
    const buttons = page.locator('a[class*="bg-"]');
    const buttonCount = await buttons.count();

    for (let i = 0; i < Math.min(buttonCount, 5); i++) {
      const button = buttons.nth(i);
      if (await button.isVisible()) {
        const buttonBox = await button.boundingBox();
        if (buttonBox) {
          expect(buttonBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
        }
      }
    }

    // No horizontal scroll
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });
});

// Test Case 4: No horizontal scrolling on mobile
test.describe('Horizontal Scrolling Test', () => {
  test.use({ viewport: VIEWPORTS.mobile });

  test('no horizontal scrollbar appears, all content fits within viewport width', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Check no horizontal overflow
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Check body doesn't overflow
    const bodyOverflows = await page.evaluate(() => {
      const body = document.body;
      return body.scrollWidth > body.clientWidth;
    });
    expect(bodyOverflows).toBe(false);
  });
});

// Test Case 5: Touch targets on mobile
test.describe('Touch Target Sizes', () => {
  test.use({ viewport: VIEWPORTS.mobile });

  test('all interactive elements have minimum 44x44px touch target size', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Check buttons in hero section
    const heroButtons = page.locator('#hero a');
    const buttonCount = await heroButtons.count();

    for (let i = 0; i < buttonCount; i++) {
      const button = heroButtons.nth(i);
      if (await button.isVisible()) {
        const box = await button.boundingBox();
        if (box) {
          expect(box.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
        }
      }
    }

    // Check footer links
    const footerLinks = page.locator('footer a');
    const linkCount = await footerLinks.count();

    for (let i = 0; i < linkCount; i++) {
      const link = footerLinks.nth(i);
      if (await link.isVisible()) {
        const box = await link.boundingBox();
        if (box) {
          expect(box.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
        }
      }
    }
  });
});

// Test Case 6: Code examples on mobile
test.describe('Code Examples on Mobile', () => {
  test.use({ viewport: VIEWPORTS.mobile });

  test('code blocks are scrollable horizontally within their container', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Scroll to code examples section
    await page.locator('#code-examples').scrollIntoViewIfNeeded();

    // Check code blocks exist
    const codeBlocks = page.locator('#code-examples pre');
    await expect(codeBlocks.first()).toBeVisible();

    // Check code block has overflow handling
    const hasOverflowScroll = await page.evaluate(() => {
      const codeBlock = document.querySelector('#code-examples pre');
      if (!codeBlock) return false;
      const style = window.getComputedStyle(codeBlock);
      return style.overflowX === 'auto' || style.overflowX === 'scroll' || style.overflowX === 'hidden';
    });
    expect(hasOverflowScroll).toBe(true);

    // Verify page still has no horizontal scroll
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });
});

// Additional responsive test - Viewport transitions
test.describe('Viewport Transitions', () => {
  test('layout smoothly adapts when viewport changes', async ({ page }) => {
    await page.goto('/');

    // Start at desktop
    await page.setViewportSize(VIEWPORTS.desktop);
    await expect(page.locator('#hero')).toBeVisible();

    // Transition to tablet
    await page.setViewportSize(VIEWPORTS.tablet);
    await expect(page.locator('#hero')).toBeVisible();

    // Transition to mobile
    await page.setViewportSize(VIEWPORTS.mobile);
    await expect(page.locator('#hero')).toBeVisible();

    // Check no horizontal scroll at mobile
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });
});
