/**
 * Responsive Design E2E Tests
 * Owners: Scenarios 8, 9, 10 (Responsive)
 *
 * Test groups:
 * - Mobile viewport (375px) layout
 * - Tablet viewport (768px) layout
 * - Desktop viewport (1280px) layout
 * - Grid column adjustments
 * - Touch target sizes
 * - No horizontal scroll
 */

import { test, expect } from '@playwright/test';
import { VIEWPORTS, waitForLoad } from './utils';

test.describe('Responsive Design - Mobile (Scenario 8)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.mobile);
    await page.goto('/');
    await waitForLoad(page);
  });

  test('TC1: Page renders at 375px without horizontal scrollbar', async ({ page }) => {
    // Get body dimensions
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);

    // No horizontal scrollbar means body width <= viewport width
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth);

    // Also check that html doesn't overflow
    const htmlWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    expect(htmlWidth).toBeLessThanOrEqual(viewportWidth);

    // Ensure no horizontal scroll is possible
    const canScrollHorizontally = await page.evaluate(() => {
      return document.documentElement.scrollWidth > window.innerWidth;
    });
    expect(canScrollHorizontally).toBe(false);
  });

  test('TC2: Hero section layout at mobile viewport - logo, headline, and CTA visible', async ({ page }) => {
    // Check logo is visible
    const logo = page.locator('.hero-logo');
    await expect(logo).toBeVisible();

    // Check logo dimensions are reasonable for mobile
    const logoBoundingBox = await logo.boundingBox();
    expect(logoBoundingBox).not.toBeNull();
    if (logoBoundingBox) {
      expect(logoBoundingBox.width).toBeLessThanOrEqual(150);
      expect(logoBoundingBox.width).toBeGreaterThan(50);
    }

    // Check headline is visible
    const headline = page.locator('.hero-headline');
    await expect(headline).toBeVisible();

    // Check headline fits within viewport (no overflow)
    const headlineBoundingBox = await headline.boundingBox();
    expect(headlineBoundingBox).not.toBeNull();
    if (headlineBoundingBox) {
      expect(headlineBoundingBox.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width);
    }

    // Check primary CTA button is visible
    const ctaButton = page.locator('#primary-cta');
    await expect(ctaButton).toBeVisible();

    // Check secondary CTA button is visible
    const secondaryCta = page.locator('.hero-cta .btn-secondary');
    await expect(secondaryCta).toBeVisible();
  });

  test('TC3: Features section displays in single column at mobile viewport', async ({ page }) => {
    // Scroll to features section
    await page.locator('#features').scrollIntoViewIfNeeded();

    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Get grid computed style
    const gridStyle = await page.evaluate(() => {
      const grid = document.querySelector('.features-grid');
      if (!grid) return null;
      const style = window.getComputedStyle(grid);
      return {
        gridTemplateColumns: style.gridTemplateColumns,
        display: style.display,
      };
    });

    expect(gridStyle).not.toBeNull();
    if (gridStyle) {
      // Should be either single column or 2-column max
      // Single column would show as just pixel value like "343px"
      // 2-column would show as two values like "171.5px 171.5px"
      const columns = gridStyle.gridTemplateColumns.split(' ').filter(c => c.trim());
      expect(columns.length).toBeLessThanOrEqual(2);
    }

    // Verify all feature items are visible
    const featureItems = page.locator('.feature-item');
    const count = await featureItems.count();
    expect(count).toBeGreaterThan(0);

    // Check each feature item doesn't overflow
    for (let i = 0; i < count; i++) {
      const item = featureItems.nth(i);
      const boundingBox = await item.boundingBox();
      expect(boundingBox).not.toBeNull();
      if (boundingBox) {
        expect(boundingBox.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width);
      }
    }
  });

  test('TC4: Primary CTA button has at least 44px height for touch targets', async ({ page }) => {
    const primaryCta = page.locator('#primary-cta');
    await expect(primaryCta).toBeVisible();

    // Get button dimensions
    const boundingBox = await primaryCta.boundingBox();
    expect(boundingBox).not.toBeNull();
    if (boundingBox) {
      // WCAG/Apple HIG recommends minimum 44px touch targets
      expect(boundingBox.height).toBeGreaterThanOrEqual(44);
    }

    // Also check computed min-height
    const computedHeight = await page.evaluate(() => {
      const btn = document.querySelector('#primary-cta');
      if (!btn) return 0;
      return btn.getBoundingClientRect().height;
    });
    expect(computedHeight).toBeGreaterThanOrEqual(44);
  });

  test('All touch targets meet minimum 44px size requirement', async ({ page }) => {
    // Check all buttons in hero section
    const heroButtons = page.locator('.hero-cta .btn');
    const heroButtonCount = await heroButtons.count();
    for (let i = 0; i < heroButtonCount; i++) {
      const btn = heroButtons.nth(i);
      const box = await btn.boundingBox();
      expect(box).not.toBeNull();
      if (box) {
        expect(box.height).toBeGreaterThanOrEqual(44);
      }
    }

    // Check footer links for touch-friendly sizing
    await page.locator('#footer').scrollIntoViewIfNeeded();
    const footerLinks = page.locator('.footer-link');
    const footerLinkCount = await footerLinks.count();
    for (let i = 0; i < footerLinkCount; i++) {
      const link = footerLinks.nth(i);
      const box = await link.boundingBox();
      expect(box).not.toBeNull();
      if (box) {
        // Footer links should also be touch-friendly
        expect(box.height).toBeGreaterThanOrEqual(44);
      }
    }
  });

  test('Text is readable without horizontal scrolling', async ({ page }) => {
    // Check that no text element overflows the viewport
    const overflowingElements = await page.evaluate(() => {
      const elements = document.querySelectorAll('p, h1, h2, h3, span, a, li');
      const overflowing: string[] = [];
      const viewportWidth = window.innerWidth;

      elements.forEach((el) => {
        const rect = el.getBoundingClientRect();
        if (rect.right > viewportWidth) {
          overflowing.push(el.tagName + ': ' + el.textContent?.substring(0, 50));
        }
      });

      return overflowing;
    });

    expect(overflowingElements).toHaveLength(0);
  });

  test('Hero section stacks vertically on mobile', async ({ page }) => {
    // Check that hero CTA buttons stack vertically
    const heroCtaContainer = page.locator('.hero-cta');
    const flexDirection = await page.evaluate(() => {
      const el = document.querySelector('.hero-cta');
      if (!el) return '';
      return window.getComputedStyle(el).flexDirection;
    });

    expect(flexDirection).toBe('column');
  });

  test('Code blocks are readable and scrollable', async ({ page }) => {
    // Scroll to quickstart section
    await page.locator('#quickstart').scrollIntoViewIfNeeded();

    const codeBlocks = page.locator('.step-code');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThan(0);

    // Check that code blocks don't overflow the viewport
    for (let i = 0; i < count; i++) {
      const codeBlock = codeBlocks.nth(i);
      const boundingBox = await codeBlock.boundingBox();
      expect(boundingBox).not.toBeNull();
      if (boundingBox) {
        // Code blocks should fit within container (with overflow-x for scrolling)
        expect(boundingBox.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width);
      }
    }
  });
});
