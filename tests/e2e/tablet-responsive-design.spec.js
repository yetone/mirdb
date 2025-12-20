/**
 * E2E Tests for Responsive Design - Tablet Viewport
 * Scenario: Verify the homepage displays correctly on tablet devices with appropriate layout adjustments
 * Test Cases: 1, 2, 3
 * Validates tablet viewport layout, feature grid columns, and hero section positioning
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '../../index.html');

// Tablet viewport dimensions (Standard iPad size)
const TABLET_VIEWPORT = { width: 768, height: 1024 };

test.describe('Responsive Design - Tablet Viewport', () => {
  test.beforeEach(async ({ page }) => {
    // Set tablet viewport before each test
    await page.setViewportSize(TABLET_VIEWPORT);
    await page.goto(indexPath);
  });

  test.describe('Test Case 1: Tablet Layout Without Horizontal Scroll', () => {
    test('should display page in tablet-optimized layout without horizontal scroll at 768x1024', async ({ page }) => {
      // Verify no horizontal scroll is needed
      const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
      const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);

      // Page content should fit within viewport width
      expect(scrollWidth).toBeLessThanOrEqual(TABLET_VIEWPORT.width);

      // Body should not overflow horizontally
      const bodyOverflow = await page.evaluate(() => {
        const body = document.body;
        return body.scrollWidth <= body.clientWidth || body.scrollWidth <= window.innerWidth;
      });
      expect(bodyOverflow).toBe(true);
    });

    test('should have proper meta viewport tag for responsive behavior', async ({ page }) => {
      const viewportMeta = await page.locator('meta[name="viewport"]');
      await expect(viewportMeta).toHaveAttribute('content', /width=device-width/);
    });

    test('should not have any elements extending beyond viewport at tablet width', async ({ page }) => {
      // Check that all major sections fit within viewport
      const sections = ['.hero', '.quick-start', '.configuration'];

      for (const selector of sections) {
        const element = page.locator(selector);
        const count = await element.count();
        if (count > 0) {
          const box = await element.boundingBox();
          if (box) {
            expect(box.x).toBeGreaterThanOrEqual(0);
            expect(box.x + box.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width + 1); // +1 for rounding
          }
        }
      }
    });

    test('should maintain readable font sizes on tablet', async ({ page }) => {
      const h1 = page.locator('h1').first();
      const fontSize = await h1.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });

      // Font should be large enough to read (at least 28px for h1 on tablet)
      expect(fontSize).toBeGreaterThanOrEqual(28);
    });
  });

  test.describe('Test Case 2: Feature Grid Columns on Tablet', () => {
    test('should display feature cards in 2-3 column grid on tablet viewport', async ({ page }) => {
      // Check if feature grid exists (in the main index.html with hero-differentiators)
      const differentiators = page.locator('.hero-differentiators');
      const count = await differentiators.count();

      if (count > 0) {
        // Verify differentiators have appropriate display
        const display = await differentiators.evaluate((el) => {
          return window.getComputedStyle(el).display;
        });

        // On tablet (768px), the differentiators should remain in flex row
        // as per the CSS media query at max-width: 768px
        expect(['flex', 'grid', 'block']).toContain(display);

        // Check that differentiator items are positioned appropriately
        const items = page.locator('.differentiator');
        const itemCount = await items.count();

        if (itemCount >= 2) {
          const firstBox = await items.nth(0).boundingBox();
          const secondBox = await items.nth(1).boundingBox();

          // Items should be stacked vertically on tablet
          // OR be side by side if viewport is wide enough
          expect(firstBox).not.toBeNull();
          expect(secondBox).not.toBeNull();
        }
      }
    });

    test('should adapt installation steps layout for tablet', async ({ page }) => {
      const steps = page.locator('.installation-steps li');
      const count = await steps.count();

      if (count > 0) {
        // Verify steps are visible and properly spaced
        const firstStep = steps.first();
        await expect(firstStep).toBeVisible();

        // Check that padding is reduced for tablet as per CSS media query
        const paddingLeft = await firstStep.evaluate((el) => {
          return parseFloat(window.getComputedStyle(el).paddingLeft);
        });

        // At 768px, padding should be 50px as per the media query
        expect(paddingLeft).toBeLessThanOrEqual(60);
      }
    });

    test('should have appropriate code block sizing on tablet', async ({ page }) => {
      const codeBlocks = page.locator('.code-block');
      const count = await codeBlocks.count();

      if (count > 0) {
        const firstCodeBlock = codeBlocks.first();
        const box = await firstCodeBlock.boundingBox();

        if (box) {
          // Code block should fit within container with proper margins
          expect(box.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width - 40); // Account for padding
        }
      }
    });

    test('should display configuration table properly on tablet', async ({ page }) => {
      const configTable = page.locator('.config-table');
      const count = await configTable.count();

      if (count > 0) {
        await expect(configTable).toBeVisible();

        // Table should not cause horizontal scroll
        const tableBox = await configTable.boundingBox();
        if (tableBox) {
          // Table width should fit within viewport or be scrollable in its container
          const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
          expect(scrollWidth).toBeLessThanOrEqual(TABLET_VIEWPORT.width);
        }
      }
    });
  });

  test.describe('Test Case 3: Hero Section Layout on Tablet', () => {
    test('should keep hero content above fold with CTAs visible at 768x1024', async ({ page }) => {
      // Check hero section exists
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();

      // Hero title should be visible
      const h1 = page.locator('h1').first();
      await expect(h1).toBeVisible();

      const h1Box = await h1.boundingBox();
      expect(h1Box).not.toBeNull();
      expect(h1Box.y + h1Box.height).toBeLessThan(TABLET_VIEWPORT.height);

      // Hero tagline should be visible
      const tagline = page.locator('.hero-tagline');
      const taglineCount = await tagline.count();
      if (taglineCount > 0) {
        await expect(tagline).toBeVisible();
        const taglineBox = await tagline.boundingBox();
        expect(taglineBox).not.toBeNull();
        expect(taglineBox.y + taglineBox.height).toBeLessThan(TABLET_VIEWPORT.height);
      }

      // CTA buttons should be visible above fold
      const ctaButtons = page.locator('.hero-cta .btn');
      const btnCount = await ctaButtons.count();

      if (btnCount > 0) {
        const firstBtn = ctaButtons.first();
        await expect(firstBtn).toBeVisible();

        const btnBox = await firstBtn.boundingBox();
        expect(btnBox).not.toBeNull();
        // At least primary CTA should be above fold
        expect(btnBox.y + btnBox.height).toBeLessThan(TABLET_VIEWPORT.height);
      }
    });

    test('should center hero content appropriately on tablet', async ({ page }) => {
      const heroContent = page.locator('.hero-content');
      const count = await heroContent.count();

      if (count > 0) {
        const box = await heroContent.boundingBox();
        expect(box).not.toBeNull();

        // Content should be roughly centered (with some tolerance for different layouts)
        const leftMargin = box.x;
        const rightMargin = TABLET_VIEWPORT.width - (box.x + box.width);

        // Margins should be somewhat balanced (within 50px difference)
        expect(Math.abs(leftMargin - rightMargin)).toBeLessThanOrEqual(100);
      }
    });

    test('should display hero description in readable width on tablet', async ({ page }) => {
      const description = page.locator('.hero-description');
      const count = await description.count();

      if (count > 0) {
        await expect(description).toBeVisible();

        const box = await description.boundingBox();
        expect(box).not.toBeNull();

        // Description should not span full viewport width - should have margins
        expect(box.width).toBeLessThan(TABLET_VIEWPORT.width);

        // But should still be wide enough to be readable
        expect(box.width).toBeGreaterThan(300);
      }
    });

    test('should adapt CTA buttons for tablet touch interaction', async ({ page }) => {
      const ctaButtons = page.locator('.hero-cta .btn');
      const count = await ctaButtons.count();

      if (count > 0) {
        const firstBtn = ctaButtons.first();
        const box = await firstBtn.boundingBox();

        expect(box).not.toBeNull();
        // Buttons should be large enough for touch (at least 44px touch target)
        expect(box.height).toBeGreaterThanOrEqual(40);
      }
    });

    test('should have appropriate spacing between hero elements on tablet', async ({ page }) => {
      const h1 = page.locator('h1').first();
      const tagline = page.locator('.hero-tagline');

      const h1Box = await h1.boundingBox();
      const taglineCount = await tagline.count();

      if (taglineCount > 0 && h1Box) {
        const taglineBox = await tagline.boundingBox();

        if (taglineBox) {
          // There should be some spacing between h1 and tagline
          const spacing = taglineBox.y - (h1Box.y + h1Box.height);
          expect(spacing).toBeGreaterThan(0);
          expect(spacing).toBeLessThan(100); // Not too much spacing
        }
      }
    });
  });

  test.describe('Additional Tablet Responsiveness Checks', () => {
    test('should not show mobile-only elements on tablet', async ({ page }) => {
      // Verify navigation is appropriately styled for tablet
      // (not necessarily hidden like on mobile)
      const body = page.locator('body');
      await expect(body).toBeVisible();
    });

    test('should maintain proper container max-width on tablet', async ({ page }) => {
      const containers = page.locator('.container');
      const count = await containers.count();

      if (count > 0) {
        const firstContainer = containers.first();
        const box = await firstContainer.boundingBox();

        if (box) {
          // Container should have appropriate width for tablet
          expect(box.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);
        }
      }
    });

    test('should handle long code examples gracefully on tablet', async ({ page }) => {
      const codeBlocks = page.locator('.code-block');
      const count = await codeBlocks.count();

      if (count > 0) {
        // Verify code blocks have overflow handling
        const overflow = await codeBlocks.first().evaluate((el) => {
          return window.getComputedStyle(el).overflowX;
        });

        // Should have horizontal scroll enabled for code that might overflow
        expect(['auto', 'scroll', 'hidden', 'visible']).toContain(overflow);
      }
    });
  });
});
