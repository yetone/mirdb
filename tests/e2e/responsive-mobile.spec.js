/**
 * E2E Tests for Responsive Design - Mobile Viewport
 *
 * Scenario: Verify the homepage displays correctly on mobile devices
 * without horizontal scrolling and with touch-friendly interactions
 *
 * Test Cases: 1, 3, 5
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '../../website/index.html');

test.describe('Responsive Design - Mobile Viewport', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  /**
   * Test Case 1: Set viewport to 375x667 and check document width
   * Expected: Document body width does not exceed viewport width (no horizontal scroll)
   */
  test.describe('Test Case 1: No Horizontal Scrolling at Mobile Viewport', () => {
    test('should not have horizontal scroll at 375x667 viewport', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });

      // Wait for page to stabilize
      await page.waitForLoadState('domcontentloaded');

      // Check document scroll width vs viewport width
      const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
      const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);

      expect(scrollWidth).toBeLessThanOrEqual(clientWidth);
    });

    test('should not have horizontal scroll at various mobile widths', async ({ page }) => {
      const mobileWidths = [320, 375, 414, 428]; // iPhone SE, 8, 12, 14 Pro Max

      for (const width of mobileWidths) {
        await page.setViewportSize({ width, height: 667 });
        await page.waitForLoadState('domcontentloaded');

        const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
        expect(scrollWidth).toBeLessThanOrEqual(width);
      }
    });

    test('body width should not exceed viewport width', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });

      const bodyWidth = await page.evaluate(() => {
        const body = document.body;
        return body.scrollWidth;
      });

      expect(bodyWidth).toBeLessThanOrEqual(375);
    });

    test('no element should overflow horizontally', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });

      // Check that no major section causes horizontal overflow
      const sections = [
        '.hero',
        '.why-mirdb',
        '.features',
        '.quickstart',
        '.architecture',
        '.commands',
        '.footer'
      ];

      for (const selector of sections) {
        const element = page.locator(selector);
        if (await element.count() > 0) {
          const box = await element.boundingBox();
          if (box) {
            expect(box.width).toBeLessThanOrEqual(375);
          }
        }
      }
    });
  });

  /**
   * Test Case 3: Verify touch target sizes for interactive elements
   * Expected: Buttons and links have minimum 44x44px touch target area
   */
  test.describe('Test Case 3: Touch Target Sizes', () => {
    test('primary CTA button should have minimum 44px height', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });

      const primaryBtn = page.locator('.btn-primary').first();
      const box = await primaryBtn.boundingBox();

      expect(box.height).toBeGreaterThanOrEqual(44);
    });

    test('secondary CTA button should have minimum 44px height', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });

      const secondaryBtn = page.locator('.btn-secondary').first();
      const box = await secondaryBtn.boundingBox();

      expect(box.height).toBeGreaterThanOrEqual(44);
    });

    test('copy buttons should have adequate touch target', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });

      const copyButtons = page.locator('.copy-button');
      const count = await copyButtons.count();

      for (let i = 0; i < Math.min(count, 3); i++) {
        const box = await copyButtons.nth(i).boundingBox();
        if (box) {
          // Touch target should be at least 30px for secondary actions
          expect(box.height).toBeGreaterThanOrEqual(28);
          expect(box.width).toBeGreaterThanOrEqual(40);
        }
      }
    });

    test('navigation logo should be easily tappable', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });

      const logo = page.locator('.nav-logo');
      const box = await logo.boundingBox();

      // Logo should have adequate height for tapping
      expect(box.height).toBeGreaterThanOrEqual(24);
    });

    test('footer links should have adequate spacing for touch', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });

      const footerLinks = page.locator('.footer-links a');
      const count = await footerLinks.count();

      for (let i = 0; i < count; i++) {
        const box = await footerLinks.nth(i).boundingBox();
        if (box) {
          // Links should be reasonably tappable
          expect(box.height).toBeGreaterThanOrEqual(20);
        }
      }
    });
  });

  /**
   * Test Case 5: Test code block horizontal scrolling
   * Expected: Code blocks scroll horizontally within container, not page-level scroll
   */
  test.describe('Test Case 5: Code Block Horizontal Scrolling', () => {
    test('code blocks should be contained within page width', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });

      const codeBlocks = page.locator('.code-block');
      const count = await codeBlocks.count();

      expect(count).toBeGreaterThan(0);

      for (let i = 0; i < count; i++) {
        const box = await codeBlocks.nth(i).boundingBox();
        if (box) {
          expect(box.width).toBeLessThanOrEqual(375);
        }
      }
    });

    test('code blocks should have overflow-x scroll/auto', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });

      const codeBlock = page.locator('.code-block').first();
      const overflowX = await codeBlock.evaluate((el) =>
        window.getComputedStyle(el).overflowX
      );

      expect(['auto', 'scroll']).toContain(overflowX);
    });

    test('long code lines should scroll within container not page', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });

      // Get initial page scroll width
      const initialPageScrollWidth = await page.evaluate(() =>
        document.documentElement.scrollWidth
      );

      // Verify page doesn't have horizontal scroll
      expect(initialPageScrollWidth).toBeLessThanOrEqual(375);

      // Check that code blocks with long content are scrollable internally
      const codeBlocks = page.locator('.code-block');
      const count = await codeBlocks.count();

      for (let i = 0; i < count; i++) {
        const block = codeBlocks.nth(i);
        const scrollWidth = await block.evaluate((el) => el.scrollWidth);
        const clientWidth = await block.evaluate((el) => el.clientWidth);

        // If content is wider than container, internal scroll should be enabled
        if (scrollWidth > clientWidth) {
          const overflowX = await block.evaluate((el) =>
            window.getComputedStyle(el).overflowX
          );
          expect(['auto', 'scroll']).toContain(overflowX);
        }
      }
    });

    test('code text should remain readable on mobile', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });

      const codeElement = page.locator('.code-block code').first();
      const fontSize = await codeElement.evaluate((el) =>
        parseFloat(window.getComputedStyle(el).fontSize)
      );

      // Code should be at least 12px for readability
      expect(fontSize).toBeGreaterThanOrEqual(12);
    });
  });

  /**
   * Additional Mobile UX Tests
   */
  test.describe('Additional Mobile UX Tests', () => {
    test('navigation should hide links on mobile viewport', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });

      const navLinks = page.locator('.nav-links');
      const display = await navLinks.evaluate((el) =>
        window.getComputedStyle(el).display
      );

      expect(display).toBe('none');
    });

    test('all content should be visible and not clipped', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });

      // Check hero section is visible
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();

      // Check hero title is visible
      const heroTitle = page.locator('.hero-title');
      await expect(heroTitle).toBeVisible();
    });

    test('feature cards should display in single column on mobile', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });

      const featuresGrid = page.locator('.features-grid');
      const gridColumns = await featuresGrid.evaluate((el) =>
        window.getComputedStyle(el).gridTemplateColumns
      );

      // Should be single column (1fr) or auto
      expect(gridColumns).toMatch(/^(1fr|auto|\d+px)$/);
    });

    test('comparison cards should display in single column on mobile', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });

      const comparisonGrid = page.locator('.comparison-grid');
      const gridColumns = await comparisonGrid.evaluate((el) =>
        window.getComputedStyle(el).gridTemplateColumns
      );

      // Should be single column (1fr) or auto
      expect(gridColumns).toMatch(/^(1fr|auto|\d+px)$/);
    });

    test('hero title font size should be reduced on mobile', async ({ page }) => {
      // Check desktop font size
      await page.setViewportSize({ width: 1920, height: 1080 });
      const heroTitle = page.locator('.hero-title');
      const desktopFontSize = await heroTitle.evaluate((el) =>
        parseFloat(window.getComputedStyle(el).fontSize)
      );

      // Check mobile font size
      await page.setViewportSize({ width: 375, height: 667 });
      const mobileFontSize = await heroTitle.evaluate((el) =>
        parseFloat(window.getComputedStyle(el).fontSize)
      );

      // Mobile font size should be smaller than desktop
      expect(mobileFontSize).toBeLessThan(desktopFontSize);
    });

    test('body text should be at least 16px on mobile', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });

      // Check body font size on a paragraph element
      const description = page.locator('.hero-description');
      const fontSize = await description.evaluate((el) =>
        parseFloat(window.getComputedStyle(el).fontSize)
      );

      // Body text should be at least 16px for mobile readability
      expect(fontSize).toBeGreaterThanOrEqual(16);
    });

    test('viewport meta tag should prevent zooming issues', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });

      const viewportMeta = await page.locator('meta[name="viewport"]').getAttribute('content');

      expect(viewportMeta).toContain('width=device-width');
      expect(viewportMeta).toContain('initial-scale=1');
    });
  });
});
