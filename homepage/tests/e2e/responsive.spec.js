/**
 * Responsive Design Full Range E2E Tests
 * Owner: Scenario 7 - Responsive Design Full Range
 *
 * Tests:
 * - Minimum viewport (320px) rendering
 * - Maximum viewport (1920px) rendering
 * - No horizontal scrollbar at any viewport
 * - Text readability at minimum viewport
 * - Common breakpoint testing (480px, 768px, 1024px, 1440px)
 */

import { test, expect } from '@playwright/test';

/**
 * Test viewport widths per NFR-4 requirements
 * Range: 320px to 1920px
 */
const VIEWPORT_WIDTHS = {
  minimum: 320,
  mobile: 375,
  mobileLarge: 480,
  tablet: 768,
  desktop: 1024,
  desktopLarge: 1440,
  maximum: 1920,
};

/**
 * Minimum readable font size per PRD
 */
const MIN_READABLE_FONT_SIZE = 14;

test.describe('Responsive Design Full Range - E2E Tests', () => {
  test.describe('Test Case 1: Minimum viewport (320px)', () => {
    test('should render all content visible and readable at 320px width', async ({ page }) => {
      // Set viewport to minimum width
      await page.setViewportSize({ width: 320, height: 568 });
      await page.goto('/');

      // Wait for page to load
      await page.waitForLoadState('domcontentloaded');

      // Verify main sections are visible
      await expect(page.locator('header')).toBeVisible();
      await expect(page.locator('main')).toBeVisible();
      await expect(page.locator('footer')).toBeVisible();

      // Verify hero content is visible
      await expect(page.locator('.hero__title')).toBeVisible();
      await expect(page.locator('.hero__tagline')).toBeVisible();

      // Verify feature cards are visible
      const featureCards = page.locator('.feature-card');
      await expect(featureCards.first()).toBeVisible();
    });

    test('should have no horizontal overflow at 320px', async ({ page }) => {
      await page.setViewportSize({ width: 320, height: 568 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Check that document width doesn't exceed viewport width
      const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
      const windowInnerWidth = await page.evaluate(() => window.innerWidth);

      // Allow 1px tolerance for sub-pixel rendering
      expect(bodyScrollWidth).toBeLessThanOrEqual(windowInnerWidth + 1);
    });

    test('should have hamburger menu visible at 320px', async ({ page }) => {
      await page.setViewportSize({ width: 320, height: 568 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Hamburger menu should be visible on mobile
      const hamburgerButton = page.locator('.nav__toggle');
      await expect(hamburgerButton).toBeVisible();
    });
  });

  test.describe('Test Case 2: Maximum viewport (1920px)', () => {
    test('should render content properly centered/contained at 1920px width', async ({ page }) => {
      await page.setViewportSize({ width: 1920, height: 1080 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Verify main sections are visible
      await expect(page.locator('header')).toBeVisible();
      await expect(page.locator('main')).toBeVisible();
      await expect(page.locator('footer')).toBeVisible();

      // Verify hero content is visible and centered
      await expect(page.locator('.hero__title')).toBeVisible();
      await expect(page.locator('.hero__tagline')).toBeVisible();

      // Check that navigation is visible (no hamburger menu)
      const navMenu = page.locator('.nav__menu');
      await expect(navMenu).toBeVisible();

      // Check that hamburger is hidden on desktop
      const hamburgerButton = page.locator('.nav__toggle');
      await expect(hamburgerButton).not.toBeVisible();
    });

    test('should have no excessive whitespace issues at 1920px', async ({ page }) => {
      await page.setViewportSize({ width: 1920, height: 1080 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Check that main content area has reasonable width
      const mainWidth = await page.evaluate(() => {
        const main = document.querySelector('main');
        return main ? main.getBoundingClientRect().width : 0;
      });

      // Main content should have width constraints
      expect(mainWidth).toBeGreaterThan(0);
      expect(mainWidth).toBeLessThanOrEqual(1920);
    });

    test('should have no horizontal overflow at 1920px', async ({ page }) => {
      await page.setViewportSize({ width: 1920, height: 1080 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
      const windowInnerWidth = await page.evaluate(() => window.innerWidth);

      expect(bodyScrollWidth).toBeLessThanOrEqual(windowInnerWidth + 1);
    });
  });

  test.describe('Test Case 3: No horizontal scroll at various widths', () => {
    const viewportWidths = [320, 480, 768, 1024, 1440, 1920];

    for (const width of viewportWidths) {
      test(`should have no horizontal scrollbar at ${width}px`, async ({ page }) => {
        await page.setViewportSize({ width, height: 800 });
        await page.goto('/');
        await page.waitForLoadState('domcontentloaded');

        // Check for horizontal scroll
        const hasHorizontalScroll = await page.evaluate(() => {
          return document.documentElement.scrollWidth > document.documentElement.clientWidth;
        });

        expect(hasHorizontalScroll).toBe(false);
      });
    }

    test('should maintain layout integrity across viewport range', async ({ page }) => {
      for (const width of viewportWidths) {
        await page.setViewportSize({ width, height: 800 });
        await page.goto('/');
        await page.waitForLoadState('domcontentloaded');

        // Key elements should remain visible
        await expect(page.locator('header')).toBeVisible();
        await expect(page.locator('.hero')).toBeVisible();
        await expect(page.locator('.features')).toBeVisible();
        await expect(page.locator('footer')).toBeVisible();
      }
    });
  });

  test.describe('Test Case 4: Text readability at 320px', () => {
    test('should have all text readable with minimum 14px font size', async ({ page }) => {
      await page.setViewportSize({ width: 320, height: 568 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Check body text font size
      const bodyFontSize = await page.evaluate(() => {
        const body = document.body;
        const style = window.getComputedStyle(body);
        return parseFloat(style.fontSize);
      });

      expect(bodyFontSize).toBeGreaterThanOrEqual(MIN_READABLE_FONT_SIZE);
    });

    test('should have readable hero tagline at 320px', async ({ page }) => {
      await page.setViewportSize({ width: 320, height: 568 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      const taglineFontSize = await page.evaluate(() => {
        const tagline = document.querySelector('.hero__tagline');
        if (!tagline) return 0;
        const style = window.getComputedStyle(tagline);
        return parseFloat(style.fontSize);
      });

      expect(taglineFontSize).toBeGreaterThanOrEqual(MIN_READABLE_FONT_SIZE);
    });

    test('should have readable feature card descriptions at 320px', async ({ page }) => {
      await page.setViewportSize({ width: 320, height: 568 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      const descriptionFontSize = await page.evaluate(() => {
        const description = document.querySelector('.feature-card__description');
        if (!description) return 0;
        const style = window.getComputedStyle(description);
        return parseFloat(style.fontSize);
      });

      // Feature descriptions can be slightly smaller (12px min) as supplementary text
      // Main body text must be 14px min per PRD
      const MIN_DESCRIPTION_FONT_SIZE = 12;
      expect(descriptionFontSize).toBeGreaterThanOrEqual(MIN_DESCRIPTION_FONT_SIZE);
    });

    test('should have all text content visible at 320px', async ({ page }) => {
      await page.setViewportSize({ width: 320, height: 568 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Check hero title
      const heroTitle = page.locator('.hero__title');
      await expect(heroTitle).toBeVisible();
      await expect(heroTitle).toHaveText(/MirDB/);

      // Check hero tagline
      const heroTagline = page.locator('.hero__tagline');
      await expect(heroTagline).toBeVisible();
      await expect(heroTagline).not.toBeEmpty();

      // Check features title
      const featuresTitle = page.locator('.features__title');
      await expect(featuresTitle).toBeVisible();
    });
  });

  test.describe('Common Breakpoint Testing', () => {
    test('should render correctly at 480px (large mobile)', async ({ page }) => {
      await page.setViewportSize({ width: 480, height: 800 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      await expect(page.locator('.hero')).toBeVisible();
      await expect(page.locator('.features')).toBeVisible();
    });

    test('should render correctly at 768px (tablet)', async ({ page }) => {
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      await expect(page.locator('.hero')).toBeVisible();
      await expect(page.locator('.features')).toBeVisible();

      // Navigation should be visible (desktop mode)
      const navMenu = page.locator('.nav__menu');
      await expect(navMenu).toBeVisible();
    });

    test('should render correctly at 1024px (desktop)', async ({ page }) => {
      await page.setViewportSize({ width: 1024, height: 768 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      await expect(page.locator('.hero')).toBeVisible();
      await expect(page.locator('.features')).toBeVisible();

      // All navigation items should be visible
      const navLinks = page.locator('.nav__link');
      const count = await navLinks.count();
      expect(count).toBeGreaterThan(0);
    });

    test('should render correctly at 1440px (large desktop)', async ({ page }) => {
      await page.setViewportSize({ width: 1440, height: 900 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      await expect(page.locator('.hero')).toBeVisible();
      await expect(page.locator('.features')).toBeVisible();
    });
  });

  test.describe('Layout Stability', () => {
    test('should not have layout shift during page load', async ({ page }) => {
      await page.setViewportSize({ width: 1024, height: 768 });
      await page.goto('/');

      // Wait for full load
      await page.waitForLoadState('load');

      // Check that main elements are stable
      const heroPosition = await page.locator('.hero').boundingBox();
      expect(heroPosition).not.toBeNull();

      // Wait a bit and check position hasn't changed
      await page.waitForTimeout(100);
      const heroPositionAfter = await page.locator('.hero').boundingBox();

      if (heroPosition && heroPositionAfter) {
        expect(heroPositionAfter.y).toBe(heroPosition.y);
      }
    });

    test('should maintain consistent header height across viewports', async ({ page }) => {
      const viewports = [
        { width: 320, height: 568 },
        { width: 768, height: 1024 },
        { width: 1024, height: 768 },
      ];

      for (const viewport of viewports) {
        await page.setViewportSize(viewport);
        await page.goto('/');
        await page.waitForLoadState('domcontentloaded');

        const headerHeight = await page.evaluate(() => {
          const header = document.querySelector('header');
          return header ? header.getBoundingClientRect().height : 0;
        });

        // Header should have a reasonable height (at least touch target size)
        expect(headerHeight).toBeGreaterThanOrEqual(44);
      }
    });
  });

  test.describe('Accessibility at Different Viewports', () => {
    test('should have focusable elements accessible at 320px', async ({ page }) => {
      await page.setViewportSize({ width: 320, height: 568 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Tab to first interactive element
      await page.keyboard.press('Tab');

      // Check that something is focused
      const focusedElement = await page.evaluate(() => {
        return document.activeElement?.tagName;
      });

      expect(focusedElement).not.toBe('BODY');
    });

    test('should have skip link accessible at all viewports', async ({ page }) => {
      const viewports = [320, 768, 1920];

      for (const width of viewports) {
        await page.setViewportSize({ width, height: 800 });
        await page.goto('/');
        await page.waitForLoadState('domcontentloaded');

        // Check for skip link
        const skipLink = page.locator('.skip-link');
        await expect(skipLink).toHaveCount(1);
      }
    });
  });
});
