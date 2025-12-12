import { test, expect } from '@playwright/test';

/**
 * E2E Tests for REQ-5: Responsive Design - Desktop
 * Verifies homepage displays correctly on desktop devices
 */
test.describe('Responsive Design - Desktop', () => {
  test.describe('Test Case 1: 1920x1080 Viewport', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 1920, height: 1080 });
      await page.goto('/');
    });

    test('All content displays without horizontal scrolling at 1920x1080', async ({ page }) => {
      // Verify page has loaded
      await expect(page.locator('body')).toBeVisible();

      // Check document width does not exceed viewport (no horizontal scroll)
      const documentWidth = await page.evaluate(() => document.documentElement.scrollWidth);
      const viewportWidth = page.viewportSize()?.width ?? 1920;

      expect(documentWidth).toBeLessThanOrEqual(viewportWidth);

      // Verify horizontal scrollbar is not present
      const hasHorizontalScrollbar = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScrollbar).toBe(false);
    });

    test('Hero section spans full width at 1920x1080', async ({ page }) => {
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      const boundingBox = await heroSection.boundingBox();
      expect(boundingBox).not.toBeNull();

      if (boundingBox) {
        // Hero should span at least 95% of viewport width
        expect(boundingBox.width).toBeGreaterThanOrEqual(1920 * 0.95);
      }
    });

    test('Navigation is fully visible at 1920x1080', async ({ page }) => {
      const navigation = page.locator('[data-testid="main-navigation"]');
      await expect(navigation).toBeVisible();

      // Navigation links should be visible
      const navLinks = page.locator('[data-testid="navigation-links"]');
      await expect(navLinks).toBeVisible();
    });

    test('Navigation displays horizontally at 1920x1080', async ({ page }) => {
      const navLinks = page.locator('[data-testid="navigation-links"]');
      await expect(navLinks).toBeVisible();

      // Check that navigation items are displayed in a row (horizontal layout)
      const flexDirection = await navLinks.evaluate((el) => {
        return window.getComputedStyle(el).flexDirection;
      });
      expect(flexDirection).toBe('row');
    });

    test('All major page sections are visible at 1920x1080', async ({ page }) => {
      // Hero section
      await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();

      // Navigation
      await expect(page.locator('[data-testid="main-navigation"]')).toBeVisible();

      // Footer
      await expect(page.locator('[data-testid="footer"]')).toBeVisible();
    });
  });

  test.describe('Test Case 2: 1024x768 Viewport (Smaller Desktop)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 1024, height: 768 });
      await page.goto('/');
    });

    test('Layout adapts appropriately for smaller desktop at 1024x768', async ({ page }) => {
      // Verify page has loaded
      await expect(page.locator('body')).toBeVisible();

      // Check no horizontal scrolling
      const documentWidth = await page.evaluate(() => document.documentElement.scrollWidth);
      const viewportWidth = page.viewportSize()?.width ?? 1024;

      expect(documentWidth).toBeLessThanOrEqual(viewportWidth);
    });

    test('Hero section adapts to 1024x768 viewport', async ({ page }) => {
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      const boundingBox = await heroSection.boundingBox();
      expect(boundingBox).not.toBeNull();

      if (boundingBox) {
        // Hero should still span most of viewport width
        expect(boundingBox.width).toBeGreaterThanOrEqual(1024 * 0.95);
      }
    });

    test('Navigation remains visible at 1024x768', async ({ page }) => {
      const navigation = page.locator('[data-testid="main-navigation"]');
      await expect(navigation).toBeVisible();

      const navLinks = page.locator('[data-testid="navigation-links"]');
      await expect(navLinks).toBeVisible();
    });

    test('Content containers respect max-width at 1024x768', async ({ page }) => {
      // Check that content is properly constrained
      const navigation = page.locator('[data-testid="main-navigation"]');
      const navBoundingBox = await navigation.boundingBox();

      expect(navBoundingBox).not.toBeNull();
      if (navBoundingBox) {
        // Navigation should be within viewport
        expect(navBoundingBox.width).toBeLessThanOrEqual(1024);
      }
    });
  });

  test.describe('Test Case 3: Full Horizontal Navigation at Desktop', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 800 });
      await page.goto('/');
    });

    test('Full horizontal navigation menu is displayed (not hamburger)', async ({ page }) => {
      // Navigation should be visible
      const navigation = page.locator('[data-testid="main-navigation"]');
      await expect(navigation).toBeVisible();

      // Navigation links list should be visible
      const navLinks = page.locator('[data-testid="navigation-links"]');
      await expect(navLinks).toBeVisible();

      // Hamburger menu should NOT be visible at desktop viewport
      const hamburgerMenu = page.locator('[data-testid="hamburger-menu"]');
      await expect(hamburgerMenu).not.toBeVisible();
    });

    test('All navigation links are visible at desktop viewport', async ({ page }) => {
      const navLinks = page.locator('[data-testid="navigation-links"] a, [data-testid="navigation-links"] [data-testid="nav-link"]');

      // Get count of navigation links
      const count = await navLinks.count();
      expect(count).toBeGreaterThan(0);

      // All links should be visible
      for (let i = 0; i < count; i++) {
        await expect(navLinks.nth(i)).toBeVisible();
      }
    });

    test('Navigation links are in horizontal layout', async ({ page }) => {
      const navLinks = page.locator('[data-testid="navigation-links"]');

      // Check flex direction is row (horizontal)
      const flexDirection = await navLinks.evaluate((el) => {
        return window.getComputedStyle(el).flexDirection;
      });
      expect(flexDirection).toBe('row');

      // Verify navigation is not wrapped to multiple lines
      const navLinksBox = await navLinks.boundingBox();
      const firstLink = page.locator('[data-testid="navigation-links"] li').first();
      const lastLink = page.locator('[data-testid="navigation-links"] li').last();

      const firstLinkBox = await firstLink.boundingBox();
      const lastLinkBox = await lastLink.boundingBox();

      if (firstLinkBox && lastLinkBox) {
        // All links should be on roughly the same vertical position (horizontal layout)
        const verticalDifference = Math.abs(firstLinkBox.y - lastLinkBox.y);
        expect(verticalDifference).toBeLessThan(50); // Allow small variance for alignment
      }
    });

    test('Logo is visible and positioned correctly at desktop', async ({ page }) => {
      const logo = page.locator('[data-testid="navigation-logo"]');
      await expect(logo).toBeVisible();

      const boundingBox = await logo.boundingBox();
      expect(boundingBox).not.toBeNull();

      if (boundingBox) {
        // Logo should be near the left side of viewport
        expect(boundingBox.x).toBeLessThan(200);
      }
    });
  });

  test.describe('Desktop Layout Integrity', () => {
    test('Page maintains proper desktop layout at 1440x900', async ({ page }) => {
      await page.setViewportSize({ width: 1440, height: 900 });
      await page.goto('/');

      // Verify no horizontal overflow
      const hasHorizontalScrollbar = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScrollbar).toBe(false);

      // All main sections should be visible
      await expect(page.locator('[data-testid="main-navigation"]')).toBeVisible();
      await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();
      await expect(page.locator('[data-testid="footer"]')).toBeVisible();
    });

    test('Footer displays 4-column grid at desktop viewport', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 800 });
      await page.goto('/');

      const footerContent = page.locator('[data-testid="footer-content"]');
      await expect(footerContent).toBeVisible();

      // Check grid-template-columns for 4-column layout
      const gridColumns = await footerContent.evaluate((el) => {
        return window.getComputedStyle(el).gridTemplateColumns;
      });

      // Should have 4 columns (4 values separated by spaces)
      const columnCount = gridColumns.split(' ').filter(col => col.length > 0).length;
      expect(columnCount).toBe(4);
    });
  });
});
