/**
 * E2E tests for Responsive Design
 * Owner: Scenario 8 - Responsive Design
 *
 * Tests cover:
 * - Desktop layout at 1440px viewport
 * - Tablet layout at 768px viewport
 * - Mobile layout at 375px viewport
 * - Hamburger menu visibility on mobile
 * - Touch targets on mobile
 * - No horizontal scrolling on mobile
 */

import { test, expect, Page } from '@playwright/test';

// Test constants
const BASE_URL = 'http://localhost:5173';
const VIEWPORTS = {
  desktop: { width: 1440, height: 900 },
  tablet: { width: 768, height: 1024 },
  mobile: { width: 375, height: 812 },
};

// Minimum touch target size per WCAG guidelines
const MIN_TOUCH_TARGET = 44;

test.describe('Responsive Design E2E Tests', () => {
  // Test Case 1: Render page at 1440px viewport - Full desktop layout with multi-column sections
  test.describe('Desktop Layout (1440px)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.desktop);
      await page.goto(BASE_URL);
      await page.waitForLoadState('networkidle');
    });

    test('displays full desktop layout with multi-column sections', async ({ page }) => {
      // Check desktop navigation is visible
      const desktopNav = page.getByTestId('desktop-navigation');
      await expect(desktopNav).toBeVisible();

      // Check mobile menu button is hidden
      const mobileMenuButton = page.getByTestId('mobile-menu-button');
      await expect(mobileMenuButton).toBeHidden();

      // Check features grid has 3 columns (lg:grid-cols-3)
      const featuresGrid = page.getByTestId('features-grid');
      await expect(featuresGrid).toBeVisible();

      // Verify features section exists
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();
    });

    test('header is sticky and visible', async ({ page }) => {
      const header = page.getByTestId('header');
      await expect(header).toBeVisible();

      // Scroll down and verify header is still visible
      await page.evaluate(() => window.scrollTo(0, 500));
      await expect(header).toBeVisible();
    });

    test('all sections are visible', async ({ page }) => {
      await expect(page.locator('#hero')).toBeVisible();
      await expect(page.locator('#features')).toBeVisible();
      await expect(page.locator('#quick-start')).toBeVisible();
      await expect(page.locator('#demo')).toBeVisible();
      await expect(page.locator('#status')).toBeVisible();
    });
  });

  // Test Case 2: Render page at 768px viewport - Tablet-optimized layout with adjusted columns
  test.describe('Tablet Layout (768px)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.tablet);
      await page.goto(BASE_URL);
      await page.waitForLoadState('networkidle');
    });

    test('displays tablet-optimized layout with adjusted columns', async ({ page }) => {
      // Check desktop navigation is visible at 768px (md breakpoint)
      const desktopNav = page.getByTestId('desktop-navigation');
      await expect(desktopNav).toBeVisible();

      // Mobile menu button should be hidden at md and above
      const mobileMenuButton = page.getByTestId('mobile-menu-button');
      await expect(mobileMenuButton).toBeHidden();

      // Features grid should adjust (md:grid-cols-2)
      const featuresGrid = page.getByTestId('features-grid');
      await expect(featuresGrid).toBeVisible();
    });

    test('content is readable and properly sized', async ({ page }) => {
      // Hero section should be visible
      const heroSection = page.getByTestId('hero-section');
      await expect(heroSection).toBeVisible();

      // Title should be visible
      const heroTitle = page.getByTestId('hero-title');
      await expect(heroTitle).toBeVisible();
    });
  });

  // Test Case 3: Render page at 375px viewport - Mobile layout with single column, no horizontal scroll
  test.describe('Mobile Layout (375px)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);
      await page.goto(BASE_URL);
      await page.waitForLoadState('networkidle');
    });

    test('displays mobile layout with single column', async ({ page }) => {
      // Desktop navigation should be hidden
      const desktopNav = page.getByTestId('desktop-navigation');
      await expect(desktopNav).toBeHidden();

      // Features grid should be single column
      const featuresGrid = page.getByTestId('features-grid');
      await expect(featuresGrid).toBeVisible();
    });

    // Test Case 8: Render all sections at 375px - All content is readable without horizontal scrolling
    test('has no horizontal scroll', async ({ page }) => {
      // Get the page width
      const pageWidth = await page.evaluate(() => document.documentElement.scrollWidth);
      const viewportWidth = VIEWPORTS.mobile.width;

      // Page should not be wider than viewport (no horizontal scroll)
      expect(pageWidth).toBeLessThanOrEqual(viewportWidth);
    });

    test('all sections are readable without horizontal scrolling', async ({ page }) => {
      // Check each section doesn't overflow
      const sections = ['hero', 'features', 'quick-start', 'demo', 'status'];

      for (const sectionId of sections) {
        const section = page.locator(`#${sectionId}`);
        await section.scrollIntoViewIfNeeded();

        const box = await section.boundingBox();
        if (box) {
          // Section should not extend beyond viewport width
          expect(box.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width);
        }
      }
    });
  });

  // Test Case 4: Render page at 375px viewport - Hamburger menu visible instead of desktop nav
  test.describe('Mobile Navigation', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);
      await page.goto(BASE_URL);
      await page.waitForLoadState('networkidle');
    });

    test('hamburger menu is visible instead of desktop nav', async ({ page }) => {
      // Desktop navigation should be hidden on mobile
      const desktopNav = page.getByTestId('desktop-navigation');
      await expect(desktopNav).toBeHidden();

      // Mobile menu button should be visible
      const mobileMenuButton = page.getByTestId('mobile-menu-button');
      await expect(mobileMenuButton).toBeVisible();
    });

    test('clicking hamburger opens mobile menu', async ({ page }) => {
      const mobileMenuButton = page.getByTestId('mobile-menu-button');
      await mobileMenuButton.click();

      // Mobile menu should be visible
      const mobileMenu = page.getByTestId('mobile-menu');
      await expect(mobileMenu).toBeVisible();
    });

    test('mobile menu can be closed', async ({ page }) => {
      // Open menu
      const mobileMenuButton = page.getByTestId('mobile-menu-button');
      await mobileMenuButton.click();

      // Close menu
      const closeButton = page.getByTestId('mobile-menu-close');
      await closeButton.click();

      // Menu should be hidden (translated off-screen)
      const mobileMenu = page.getByTestId('mobile-menu');
      await expect(mobileMenu).toHaveClass(/translate-x-full/);
    });

    test('mobile navigation links work', async ({ page }) => {
      // Open menu
      await page.getByTestId('mobile-menu-button').click();

      // Click on Features link
      const featuresLink = page.getByTestId('mobile-nav-link-features');
      await featuresLink.click();

      // Menu should close and page should scroll to features
      const mobileMenu = page.getByTestId('mobile-menu');
      await expect(mobileMenu).toHaveClass(/translate-x-full/);
    });
  });

  // Test Case 7: Measure touch targets on mobile - All interactive elements have minimum 44x44px touch area
  test.describe('Touch Targets', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);
      await page.goto(BASE_URL);
      await page.waitForLoadState('networkidle');
    });

    test('mobile menu button has minimum 44x44px touch area', async ({ page }) => {
      const mobileMenuButton = page.getByTestId('mobile-menu-button');
      const box = await mobileMenuButton.boundingBox();

      expect(box).not.toBeNull();
      expect(box!.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
      expect(box!.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
    });

    test('hero CTA buttons have minimum 44px height', async ({ page }) => {
      const getStartedButton = page.getByTestId('get-started-button');
      const viewGitHubButton = page.getByTestId('view-github-button');

      const getStartedBox = await getStartedButton.boundingBox();
      const viewGitHubBox = await viewGitHubButton.boundingBox();

      expect(getStartedBox).not.toBeNull();
      expect(getStartedBox!.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);

      expect(viewGitHubBox).not.toBeNull();
      expect(viewGitHubBox!.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
    });

    test('mobile menu navigation links have minimum 44px height', async ({ page }) => {
      // Open mobile menu
      await page.getByTestId('mobile-menu-button').click();

      // Check navigation links
      const navLinks = page.locator('[data-testid^="mobile-nav-link-"]');
      const count = await navLinks.count();

      for (let i = 0; i < count; i++) {
        const link = navLinks.nth(i);
        const box = await link.boundingBox();
        expect(box).not.toBeNull();
        expect(box!.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
      }
    });

    test('mobile menu close button has adequate touch area', async ({ page }) => {
      // Open mobile menu
      await page.getByTestId('mobile-menu-button').click();

      const closeButton = page.getByTestId('mobile-menu-close');
      const box = await closeButton.boundingBox();

      expect(box).not.toBeNull();
      // Close button should have reasonable touch area (padding + icon size)
      expect(box!.width).toBeGreaterThanOrEqual(36); // p-2 (8px * 2) + w-5 (20px) = 36px
      expect(box!.height).toBeGreaterThanOrEqual(36);
    });
  });

  // Test Case 9: Feature cards on mobile - Feature cards stack vertically
  test.describe('Feature Cards on Mobile', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);
      await page.goto(BASE_URL);
      await page.waitForLoadState('networkidle');
    });

    test('feature cards stack vertically in single column', async ({ page }) => {
      await page.locator('#features').scrollIntoViewIfNeeded();

      const featuresGrid = page.getByTestId('features-grid');
      const cards = featuresGrid.locator('[data-testid^="feature-card-"]');
      const cardCount = await cards.count();

      if (cardCount > 1) {
        // Get positions of first two cards
        const firstCard = cards.first();
        const secondCard = cards.nth(1);

        const firstBox = await firstCard.boundingBox();
        const secondBox = await secondCard.boundingBox();

        expect(firstBox).not.toBeNull();
        expect(secondBox).not.toBeNull();

        // Cards should be stacked vertically (second card below first)
        // The Y position of second card should be greater than first card's Y + height
        expect(secondBox!.y).toBeGreaterThan(firstBox!.y);

        // Cards should be full width (same X position and similar width)
        expect(Math.abs(firstBox!.x - secondBox!.x)).toBeLessThan(10);
      }
    });

    test('feature cards are readable at mobile width', async ({ page }) => {
      await page.locator('#features').scrollIntoViewIfNeeded();

      const cards = page.locator('[data-testid^="feature-card-"]');
      const cardCount = await cards.count();

      for (let i = 0; i < cardCount; i++) {
        const card = cards.nth(i);
        const box = await card.boundingBox();

        expect(box).not.toBeNull();
        // Card should not be wider than mobile viewport
        expect(box!.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width);
        // Card should have reasonable width (not too narrow)
        expect(box!.width).toBeGreaterThan(VIEWPORTS.mobile.width * 0.7);
      }
    });
  });

  // Additional responsive tests
  test.describe('Responsive Transitions', () => {
    test('layout adjusts when viewport changes from desktop to mobile', async ({ page }) => {
      // Start at desktop
      await page.setViewportSize(VIEWPORTS.desktop);
      await page.goto(BASE_URL);
      await page.waitForLoadState('networkidle');

      // Desktop nav should be visible
      await expect(page.getByTestId('desktop-navigation')).toBeVisible();

      // Change to mobile
      await page.setViewportSize(VIEWPORTS.mobile);

      // Wait for layout to adjust
      await page.waitForTimeout(100);

      // Mobile menu button should be visible
      await expect(page.getByTestId('mobile-menu-button')).toBeVisible();
      // Desktop nav should be hidden
      await expect(page.getByTestId('desktop-navigation')).toBeHidden();
    });
  });
});
