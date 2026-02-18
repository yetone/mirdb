/**
 * Navigation E2E Tests
 * Owner: Scenario 2 - Navigation & Menu
 *
 * Test cases:
 * - Click navigation anchor link - page scrolls smoothly
 * - Click hamburger menu icon on mobile viewport - menu expands
 * - Press Escape key while mobile menu is open - menu closes
 */

const { test, expect } = require('@playwright/test');

test.describe('Navigation - E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for the page to be fully loaded
    await page.waitForLoadState('networkidle');
  });

  test.describe('Test Case 5: Navigation Anchor Links', () => {
    test('Click navigation anchor link scrolls to corresponding section', async ({ page }) => {
      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);

      // Click on Features link
      await page.click('a.nav-link[href="#features"]');

      // Wait for scroll animation
      await page.waitForTimeout(500);

      // Get new scroll position
      const newScrollY = await page.evaluate(() => window.scrollY);

      // Verify that page scrolled
      expect(newScrollY).toBeGreaterThan(initialScrollY);

      // Verify that features section is in viewport
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();
    });

    test('All navigation links point to existing sections', async ({ page }) => {
      const navLinks = page.locator('.nav-links .nav-link');
      const count = await navLinks.count();

      for (let i = 0; i < count; i++) {
        const href = await navLinks.nth(i).getAttribute('href');
        const sectionId = href.replace('#', '');
        const section = page.locator(`#${sectionId}`);
        await expect(section).toBeVisible();
      }
    });
  });

  test.describe('Test Case 6 & 7: Mobile Menu', () => {
    test.beforeEach(async ({ page }) => {
      // Set mobile viewport
      await page.setViewportSize({ width: 375, height: 667 });
    });

    test('Hamburger menu icon is visible on mobile viewport', async ({ page }) => {
      const hamburgerButton = page.locator('#nav-mobile-toggle');
      await expect(hamburgerButton).toBeVisible();
    });

    test('Desktop links are hidden on mobile viewport', async ({ page }) => {
      const desktopLinks = page.locator('.nav-links');
      await expect(desktopLinks).not.toBeVisible();
    });

    test('Click hamburger menu icon expands mobile menu with links visible', async ({ page }) => {
      const hamburgerButton = page.locator('#nav-mobile-toggle');
      const mobileMenu = page.locator('#nav-mobile-menu');
      const mobileLinks = page.locator('.nav-mobile-link').first();

      // Verify menu is initially hidden
      await expect(mobileMenu).not.toBeVisible();

      // Click hamburger button
      await hamburgerButton.click();

      // Wait for menu animation
      await page.waitForTimeout(300);

      // Verify menu is now visible
      await expect(mobileMenu).toBeVisible();
      await expect(mobileLinks).toBeVisible();

      // Verify aria-expanded is true
      await expect(hamburgerButton).toHaveAttribute('aria-expanded', 'true');
    });

    test('Clicking mobile menu link closes menu', async ({ page }) => {
      const hamburgerButton = page.locator('#nav-mobile-toggle');
      const mobileMenu = page.locator('#nav-mobile-menu');
      const mobileLink = page.locator('.nav-mobile-link').first();

      // Open menu
      await hamburgerButton.click();
      await page.waitForTimeout(300);
      await expect(mobileMenu).toBeVisible();

      // Click a link
      await mobileLink.click();
      await page.waitForTimeout(300);

      // Verify menu closed
      await expect(hamburgerButton).toHaveAttribute('aria-expanded', 'false');
    });
  });

  test.describe('Test Case 8: Escape Key', () => {
    test.beforeEach(async ({ page }) => {
      // Set mobile viewport
      await page.setViewportSize({ width: 375, height: 667 });
    });

    test('Press Escape key while mobile menu is open closes the menu', async ({ page }) => {
      const hamburgerButton = page.locator('#nav-mobile-toggle');
      const mobileMenu = page.locator('#nav-mobile-menu');

      // Open mobile menu
      await hamburgerButton.click();
      await page.waitForTimeout(300);

      // Verify menu is open
      await expect(mobileMenu).toBeVisible();
      await expect(hamburgerButton).toHaveAttribute('aria-expanded', 'true');

      // Press Escape key
      await page.keyboard.press('Escape');
      await page.waitForTimeout(300);

      // Verify menu is closed
      await expect(hamburgerButton).toHaveAttribute('aria-expanded', 'false');
    });

    test('Focus returns to hamburger button after Escape', async ({ page }) => {
      const hamburgerButton = page.locator('#nav-mobile-toggle');

      // Open mobile menu
      await hamburgerButton.click();
      await page.waitForTimeout(300);

      // Press Escape key
      await page.keyboard.press('Escape');
      await page.waitForTimeout(300);

      // Verify focus is on hamburger button
      const focusedElement = page.locator(':focus');
      await expect(focusedElement).toHaveId('nav-mobile-toggle');
    });
  });

  test.describe('Sticky Navigation', () => {
    test('Navigation remains visible when scrolling', async ({ page }) => {
      const header = page.locator('.header');

      // Verify header is visible initially
      await expect(header).toBeVisible();

      // Scroll down
      await page.evaluate(() => window.scrollTo(0, 500));
      await page.waitForTimeout(300);

      // Verify header is still visible
      await expect(header).toBeVisible();

      // Verify header is at the top of viewport
      const headerBox = await header.boundingBox();
      expect(headerBox.y).toBeLessThanOrEqual(10);
    });
  });

  test.describe('Desktop Navigation', () => {
    test.beforeEach(async ({ page }) => {
      // Set desktop viewport
      await page.setViewportSize({ width: 1280, height: 800 });
    });

    test('Desktop navigation links are visible', async ({ page }) => {
      const desktopLinks = page.locator('.nav-links');
      await expect(desktopLinks).toBeVisible();
    });

    test('Hamburger menu is hidden on desktop', async ({ page }) => {
      const hamburgerButton = page.locator('#nav-mobile-toggle');
      await expect(hamburgerButton).not.toBeVisible();
    });

    test('CTA button is visible on desktop', async ({ page }) => {
      const ctaButton = page.locator('.nav-cta');
      await expect(ctaButton).toBeVisible();
    });
  });

  test.describe('Accessibility', () => {
    test('Navigation is accessible via keyboard', async ({ page }) => {
      // Tab to first nav link
      await page.keyboard.press('Tab'); // Skip link
      await page.keyboard.press('Tab'); // Logo
      await page.keyboard.press('Tab'); // First nav link

      const focusedElement = page.locator(':focus');
      await expect(focusedElement).toHaveClass(/nav-link/);
    });

    test('Skip link is functional', async ({ page }) => {
      const skipLink = page.locator('.skip-link');

      // Focus on skip link
      await skipLink.focus();

      // Click skip link
      await skipLink.click();

      // Verify that the main content is in focus/view
      const mainContent = page.locator('#main-content');
      await expect(mainContent).toBeInViewport();
    });
  });
});
