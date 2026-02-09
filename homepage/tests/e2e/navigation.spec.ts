/**
 * E2E tests for Navigation and Header.
 * Owner: Scenario 7 - Navigation and Header
 *
 * Test cases:
 * 4. Click navigation link - Page smoothly scrolls to the corresponding section
 * 5. View header on mobile viewport - Navigation is hidden behind hamburger menu
 * 6. Click hamburger menu on mobile - Navigation menu expands and shows all links
 * 7. Scroll down the page - Header remains visible (sticky positioning)
 */

import { test, expect } from '@playwright/test';

test.describe('Navigation and Header E2E', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test.describe('Test Case 4: Click navigation link scrolls to section', () => {
    test('clicking Features link scrolls to features section', async ({ page }) => {
      await page.setViewportSize({ width: 1200, height: 800 });

      // Wait for header to be visible
      await page.waitForSelector('.header');

      // Get the initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);

      // Click on Features link
      await page.click('a[href="#features"]');

      // Wait for scroll to complete
      await page.waitForTimeout(500);

      // Verify the page scrolled down (features section is below hero)
      const featuresSection = await page.locator('#features').boundingBox();
      expect(featuresSection).not.toBeNull();

      // The features section should now be near the top of the viewport
      const scrollY = await page.evaluate(() => window.scrollY);
      expect(scrollY).toBeGreaterThan(initialScrollY);
    });

    test('clicking Quick Start link scrolls to quickstart section', async ({ page }) => {
      await page.setViewportSize({ width: 1200, height: 800 });

      await page.waitForSelector('.header');

      await page.click('a[href="#quickstart"]');

      await page.waitForTimeout(500);

      const quickstartSection = await page.locator('#quickstart');
      await expect(quickstartSection).toBeInViewport();
    });

    test('clicking Protocol link scrolls to protocol section', async ({ page }) => {
      await page.setViewportSize({ width: 1200, height: 800 });

      await page.waitForSelector('.header');

      await page.click('a[href="#protocol"]');

      await page.waitForTimeout(500);

      const protocolSection = await page.locator('#protocol');
      await expect(protocolSection).toBeInViewport();
    });
  });

  test.describe('Test Case 5: Mobile viewport shows hamburger menu', () => {
    test('navigation is hidden behind hamburger menu on mobile', async ({ page }) => {
      // Set mobile viewport
      await page.setViewportSize({ width: 375, height: 667 });

      await page.waitForSelector('.header');

      // Hamburger button should be visible
      const hamburger = page.locator('.header__hamburger');
      await expect(hamburger).toBeVisible();

      // Navigation list should be hidden (collapsed state)
      const nav = page.locator('.header__nav');

      // Check that nav does not have the open class
      await expect(nav).not.toHaveClass(/header__nav--open/);

      // The nav should have max-height: 0 when closed (hidden)
      const navStyle = await nav.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          maxHeight: computed.maxHeight,
          overflow: computed.overflow,
        };
      });

      expect(navStyle.maxHeight).toBe('0px');
    });

    test('hamburger button has correct aria attributes', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });

      await page.waitForSelector('.header__hamburger');

      const hamburger = page.locator('.header__hamburger');
      await expect(hamburger).toHaveAttribute('aria-label', 'Toggle navigation menu');
      await expect(hamburger).toHaveAttribute('aria-expanded', 'false');
      await expect(hamburger).toHaveAttribute('aria-controls', 'nav-list');
    });
  });

  test.describe('Test Case 6: Click hamburger menu expands navigation', () => {
    test('clicking hamburger menu expands navigation on mobile', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });

      await page.waitForSelector('.header__hamburger');

      const hamburger = page.locator('.header__hamburger');
      const nav = page.locator('.header__nav');

      // Initially closed
      await expect(nav).not.toHaveClass(/header__nav--open/);
      await expect(hamburger).toHaveAttribute('aria-expanded', 'false');

      // Click hamburger to open
      await hamburger.click();

      // Now nav should be open
      await expect(nav).toHaveClass(/header__nav--open/);
      await expect(hamburger).toHaveAttribute('aria-expanded', 'true');
    });

    test('expanded navigation shows all links', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });

      await page.waitForSelector('.header__hamburger');

      // Open the menu
      await page.click('.header__hamburger');

      // Wait for menu to open
      await page.waitForTimeout(300);

      // All nav links should be visible
      const expectedLinks = ['Features', 'Quick Start', 'Protocol', 'Configuration', 'Architecture'];

      for (const linkText of expectedLinks) {
        const link = page.locator('.header__nav-link', { hasText: linkText });
        await expect(link).toBeVisible();
      }
    });

    test('clicking nav link closes mobile menu', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });

      await page.waitForSelector('.header__hamburger');

      const hamburger = page.locator('.header__hamburger');
      const nav = page.locator('.header__nav');

      // Open the menu
      await hamburger.click();
      await expect(nav).toHaveClass(/header__nav--open/);

      // Click a nav link
      await page.click('a[href="#features"]');

      // Menu should be closed
      await expect(nav).not.toHaveClass(/header__nav--open/);
      await expect(hamburger).toHaveAttribute('aria-expanded', 'false');
    });

    test('hamburger icon transforms to X when menu is open', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });

      await page.waitForSelector('.header__hamburger');

      const hamburger = page.locator('.header__hamburger');

      // Initially not active
      await expect(hamburger).not.toHaveClass(/header__hamburger--active/);

      // Click to open
      await hamburger.click();

      // Should have active class for X transformation
      await expect(hamburger).toHaveClass(/header__hamburger--active/);
    });
  });

  test.describe('Test Case 7: Header remains visible when scrolling (sticky)', () => {
    test('header has sticky positioning', async ({ page }) => {
      await page.setViewportSize({ width: 1200, height: 800 });

      await page.waitForSelector('.header');

      const header = page.locator('.header');

      // Check CSS position
      const position = await header.evaluate((el) => {
        return window.getComputedStyle(el).position;
      });

      expect(position).toBe('sticky');
    });

    test('header remains visible after scrolling down', async ({ page }) => {
      await page.setViewportSize({ width: 1200, height: 800 });

      await page.waitForSelector('.header');

      // Scroll down significantly
      await page.evaluate(() => window.scrollTo(0, 1000));

      // Wait for scroll
      await page.waitForTimeout(100);

      // Header should still be visible at the top
      const header = page.locator('.header');
      await expect(header).toBeVisible();

      // Header should be at the top of the viewport
      const headerBox = await header.boundingBox();
      expect(headerBox).not.toBeNull();
      expect(headerBox!.y).toBeLessThanOrEqual(0);
    });

    test('header stays at top during scroll to bottom', async ({ page }) => {
      await page.setViewportSize({ width: 1200, height: 800 });

      await page.waitForSelector('.header');

      // Scroll to bottom of page
      await page.evaluate(() => {
        window.scrollTo(0, document.body.scrollHeight);
      });

      await page.waitForTimeout(100);

      // Header should still be visible
      const header = page.locator('.header');
      await expect(header).toBeVisible();
      await expect(header).toBeInViewport();
    });
  });

  test.describe('Header Content Tests', () => {
    test('header contains logo', async ({ page }) => {
      await page.waitForSelector('.header');

      const logo = page.locator('.header__logo');
      await expect(logo).toBeVisible();

      const logoText = page.locator('.header__logo-text');
      await expect(logoText).toHaveText('MirDB');
    });

    test('header contains logo image', async ({ page }) => {
      await page.waitForSelector('.header');

      const logoImg = page.locator('.header__logo-img');
      await expect(logoImg).toBeVisible();
      await expect(logoImg).toHaveAttribute('alt', 'MirDB Logo');
    });

    test('navigation has all required section links', async ({ page }) => {
      await page.setViewportSize({ width: 1200, height: 800 });

      await page.waitForSelector('.header');

      const expectedHrefs = ['#features', '#quickstart', '#protocol', '#configuration', '#architecture'];

      for (const href of expectedHrefs) {
        const link = page.locator(`a[href="${href}"]`);
        await expect(link).toBeVisible();
      }
    });
  });

  test.describe('Accessibility', () => {
    test('header has banner role', async ({ page }) => {
      await page.waitForSelector('.header');

      const header = page.locator('.header');
      await expect(header).toHaveAttribute('role', 'banner');
    });

    test('navigation has proper aria attributes', async ({ page }) => {
      await page.waitForSelector('.header');

      const nav = page.locator('.header__nav');
      await expect(nav).toHaveAttribute('role', 'navigation');
      await expect(nav).toHaveAttribute('aria-label', 'Main navigation');
    });

    test('logo link has aria-label', async ({ page }) => {
      await page.waitForSelector('.header');

      const logo = page.locator('.header__logo');
      await expect(logo).toHaveAttribute('aria-label', 'MirDB Home');
    });
  });
});
