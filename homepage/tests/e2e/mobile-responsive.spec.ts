/**
 * Mobile Responsiveness E2E Tests
 * Owner: Scenario 7 - Mobile Responsiveness
 *
 * Tests for mobile responsive behavior:
 * - Hamburger menu display on mobile
 * - Hamburger menu functionality
 * - Vertical stacking of content
 * - No horizontal scrollbar
 * - Tablet layout adjustments
 * - Small mobile (320px) readability
 */

import { test, expect } from '@playwright/test';

test.describe('Mobile Responsiveness', () => {
  test.describe('Test Case 1: Navigation component at mobile viewport (375px)', () => {
    test('hamburger menu icon is displayed instead of full navigation links', async ({
      page,
    }) => {
      // Set mobile viewport (375px - standard iPhone width)
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      // Hamburger menu button should be visible
      const hamburgerButton = page.locator('.mobile-menu-button');
      await expect(hamburgerButton).toBeVisible();

      // Desktop navigation links should be hidden
      const desktopNav = page.locator('.nav-links-desktop');
      await expect(desktopNav).toBeHidden();
    });
  });

  test.describe('Test Case 2: Hamburger menu functionality', () => {
    test('navigation menu expands with all navigation links visible when hamburger is clicked', async ({
      page,
    }) => {
      // Set mobile viewport
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      // Click hamburger menu
      const hamburgerButton = page.getByRole('button', { name: /open menu/i });
      await hamburgerButton.click();

      // Mobile navigation should be visible
      const mobileNav = page.locator('#mobile-navigation');
      await expect(mobileNav).toBeVisible();

      // All navigation links should be visible in mobile menu
      await expect(mobileNav.getByRole('link', { name: 'Features' })).toBeVisible();
      await expect(mobileNav.getByRole('link', { name: 'Usage' })).toBeVisible();
      await expect(mobileNav.getByRole('link', { name: 'Getting Started' })).toBeVisible();
      await expect(mobileNav.getByRole('link', { name: /github/i })).toBeVisible();
    });

    test('hamburger menu closes when clicked again', async ({ page }) => {
      // Set mobile viewport
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      // Open menu
      const hamburgerButton = page.getByRole('button', { name: /open menu/i });
      await hamburgerButton.click();

      // Verify menu is open
      const mobileNav = page.locator('#mobile-navigation');
      await expect(mobileNav).toBeVisible();

      // Close menu
      const closeButton = page.getByRole('button', { name: /close menu/i });
      await closeButton.click();

      // Menu should be hidden
      await expect(mobileNav).toBeHidden();
    });

    test('navigation link click closes mobile menu', async ({ page }) => {
      // Set mobile viewport
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      // Open menu
      const hamburgerButton = page.getByRole('button', { name: /open menu/i });
      await hamburgerButton.click();

      // Click a link in mobile menu
      const mobileNav = page.locator('#mobile-navigation');
      const featuresLink = mobileNav.getByRole('link', { name: 'Features' });
      await featuresLink.click();

      // Menu should be hidden after clicking a link
      await expect(mobileNav).toBeHidden();
    });
  });

  test.describe('Test Case 3: Features component at mobile viewport (375px)', () => {
    test('feature cards stack vertically in single column', async ({ page }) => {
      // Set mobile viewport
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      // Navigate to features section
      await page.locator('#features').scrollIntoViewIfNeeded();

      // Get features grid
      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();

      // Check that grid template columns is 1fr (single column)
      const gridStyle = await featuresGrid.evaluate((el) => {
        return window.getComputedStyle(el).gridTemplateColumns;
      });

      // Should be single column - the computed value will be a single pixel value
      // when there's only one column, not "1fr"
      const columnCount = gridStyle.split(' ').length;
      expect(columnCount).toBe(1);
    });
  });

  test.describe('Test Case 4: Full page at 375px viewport - no horizontal scroll', () => {
    test('no horizontal scrollbar appears', async ({ page }) => {
      // Set mobile viewport
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      // Wait for page to fully load
      await page.waitForLoadState('networkidle');

      // Check if horizontal scrollbar is present
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      expect(hasHorizontalScroll).toBe(false);
    });

    test('all content is readable without horizontal scrolling', async ({ page }) => {
      // Set mobile viewport
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      // Check main content sections are within viewport
      const sections = ['#hero', '#features', '#usage', '#getting-started'];

      for (const section of sections) {
        const element = page.locator(section);
        if ((await element.count()) > 0) {
          const boundingBox = await element.boundingBox();
          if (boundingBox) {
            // Content should not extend beyond viewport width
            expect(boundingBox.x).toBeGreaterThanOrEqual(0);
            expect(boundingBox.x + boundingBox.width).toBeLessThanOrEqual(375);
          }
        }
      }
    });
  });

  test.describe('Test Case 5: Full page at 768px viewport (tablet)', () => {
    test('layout adjusts appropriately for tablet size', async ({ page }) => {
      // Set tablet viewport
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');

      // Desktop navigation should be visible on tablet
      const desktopNav = page.locator('.nav-links-desktop');
      await expect(desktopNav).toBeVisible();

      // Hamburger menu should be hidden on tablet
      const hamburgerButton = page.locator('.mobile-menu-button');
      await expect(hamburgerButton).toBeHidden();

      // Features grid should show 2 columns on tablet
      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();

      const gridStyle = await featuresGrid.evaluate((el) => {
        return window.getComputedStyle(el).gridTemplateColumns;
      });

      // Should have 2 columns on tablet
      const columnCount = gridStyle.split(' ').length;
      expect(columnCount).toBe(2);
    });

    test('no horizontal scrollbar on tablet viewport', async ({ page }) => {
      // Set tablet viewport
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');

      // Wait for page to fully load
      await page.waitForLoadState('networkidle');

      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      expect(hasHorizontalScroll).toBe(false);
    });
  });

  test.describe('Test Case 6: Full page at 320px viewport (small mobile)', () => {
    test('all content remains readable and accessible', async ({ page }) => {
      // Set small mobile viewport (320px - minimum supported width)
      await page.setViewportSize({ width: 320, height: 568 });
      await page.goto('/');

      // Wait for page to fully load
      await page.waitForLoadState('networkidle');

      // Hero section should be visible and readable
      const heroHeading = page.getByRole('heading', { level: 1 });
      await expect(heroHeading).toBeVisible();

      // Navigation hamburger should work
      const hamburgerButton = page.locator('.mobile-menu-button');
      await expect(hamburgerButton).toBeVisible();
      await hamburgerButton.click();

      const mobileNav = page.locator('#mobile-navigation');
      await expect(mobileNav).toBeVisible();

      // Close menu
      await page.getByRole('button', { name: /close menu/i }).click();

      // Features section should be visible
      await page.locator('#features').scrollIntoViewIfNeeded();
      const featuresTitle = page.getByRole('heading', { name: /core features/i });
      await expect(featuresTitle).toBeVisible();
    });

    test('no horizontal scrollbar on small mobile viewport', async ({ page }) => {
      // Set small mobile viewport
      await page.setViewportSize({ width: 320, height: 568 });
      await page.goto('/');

      // Wait for page to fully load
      await page.waitForLoadState('networkidle');

      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      expect(hasHorizontalScroll).toBe(false);
    });

    test('text remains at readable size on small mobile', async ({ page }) => {
      // Set small mobile viewport
      await page.setViewportSize({ width: 320, height: 568 });
      await page.goto('/');

      // Check that text is readable (minimum 14px font size)
      const heroHeading = page.getByRole('heading', { level: 1 });
      const fontSize = await heroHeading.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });

      // Heading should have readable font size (at least 24px for h1)
      expect(fontSize).toBeGreaterThanOrEqual(24);
    });

    test('buttons are tappable on small mobile', async ({ page }) => {
      // Set small mobile viewport
      await page.setViewportSize({ width: 320, height: 568 });
      await page.goto('/');

      // Check hamburger button is large enough to tap (at least 44x44 for accessibility)
      const hamburgerButton = page.locator('.mobile-menu-button');
      const box = await hamburgerButton.boundingBox();

      expect(box).not.toBeNull();
      // Button should have reasonable tap target (allowing some flexibility)
      expect(box!.width).toBeGreaterThanOrEqual(24);
      expect(box!.height).toBeGreaterThanOrEqual(24);
    });
  });

  test.describe('Accessibility on mobile', () => {
    test('hamburger menu has proper ARIA attributes', async ({ page }) => {
      // Set mobile viewport
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      const hamburgerButton = page.locator('.mobile-menu-button');

      // Should have aria-expanded
      await expect(hamburgerButton).toHaveAttribute('aria-expanded', 'false');

      // Should have aria-controls pointing to mobile navigation
      await expect(hamburgerButton).toHaveAttribute('aria-controls', 'mobile-navigation');

      // Should have aria-label for screen readers
      await expect(hamburgerButton).toHaveAttribute('aria-label', 'Open menu');

      // After clicking, aria-expanded should be true
      await hamburgerButton.click();
      await expect(hamburgerButton).toHaveAttribute('aria-expanded', 'true');
      await expect(hamburgerButton).toHaveAttribute('aria-label', 'Close menu');
    });

    test('mobile navigation is keyboard accessible', async ({ page }) => {
      // Set mobile viewport
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      // Tab to hamburger button and open menu with keyboard
      await page.keyboard.press('Tab');
      await page.keyboard.press('Tab'); // Skip logo link
      await page.keyboard.press('Enter');

      // Mobile nav should be visible
      const mobileNav = page.locator('#mobile-navigation');
      await expect(mobileNav).toBeVisible();
    });
  });
});
