/**
 * E2E Tests for Navigation Header Functionality
 * Testing REQ-3: Navigation header with logo, menu links, and CTA button
 */

import { test, expect } from '@playwright/test';

test.describe('Navigation Header E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 5: Scroll page and verify header visibility
   */
  test('header should remain visible after scrolling 500px', async ({ page }) => {
    // Get initial header position
    const header = page.locator('header');
    await expect(header).toBeVisible();

    // Scroll down 500px
    await page.evaluate(() => window.scrollBy(0, 500));

    // Wait a moment for any animations
    await page.waitForTimeout(300);

    // Header should still be visible
    await expect(header).toBeVisible();

    // Header should be at the top of the viewport (within the visible area)
    const headerBox = await header.boundingBox();
    expect(headerBox).not.toBeNull();
    expect(headerBox.y).toBeLessThanOrEqual(10); // Header top should be near viewport top
  });

  /**
   * Test Case 6: Click navigation links
   */
  test('navigation links should scroll to respective sections', async ({ page }) => {
    // Get all navigation links
    const navLinks = page.locator('.nav-links a');
    const linkCount = await navLinks.count();

    expect(linkCount).toBeGreaterThanOrEqual(3);

    // Test the Features link (first nav link)
    const featuresLink = navLinks.first();
    const href = await featuresLink.getAttribute('href');

    if (href && href.startsWith('#')) {
      // Internal anchor link - should scroll to section
      const targetId = href.substring(1);
      await featuresLink.click();

      // Wait for smooth scroll
      await page.waitForTimeout(500);

      // Target section should be visible
      const targetSection = page.locator(`#${targetId}`);
      await expect(targetSection).toBeVisible();
    }
  });

  test('header should have position fixed when scrolling', async ({ page }) => {
    const header = page.locator('header');

    // Get computed position
    const position = await header.evaluate((el) => {
      return window.getComputedStyle(el).position;
    });

    expect(position).toBe('fixed');
  });

  test('CTA button should be clickable and styled distinctly', async ({ page }) => {
    const ctaButton = page.locator('header .nav-cta');
    await expect(ctaButton).toBeVisible();
    await expect(ctaButton).toBeEnabled();

    // CTA should have a background color (indicating it's styled as a button)
    const backgroundColor = await ctaButton.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Background should not be transparent
    expect(backgroundColor).not.toBe('rgba(0, 0, 0, 0)');
    expect(backgroundColor).not.toBe('transparent');
  });

  test('logo should be visible and positioned on the left', async ({ page }) => {
    const logo = page.locator('header .logo');
    await expect(logo).toBeVisible();

    // Logo should be in the left portion of the header
    const logoBox = await logo.boundingBox();
    const viewportSize = page.viewportSize();

    // Logo x position should be in the left third of the page
    expect(logoBox.x).toBeLessThan(viewportSize.width / 3);
  });

  test('header should have proper z-index to stay above content', async ({ page }) => {
    const header = page.locator('header');

    // Scroll to ensure content is behind header
    await page.evaluate(() => window.scrollBy(0, 200));

    const zIndex = await header.evaluate((el) => {
      return parseInt(window.getComputedStyle(el).zIndex);
    });

    expect(zIndex).toBeGreaterThan(0);
  });
});
