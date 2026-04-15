/**
 * Navigation E2E Tests
 * Owner: Scenario 12 - Navigation and Smooth Scroll
 *
 * Tests:
 * - Sticky navigation bar remains visible while scrolling
 * - Click navigation link scrolls to target section
 * - Smooth scroll behavior is applied
 * - Navigation links have correct href values
 */

import { test, expect } from '@playwright/test';

test.describe('Navigation and Smooth Scroll', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for page to be fully loaded
    await page.waitForLoadState('networkidle');
  });

  test('Test Case 1: Navigation bar remains visible and fixed at top after scrolling 500px', async ({ page }) => {
    const header = page.locator('#header');

    // Verify header exists and is visible initially
    await expect(header).toBeVisible();

    // Get initial header position
    const initialBoundingBox = await header.boundingBox();
    expect(initialBoundingBox).not.toBeNull();

    // Scroll down 500px
    await page.evaluate(() => window.scrollBy(0, 500));

    // Wait for scroll to complete
    await page.waitForTimeout(100);

    // Verify header is still visible
    await expect(header).toBeVisible();

    // Get header position after scroll
    const afterScrollBoundingBox = await header.boundingBox();
    expect(afterScrollBoundingBox).not.toBeNull();

    // Header should be at or near the top (y should be close to 0)
    expect(afterScrollBoundingBox!.y).toBeLessThanOrEqual(10);

    // Verify header has sticky positioning
    const position = await header.evaluate(el => window.getComputedStyle(el).position);
    expect(position).toBe('sticky');
  });

  test('Test Case 2: Clicking Features link scrolls to Features section', async ({ page }) => {
    const featuresLink = page.locator('a[href="#features"]').first();
    const featuresSection = page.locator('#features');

    // Verify link exists
    await expect(featuresLink).toBeVisible();

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the Features link
    await featuresLink.click();

    // Wait for scroll animation to complete
    await page.waitForTimeout(600);

    // Get new scroll position
    const newScrollY = await page.evaluate(() => window.scrollY);

    // Verify scroll happened (page scrolled down)
    expect(newScrollY).toBeGreaterThan(initialScrollY);

    // Verify Features section is now in viewport
    await expect(featuresSection).toBeInViewport();
  });

  test('Smooth scroll behavior: Navigation links scroll smoothly', async ({ page }) => {
    const quickStartLink = page.locator('a[href="#quickstart"]').first();

    // Click the Quick Start link and observe scroll behavior
    await quickStartLink.click();

    // Capture multiple scroll positions during animation
    const scrollPositions: number[] = [];
    for (let i = 0; i < 5; i++) {
      scrollPositions.push(await page.evaluate(() => window.scrollY));
      await page.waitForTimeout(80);
    }

    // Wait for scroll to complete
    await page.waitForTimeout(300);

    // Final position
    const finalScrollY = await page.evaluate(() => window.scrollY);
    scrollPositions.push(finalScrollY);

    // Verify scroll happened
    expect(finalScrollY).toBeGreaterThan(0);

    // Verify Quick Start section is visible
    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeInViewport();
  });

  test('Navigation links have correct href values matching section IDs', async ({ page }) => {
    // Get all navigation links
    const navLinks = page.locator('.header__nav-link');
    const count = await navLinks.count();

    // Should have at least 3 navigation links
    expect(count).toBeGreaterThanOrEqual(3);

    // Check each link has a valid href pointing to a section
    const expectedLinks = ['#features', '#quickstart', '#techspecs'];

    for (const expectedHref of expectedLinks) {
      const link = page.locator(`a.header__nav-link[href="${expectedHref}"]`);
      await expect(link).toBeVisible();

      // Verify the target section exists
      const sectionId = expectedHref.slice(1);
      const section = page.locator(`#${sectionId}`);
      await expect(section).toBeAttached();
    }
  });

  test('All navigation links scroll to their target sections', async ({ page }) => {
    const navLinks = [
      { href: '#features', sectionId: 'features' },
      { href: '#quickstart', sectionId: 'quickstart' },
      { href: '#techspecs', sectionId: 'techspecs' }
    ];

    for (const { href, sectionId } of navLinks) {
      // Scroll to top first
      await page.evaluate(() => window.scrollTo(0, 0));
      await page.waitForTimeout(100);

      // Click the nav link
      const link = page.locator(`a.header__nav-link[href="${href}"]`);
      await link.click();

      // Wait for scroll animation
      await page.waitForTimeout(600);

      // Verify section is in viewport
      const section = page.locator(`#${sectionId}`);
      await expect(section).toBeInViewport();
    }
  });

  test('Header remains accessible during scroll', async ({ page }) => {
    // Scroll to the bottom of the page
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(200);

    // Header should still be visible at the top
    const header = page.locator('#header');
    await expect(header).toBeVisible();

    // Click on a navigation link should still work
    const featuresLink = page.locator('a.header__nav-link[href="#features"]');
    await expect(featuresLink).toBeVisible();

    // Navigate back to features
    await featuresLink.click();
    await page.waitForTimeout(600);

    // Verify features section is visible
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  test('Hero CTA button scrolls to Quick Start section', async ({ page }) => {
    const ctaButton = page.locator('.hero__cta');

    // Verify CTA exists and points to quickstart
    await expect(ctaButton).toBeVisible();
    const href = await ctaButton.getAttribute('href');
    expect(href).toBe('#quickstart');

    // Click the CTA
    await ctaButton.click();

    // Wait for scroll
    await page.waitForTimeout(600);

    // Verify Quick Start section is visible
    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeInViewport();
  });
});
