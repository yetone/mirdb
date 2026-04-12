/**
 * Responsive Design - Mobile Tests
 * Owner: Scenario 7 - Responsive Design - Mobile
 *
 * Tests for mobile responsiveness:
 * - No horizontal overflow at mobile viewport
 * - Touch targets are at least 44x44px
 * - Hero section is visible and readable on mobile
 * - Code blocks scroll horizontally without breaking layout
 * - All sections are accessible on mobile
 * - Navigation works with touch on mobile
 */

import { test, expect } from '@playwright/test';

// Mobile viewport dimensions (iPhone SE)
const MOBILE_VIEWPORT = { width: 375, height: 667 };
const MIN_TOUCH_TARGET = 44;

test.describe('Responsive Design - Mobile', () => {
  test.beforeEach(async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize(MOBILE_VIEWPORT);
    await page.goto('/');
  });

  test('Page renders without horizontal overflow at 375x667 viewport', async ({ page }) => {
    // Wait for page to load
    await page.waitForLoadState('domcontentloaded');

    // Check that the document doesn't have horizontal overflow
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const windowWidth = await page.evaluate(() => window.innerWidth);

    expect(bodyScrollWidth).toBeLessThanOrEqual(windowWidth);

    // Also check html element
    const htmlScrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    expect(htmlScrollWidth).toBeLessThanOrEqual(windowWidth);

    // Verify no horizontal scrollbar is present
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });

  test('All buttons and links have minimum 44x44px touch area', async ({ page }) => {
    await page.waitForLoadState('domcontentloaded');

    // Get all interactive elements (buttons and links)
    const interactiveElements = await page.locator('a, button').all();

    for (const element of interactiveElements) {
      // Skip hidden elements
      const isVisible = await element.isVisible();
      if (!isVisible) continue;

      const box = await element.boundingBox();
      if (box) {
        // Calculate effective touch target size (including padding)
        // Touch target should be at least 44x44px
        expect(box.width, `Element width should be at least ${MIN_TOUCH_TARGET}px`).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
        expect(box.height, `Element height should be at least ${MIN_TOUCH_TARGET}px`).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
      }
    }
  });

  test('Hero content is readable and logo is visible on mobile', async ({ page }) => {
    await page.waitForLoadState('domcontentloaded');

    // Check hero section is visible
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Check logo is visible
    const logo = page.locator('.hero-logo');
    await expect(logo).toBeVisible();

    // Verify logo has reasonable size on mobile
    const logoBox = await logo.boundingBox();
    expect(logoBox).not.toBeNull();
    if (logoBox) {
      // Logo should be visible and not too large for mobile
      expect(logoBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width - 32); // Leave some padding
      expect(logoBox.width).toBeGreaterThan(50); // But still visible
    }

    // Check h1 is visible and readable
    const heading = page.locator('.hero h1');
    await expect(heading).toBeVisible();

    // Check subtitle is visible
    const subtitle = page.locator('.hero-subtitle');
    await expect(subtitle).toBeVisible();

    // Check CTA button is visible
    const ctaButton = page.locator('.hero .btn-primary');
    await expect(ctaButton).toBeVisible();
  });

  test('Code blocks scroll horizontally or wrap without breaking layout', async ({ page }) => {
    await page.waitForLoadState('domcontentloaded');

    // Navigate to quickstart section
    await page.locator('#quickstart').scrollIntoViewIfNeeded();

    // Get all code blocks
    const codeBlocks = await page.locator('.code-block').all();
    expect(codeBlocks.length).toBeGreaterThan(0);

    for (const codeBlock of codeBlocks) {
      await expect(codeBlock).toBeVisible();

      // Get code block dimensions
      const box = await codeBlock.boundingBox();
      expect(box).not.toBeNull();

      if (box) {
        // Code block should not exceed viewport width
        expect(box.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);

        // Code block should be scrollable (has overflow-x: auto)
        const overflowX = await codeBlock.evaluate((el) => {
          return window.getComputedStyle(el).overflowX;
        });

        // Should have scroll or auto for horizontal overflow handling
        expect(['auto', 'scroll']).toContain(overflowX);
      }
    }

    // Verify page still has no horizontal overflow after scrolling to code
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });

  test('All sections (hero, features, quickstart, usage, footer) are visible and readable', async ({ page }) => {
    await page.waitForLoadState('domcontentloaded');

    // Define all sections that should be present
    const sections = [
      { selector: '#hero', name: 'Hero' },
      { selector: '#features', name: 'Features' },
      { selector: '#quickstart', name: 'Quickstart' },
      { selector: '#usage', name: 'Usage' },
      { selector: 'footer', name: 'Footer' }
    ];

    for (const section of sections) {
      const element = page.locator(section.selector);

      // Scroll to section
      await element.scrollIntoViewIfNeeded();

      // Verify section is visible
      await expect(element, `${section.name} section should be visible`).toBeVisible();

      // Verify section has content (not empty)
      const content = await element.textContent();
      expect(content?.trim().length, `${section.name} section should have content`).toBeGreaterThan(0);

      // Verify section fits within viewport width
      const box = await element.boundingBox();
      if (box) {
        expect(box.width, `${section.name} section should fit viewport`).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
      }
    }
  });

  test('Navigation elements respond to touch and work correctly', async ({ page }) => {
    await page.waitForLoadState('domcontentloaded');

    // On mobile, nav links may be hidden (hamburger menu pattern)
    // First check if nav links are visible or hidden
    const navLinks = page.locator('.nav-links');
    const navLinksVisible = await navLinks.isVisible();

    if (navLinksVisible) {
      // If nav links are visible, test they work
      const featureLink = page.locator('nav a[href="#features"]');
      await expect(featureLink).toBeVisible();

      // Click the features link
      await featureLink.click();

      // Verify we scrolled to features section
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();
    }

    // Test the main CTA button
    const ctaButton = page.locator('.hero .btn-primary');
    await expect(ctaButton).toBeVisible();

    // Verify CTA button has proper touch target size
    const ctaBox = await ctaButton.boundingBox();
    expect(ctaBox).not.toBeNull();
    if (ctaBox) {
      expect(ctaBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
      expect(ctaBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
    }

    // Click CTA button and verify navigation
    await ctaButton.click();

    // Verify quickstart section is in viewport
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();

    // Test copy buttons have proper touch targets
    const copyButtons = page.locator('.copy-btn');
    const copyButtonCount = await copyButtons.count();

    if (copyButtonCount > 0) {
      const firstCopyBtn = copyButtons.first();
      await firstCopyBtn.scrollIntoViewIfNeeded();

      const copyBtnBox = await firstCopyBtn.boundingBox();
      expect(copyBtnBox).not.toBeNull();
      if (copyBtnBox) {
        expect(copyBtnBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
        expect(copyBtnBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
      }
    }
  });

  test('Feature cards stack vertically on mobile', async ({ page }) => {
    await page.waitForLoadState('domcontentloaded');

    // Navigate to features section
    await page.locator('#features').scrollIntoViewIfNeeded();

    // Get feature cards
    const featureCards = await page.locator('.feature-card').all();
    expect(featureCards.length).toBe(4);

    // Verify cards stack vertically (each card takes full width or nearly)
    for (const card of featureCards) {
      const box = await card.boundingBox();
      expect(box).not.toBeNull();
      if (box) {
        // Cards should be nearly full width on mobile (accounting for container padding)
        expect(box.width).toBeGreaterThan(MOBILE_VIEWPORT.width * 0.7);
      }
    }

    // Check grid is single column by verifying computed columns is a single value
    // Browser computes "1fr" as a pixel value, so we check for single column
    const featuresGrid = page.locator('.features-grid');
    const gridStyle = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });

    // Should be single column - computed style will be a single pixel value like "343px"
    // Split by space and check only one column value (not multiple like "343px 343px")
    const columns = gridStyle.trim().split(/\s+/);
    expect(columns.length, `Expected single column but got: ${gridStyle}`).toBe(1);
  });

  test('Footer content is accessible on mobile', async ({ page }) => {
    await page.waitForLoadState('domcontentloaded');

    // Scroll to footer
    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Check footer links are clickable
    const footerLinks = await page.locator('footer a').all();
    for (const link of footerLinks) {
      const box = await link.boundingBox();
      if (box) {
        expect(box.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
        expect(box.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
      }
    }
  });
});
