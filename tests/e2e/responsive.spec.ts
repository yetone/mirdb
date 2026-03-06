/**
 * Mobile Responsive Design E2E Tests
 * Owner: Scenario 9 - Mobile Responsive Design
 *
 * Test coverage:
 * - 375px viewport width
 * - Mobile navigation
 * - Vertical stacking of feature cards
 * - Readable font sizes
 * - Touch target sizes
 */

import { test, expect } from '@playwright/test';
import { waitForPageLoad, setViewport } from './test-utils';

test.describe('Mobile Responsive Design', () => {
  test.beforeEach(async ({ page }) => {
    // Set mobile viewport (375px width - iPhone SE/small mobile)
    await setViewport(page, 'mobile');
    await page.goto('/');
    await waitForPageLoad(page);
  });

  test('TC1: no horizontal scrollbar at 375px viewport width', async ({ page }) => {
    // Check that document width does not exceed viewport width
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);

    // The body scroll width should not exceed the viewport width (no horizontal overflow)
    expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth);

    // Also verify the html element doesn't have horizontal overflow
    const htmlOverflowX = await page.evaluate(() => {
      const html = document.documentElement;
      return html.scrollWidth <= html.clientWidth;
    });
    expect(htmlOverflowX).toBe(true);
  });

  test('TC2: navigation is accessible on mobile', async ({ page }) => {
    // Navigation should either be visible or accessible via hamburger menu
    const nav = page.locator('nav');
    const navList = page.locator('.nav-list');

    // Check if nav element exists
    await expect(nav).toBeVisible();

    // Check if navigation links exist and are visible (or hamburger menu exists)
    const navLinks = page.locator('.nav-link');
    const navLinkCount = await navLinks.count();

    // At least one navigation element should exist
    expect(navLinkCount).toBeGreaterThan(0);

    // Check that nav items have proper touch target size on mobile
    // Navigation should be accessible either directly or via a hamburger menu
    const isNavListVisible = await navList.isVisible();

    if (isNavListVisible) {
      // If visible, all nav links should be interactable
      for (let i = 0; i < navLinkCount; i++) {
        const link = navLinks.nth(i);
        await expect(link).toBeVisible();
      }
    } else {
      // If nav list is hidden, there should be a hamburger menu button
      const hamburgerMenu = page.locator('[data-testid="mobile-menu-btn"], .hamburger-menu, .mobile-menu-toggle, button[aria-label*="menu"]');
      await expect(hamburgerMenu).toBeVisible();
    }
  });

  test('TC3: feature cards stack vertically on mobile', async ({ page }) => {
    // Navigate to features section
    await page.locator('#features').scrollIntoViewIfNeeded();

    // Get all feature cards
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();

    expect(cardCount).toBeGreaterThan(0);

    // Get bounding boxes of first two cards to verify vertical stacking
    if (cardCount >= 2) {
      const card1Box = await featureCards.nth(0).boundingBox();
      const card2Box = await featureCards.nth(1).boundingBox();

      expect(card1Box).not.toBeNull();
      expect(card2Box).not.toBeNull();

      if (card1Box && card2Box) {
        // Cards should be stacked vertically (second card's top should be below first card's bottom)
        // Allow a small tolerance for gaps
        expect(card2Box.y).toBeGreaterThan(card1Box.y);

        // Cards should have similar x positions (left-aligned in single column)
        // Allow some tolerance for padding differences
        expect(Math.abs(card1Box.x - card2Box.x)).toBeLessThan(50);
      }
    }
  });

  test('TC4: text remains readable on mobile (font size at least 16px)', async ({ page }) => {
    // Check body font size
    const bodyFontSize = await page.evaluate(() => {
      const body = document.body;
      const style = window.getComputedStyle(body);
      return parseFloat(style.fontSize);
    });
    expect(bodyFontSize).toBeGreaterThanOrEqual(16);

    // Check paragraph text
    const paragraphs = page.locator('p');
    const pCount = await paragraphs.count();

    for (let i = 0; i < Math.min(pCount, 5); i++) {
      const fontSize = await paragraphs.nth(i).evaluate((el) => {
        const style = window.getComputedStyle(el);
        return parseFloat(style.fontSize);
      });
      expect(fontSize).toBeGreaterThanOrEqual(16);
    }

    // Check feature card descriptions
    const featureDescriptions = page.locator('.feature-card p');
    const descCount = await featureDescriptions.count();

    for (let i = 0; i < descCount; i++) {
      const fontSize = await featureDescriptions.nth(i).evaluate((el) => {
        const style = window.getComputedStyle(el);
        return parseFloat(style.fontSize);
      });
      expect(fontSize).toBeGreaterThanOrEqual(16);
    }
  });

  test('TC5: touch targets are adequate size (minimum 44x44px)', async ({ page }) => {
    // Minimum touch target size as per WCAG 2.1 and Apple/Google guidelines
    const MIN_TOUCH_TARGET = 44;

    // Check CTA button
    const ctaButton = page.locator('.hero-cta');
    if (await ctaButton.isVisible()) {
      const ctaBox = await ctaButton.boundingBox();
      if (ctaBox) {
        expect(ctaBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
        expect(ctaBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
      }
    }

    // Check navigation links
    const navLinks = page.locator('.nav-link');
    const navCount = await navLinks.count();

    for (let i = 0; i < navCount; i++) {
      const link = navLinks.nth(i);
      if (await link.isVisible()) {
        const linkBox = await link.boundingBox();
        if (linkBox) {
          // Either width or height should meet minimum, or both
          // Links can be wider than tall, so we check the combined touch target area
          expect(linkBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
          expect(linkBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
        }
      }
    }

    // Check copy button if visible
    const copyBtn = page.locator('.code-copy-btn');
    if (await copyBtn.isVisible()) {
      const copyBox = await copyBtn.boundingBox();
      if (copyBox) {
        expect(copyBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
        expect(copyBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
      }
    }
  });
});

test.describe('Mobile Responsive Design - Additional Viewport Tests', () => {
  test('content fits at various mobile widths', async ({ page }) => {
    const mobileWidths = [320, 375, 414];

    for (const width of mobileWidths) {
      await page.setViewportSize({ width, height: 667 });
      await page.goto('/');
      await waitForPageLoad(page);

      const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
      const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);

      expect(scrollWidth).toBeLessThanOrEqual(clientWidth + 1); // Allow 1px tolerance
    }
  });
});
