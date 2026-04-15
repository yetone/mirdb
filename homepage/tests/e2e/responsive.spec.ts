/**
 * Responsive Design E2E Tests
 * Owner: Scenarios 6 & 7 - Responsive Design
 *
 * Tests for mobile (Scenario 6) and tablet (Scenario 7) responsiveness.
 * Verifies layout adaptation, touch targets, and navigation accessibility.
 *
 * Test Cases:
 * 1. Page renders without horizontal scrollbar at 375px
 * 2. Content displays in single-column layout on mobile
 * 3. Navigation accessible via hamburger menu or visible links
 * 4. All buttons and links have minimum 44x44px touch targets
 * 5. All sections are accessible and readable on mobile
 */

import { test, expect } from '@playwright/test';
import { SELECTORS, TEST_URLS } from '../fixtures/test-data';

// Mobile viewport configuration (iPhone SE)
const MOBILE_VIEWPORT = { width: 375, height: 667 };

// Minimum touch target size per WCAG 2.5.5
const MIN_TOUCH_TARGET = 44;

test.describe('Scenario 6 - Responsive Design - Mobile', () => {
  test.beforeEach(async ({ page }) => {
    // Set mobile viewport before navigating
    await page.setViewportSize(MOBILE_VIEWPORT);
    await page.goto(TEST_URLS.HOME);
  });

  test('Test Case 1: Page renders without horizontal scrollbar at 375px viewport', async ({ page }) => {
    // Check that the document doesn't have horizontal overflow
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });

    expect(hasHorizontalScroll).toBe(false);

    // Verify viewport width matches expected
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(viewportWidth).toBe(MOBILE_VIEWPORT.width);
  });

  test('Test Case 2: Content displays in single-column layout on mobile', async ({ page }) => {
    // Check features grid is single column
    const featuresGrid = page.locator('.features__grid');
    await expect(featuresGrid).toBeVisible();

    const gridColumns = await featuresGrid.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return computed.gridTemplateColumns;
    });

    // Should be a single column (one value, not multiple)
    const columnCount = gridColumns.split(' ').filter(col => col !== 'none' && col.trim() !== '').length;
    expect(columnCount).toBe(1);

    // Verify feature cards stack vertically
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThan(0);

    // Check that cards are full width (single column behavior)
    const firstCard = featureCards.first();
    const cardBox = await firstCard.boundingBox();
    const containerBox = await page.locator('.features .container').boundingBox();

    if (cardBox && containerBox) {
      // Card should span nearly full container width (accounting for padding)
      const widthRatio = cardBox.width / containerBox.width;
      expect(widthRatio).toBeGreaterThan(0.9);
    }
  });

  test('Test Case 3: Navigation accessible via hamburger menu or visible links', async ({ page }) => {
    // Check that navigation container exists
    const nav = page.locator('.nav');
    await expect(nav).toBeVisible();

    // Check for hamburger menu toggle button on mobile
    const navToggle = page.locator('.nav__toggle');
    const isToggleVisible = await navToggle.isVisible();

    if (isToggleVisible) {
      // Hamburger menu mode - verify toggle is functional
      await expect(navToggle).toHaveAttribute('aria-label', /toggle|menu/i);

      // Click to open menu
      await navToggle.click();

      // Verify navigation links become visible
      const navLinks = page.locator('.nav__links');
      await expect(navLinks).toHaveClass(/nav__links--open/);

      // Verify aria-expanded is updated
      await expect(navToggle).toHaveAttribute('aria-expanded', 'true');

      // Verify links are accessible
      const links = page.locator('.nav__link');
      const linkCount = await links.count();
      expect(linkCount).toBeGreaterThan(0);

      // Close menu
      await navToggle.click();
      await expect(navToggle).toHaveAttribute('aria-expanded', 'false');
    } else {
      // Navigation links should be visible without hamburger
      const navLinks = page.locator('.nav__links');
      await expect(navLinks).toBeVisible();

      const links = page.locator('.nav__link');
      const linkCount = await links.count();
      expect(linkCount).toBeGreaterThan(0);
    }
  });

  test('Test Case 4: All buttons and links have minimum 44x44px touch targets', async ({ page }) => {
    // List of interactive elements to check
    const interactiveSelectors = [
      '.hero__cta',
      '.code-block__copy-btn',
      '.nav__toggle',
    ];

    for (const selector of interactiveSelectors) {
      const element = page.locator(selector).first();
      const isVisible = await element.isVisible().catch(() => false);

      if (isVisible) {
        const box = await element.boundingBox();
        if (box) {
          expect(box.height, `${selector} height should be >= ${MIN_TOUCH_TARGET}px`).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
          expect(box.width, `${selector} width should be >= ${MIN_TOUCH_TARGET}px`).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
        }
      }
    }

    // Check hero CTA specifically since it's a primary action
    const heroCta = page.locator('.hero__cta');
    await expect(heroCta).toBeVisible();
    const ctaBox = await heroCta.boundingBox();
    expect(ctaBox).not.toBeNull();
    if (ctaBox) {
      expect(ctaBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
    }

    // Open navigation and check link touch targets
    const navToggle = page.locator('.nav__toggle');
    if (await navToggle.isVisible()) {
      await navToggle.click();

      const navLinks = page.locator('.nav__link');
      const linkCount = await navLinks.count();

      for (let i = 0; i < linkCount; i++) {
        const link = navLinks.nth(i);
        const linkBox = await link.boundingBox();
        if (linkBox) {
          expect(linkBox.height, `Nav link ${i + 1} height should be >= ${MIN_TOUCH_TARGET}px`).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
        }
      }
    }
  });

  test('Test Case 5: All sections accessible and readable on mobile', async ({ page }) => {
    // List of main sections to verify
    const sections = [
      { selector: '#hero', name: 'Hero' },
      { selector: '#features', name: 'Features' },
      { selector: '#quickstart', name: 'Quick Start' },
      { selector: '#techspecs', name: 'Tech Specs' },
      { selector: '#footer', name: 'Footer' },
    ];

    for (const section of sections) {
      const sectionEl = page.locator(section.selector);
      await expect(sectionEl, `${section.name} section should exist`).toBeAttached();

      // Scroll section into view
      await sectionEl.scrollIntoViewIfNeeded();
      await expect(sectionEl, `${section.name} section should be visible`).toBeVisible();

      // Check section doesn't overflow viewport horizontally
      const sectionBox = await sectionEl.boundingBox();
      if (sectionBox) {
        expect(sectionBox.width, `${section.name} should not exceed viewport width`).toBeLessThanOrEqual(MOBILE_VIEWPORT.width + 1);
      }
    }

    // Verify no horizontal scroll after scrolling through all sections
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });

  test('Hero section adapts to mobile layout', async ({ page }) => {
    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();

    // Hero title should be visible and readable
    const heroTitle = page.locator('.hero__title');
    await expect(heroTitle).toBeVisible();

    // Hero CTA should be full width on mobile
    const cta = page.locator('.hero__cta');
    await expect(cta).toBeVisible();

    const ctaBox = await cta.boundingBox();
    const heroBox = await hero.locator('.container').boundingBox();

    if (ctaBox && heroBox) {
      // CTA should be nearly full container width
      const widthRatio = ctaBox.width / heroBox.width;
      expect(widthRatio).toBeGreaterThan(0.9);
    }
  });

  test('Code block is readable on mobile', async ({ page }) => {
    const quickstart = page.locator('#quickstart');
    await quickstart.scrollIntoViewIfNeeded();

    const codeBlock = page.locator('.code-block');
    await expect(codeBlock).toBeVisible();

    // Code block should not overflow horizontally beyond viewport
    const codeBlockBox = await codeBlock.boundingBox();
    expect(codeBlockBox).not.toBeNull();
    if (codeBlockBox) {
      expect(codeBlockBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
    }

    // Copy button should be accessible
    const copyBtn = page.locator('.code-block__copy-btn');
    await expect(copyBtn).toBeVisible();
    const copyBtnBox = await copyBtn.boundingBox();
    if (copyBtnBox) {
      expect(copyBtnBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
      expect(copyBtnBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
    }
  });

  test('Footer stacks vertically on mobile', async ({ page }) => {
    const footer = page.locator('#footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Footer content should be stacked
    const footerContent = page.locator('.footer__content');
    const footerComputedStyle = await footerContent.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        flexDirection: style.flexDirection,
        textAlign: style.textAlign,
      };
    });

    expect(footerComputedStyle.flexDirection).toBe('column');
    expect(footerComputedStyle.textAlign).toBe('center');

    // Footer links should be touchable
    const footerLinks = page.locator('.footer__links a');
    const linkCount = await footerLinks.count();

    for (let i = 0; i < linkCount; i++) {
      const link = footerLinks.nth(i);
      const linkBox = await link.boundingBox();
      if (linkBox) {
        expect(linkBox.height, `Footer link ${i + 1} should have adequate touch target`).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
      }
    }
  });

  test('Tech specs grid adapts to mobile', async ({ page }) => {
    const techspecs = page.locator('#techspecs');
    await techspecs.scrollIntoViewIfNeeded();
    await expect(techspecs).toBeVisible();

    const grid = page.locator('.techspecs__grid');
    const gridColumns = await grid.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return computed.gridTemplateColumns;
    });

    // Should have 2 columns on mobile (as defined in responsive.css)
    const columnValues = gridColumns.split(' ').filter(v => v.trim() !== '');
    expect(columnValues.length).toBeLessThanOrEqual(2);
  });
});

test.describe('Responsive - Additional Mobile Tests', () => {
  test('Page is scrollable vertically', async ({ page }) => {
    await page.setViewportSize(MOBILE_VIEWPORT);
    await page.goto(TEST_URLS.HOME);

    // Page should be scrollable vertically - content is taller than viewport
    const scrollHeight = await page.evaluate(() => document.documentElement.scrollHeight);
    const clientHeight = await page.evaluate(() => document.documentElement.clientHeight);

    expect(scrollHeight).toBeGreaterThan(clientHeight);

    // Verify footer section can be scrolled into view (confirms page scrollability)
    const footer = page.locator('#footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeInViewport();
  });

  test('Text is readable without zooming', async ({ page }) => {
    await page.setViewportSize(MOBILE_VIEWPORT);
    await page.goto(TEST_URLS.HOME);

    // Check that base font size is at least 16px (prevents auto-zoom on iOS)
    const baseFontSize = await page.evaluate(() => {
      const body = document.body;
      return parseFloat(window.getComputedStyle(body).fontSize);
    });

    expect(baseFontSize).toBeGreaterThanOrEqual(16);
  });

  test('Viewport meta tag prevents zooming issues', async ({ page }) => {
    await page.setViewportSize(MOBILE_VIEWPORT);
    await page.goto(TEST_URLS.HOME);

    // Check viewport meta tag exists and has proper settings
    const viewportMeta = await page.evaluate(() => {
      const meta = document.querySelector('meta[name="viewport"]');
      return meta ? meta.getAttribute('content') : null;
    });

    expect(viewportMeta).not.toBeNull();
    expect(viewportMeta).toContain('width=device-width');
    expect(viewportMeta).toContain('initial-scale=1');
  });
});
