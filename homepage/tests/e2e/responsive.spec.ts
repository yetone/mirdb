/**
 * Responsive Design E2E Tests
 * Owner: Scenarios 6 & 7 - Responsive Design
 *
 * Tests for mobile (Scenario 6) and tablet (Scenario 7) responsiveness.
 * Verifies layout adaptation, touch targets, and navigation accessibility.
 *
 * Test Cases:
 * Mobile (Scenario 6):
 * 1. Page renders without horizontal scrollbar at 375px
 * 2. Content displays in single-column layout on mobile
 * 3. Navigation accessible via hamburger menu or visible links
 * 4. All buttons and links have minimum 44x44px touch targets
 * 5. All sections are accessible and readable on mobile
 *
 * Tablet (Scenario 7):
 * 1. Page renders without horizontal scrollbar at 768px viewport
 * 2. Features display in 2-column grid layout
 * 3. All sections (Hero, Features, Quick Start, Tech Specs, Footer) render correctly
 */

import { test, expect } from '@playwright/test';
import { SELECTORS, TEST_URLS } from '../fixtures/test-data';

// Mobile viewport configuration (iPhone SE)
const MOBILE_VIEWPORT = { width: 375, height: 667 };

// Tablet viewport configuration (iPad portrait)
const TABLET_VIEWPORT = { width: 768, height: 1024 };

// Desktop viewport for comparison
const DESKTOP_VIEWPORT = { width: 1280, height: 800 };

// Minimum touch target size per WCAG 2.5.5
const MIN_TOUCH_TARGET = 44;

/* =================================================================
   SCENARIO 6 - MOBILE RESPONSIVE DESIGN TESTS
   ================================================================= */

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

/* =================================================================
   SCENARIO 7 - TABLET RESPONSIVE DESIGN TESTS
   ================================================================= */

test.describe('Scenario 7 - Responsive Design - Tablet Viewport', () => {
  test.beforeEach(async ({ page }) => {
    // Set tablet viewport before each test
    await page.setViewportSize(TABLET_VIEWPORT);
    await page.goto('/');
    // Wait for the page to fully load
    await page.waitForLoadState('networkidle');
  });

  test('Test Case 1: Page renders without horizontal scrollbar at 768px viewport', async ({ page }) => {
    // Get the document width and viewport width
    const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);

    // Document should not be wider than viewport (no horizontal scroll needed)
    expect(scrollWidth).toBeLessThanOrEqual(clientWidth);

    // Verify body doesn't overflow
    const bodyOverflowX = await page.evaluate(() => {
      const body = document.body;
      return window.getComputedStyle(body).overflowX;
    });
    // Body overflow-x should be hidden or the content shouldn't exceed viewport
    expect(scrollWidth).toBeLessThanOrEqual(TABLET_VIEWPORT.width);
  });

  test('Test Case 2: Features display in 2-column grid layout at tablet viewport', async ({ page }) => {
    // Navigate to features section
    const featuresGrid = page.locator('.features__grid');
    await expect(featuresGrid).toBeVisible();

    // Get computed grid-template-columns style
    const gridColumns = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });

    // Should have exactly 2 columns (the value will be like "XXXpx XXXpx")
    const columnCount = gridColumns.split(' ').filter(col => col.trim() !== '').length;
    expect(columnCount).toBe(2);
  });

  test('Test Case 3: All sections visible and render correctly at tablet viewport', async ({ page }) => {
    // Check Hero section
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();
    await expect(page.locator('.hero__title')).toBeVisible();
    await expect(page.locator('.hero__headline')).toBeVisible();
    await expect(page.locator('.hero__cta')).toBeVisible();

    // Check Features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();
    await expect(page.locator('.features__title')).toBeVisible();
    await expect(page.locator('.features__grid')).toBeVisible();

    // Verify all 4 feature cards are visible
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(4);
    for (let i = 0; i < 4; i++) {
      await expect(featureCards.nth(i)).toBeVisible();
    }

    // Check Quick Start section
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();
    await expect(page.locator('.quickstart__title')).toBeVisible();
    await expect(page.locator('.code-block')).toBeVisible();

    // Check Tech Specs section
    const techspecsSection = page.locator('#techspecs');
    await expect(techspecsSection).toBeVisible();
    await expect(page.locator('.techspecs__title')).toBeVisible();
    await expect(page.locator('.techspecs__grid')).toBeVisible();

    // Check Footer
    const footer = page.locator('#footer');
    await expect(footer).toBeVisible();
    await expect(page.locator('.footer__copyright')).toBeVisible();
    await expect(page.locator('.footer__links')).toBeVisible();
  });

  test('no content is cut off or overlapping at tablet viewport', async ({ page }) => {
    // Check that all sections have proper dimensions and are within viewport
    const sections = ['#hero', '#features', '#quickstart', '#techspecs', '#footer'];

    for (const sectionId of sections) {
      const section = page.locator(sectionId);
      await expect(section).toBeVisible();

      // Get section bounding box
      const boundingBox = await section.boundingBox();
      expect(boundingBox).not.toBeNull();

      if (boundingBox) {
        // Section should not extend beyond viewport width
        expect(boundingBox.x).toBeGreaterThanOrEqual(0);
        expect(boundingBox.x + boundingBox.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width + 1); // +1 for rounding tolerance
      }
    }

    // Check that feature cards don't overlap
    const featureCards = page.locator('.feature-card');
    const count = await featureCards.count();

    for (let i = 0; i < count - 1; i++) {
      const currentCard = await featureCards.nth(i).boundingBox();
      const nextCard = await featureCards.nth(i + 1).boundingBox();

      if (currentCard && nextCard) {
        // Cards in same row should not overlap horizontally
        const sameRow = Math.abs(currentCard.y - nextCard.y) < currentCard.height / 2;
        if (sameRow) {
          expect(currentCard.x + currentCard.width).toBeLessThanOrEqual(nextCard.x + 1);
        }
      }
    }
  });

  test('hero CTA button has adequate touch target size', async ({ page }) => {
    const ctaButton = page.locator('.hero__cta');
    await expect(ctaButton).toBeVisible();

    const boundingBox = await ctaButton.boundingBox();
    expect(boundingBox).not.toBeNull();

    if (boundingBox) {
      // WCAG 2.1 AA requires minimum 44x44px touch targets
      expect(boundingBox.height).toBeGreaterThanOrEqual(44);
      expect(boundingBox.width).toBeGreaterThanOrEqual(44);
    }
  });

  test('navigation links are accessible at tablet viewport', async ({ page }) => {
    const navLinks = page.locator('.nav__links .nav__link');
    await expect(navLinks).toHaveCount(4);

    // All nav links should be visible
    for (let i = 0; i < 4; i++) {
      await expect(navLinks.nth(i)).toBeVisible();
    }
  });

  test('tech specs grid adapts appropriately for tablet', async ({ page }) => {
    const techspecsGrid = page.locator('.techspecs__grid');
    await expect(techspecsGrid).toBeVisible();

    // Get computed grid-template-columns style
    const gridColumns = await techspecsGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });

    // Should have 3 columns for tablet (as defined in responsive.css)
    const columnCount = gridColumns.split(' ').filter(col => col.trim() !== '').length;
    expect(columnCount).toBe(3);
  });

  test('code block is readable and not cut off at tablet viewport', async ({ page }) => {
    const codeBlock = page.locator('.code-block');
    await expect(codeBlock).toBeVisible();

    const boundingBox = await codeBlock.boundingBox();
    expect(boundingBox).not.toBeNull();

    if (boundingBox) {
      // Code block should fit within viewport
      expect(boundingBox.x).toBeGreaterThanOrEqual(0);
      expect(boundingBox.x + boundingBox.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);
    }

    // Code content should be visible
    const codeContent = page.locator('.code-block__content');
    await expect(codeContent).toBeVisible();
  });
});

test.describe('Responsive Design - Viewport Comparisons', () => {
  test('features grid changes from 2 columns (tablet) to 4 columns (desktop)', async ({ page }) => {
    // First check tablet viewport
    await page.setViewportSize(TABLET_VIEWPORT);
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    const featuresGrid = page.locator('.features__grid');
    let gridColumns = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });
    let columnCount = gridColumns.split(' ').filter(col => col.trim() !== '').length;
    expect(columnCount).toBe(2);

    // Now check desktop viewport
    await page.setViewportSize(DESKTOP_VIEWPORT);
    await page.waitForTimeout(100); // Allow reflow

    gridColumns = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });
    columnCount = gridColumns.split(' ').filter(col => col.trim() !== '').length;
    expect(columnCount).toBe(4);
  });

  test('features grid changes from 1 column (mobile) to 2 columns (tablet)', async ({ page }) => {
    // First check mobile viewport
    await page.setViewportSize(MOBILE_VIEWPORT);
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    const featuresGrid = page.locator('.features__grid');
    let gridColumns = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });
    let columnCount = gridColumns.split(' ').filter(col => col.trim() !== '').length;
    expect(columnCount).toBe(1);

    // Now check tablet viewport
    await page.setViewportSize(TABLET_VIEWPORT);
    await page.waitForTimeout(100); // Allow reflow

    gridColumns = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });
    columnCount = gridColumns.split(' ').filter(col => col.trim() !== '').length;
    expect(columnCount).toBe(2);
  });
});

test.describe('Responsive Design - Additional Tablet Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(TABLET_VIEWPORT);
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('page title and meta viewport are correct', async ({ page }) => {
    // Check page title
    await expect(page).toHaveTitle(/MirDB/);

    // Check viewport meta tag exists
    const viewportMeta = await page.locator('meta[name="viewport"]').getAttribute('content');
    expect(viewportMeta).toContain('width=device-width');
  });

  test('container has appropriate padding for tablet', async ({ page }) => {
    const container = page.locator('.container').first();
    await expect(container).toBeVisible();

    const paddingLeft = await container.evaluate((el) => {
      return window.getComputedStyle(el).paddingLeft;
    });
    const paddingRight = await container.evaluate((el) => {
      return window.getComputedStyle(el).paddingRight;
    });

    // Padding should be at least spacing-xl (2rem = 32px) for tablet
    expect(parseFloat(paddingLeft)).toBeGreaterThanOrEqual(24);
    expect(parseFloat(paddingRight)).toBeGreaterThanOrEqual(24);
  });

  test('scrolling works smoothly through all sections', async ({ page }) => {
    // Scroll to each section and verify visibility
    const sections = ['#hero', '#features', '#quickstart', '#techspecs', '#footer'];

    for (const sectionId of sections) {
      await page.locator(sectionId).scrollIntoViewIfNeeded();
      await expect(page.locator(sectionId)).toBeInViewport();
    }
  });

  test('landscape tablet orientation also renders correctly', async ({ page }) => {
    // Test landscape tablet (e.g., iPad landscape)
    await page.setViewportSize({ width: 1024, height: 768 });
    await page.waitForTimeout(100);

    // Page should still render without horizontal scroll
    const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);
    expect(scrollWidth).toBeLessThanOrEqual(clientWidth);

    // All sections should still be visible
    await expect(page.locator('#hero')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('#quickstart')).toBeVisible();
    await expect(page.locator('#techspecs')).toBeVisible();
    await expect(page.locator('#footer')).toBeVisible();
  });
});
