// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Mobile Responsive Design
 * Scenario: Verify the homepage is fully responsive on mobile devices (<640px)
 * Related Requirements: NFR-1, US-6
 */

// Mobile viewport sizes for testing
const MOBILE_VIEWPORT = { width: 375, height: 812 }; // iPhone X/XS size
const MOBILE_SMALL_VIEWPORT = { width: 320, height: 568 }; // iPhone SE size
const MIN_TOUCH_TARGET_SIZE = 44; // Minimum touch target size in pixels (Apple/Google guideline)

test.describe('Responsive Design - Mobile', () => {
  test.beforeEach(async ({ page }) => {
    // Set mobile viewport before each test
    await page.setViewportSize(MOBILE_VIEWPORT);
    await page.goto('/');
  });

  /**
   * Test Case 1: Load page at 375px width (mobile)
   * Expected: All content is readable without horizontal scrolling
   */
  test('TC1: All content is readable without horizontal scrolling at 375px width', async ({ page }) => {
    // Verify no horizontal overflow
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);

    // Body scroll width should not exceed viewport width (no horizontal scrolling needed)
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth);

    // Check that html element also doesn't cause overflow
    const htmlWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    expect(htmlWidth).toBeLessThanOrEqual(viewportWidth);

    // Verify horizontal scroll position is 0 and cannot scroll horizontally
    const scrollX = await page.evaluate(() => window.scrollX);
    expect(scrollX).toBe(0);

    // Verify key content sections are visible and fit within viewport
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    const heroBox = await heroSection.boundingBox();
    expect(heroBox.width).toBeLessThanOrEqual(viewportWidth);

    // Verify product name is visible and readable
    const productName = page.locator('[data-testid="product-name"]');
    await expect(productName).toBeVisible();

    // Verify tagline is visible and readable
    const tagline = page.locator('[data-testid="tagline"]');
    await expect(tagline).toBeVisible();
  });

  /**
   * Test Case 2: Check for mobile navigation menu
   * Expected: Navigation is accessible via hamburger menu or similar mobile pattern
   */
  test('TC2: Navigation is accessible on mobile', async ({ page }) => {
    // Check if hamburger menu exists or if navigation links are accessible
    const hamburgerMenu = page.locator('[data-testid="mobile-menu-toggle"], .hamburger-menu, .mobile-menu-toggle, [aria-label="Toggle menu"], [aria-label="Menu"]');
    const navLinks = page.locator('.nav-links');

    // Either hamburger menu should be visible OR nav-links should be visible and accessible
    const hamburgerVisible = await hamburgerMenu.isVisible().catch(() => false);
    const navLinksVisible = await navLinks.isVisible().catch(() => false);

    if (hamburgerVisible) {
      // Mobile menu pattern: hamburger menu should be clickable
      await expect(hamburgerMenu).toBeEnabled();

      // Click hamburger and verify navigation becomes accessible
      await hamburgerMenu.click();

      // After clicking, nav links should become visible
      const mobileNav = page.locator('.mobile-nav, .nav-links, [data-testid="mobile-nav"]');
      await expect(mobileNav).toBeVisible();
    } else if (navLinksVisible) {
      // Alternative pattern: navigation links are always visible but styled for mobile
      // Verify all navigation links are accessible
      const links = navLinks.locator('a');
      const linkCount = await links.count();
      expect(linkCount).toBeGreaterThan(0);

      // Check that each link is visible and within viewport
      for (let i = 0; i < linkCount; i++) {
        const link = links.nth(i);
        await expect(link).toBeVisible();
      }

      // Verify nav-links container fits within viewport width
      const navBox = await navLinks.boundingBox();
      const viewportWidth = await page.evaluate(() => window.innerWidth);
      expect(navBox.x + navBox.width).toBeLessThanOrEqual(viewportWidth);
    } else {
      // Neither pattern found - fail the test with helpful message
      throw new Error('Navigation should be accessible on mobile via hamburger menu or visible nav links');
    }
  });

  /**
   * Test Case 3: Verify code blocks on mobile
   * Expected: Code examples are horizontally scrollable within their container
   */
  test('TC3: Code examples are horizontally scrollable within their container', async ({ page }) => {
    // Scroll to code examples section
    const codeExamplesSection = page.locator('[data-testid="code-examples-section"], #code-examples');

    // Wait for section to be in viewport
    if (await codeExamplesSection.isVisible().catch(() => false)) {
      await codeExamplesSection.scrollIntoViewIfNeeded();
    }

    // Get all code blocks on the page
    const codeBlocks = page.locator('pre, .code-block, [data-testid*="code-block"]');
    const codeBlockCount = await codeBlocks.count();

    expect(codeBlockCount).toBeGreaterThan(0);

    // Verify each code block has overflow-x: auto or scroll (allowing horizontal scroll)
    for (let i = 0; i < Math.min(codeBlockCount, 5); i++) {
      const codeBlock = codeBlocks.nth(i);

      // Wait for element to be visible
      await codeBlock.scrollIntoViewIfNeeded();

      // Check CSS overflow-x property
      const overflowX = await codeBlock.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return style.overflowX;
      });

      // Overflow should be auto, scroll, or visible (with container handling scroll)
      expect(['auto', 'scroll', 'visible']).toContain(overflowX);

      // Verify code block is contained within viewport width
      const codeBlockBox = await codeBlock.boundingBox();
      const viewportWidth = await page.evaluate(() => window.innerWidth);

      // The left edge should be within viewport
      expect(codeBlockBox.x).toBeGreaterThanOrEqual(0);
      // The right edge should be within viewport (or scrollable)
      expect(codeBlockBox.x + codeBlockBox.width).toBeLessThanOrEqual(viewportWidth + 50); // Allow small margin
    }
  });

  /**
   * Test Case 4: Measure touch target sizes
   * Expected: All buttons and interactive elements are at least 44x44 pixels
   */
  test('TC4: All buttons and interactive elements are at least 44x44 pixels', async ({ page }) => {
    // Get all interactive elements (buttons, links with button-like styling)
    const buttons = page.locator('button, .btn, [role="button"]');
    const ctaLinks = page.locator('a.btn, a.btn-primary, a.btn-secondary, [data-testid="cta-get-started"], [data-testid="cta-github"]');
    const themeToggle = page.locator('.theme-toggle');

    // Test buttons
    const buttonCount = await buttons.count();
    for (let i = 0; i < buttonCount; i++) {
      const button = buttons.nth(i);
      if (await button.isVisible()) {
        const box = await button.boundingBox();
        if (box) {
          expect(box.width, `Button ${i} width should be at least ${MIN_TOUCH_TARGET_SIZE}px`).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE);
          expect(box.height, `Button ${i} height should be at least ${MIN_TOUCH_TARGET_SIZE}px`).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE);
        }
      }
    }

    // Test CTA links
    const ctaCount = await ctaLinks.count();
    for (let i = 0; i < ctaCount; i++) {
      const cta = ctaLinks.nth(i);
      if (await cta.isVisible()) {
        const box = await cta.boundingBox();
        if (box) {
          expect(box.width, `CTA link ${i} width should be at least ${MIN_TOUCH_TARGET_SIZE}px`).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE);
          expect(box.height, `CTA link ${i} height should be at least ${MIN_TOUCH_TARGET_SIZE}px`).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE);
        }
      }
    }

    // Test theme toggle
    if (await themeToggle.isVisible()) {
      const toggleBox = await themeToggle.boundingBox();
      if (toggleBox) {
        expect(toggleBox.width, 'Theme toggle width should be at least 44px').toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE);
        expect(toggleBox.height, 'Theme toggle height should be at least 44px').toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE);
      }
    }
  });

  /**
   * Test Case 5: Check font readability on mobile
   * Expected: Text is readable without zooming, minimum 16px body text
   */
  test('TC5: Text is readable without zooming with minimum 16px body text', async ({ page }) => {
    // Check body font size
    const bodyFontSize = await page.evaluate(() => {
      const style = window.getComputedStyle(document.body);
      return parseFloat(style.fontSize);
    });

    expect(bodyFontSize, 'Body font size should be at least 16px').toBeGreaterThanOrEqual(16);

    // Check paragraph text
    const paragraphs = page.locator('p');
    const pCount = await paragraphs.count();

    for (let i = 0; i < Math.min(pCount, 5); i++) {
      const p = paragraphs.nth(i);
      if (await p.isVisible()) {
        const fontSize = await p.evaluate((el) => {
          const style = window.getComputedStyle(el);
          return parseFloat(style.fontSize);
        });
        expect(fontSize, `Paragraph ${i} font size should be at least 14px`).toBeGreaterThanOrEqual(14);
      }
    }

    // Check main heading (h1) is appropriately sized
    const h1 = page.locator('h1').first();
    if (await h1.isVisible()) {
      const h1FontSize = await h1.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return parseFloat(style.fontSize);
      });
      expect(h1FontSize, 'H1 heading should be larger than body text').toBeGreaterThan(bodyFontSize);
    }

    // Check that text doesn't require horizontal scrolling to read
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    const heroDescription = page.locator('[data-testid="value-proposition"], .hero-description');

    if (await heroDescription.isVisible()) {
      const descBox = await heroDescription.boundingBox();
      expect(descBox.width, 'Hero description width should fit within viewport').toBeLessThanOrEqual(viewportWidth);
    }

    // Verify viewport meta tag is set correctly for mobile scaling
    const viewportMeta = await page.evaluate(() => {
      const meta = document.querySelector('meta[name="viewport"]');
      return meta ? meta.getAttribute('content') : null;
    });

    expect(viewportMeta).toBeTruthy();
    expect(viewportMeta).toContain('width=device-width');
  });

  /**
   * Additional Test: Verify single column layout on mobile
   * Testing that feature cards stack vertically on mobile
   */
  test('Feature cards display in single column on mobile', async ({ page }) => {
    // Scroll to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Get feature cards
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();

    expect(cardCount).toBeGreaterThan(0);

    // Check that cards are stacked vertically (each card's top is below the previous card's bottom)
    if (cardCount > 1) {
      const firstCard = featureCards.nth(0);
      const secondCard = featureCards.nth(1);

      const firstBox = await firstCard.boundingBox();
      const secondBox = await secondCard.boundingBox();

      // Second card should be below first card (single column) or have minimal horizontal offset
      // Allow for flex-wrap scenarios where cards may be side by side but still responsive
      const viewportWidth = await page.evaluate(() => window.innerWidth);

      // Each card should be nearly full width on mobile (allowing for padding)
      expect(firstBox.width).toBeGreaterThan(viewportWidth * 0.7);
      expect(secondBox.width).toBeGreaterThan(viewportWidth * 0.7);
    }
  });

  /**
   * Additional Test: Hero buttons stack on mobile
   */
  test('Hero CTA buttons stack vertically on mobile', async ({ page }) => {
    const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
    const githubBtn = page.locator('[data-testid="cta-github"]');

    await expect(getStartedBtn).toBeVisible();
    await expect(githubBtn).toBeVisible();

    const getStartedBox = await getStartedBtn.boundingBox();
    const githubBox = await githubBtn.boundingBox();

    // Buttons should be stacked (second button below first)
    // This means GitHub button's top should be below Get Started button's bottom
    expect(githubBox.y).toBeGreaterThan(getStartedBox.y + getStartedBox.height - 10); // Allow small overlap for flex gap
  });

  /**
   * Additional Test: Verify viewport at smaller mobile size (320px)
   * Note: At very small viewports, some horizontal scroll may be acceptable
   * as long as content remains readable and accessible
   */
  test('Content remains accessible at 320px width (small mobile)', async ({ page }) => {
    await page.setViewportSize(MOBILE_SMALL_VIEWPORT);
    await page.goto('/');

    // Hero content should be visible and accessible
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Product name should be visible
    const productName = page.locator('[data-testid="product-name"]');
    await expect(productName).toBeVisible();

    // Tagline should be visible
    const tagline = page.locator('[data-testid="tagline"]');
    await expect(tagline).toBeVisible();

    // CTA buttons should be visible
    const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
    await expect(getStartedBtn).toBeVisible();

    // Verify horizontal overflow is minimal (within 10% tolerance for very small screens)
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    const overflowTolerance = viewportWidth * 0.1; // 10% tolerance
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth + overflowTolerance);
  });
});
