// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: Responsive Design - Mobile
 * Scenario: Verify the homepage renders correctly on mobile devices (REQ-10)
 */

// Mobile viewport configuration (iPhone SE dimensions)
const MOBILE_VIEWPORT = { width: 375, height: 667 };

test.describe('Responsive Design - Mobile', () => {
  test.beforeEach(async ({ page }) => {
    // Set mobile viewport before navigating
    await page.setViewportSize(MOBILE_VIEWPORT);
    await page.goto('/');
  });

  /**
   * Test Case 1: Page displays without horizontal scrollbar at 375px width
   * Input: Load page at 375px width (mobile)
   * Expected: Page displays without horizontal scrollbar
   */
  test('TC1: Page displays without horizontal scrollbar at 375px width', async ({ page }) => {
    // Wait for page to fully load
    await page.waitForLoadState('domcontentloaded');

    // Check that the document width does not exceed viewport width
    const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);

    // Document should not be wider than viewport (no horizontal scroll)
    expect(scrollWidth).toBeLessThanOrEqual(clientWidth + 1); // +1 for rounding tolerance

    // Also verify no overflow by checking body's computed style
    const overflowX = await page.evaluate(() => {
      const body = document.body;
      return window.getComputedStyle(body).overflowX;
    });

    // Verify there's no visible horizontal scrollbar
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });

  /**
   * Test Case 2: Hero section content is readable and CTAs are tappable
   * Input: Check hero section on mobile
   * Expected: Hero section content is readable and CTAs are tappable
   */
  test('TC2: Hero section content is readable and CTAs are tappable', async ({ page }) => {
    // Verify hero section is visible
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify product name is visible and readable
    const productName = page.locator('#product-name');
    await expect(productName).toBeVisible();

    // Check font size is readable on mobile (at least 16px equivalent)
    const fontSize = await productName.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    expect(fontSize).toBeGreaterThanOrEqual(16);

    // Verify tagline is visible
    const tagline = page.locator('#tagline');
    await expect(tagline).toBeVisible();

    // Verify CTA buttons are visible and tappable
    const getStartedBtn = page.locator('#get-started-btn');
    const githubBtn = page.locator('#github-btn');

    await expect(getStartedBtn).toBeVisible();
    await expect(githubBtn).toBeVisible();

    // Check button size is tappable (at least 44px touch target recommended by Apple)
    const getStartedBtnBox = await getStartedBtn.boundingBox();
    expect(getStartedBtnBox).not.toBeNull();
    expect(getStartedBtnBox.height).toBeGreaterThanOrEqual(44);
    expect(getStartedBtnBox.width).toBeGreaterThanOrEqual(44);

    const githubBtnBox = await githubBtn.boundingBox();
    expect(githubBtnBox).not.toBeNull();
    expect(githubBtnBox.height).toBeGreaterThanOrEqual(44);
    expect(githubBtnBox.width).toBeGreaterThanOrEqual(44);

    // Verify buttons are clickable (test Get Started button)
    await getStartedBtn.click();
    await page.waitForTimeout(500);

    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();
  });

  /**
   * Test Case 3: Value propositions stack vertically instead of 3 columns
   * Input: Check value propositions on mobile
   * Expected: Value propositions stack vertically instead of 3 columns
   */
  test('TC3: Value propositions stack vertically instead of 3 columns', async ({ page }) => {
    // Navigate to value propositions section
    const valuePropsSection = page.locator('[data-testid="value-propositions-section"]');
    await expect(valuePropsSection).toBeVisible();

    // Get all value proposition cards
    const valuePropsGrid = page.locator('[data-testid="value-props-grid"]');
    const valuePropCards = page.locator('.value-prop-card');

    // Verify there are 3 value prop cards
    const cardCount = await valuePropCards.count();
    expect(cardCount).toBe(3);

    // Get bounding boxes of all cards
    const firstCard = valuePropCards.nth(0);
    const secondCard = valuePropCards.nth(1);
    const thirdCard = valuePropCards.nth(2);

    const firstBox = await firstCard.boundingBox();
    const secondBox = await secondCard.boundingBox();
    const thirdBox = await thirdCard.boundingBox();

    expect(firstBox).not.toBeNull();
    expect(secondBox).not.toBeNull();
    expect(thirdBox).not.toBeNull();

    // Verify cards are stacked vertically (each card below the previous)
    // In vertical layout, the top of each subsequent card should be below the previous card's bottom
    expect(secondBox.y).toBeGreaterThan(firstBox.y);
    expect(thirdBox.y).toBeGreaterThan(secondBox.y);

    // Verify the grid has single column layout by checking CSS
    const gridColumns = await valuePropsGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });

    // Should be single column (1fr) or auto
    // The computed value will be the actual pixel width, but for single column
    // there should only be one value (not multiple space-separated values like "200px 200px 200px")
    const columnCount = gridColumns.split(' ').filter(col => col.trim() !== '').length;
    expect(columnCount).toBe(1);
  });

  /**
   * Test Case 4: Code blocks are scrollable horizontally without breaking layout
   * Input: Check code blocks on mobile
   * Expected: Code blocks are scrollable horizontally without breaking layout
   */
  test('TC4: Code blocks are scrollable horizontally without breaking layout', async ({ page }) => {
    // Navigate to quick start section where code blocks are located
    const quickStartSection = page.locator('#quick-start');
    await quickStartSection.scrollIntoViewIfNeeded();

    // Get all code blocks
    const codeBlocks = page.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();

    // Verify there are code blocks on the page
    expect(codeBlockCount).toBeGreaterThan(0);

    // Check each code block
    for (let i = 0; i < Math.min(codeBlockCount, 3); i++) {
      const codeBlock = codeBlocks.nth(i);
      await codeBlock.scrollIntoViewIfNeeded();

      // Verify code block is visible
      await expect(codeBlock).toBeVisible();

      // Get the code block's bounding box
      const codeBlockBox = await codeBlock.boundingBox();
      expect(codeBlockBox).not.toBeNull();

      // Code block should fit within viewport width
      expect(codeBlockBox.x).toBeGreaterThanOrEqual(0);
      expect(codeBlockBox.x + codeBlockBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width + 10);

      // Check that the pre element inside has overflow-x: auto for scrolling
      const preElement = codeBlock.locator('pre');
      const overflowX = await preElement.evaluate((el) => {
        return window.getComputedStyle(el).overflowX;
      });
      expect(['auto', 'scroll']).toContain(overflowX);
    }

    // Verify page still has no horizontal scrollbar after checking code blocks
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });

  /**
   * Test Case 5: Mobile navigation (hamburger menu) is functional
   * Input: Test navigation on mobile
   * Expected: Mobile navigation (hamburger menu) is functional
   */
  test('TC5: Mobile navigation (hamburger menu) is functional', async ({ page }) => {
    // Check that the hamburger menu button is visible on mobile
    const hamburgerBtn = page.locator('[data-testid="mobile-menu-toggle"]');
    await expect(hamburgerBtn).toBeVisible();

    // Check that nav links are hidden initially on mobile
    const navLinks = page.locator('.nav-links');
    const isHidden = await navLinks.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0';
    });
    expect(isHidden).toBe(true);

    // Click the hamburger menu to open navigation
    await hamburgerBtn.click();

    // Wait for animation
    await page.waitForTimeout(300);

    // Verify nav links are now visible
    const isVisible = await navLinks.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.display !== 'none' && style.visibility !== 'hidden' && style.opacity !== '0';
    });
    expect(isVisible).toBe(true);

    // Verify all navigation items are accessible
    const featuresLink = page.locator('.nav-links a[href="#features"]');
    const quickStartLink = page.locator('.nav-links a[href="#quick-start"]');
    const githubLink = page.locator('.nav-links a[href*="github.com"]');

    await expect(featuresLink).toBeVisible();
    await expect(quickStartLink).toBeVisible();
    await expect(githubLink).toBeVisible();

    // Test that clicking a nav link works
    await featuresLink.click();
    await page.waitForTimeout(500);

    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Navigation should close after clicking a link (or the menu should still be usable)
    // This depends on implementation - verify page navigation worked
    const currentUrl = page.url();
    expect(currentUrl).toContain('#features');
  });
});
