// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E tests for mobile viewport responsiveness
 * Tests that the homepage displays correctly on mobile devices (375px width)
 */

test.describe('Responsive Design - Mobile Viewport', () => {
  test.beforeEach(async ({ page }) => {
    // Set mobile viewport (iPhone-equivalent: 375px width)
    await page.setViewportSize({ width: 375, height: 812 });
  });

  test('TC1: No horizontal overflow on body at 375px width', async ({ page }) => {
    // Load homepage at mobile viewport
    await page.goto('/');

    // Wait for page to fully load
    await page.waitForLoadState('domcontentloaded');

    // Check that body does not have horizontal overflow
    const bodyOverflow = await page.evaluate(() => {
      const body = document.body;
      const html = document.documentElement;

      // Check if content overflows horizontally
      const hasHorizontalScroll = body.scrollWidth > window.innerWidth;
      const htmlOverflow = html.scrollWidth > window.innerWidth;

      return {
        bodyScrollWidth: body.scrollWidth,
        windowInnerWidth: window.innerWidth,
        hasHorizontalScroll: hasHorizontalScroll || htmlOverflow
      };
    });

    // Body scroll width should not exceed viewport width (with small tolerance)
    expect(bodyOverflow.bodyScrollWidth).toBeLessThanOrEqual(375 + 1);
    expect(bodyOverflow.hasHorizontalScroll).toBe(false);

    // Verify all main sections are accessible
    await expect(page.locator('.hero')).toBeVisible();
    await expect(page.locator('.features-section')).toBeVisible();
    await expect(page.locator('.quickstart-section')).toBeVisible();
    await expect(page.locator('.footer')).toBeVisible();
  });

  test('TC2: CTA buttons are appropriately sized for touch on mobile', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Get CTA buttons in hero section
    const primaryBtn = page.locator('.hero-cta .btn-primary');
    const secondaryBtn = page.locator('.hero-cta .btn-secondary');

    // Verify buttons are visible
    await expect(primaryBtn).toBeVisible();
    await expect(secondaryBtn).toBeVisible();

    // Check button dimensions - should be touch-friendly (min 44px height, good width)
    const primaryBox = await primaryBtn.boundingBox();
    const secondaryBox = await secondaryBtn.boundingBox();

    // Buttons should have adequate touch target size (at least 44px height per Apple HIG)
    expect(primaryBox.height).toBeGreaterThanOrEqual(44);
    expect(secondaryBox.height).toBeGreaterThanOrEqual(44);

    // Buttons should be appropriately wide on mobile (either full-width or substantial)
    // At 375px viewport, with max-width 280px, buttons should be at least 200px wide
    expect(primaryBox.width).toBeGreaterThanOrEqual(200);
    expect(secondaryBox.width).toBeGreaterThanOrEqual(200);

    // Verify buttons stack vertically on mobile (column layout)
    const ctaContainer = page.locator('.hero-cta');
    const ctaFlexDirection = await ctaContainer.evaluate(el => {
      return window.getComputedStyle(el).flexDirection;
    });
    expect(ctaFlexDirection).toBe('column');
  });

  test('TC3: Navigation is accessible on mobile', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Verify navigation bar is visible
    const navbar = page.locator('.navbar');
    await expect(navbar).toBeVisible();

    // Check that logo is visible
    const logo = page.locator('.logo');
    await expect(logo).toBeVisible();

    // Verify nav links are present (may be in hamburger menu or visible)
    const navLinks = page.locator('.nav-links');

    // Check if navigation links are accessible
    // On mobile, links might be condensed but should still be functional
    const navLinksVisible = await navLinks.isVisible();

    if (navLinksVisible) {
      // If visible, verify links are reachable
      const featuresLink = page.locator('.nav-links a[href="#features"]');
      const quickStartLink = page.locator('.nav-links a[href="#quick-start"]');
      const githubLink = page.locator('.nav-links a[href*="github"]');

      await expect(featuresLink).toBeVisible();
      await expect(quickStartLink).toBeVisible();
      await expect(githubLink).toBeVisible();

      // Check that links are clickable (have appropriate touch target)
      const featuresBox = await featuresLink.boundingBox();
      expect(featuresBox.height).toBeGreaterThanOrEqual(20);
    }

    // Verify navigation container fits within viewport
    const navContainer = page.locator('.nav-container');
    const navBox = await navContainer.boundingBox();
    expect(navBox.width).toBeLessThanOrEqual(375);
  });

  test('TC4: Text is readable without zooming (minimum 16px for body text)', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Check body font size (should be at least 16px for readability)
    const bodyFontSize = await page.evaluate(() => {
      const body = document.body;
      const style = window.getComputedStyle(body);
      return parseFloat(style.fontSize);
    });
    expect(bodyFontSize).toBeGreaterThanOrEqual(16);

    // Check hero description font size
    const heroDescFontSize = await page.evaluate(() => {
      const heroDesc = document.querySelector('.hero-description');
      if (!heroDesc) return 16;
      const style = window.getComputedStyle(heroDesc);
      return parseFloat(style.fontSize);
    });
    expect(heroDescFontSize).toBeGreaterThanOrEqual(16);

    // Check feature description font size
    const featureDescFontSize = await page.evaluate(() => {
      const featureDesc = document.querySelector('.feature-description');
      if (!featureDesc) return 16;
      const style = window.getComputedStyle(featureDesc);
      return parseFloat(style.fontSize);
    });
    expect(featureDescFontSize).toBeGreaterThanOrEqual(16);

    // Check that line height is adequate for readability
    const lineHeight = await page.evaluate(() => {
      const body = document.body;
      const style = window.getComputedStyle(body);
      return parseFloat(style.lineHeight) / parseFloat(style.fontSize);
    });
    // Line height should be at least 1.4 for readability
    expect(lineHeight).toBeGreaterThanOrEqual(1.4);
  });

  test('Hero section is readable and properly formatted on mobile', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Verify hero section is visible and within viewport
    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    // Check hero title
    const heroTitle = page.locator('.hero-title');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toContainText('MirDB');

    // Check hero subtitle
    const heroSubtitle = page.locator('.hero-subtitle');
    await expect(heroSubtitle).toBeVisible();

    // Check hero description
    const heroDescription = page.locator('.hero-description');
    await expect(heroDescription).toBeVisible();

    // Verify hero container fits within viewport
    const heroContainer = page.locator('.hero-container');
    const heroBox = await heroContainer.boundingBox();
    expect(heroBox.width).toBeLessThanOrEqual(375);
  });

  test('Features section stacks vertically on mobile', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Navigate to features section
    const featuresSection = page.locator('.features-section');
    await expect(featuresSection).toBeVisible();

    // Check that features grid is single column on mobile
    const featuresGrid = page.locator('.features-grid');
    const gridColumns = await featuresGrid.evaluate(el => {
      const style = window.getComputedStyle(el);
      return style.gridTemplateColumns;
    });

    // Should be single column (1fr or similar) on mobile
    // Grid with minmax(320px, 1fr) at 375px should collapse to 1 column
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBe(6);

    // Verify cards are stacked vertically (each card takes full available width)
    const firstCard = featureCards.first();
    const firstCardBox = await firstCard.boundingBox();

    // Card should span most of the viewport width (accounting for padding)
    expect(firstCardBox.width).toBeGreaterThanOrEqual(300);
    expect(firstCardBox.width).toBeLessThanOrEqual(375);

    // All feature cards should be visible and accessible
    for (let i = 0; i < cardCount; i++) {
      await expect(featureCards.nth(i)).toBeVisible();
    }
  });

  test('Code blocks are functional on mobile (may have horizontal scroll)', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Navigate to quick start section
    const quickstartSection = page.locator('.quickstart-section');
    await expect(quickstartSection).toBeVisible();

    // Check code blocks are visible
    const codeBlocks = page.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThanOrEqual(1);

    // Verify first code block is accessible
    const firstCodeBlock = codeBlocks.first();
    await expect(firstCodeBlock).toBeVisible();

    // Code blocks should have overflow-x: auto for horizontal scrolling
    const preElement = firstCodeBlock.locator('pre');
    const preOverflow = await preElement.evaluate(el => {
      const style = window.getComputedStyle(el);
      return style.overflowX;
    });
    expect(preOverflow).toBe('auto');

    // Code content should be readable
    const codeContent = firstCodeBlock.locator('code');
    await expect(codeContent).toBeVisible();

    // Verify code block container fits within viewport (or has scroll)
    const codeBlockBox = await firstCodeBlock.boundingBox();
    expect(codeBlockBox.width).toBeLessThanOrEqual(375);
  });
});
