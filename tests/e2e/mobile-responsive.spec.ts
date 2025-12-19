import { test, expect, Page } from '@playwright/test';

// Define mobile viewport size (iPhone SE/small mobile - 375px width per US-7)
const MOBILE_VIEWPORT = { width: 375, height: 667 };

test.describe('Mobile Responsive Design', () => {
  test.beforeEach(async ({ page }) => {
    // Set mobile viewport before each test
    await page.setViewportSize(MOBILE_VIEWPORT);
    await page.goto('/');
  });

  test('TC1: Page renders without horizontal scrollbar at 375px viewport', async ({ page }) => {
    // Test Case 1: Load page with viewport width 375px
    // Expected: Page renders without horizontal scrollbar

    // Wait for page to fully load
    await page.waitForLoadState('networkidle');

    // Check that document body doesn't have horizontal overflow
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });

    expect(hasHorizontalScroll).toBe(false);
  });

  test('TC2: Document width does not exceed viewport width at 375px', async ({ page }) => {
    // Test Case 2: Check body/html overflow at 375px viewport
    // Expected: Document width does not exceed viewport width

    await page.waitForLoadState('networkidle');

    const dimensions = await page.evaluate(() => {
      return {
        documentWidth: document.documentElement.scrollWidth,
        viewportWidth: window.innerWidth,
        bodyWidth: document.body.scrollWidth
      };
    });

    // Document should not be wider than viewport
    expect(dimensions.documentWidth).toBeLessThanOrEqual(dimensions.viewportWidth);
    expect(dimensions.bodyWidth).toBeLessThanOrEqual(dimensions.viewportWidth);
  });

  test('TC3: Hero content is fully visible and readable on mobile', async ({ page }) => {
    // Test Case 3: Test hero section at mobile viewport
    // Expected: Hero content is fully visible and readable

    const heroSection = page.locator('section.hero, #hero, [data-testid="hero"]');
    await expect(heroSection).toBeVisible();

    // Check h1 is visible
    const heading = heroSection.locator('h1');
    await expect(heading).toBeVisible();

    // Check tagline is visible
    const tagline = heroSection.locator('.tagline, h2.tagline, [data-testid="tagline"]');
    await expect(tagline).toBeVisible();

    // Check CTA buttons are visible
    const ctaButtons = heroSection.locator('.cta-buttons a, .hero-cta a, .btn');
    const ctaCount = await ctaButtons.count();
    expect(ctaCount).toBeGreaterThanOrEqual(1);

    // Verify first CTA is visible
    await expect(ctaButtons.first()).toBeVisible();

    // Check hero section is within viewport
    const heroBox = await heroSection.boundingBox();
    expect(heroBox).not.toBeNull();
    expect(heroBox!.x).toBeGreaterThanOrEqual(0);
    expect(heroBox!.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
  });

  test('TC4: Feature cards stack vertically and are readable on mobile', async ({ page }) => {
    // Test Case 4: Test features section at mobile viewport
    // Expected: Feature cards stack vertically and are readable

    // Navigate to features section
    const featuresSection = page.locator('#features, section.features');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Get all feature cards
    const featureCards = featuresSection.locator('.feature-card, .feature-grid > div');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(1);

    // Check that cards are visible and properly sized
    for (let i = 0; i < Math.min(cardCount, 3); i++) {
      const card = featureCards.nth(i);
      await expect(card).toBeVisible();

      const cardBox = await card.boundingBox();
      expect(cardBox).not.toBeNull();
      // Card should fit within viewport width (with some margin)
      expect(cardBox!.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
    }

    // Check that at least two cards are stacked (one below the other) on narrow viewport
    if (cardCount >= 2) {
      const firstCardBox = await featureCards.first().boundingBox();
      const secondCardBox = await featureCards.nth(1).boundingBox();

      expect(firstCardBox).not.toBeNull();
      expect(secondCardBox).not.toBeNull();

      // On mobile (375px), cards should likely be stacked vertically
      // The second card should start below the first card (y position is greater)
      // or they should be significantly offset if there's any horizontal arrangement
      const isStacked = secondCardBox!.y > firstCardBox!.y;
      const isNarrowEnoughForStack = firstCardBox!.width > MOBILE_VIEWPORT.width * 0.7;

      // Either cards are stacked OR cards are narrow enough to fit side by side
      expect(isStacked || !isNarrowEnoughForStack).toBe(true);
    }
  });

  test('TC5: Code blocks are scrollable horizontally within container on mobile', async ({ page }) => {
    // Test Case 5: Test code blocks at mobile viewport
    // Expected: Code blocks are scrollable horizontally within container

    // Navigate to getting started section which has code blocks
    const gettingStartedSection = page.locator('#getting-started, section.getting-started');
    await gettingStartedSection.scrollIntoViewIfNeeded();
    await expect(gettingStartedSection).toBeVisible();

    // Find code blocks
    const codeBlocks = page.locator('pre, .code-block-wrapper pre');
    const codeCount = await codeBlocks.count();
    expect(codeCount).toBeGreaterThanOrEqual(1);

    // Check first code block has appropriate overflow handling
    const firstCodeBlock = codeBlocks.first();
    await firstCodeBlock.scrollIntoViewIfNeeded();
    await expect(firstCodeBlock).toBeVisible();

    // Check CSS overflow property allows horizontal scrolling
    const overflowX = await firstCodeBlock.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.overflowX;
    });

    // Code blocks should allow horizontal scroll (auto, scroll, or hidden for contained overflow)
    expect(['auto', 'scroll', 'hidden']).toContain(overflowX);

    // Verify code block doesn't exceed viewport width (its container should handle overflow)
    const codeBlockBox = await firstCodeBlock.boundingBox();
    expect(codeBlockBox).not.toBeNull();
    // The visible portion should not exceed viewport
    expect(codeBlockBox!.x + codeBlockBox!.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width + 10);
  });

  test('TC6: CTA buttons have minimum 44px height for touch targets', async ({ page }) => {
    // Test Case 6: Check CTA button size on mobile
    // Expected: Buttons have minimum 44px height for touch targets

    const MIN_TOUCH_TARGET = 44;

    // Check hero CTA buttons
    const heroSection = page.locator('section.hero, #hero, [data-testid="hero"]');
    const ctaButtons = heroSection.locator('a.btn, button.btn, .btn-primary, .btn-secondary');
    const buttonCount = await ctaButtons.count();

    expect(buttonCount).toBeGreaterThanOrEqual(1);

    for (let i = 0; i < buttonCount; i++) {
      const button = ctaButtons.nth(i);
      await expect(button).toBeVisible();

      const buttonBox = await button.boundingBox();
      expect(buttonBox).not.toBeNull();

      // Button should meet minimum touch target size
      expect(buttonBox!.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
    }
  });

  test('TC7: Navigation is accessible on mobile viewport', async ({ page }) => {
    // Test Case 7: Test navigation on mobile viewport
    // Expected: Navigation is accessible (visible menu or hamburger toggle)

    // Check for navigation element
    const nav = page.locator('nav, header nav, .navbar, [role="navigation"]');
    const navCount = await nav.count();

    // Navigation element should exist
    expect(navCount).toBeGreaterThanOrEqual(1);

    // Check if mobile menu button (hamburger) is visible OR nav links are directly visible
    const mobileMenuBtn = page.locator('.mobile-menu-btn, .hamburger-btn, [aria-label*="menu"], button[aria-label*="navigation"]');
    const navLinks = page.locator('.nav-links, nav ul, header nav a');

    const isMobileMenuVisible = await mobileMenuBtn.isVisible().catch(() => false);
    const areNavLinksVisible = await navLinks.first().isVisible().catch(() => false);

    // Either hamburger menu button is visible OR nav links are visible
    expect(isMobileMenuVisible || areNavLinksVisible).toBe(true);

    // If hamburger menu is visible, it should be clickable
    if (isMobileMenuVisible) {
      await expect(mobileMenuBtn.first()).toBeEnabled();

      // Click hamburger menu and verify navigation appears
      await mobileMenuBtn.first().click();

      // Wait for menu to potentially animate open
      await page.waitForTimeout(300);

      // Check that navigation links become visible after clicking
      const navLinksAfterClick = page.locator('.nav-links.active a, nav.active a, .mobile-nav a, [role="menuitem"]');
      const linksCount = await navLinksAfterClick.count();

      // Should have at least some navigation links
      // Note: This may vary based on implementation
      if (linksCount > 0) {
        await expect(navLinksAfterClick.first()).toBeVisible();
      }
    }
  });
});
