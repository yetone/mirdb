// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const pageUrl = 'file://' + path.join(__dirname, '..', 'index.html');

// Mobile viewport dimensions (iPhone SE, common mobile device)
const MOBILE_VIEWPORT = { width: 375, height: 667 };

test.describe('Mobile Responsive Design', () => {
  test.beforeEach(async ({ page }) => {
    // Set mobile viewport before navigating
    await page.setViewportSize(MOBILE_VIEWPORT);
    await page.goto(pageUrl);
  });

  /**
   * Test Case 1: Page renders without horizontal scrollbar on main content
   * Input: Load page at 375px viewport width
   * Expected: Page renders without horizontal scrollbar on main content
   */
  test('TC1: Page renders without horizontal scrollbar at 375px viewport width', async ({ page }) => {
    // Wait for page to fully load
    await page.waitForLoadState('domcontentloaded');

    // Check that body doesn't have horizontal overflow
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const bodyClientWidth = await page.evaluate(() => document.body.clientWidth);

    // The scrollWidth should not exceed clientWidth significantly (allowing 1px tolerance)
    expect(bodyScrollWidth).toBeLessThanOrEqual(bodyClientWidth + 1);

    // Check that the html element also doesn't have horizontal overflow
    const htmlScrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    const htmlClientWidth = await page.evaluate(() => document.documentElement.clientWidth);

    expect(htmlScrollWidth).toBeLessThanOrEqual(htmlClientWidth + 1);
  });

  /**
   * Test Case 2: Hamburger menu icon is visible on mobile viewport
   * Input: Check for mobile hamburger menu
   * Expected: Hamburger menu icon is visible on mobile viewport
   */
  test('TC2: Hamburger menu icon is visible on mobile viewport', async ({ page }) => {
    // Find the mobile menu button
    const hamburgerMenu = page.locator('.mobile-menu-btn');

    // Verify it's visible on mobile
    await expect(hamburgerMenu).toBeVisible();

    // Verify it has the correct aria-label for accessibility
    await expect(hamburgerMenu).toHaveAttribute('aria-label', /menu/i);

    // Verify it contains three span elements (hamburger lines)
    const hamburgerLines = hamburgerMenu.locator('span');
    await expect(hamburgerLines).toHaveCount(3);
  });

  /**
   * Test Case 3: Navigation menu expands and shows all navigation links when clicked
   * Input: Click hamburger menu
   * Expected: Navigation menu expands and shows all navigation links
   */
  test('TC3: Navigation menu expands when hamburger is clicked', async ({ page }) => {
    // Find the nav links container
    const navLinks = page.locator('.nav-links');

    // Initially, nav links should be hidden on mobile (display: none)
    await expect(navLinks).not.toBeVisible();

    // Click the hamburger menu
    const hamburgerMenu = page.locator('.mobile-menu-btn');
    await hamburgerMenu.click();

    // After clicking, nav links should be visible
    await expect(navLinks).toBeVisible();

    // Verify all expected navigation links are present
    await expect(navLinks.locator('a[href="#features"]')).toBeVisible();
    await expect(navLinks.locator('a[href="#getting-started"]')).toBeVisible();
    await expect(navLinks.locator('a[href*="github.com"]')).toBeVisible();

    // Click hamburger menu again to close
    await hamburgerMenu.click();

    // Nav links should be hidden again
    await expect(navLinks).not.toBeVisible();
  });

  /**
   * Test Case 4: Hero content is readable and properly sized on mobile
   * Input: Check hero section on mobile
   * Expected: Hero content is readable and properly sized
   */
  test('TC4: Hero section is readable and properly sized on mobile', async ({ page }) => {
    // Check hero section exists and is visible
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Check hero heading (h1) is visible and readable
    const heroTitle = heroSection.locator('h1');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toContainText('MirDB');

    // Check the heading font size is appropriate for mobile (should be smaller than desktop)
    const fontSize = await heroTitle.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // Mobile font size should be 40px (2.5rem) or less based on media query
    expect(fontSize).toBeLessThanOrEqual(48);
    expect(fontSize).toBeGreaterThan(20);

    // Check tagline is visible
    const tagline = heroSection.locator('.tagline');
    await expect(tagline).toBeVisible();

    // Check description is visible
    const description = heroSection.locator('.description');
    await expect(description).toBeVisible();

    // Check CTA buttons are visible and accessible
    const ctaButtons = heroSection.locator('.cta-buttons .btn');
    await expect(ctaButtons).toHaveCount(2);
    for (const button of await ctaButtons.all()) {
      await expect(button).toBeVisible();
    }
  });

  /**
   * Test Case 5: Feature cards stack vertically on mobile
   * Input: Check feature cards on mobile
   * Expected: Feature cards stack vertically on mobile
   */
  test('TC5: Feature cards stack vertically on mobile', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Get all feature cards
    const featureCards = featuresSection.locator('.feature-card');
    await expect(featureCards).toHaveCount(3);

    // Get bounding boxes of all feature cards
    const card1 = featureCards.nth(0);
    const card2 = featureCards.nth(1);
    const card3 = featureCards.nth(2);

    const box1 = await card1.boundingBox();
    const box2 = await card2.boundingBox();
    const box3 = await card3.boundingBox();

    // Verify all cards are rendered
    expect(box1).not.toBeNull();
    expect(box2).not.toBeNull();
    expect(box3).not.toBeNull();

    // Verify cards are stacked vertically (each card's top is below previous card's bottom)
    // Card 2 should be below Card 1
    expect(box2.y).toBeGreaterThanOrEqual(box1.y + box1.height - 5); // 5px tolerance for gaps

    // Card 3 should be below Card 2
    expect(box3.y).toBeGreaterThanOrEqual(box2.y + box2.height - 5);

    // Verify cards take full width (approximately) - allowing for padding
    const viewportWidth = MOBILE_VIEWPORT.width;
    expect(box1.width).toBeGreaterThan(viewportWidth * 0.7); // At least 70% of viewport
    expect(box2.width).toBeGreaterThan(viewportWidth * 0.7);
    expect(box3.width).toBeGreaterThan(viewportWidth * 0.7);
  });

  /**
   * Test Case 6: Code blocks allow horizontal scrolling without breaking layout
   * Input: Check code blocks on mobile
   * Expected: Code blocks allow horizontal scrolling without breaking layout
   */
  test('TC6: Code blocks allow horizontal scrolling without breaking layout', async ({ page }) => {
    // Navigate to getting started section
    const gettingStartedSection = page.locator('#getting-started');
    await gettingStartedSection.scrollIntoViewIfNeeded();
    await expect(gettingStartedSection).toBeVisible();

    // Find code blocks
    const codeBlocks = gettingStartedSection.locator('pre');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Check each code block has overflow-x: auto (allows horizontal scrolling)
    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      await expect(codeBlock).toBeVisible();

      const overflowX = await codeBlock.evaluate((el) => {
        return window.getComputedStyle(el).overflowX;
      });

      // overflow-x should be 'auto' or 'scroll' to allow horizontal scrolling
      expect(['auto', 'scroll']).toContain(overflowX);
    }

    // Verify main page still doesn't have horizontal scroll
    // This ensures code blocks are contained within their container
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const bodyClientWidth = await page.evaluate(() => document.body.clientWidth);

    // Body should not overflow horizontally
    expect(bodyScrollWidth).toBeLessThanOrEqual(bodyClientWidth + 1);
  });
});

test.describe('Mobile Navigation Accessibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(MOBILE_VIEWPORT);
    await page.goto(pageUrl);
  });

  test('Hamburger menu can be focused via keyboard', async ({ page }) => {
    const hamburgerMenu = page.locator('.mobile-menu-btn');

    // Tab to focus on the hamburger menu
    await page.keyboard.press('Tab');

    // Check if hamburger menu or logo is focused
    const activeElement = await page.evaluate(() => document.activeElement?.className);

    // Continue tabbing until we reach the hamburger menu
    let maxTabs = 10;
    let found = false;
    while (maxTabs > 0 && !found) {
      const focused = await page.locator(':focus');
      const isFocusedHamburger = await focused.evaluate((el) => el.classList.contains('mobile-menu-btn'));
      if (isFocusedHamburger) {
        found = true;
        break;
      }
      await page.keyboard.press('Tab');
      maxTabs--;
    }

    expect(found).toBe(true);
  });

  test('Mobile menu links are navigable after opening', async ({ page }) => {
    const hamburgerMenu = page.locator('.mobile-menu-btn');

    // Open the menu
    await hamburgerMenu.click();

    // Verify nav links are visible
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    // Check each link is clickable
    const featuresLink = navLinks.locator('a[href="#features"]');
    await expect(featuresLink).toBeVisible();

    // Click on features link
    await featuresLink.click();

    // Verify we scrolled to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport({ ratio: 0.5 });
  });
});

test.describe('Responsive Design - Viewport Transitions', () => {
  test('Navigation changes from desktop to mobile when viewport shrinks', async ({ page }) => {
    // Start with desktop viewport
    await page.setViewportSize({ width: 1200, height: 800 });
    await page.goto(pageUrl);

    // On desktop, hamburger menu should be hidden
    const hamburgerMenu = page.locator('.mobile-menu-btn');
    await expect(hamburgerMenu).not.toBeVisible();

    // Nav links should be visible
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    // Resize to mobile viewport
    await page.setViewportSize(MOBILE_VIEWPORT);

    // Wait for CSS to apply
    await page.waitForTimeout(100);

    // Now hamburger menu should be visible
    await expect(hamburgerMenu).toBeVisible();

    // Nav links should be hidden
    await expect(navLinks).not.toBeVisible();
  });
});
