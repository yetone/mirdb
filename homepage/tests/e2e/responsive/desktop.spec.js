/**
 * Desktop Responsive Design E2E Tests
 * Owner: Scenario 9 - Responsive Design - Desktop
 *
 * Tests desktop viewport rendering at 1920x1080 and above
 * Verifies layout, navigation, and proper content display
 */

const { test, expect } = require('@playwright/test');

// Desktop viewport dimensions
const DESKTOP_VIEWPORT = { width: 1920, height: 1080 };

test.describe('Responsive Design - Desktop (1920x1080+)', () => {
  test.beforeEach(async ({ page }) => {
    // Set viewport to desktop size
    await page.setViewportSize(DESKTOP_VIEWPORT);
    await page.goto('/');
  });

  test('Hero section spans appropriate width with centered content', async ({ page }) => {
    const hero = page.locator('.hero');
    const heroContent = page.locator('.hero-content');

    // Hero section should be visible
    await expect(hero).toBeVisible();

    // Hero section should span full viewport width
    const heroBox = await hero.boundingBox();
    expect(heroBox.width).toBeGreaterThanOrEqual(DESKTOP_VIEWPORT.width - 1);

    // Hero content should be centered (check alignment)
    const heroContentBox = await heroContent.boundingBox();
    const heroContentCenterX = heroContentBox.x + heroContentBox.width / 2;
    const viewportCenterX = DESKTOP_VIEWPORT.width / 2;

    // Content center should be close to viewport center (within 50px tolerance)
    expect(Math.abs(heroContentCenterX - viewportCenterX)).toBeLessThan(50);

    // Verify hero elements are visible
    await expect(page.locator('.hero-title')).toBeVisible();
    await expect(page.locator('.hero-tagline')).toBeVisible();
    await expect(page.locator('.cta-button')).toBeVisible();
  });

  test('Features grid displays in multi-column layout', async ({ page }) => {
    const featuresGrid = page.locator('.features-grid');
    const featureCards = page.locator('.feature-card');

    // Features grid should be visible
    await expect(featuresGrid).toBeVisible();

    // Should have multiple feature cards
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThan(1);

    // Get bounding boxes of first two cards to check they're side by side
    const card1Box = await featureCards.nth(0).boundingBox();
    const card2Box = await featureCards.nth(1).boundingBox();

    // Cards should be on the same row (same or similar Y position)
    // In a multi-column layout, cards in the same row have approximately equal Y
    expect(Math.abs(card1Box.y - card2Box.y)).toBeLessThan(10);

    // Cards should be side by side (different X positions)
    expect(card1Box.x).not.toEqual(card2Box.x);

    // Grid should utilize available width appropriately
    const gridBox = await featuresGrid.boundingBox();
    expect(gridBox.width).toBeGreaterThan(600);
  });

  test('Navigation displays horizontally without hamburger menu', async ({ page }) => {
    const nav = page.locator('.main-nav');
    const navList = page.locator('.nav-list');
    const navItems = page.locator('.nav-item');
    const mobileMenuToggle = page.locator('.mobile-menu-toggle');

    // Navigation should be visible
    await expect(nav).toBeVisible();
    await expect(navList).toBeVisible();

    // Hamburger menu should NOT be visible on desktop
    await expect(mobileMenuToggle).toBeHidden();

    // Nav items should be displayed horizontally
    const itemCount = await navItems.count();
    expect(itemCount).toBeGreaterThan(1);

    // Check that nav items are on the same horizontal line
    const item1Box = await navItems.nth(0).boundingBox();
    const item2Box = await navItems.nth(1).boundingBox();

    // Items should have similar Y position (horizontal layout)
    expect(Math.abs(item1Box.y - item2Box.y)).toBeLessThan(5);

    // Items should have different X positions
    expect(item1Box.x).not.toEqual(item2Box.x);

    // Verify all expected nav links are visible
    await expect(page.locator('.nav-link[data-section="features"]')).toBeVisible();
    await expect(page.locator('.nav-link[data-section="quickstart"]')).toBeVisible();
    await expect(page.locator('.nav-link[data-section="architecture"]')).toBeVisible();
  });

  test('No horizontal scrollbar appears', async ({ page }) => {
    // Check that document does not have horizontal overflow
    const hasHorizontalScrollbar = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });

    expect(hasHorizontalScrollbar).toBe(false);

    // Additional check: body should not overflow
    const bodyOverflow = await page.evaluate(() => {
      const body = document.body;
      const computedStyle = window.getComputedStyle(body);
      return {
        overflowX: computedStyle.overflowX,
        scrollWidth: body.scrollWidth,
        clientWidth: body.clientWidth
      };
    });

    // Body scroll width should not exceed client width
    expect(bodyOverflow.scrollWidth).toBeLessThanOrEqual(bodyOverflow.clientWidth + 1);
  });

  test('Desktop layout utilizes full width appropriately', async ({ page }) => {
    // Check hero section width usage
    const hero = page.locator('.hero');
    const heroBox = await hero.boundingBox();
    expect(heroBox.width).toBeGreaterThanOrEqual(DESKTOP_VIEWPORT.width - 1);

    // Check features section width usage
    const features = page.locator('.features');
    await expect(features).toBeVisible();
    const featuresBox = await features.boundingBox();
    expect(featuresBox.width).toBeGreaterThanOrEqual(DESKTOP_VIEWPORT.width - 1);

    // Features grid should have a sensible max-width and be centered
    const featuresGrid = page.locator('.features-grid');
    const gridBox = await featuresGrid.boundingBox();

    // Grid should have horizontal margin (centered layout)
    const leftMargin = gridBox.x;
    const rightMargin = DESKTOP_VIEWPORT.width - (gridBox.x + gridBox.width);

    // Margins should be approximately equal (centered)
    expect(Math.abs(leftMargin - rightMargin)).toBeLessThan(50);
  });

  test('Header is properly styled for desktop', async ({ page }) => {
    const header = page.locator('.site-header');
    const headerContainer = page.locator('.header-container');

    // Header should span full width
    await expect(header).toBeVisible();
    const headerBox = await header.boundingBox();
    expect(headerBox.width).toBeGreaterThanOrEqual(DESKTOP_VIEWPORT.width - 1);

    // Header container should have proper centering
    const containerBox = await headerContainer.boundingBox();
    const leftMargin = containerBox.x;
    const rightMargin = DESKTOP_VIEWPORT.width - (containerBox.x + containerBox.width);

    // Should have approximately equal margins
    expect(Math.abs(leftMargin - rightMargin)).toBeLessThan(50);

    // Logo should be visible
    await expect(page.locator('.header-logo')).toBeVisible();
  });

  test('Footer displays correctly on desktop', async ({ page }) => {
    const footer = page.locator('.site-footer');

    // Footer should be visible
    await expect(footer).toBeVisible();

    // Footer should span full width
    const footerBox = await footer.boundingBox();
    expect(footerBox.width).toBeGreaterThanOrEqual(DESKTOP_VIEWPORT.width - 1);

    // Footer content should be visible
    await expect(page.locator('.footer-links')).toBeVisible();
    await expect(page.locator('.footer-info')).toBeVisible();
  });

  test('All sections render correctly with proper spacing', async ({ page }) => {
    // Verify all main sections are present and visible
    const sections = [
      { selector: '.hero', name: 'Hero' },
      { selector: '.features', name: 'Features' },
      { selector: '.quickstart', name: 'Quick Start' },
      { selector: '.roadmap', name: 'Roadmap' }
    ];

    for (const section of sections) {
      const element = page.locator(section.selector);
      await expect(element, `${section.name} section should be visible`).toBeVisible();

      // Each section should have proper padding
      const box = await element.boundingBox();
      expect(box.height, `${section.name} section should have content`).toBeGreaterThan(50);
    }

    // Check sections are stacked vertically (not overlapping)
    const heroBox = await page.locator('.hero').boundingBox();
    const featuresBox = await page.locator('.features').boundingBox();

    // Features should come after hero
    expect(featuresBox.y).toBeGreaterThan(heroBox.y);
  });
});
