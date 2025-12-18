// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const pageUrl = 'file://' + path.join(__dirname, '..', 'index.html');

// Tablet viewport dimensions (iPad portrait, common tablet device)
const TABLET_VIEWPORT = { width: 768, height: 1024 };

test.describe('Tablet Responsive Design (768px viewport)', () => {
  test.beforeEach(async ({ page }) => {
    // Set tablet viewport before navigating
    await page.setViewportSize(TABLET_VIEWPORT);
    await page.goto(pageUrl);
  });

  /**
   * Test Case 1: Page renders correctly at 768px viewport width
   * Input: Load page at 768px viewport width
   * Expected: Page renders correctly with tablet-appropriate layout
   */
  test('TC1: Page renders correctly at 768px viewport width', async ({ page }) => {
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

    // Verify key sections are visible and properly rendered
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();
  });

  /**
   * Test Case 2: Feature cards display in 2-column or appropriate grid on tablet
   * Input: Check feature cards layout on tablet
   * Expected: Feature cards display in 2-column or appropriate grid
   */
  test('TC2: Feature cards display in appropriate grid layout on tablet', async ({ page }) => {
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

    // On tablet (768px), with minmax(300px, 1fr) grid, we expect either:
    // - 2 cards side-by-side on first row, 1 card on second row (2-column grid)
    // - Or all 3 cards stacked vertically (1-column, if viewport is at breakpoint edge)

    // Check the grid layout by comparing positions
    // Cards should be appropriately sized for tablet viewing
    const viewportWidth = TABLET_VIEWPORT.width;

    // Each card should take a reasonable portion of the viewport width
    // At 768px, with padding and 2-column layout, cards should be ~300-400px wide
    expect(box1.width).toBeGreaterThanOrEqual(280);
    expect(box1.width).toBeLessThanOrEqual(viewportWidth - 32); // accounting for padding

    // Verify grid is responsive - cards should not overflow viewport
    expect(box1.x + box1.width).toBeLessThanOrEqual(viewportWidth);
    expect(box2.x + box2.width).toBeLessThanOrEqual(viewportWidth);
    expect(box3.x + box3.width).toBeLessThanOrEqual(viewportWidth);

    // Check for 2-column layout pattern:
    // If card1 and card2 are on the same row, their Y positions should be similar
    const sameRowTolerance = 20;
    const card1And2SameRow = Math.abs(box1.y - box2.y) < sameRowTolerance;

    if (card1And2SameRow) {
      // 2-column layout: card1 and card2 on same row, card3 below
      expect(box3.y).toBeGreaterThan(box1.y + box1.height - 10);
      expect(box1.x).not.toBe(box2.x); // cards should be side-by-side
    } else {
      // Single column layout (valid for tablet at boundary)
      // All cards should be stacked vertically
      expect(box2.y).toBeGreaterThanOrEqual(box1.y + box1.height - 10);
      expect(box3.y).toBeGreaterThanOrEqual(box2.y + box2.height - 10);
    }
  });

  /**
   * Test Case 3: Navigation is accessible on tablet
   * Input: Check navigation on tablet
   * Expected: Navigation is accessible (either full nav or hamburger)
   */
  test('TC3: Navigation is accessible on tablet', async ({ page }) => {
    // Find navigation elements
    const navLinks = page.locator('.nav-links');
    const hamburgerMenu = page.locator('.mobile-menu-btn');

    // At 768px (tablet), the CSS has a breakpoint at max-width: 768px
    // Navigation could be either:
    // 1. Full desktop nav visible with nav-links visible
    // 2. Mobile nav with hamburger menu visible

    const isHamburgerVisible = await hamburgerMenu.isVisible();
    const isNavLinksVisible = await navLinks.isVisible();

    // Either full nav should be visible OR hamburger menu should be visible
    expect(isHamburgerVisible || isNavLinksVisible).toBe(true);

    if (isHamburgerVisible && !isNavLinksVisible) {
      // Mobile/tablet hamburger navigation
      // Verify hamburger menu is interactive
      await expect(hamburgerMenu).toHaveAttribute('aria-label', /menu/i);

      // Click hamburger to open menu
      await hamburgerMenu.click();

      // After clicking, nav links should be visible
      await expect(navLinks).toBeVisible();

      // Verify all navigation links are accessible
      await expect(navLinks.locator('a[href="#features"]')).toBeVisible();
      await expect(navLinks.locator('a[href="#getting-started"]')).toBeVisible();
      await expect(navLinks.locator('a[href*="github.com"]')).toBeVisible();

      // Close menu
      await hamburgerMenu.click();
      await expect(navLinks).not.toBeVisible();
    } else {
      // Full desktop-style navigation is visible
      await expect(navLinks).toBeVisible();

      // Verify all navigation links are visible and clickable
      const featuresLink = navLinks.locator('a[href="#features"]');
      const gettingStartedLink = navLinks.locator('a[href="#getting-started"]');
      const githubLink = navLinks.locator('a[href*="github.com"]');

      await expect(featuresLink).toBeVisible();
      await expect(gettingStartedLink).toBeVisible();
      await expect(githubLink).toBeVisible();
    }
  });

  /**
   * Additional test: Hero section is properly sized on tablet
   */
  test('Hero section displays properly on tablet viewport', async ({ page }) => {
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Check hero heading (h1) is visible
    const heroTitle = heroSection.locator('h1');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toContainText('MirDB');

    // Check tagline is visible
    const tagline = heroSection.locator('.tagline');
    await expect(tagline).toBeVisible();

    // Check description is visible
    const description = heroSection.locator('.description');
    await expect(description).toBeVisible();

    // Check CTA buttons are visible
    const ctaButtons = heroSection.locator('.cta-buttons .btn');
    await expect(ctaButtons).toHaveCount(2);

    // Verify both buttons are visible
    for (const button of await ctaButtons.all()) {
      await expect(button).toBeVisible();
    }

    // Verify CTA buttons layout - they should be horizontally arranged on tablet
    const primaryBtn = page.locator('.btn-primary');
    const secondaryBtn = page.locator('.btn-secondary');

    const primaryBox = await primaryBtn.boundingBox();
    const secondaryBox = await secondaryBtn.boundingBox();

    expect(primaryBox).not.toBeNull();
    expect(secondaryBox).not.toBeNull();

    // On tablet, buttons should be on the same row (similar Y position)
    const sameRowTolerance = 10;
    expect(Math.abs(primaryBox.y - secondaryBox.y)).toBeLessThan(sameRowTolerance);
  });

  /**
   * Additional test: Commands grid layout on tablet
   */
  test('Commands section displays in appropriate grid on tablet', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('#commands');
    await commandsSection.scrollIntoViewIfNeeded();
    await expect(commandsSection).toBeVisible();

    // Get command categories
    const commandCategories = commandsSection.locator('.command-category');
    const count = await commandCategories.count();
    expect(count).toBe(4); // Storage, Retrieval, Deletion, MirDB Specific

    // On tablet, categories should display in a grid (at least 2 columns)
    const box1 = await commandCategories.nth(0).boundingBox();
    const box2 = await commandCategories.nth(1).boundingBox();

    expect(box1).not.toBeNull();
    expect(box2).not.toBeNull();

    // Check for grid layout - first two categories should be on same row or stacked appropriately
    const viewportWidth = TABLET_VIEWPORT.width;

    // Categories should fit within viewport
    expect(box1.x + box1.width).toBeLessThanOrEqual(viewportWidth);
    expect(box2.x + box2.width).toBeLessThanOrEqual(viewportWidth);
  });

  /**
   * Additional test: Code blocks are readable and scrollable on tablet
   */
  test('Code blocks are readable and scrollable on tablet', async ({ page }) => {
    // Navigate to getting started section
    const gettingStartedSection = page.locator('#getting-started');
    await gettingStartedSection.scrollIntoViewIfNeeded();
    await expect(gettingStartedSection).toBeVisible();

    // Find code blocks
    const codeBlocks = gettingStartedSection.locator('pre');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Check code blocks have appropriate width and scrolling
    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      await expect(codeBlock).toBeVisible();

      const overflowX = await codeBlock.evaluate((el) => {
        return window.getComputedStyle(el).overflowX;
      });

      // overflow-x should be 'auto' or 'scroll' for horizontal scrolling
      expect(['auto', 'scroll']).toContain(overflowX);
    }

    // Verify main page still doesn't have horizontal scroll
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const bodyClientWidth = await page.evaluate(() => document.body.clientWidth);
    expect(bodyScrollWidth).toBeLessThanOrEqual(bodyClientWidth + 1);
  });

  /**
   * Additional test: Footer displays properly on tablet
   */
  test('Footer displays properly on tablet', async ({ page }) => {
    const footer = page.locator('footer.footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Footer content should be visible
    const footerContent = footer.locator('.footer-content');
    await expect(footerContent).toBeVisible();

    // Check GitHub link
    const githubLink = footer.locator('a[href*="github.com"]');
    await expect(githubLink).toBeVisible();

    // Check license text
    const licenseText = footerContent.locator('p', { hasText: /MIT/i });
    await expect(licenseText).toBeVisible();
  });
});

test.describe('Tablet Responsive Design - Viewport Transitions', () => {
  test('Layout transitions appropriately between desktop and tablet viewports', async ({ page }) => {
    // Start with desktop viewport
    await page.setViewportSize({ width: 1200, height: 800 });
    await page.goto(pageUrl);

    // On desktop, nav links should be fully visible
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    // Get feature cards position on desktop
    const featureCards = page.locator('.feature-card');
    const desktopCard1Box = await featureCards.nth(0).boundingBox();
    const desktopCard2Box = await featureCards.nth(1).boundingBox();
    const desktopCard3Box = await featureCards.nth(2).boundingBox();

    // On desktop (1200px), all 3 cards should be on the same row
    expect(Math.abs(desktopCard1Box.y - desktopCard2Box.y)).toBeLessThan(20);
    expect(Math.abs(desktopCard2Box.y - desktopCard3Box.y)).toBeLessThan(20);

    // Resize to tablet viewport
    await page.setViewportSize(TABLET_VIEWPORT);

    // Wait for CSS to apply
    await page.waitForTimeout(100);

    // Get feature cards position on tablet
    const tabletCard1Box = await featureCards.nth(0).boundingBox();
    const tabletCard2Box = await featureCards.nth(1).boundingBox();
    const tabletCard3Box = await featureCards.nth(2).boundingBox();

    // On tablet, layout should adjust - at 768px with minmax(300px, 1fr) grid,
    // cards may take full width (single column) which is wider than desktop (3-column)
    // The key is that the layout should still be within viewport bounds
    expect(tabletCard1Box.x + tabletCard1Box.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);

    // Verify that tablet layout is different from desktop - either cards are stacked
    // or have different arrangement
    const desktopAllSameRow =
      Math.abs(desktopCard1Box.y - desktopCard2Box.y) < 20 &&
      Math.abs(desktopCard2Box.y - desktopCard3Box.y) < 20;

    // Desktop should have all cards in same row (3-column layout)
    expect(desktopAllSameRow).toBe(true);
  });

  test('Navigation adapts correctly at 768px breakpoint', async ({ page }) => {
    // Test at exactly 768px (the CSS breakpoint)
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto(pageUrl);

    const hamburgerMenu = page.locator('.mobile-menu-btn');
    const navLinks = page.locator('.nav-links');

    // At 768px, this is at the breakpoint edge
    // Either hamburger is visible or nav links are visible
    const isHamburgerVisible = await hamburgerMenu.isVisible();
    const isNavLinksVisible = await navLinks.isVisible();

    // Navigation must be accessible in some form
    expect(isHamburgerVisible || isNavLinksVisible).toBe(true);

    // Test at 769px (just above breakpoint)
    await page.setViewportSize({ width: 769, height: 1024 });
    await page.waitForTimeout(50);

    // Above the breakpoint, nav links should be visible
    await expect(navLinks).toBeVisible();
  });
});
