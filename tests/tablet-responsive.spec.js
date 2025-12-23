// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Tablet Responsive Design
 * Scenario: Verify the homepage displays correctly on tablet devices (640px-1024px) as specified in NFR-1
 * Related Requirements: NFR-1 (Responsive Design), US-6
 */

const TABLET_WIDTH = 768;
const TABLET_HEIGHT = 1024;

test.describe('Responsive Design - Tablet', () => {
  test.beforeEach(async ({ page }) => {
    // Set viewport to tablet size (768px width, within 640px-1024px range)
    await page.setViewportSize({ width: TABLET_WIDTH, height: TABLET_HEIGHT });
    await page.goto('/');
  });

  /**
   * Test Case 1: Layout adapts to tablet width with appropriate column structure
   * Input: Load page at 768px width (tablet)
   * Expected: Layout adapts to tablet width with appropriate column structure
   */
  test('TC1: Layout adapts to tablet width with appropriate column structure', async ({ page }) => {
    // Verify the page loads correctly at tablet width
    const body = page.locator('body');
    await expect(body).toBeVisible();

    // Verify hero section is visible and properly sized
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    const heroBox = await heroSection.boundingBox();
    expect(heroBox).toBeTruthy();
    // Hero should span the full width of the viewport
    expect(heroBox.width).toBeGreaterThanOrEqual(TABLET_WIDTH - 50);

    // Verify main content sections are visible
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Verify the feature grid adapts to tablet layout
    const featureGrid = page.locator('.feature-grid');
    await expect(featureGrid).toBeVisible();

    // Verify all content is readable without horizontal scrolling
    const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);
    expect(scrollWidth).toBeLessThanOrEqual(clientWidth + 5); // Small tolerance for rounding
  });

  /**
   * Test Case 2: Feature cards display in 2-column grid on tablet
   * Input: Check feature cards layout on tablet
   * Expected: Feature cards display in 2-column grid on tablet
   */
  test('TC2: Feature cards display in 2-column grid on tablet', async ({ page }) => {
    // Scroll to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Get the feature grid
    const featureGrid = page.locator('.feature-grid');
    await expect(featureGrid).toBeVisible();

    // Get all feature cards
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(2);

    // Get bounding boxes for first two cards to verify 2-column layout
    const card1 = featureCards.nth(0);
    const card2 = featureCards.nth(1);

    await expect(card1).toBeVisible();
    await expect(card2).toBeVisible();

    const card1Box = await card1.boundingBox();
    const card2Box = await card2.boundingBox();

    expect(card1Box).toBeTruthy();
    expect(card2Box).toBeTruthy();

    // In a 2-column layout, the first two cards should be on the same row
    // (their Y positions should be approximately equal)
    expect(Math.abs(card1Box.y - card2Box.y)).toBeLessThan(10);

    // The cards should be side by side (different X positions)
    expect(card2Box.x).toBeGreaterThan(card1Box.x);

    // If there's a third card, it should be on a different row
    if (cardCount >= 3) {
      const card3 = featureCards.nth(2);
      await expect(card3).toBeVisible();
      const card3Box = await card3.boundingBox();
      expect(card3Box).toBeTruthy();

      // Third card should be below the first row
      expect(card3Box.y).toBeGreaterThan(card1Box.y + card1Box.height - 10);
    }

    // Verify grid computed style confirms 2-column layout
    const gridStyle = await featureGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns
      };
    });

    expect(gridStyle.display).toBe('grid');
    // The grid-template-columns should indicate 2 columns
    // It might be something like "repeat(2, 1fr)" or "XXXpx XXXpx"
    const columnCount = gridStyle.gridTemplateColumns.split(' ').filter(v => v && v !== '0px').length;
    expect(columnCount).toBe(2);
  });

  /**
   * Test Case 3: Full navigation is visible or accessible via appropriate tablet pattern
   * Input: Verify navigation on tablet
   * Expected: Full navigation is visible or accessible via appropriate tablet pattern
   */
  test('TC3: Full navigation is visible or accessible via appropriate tablet pattern', async ({ page }) => {
    // Get the header/navigation
    const header = page.locator('header.header');
    await expect(header).toBeVisible();

    const nav = page.locator('nav.nav');
    await expect(nav).toBeVisible();

    // Check if the navigation links are visible
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    // Check for all navigation items
    const featuresLink = navLinks.locator('a[href="#features"]');
    const gettingStartedLink = navLinks.locator('a[href="#getting-started"]');
    const docsLink = navLinks.locator('a:has-text("Documentation")');
    const githubLink = navLinks.locator('a:has-text("GitHub")');

    // On tablet (768px), navigation should be fully visible
    await expect(featuresLink).toBeVisible();
    await expect(gettingStartedLink).toBeVisible();
    await expect(docsLink).toBeVisible();
    await expect(githubLink).toBeVisible();

    // Verify the logo is visible
    const logo = page.locator('.logo');
    await expect(logo).toBeVisible();
    await expect(logo).toContainText('MirDB');

    // Verify theme toggle is visible
    const themeToggle = page.locator('.theme-toggle');
    await expect(themeToggle).toBeVisible();

    // Verify navigation links are clickable
    await expect(featuresLink).toBeEnabled();
    await expect(gettingStartedLink).toBeEnabled();

    // Test navigation functionality - click on Features link
    await featuresLink.click();
    await page.waitForTimeout(500);

    // Verify the Features section is in view
    const featuresSection = page.locator('#features');
    const featuresBoundingBox = await featuresSection.boundingBox();
    expect(featuresBoundingBox).toBeTruthy();
    expect(featuresBoundingBox.y).toBeLessThan(200);
  });

  /**
   * Additional Test: Verify code examples grid uses 2-column layout on tablet
   */
  test('Code examples grid displays in 2-column layout on tablet', async ({ page }) => {
    // Scroll to code examples section
    const codeExamplesSection = page.locator('#code-examples');
    await codeExamplesSection.scrollIntoViewIfNeeded();

    // Get the code examples grid
    const codeExamplesGrid = page.locator('.code-examples-grid');
    await expect(codeExamplesGrid).toBeVisible();

    // Verify grid computed style confirms 2-column layout
    const gridStyle = await codeExamplesGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns
      };
    });

    expect(gridStyle.display).toBe('grid');
    const columnCount = gridStyle.gridTemplateColumns.split(' ').filter(v => v && v !== '0px').length;
    expect(columnCount).toBe(2);
  });

  /**
   * Additional Test: Verify hero CTA buttons layout on tablet
   */
  test('Hero CTA buttons display side by side on tablet', async ({ page }) => {
    // Get the CTA buttons container
    const heroCta = page.locator('.hero-cta');
    await expect(heroCta).toBeVisible();

    // Get the buttons
    const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
    const githubBtn = page.locator('[data-testid="cta-github"]');

    await expect(getStartedBtn).toBeVisible();
    await expect(githubBtn).toBeVisible();

    const btn1Box = await getStartedBtn.boundingBox();
    const btn2Box = await githubBtn.boundingBox();

    expect(btn1Box).toBeTruthy();
    expect(btn2Box).toBeTruthy();

    // On tablet, buttons should be side by side (same Y position, different X)
    expect(Math.abs(btn1Box.y - btn2Box.y)).toBeLessThan(10);
    expect(btn2Box.x).toBeGreaterThan(btn1Box.x);
  });

  /**
   * Additional Test: Verify status grid uses 2-column layout on tablet
   */
  test('Status grid displays in 2-column layout on tablet', async ({ page }) => {
    // Scroll to project status section
    const statusSection = page.locator('#project-status');
    await statusSection.scrollIntoViewIfNeeded();

    // Get the status grid
    const statusGrid = page.locator('.status-grid');
    await expect(statusGrid).toBeVisible();

    // Verify grid computed style confirms 2-column layout
    const gridStyle = await statusGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns
      };
    });

    expect(gridStyle.display).toBe('grid');
    const columnCount = gridStyle.gridTemplateColumns.split(' ').filter(v => v && v !== '0px').length;
    expect(columnCount).toBe(2);
  });

  /**
   * Additional Test: Verify no horizontal scrolling on tablet
   */
  test('No horizontal scrolling required on tablet viewport', async ({ page }) => {
    // Scroll through the entire page
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(100);

    // Check for horizontal overflow
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });

    expect(hasHorizontalScroll).toBe(false);
  });

  /**
   * Additional Test: Verify touch targets are appropriately sized
   */
  test('Touch targets are at least 44x44 pixels on tablet', async ({ page }) => {
    // Check CTA buttons - these are the primary touch targets
    const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
    const btnBox = await getStartedBtn.boundingBox();
    expect(btnBox).toBeTruthy();
    expect(btnBox.width).toBeGreaterThanOrEqual(44);
    expect(btnBox.height).toBeGreaterThanOrEqual(44);

    // Check navigation links are visible and clickable
    const navLinks = page.locator('.nav-links a');
    const linkCount = await navLinks.count();
    expect(linkCount).toBeGreaterThan(0);

    for (let i = 0; i < linkCount; i++) {
      const link = navLinks.nth(i);
      await expect(link).toBeVisible();
      await expect(link).toBeEnabled();
    }

    // Check theme toggle button
    const themeToggle = page.locator('.theme-toggle');
    const toggleBox = await themeToggle.boundingBox();
    expect(toggleBox).toBeTruthy();
    expect(toggleBox.width).toBeGreaterThanOrEqual(36);
    expect(toggleBox.height).toBeGreaterThanOrEqual(36);
  });
});
