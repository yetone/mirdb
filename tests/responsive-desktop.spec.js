// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = path.resolve(__dirname, '../index.html');

/**
 * Responsive Design - Desktop Tests
 * Verify the landing page displays correctly on desktop devices (width > 1024px)
 */
test.describe('Responsive Design - Desktop', () => {
  /**
   * Test Case 1: Load page at 1440px width
   * Input: Load page at 1440px width
   * Expected: Full desktop layout with multi-column feature cards
   */
  test('TC1: Full desktop layout at 1440px width', async ({ page }) => {
    // Set viewport to standard desktop width
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.goto(`file://${indexPath}`);

    // Verify page loads successfully
    const body = page.locator('body');
    await expect(body).toBeVisible();

    // Verify hero section displays in full desktop layout
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify hero title is displayed with proper font size
    const heroTitle = page.locator('[data-testid="hero-title"]');
    await expect(heroTitle).toBeVisible();
    const heroTitleFontSize = await heroTitle.evaluate(el =>
      parseFloat(window.getComputedStyle(el).fontSize)
    );
    // Desktop should have large hero title (4rem = 64px at default)
    expect(heroTitleFontSize).toBeGreaterThanOrEqual(40);

    // Verify features section is visible
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify feature cards container uses grid layout
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();
    const gridDisplay = await featuresGrid.evaluate(el =>
      window.getComputedStyle(el).display
    );
    expect(gridDisplay).toBe('grid');

    // Verify there are 3 feature cards visible
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(3);

    // Check that all feature cards are visible
    for (let i = 0; i < 3; i++) {
      await expect(featureCards.nth(i)).toBeVisible();
    }
  });

  /**
   * Test Case 2: Feature cards at desktop width
   * Input: Check feature cards at desktop width
   * Expected: Feature cards display in 3-4 column grid
   */
  test('TC2: Feature cards display in multi-column grid at desktop width', async ({ page }) => {
    // Set viewport to desktop width
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.goto(`file://${indexPath}`);

    // Scroll to features section
    await page.locator('#features').scrollIntoViewIfNeeded();
    await page.waitForTimeout(100);

    // Verify features grid exists
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Verify feature cards exist
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(3);

    // Get positions of all feature cards
    const cardBoundingBoxes = await Promise.all([
      featureCards.nth(0).boundingBox(),
      featureCards.nth(1).boundingBox(),
      featureCards.nth(2).boundingBox()
    ]);

    // All cards should have valid bounding boxes
    expect(cardBoundingBoxes[0]).not.toBeNull();
    expect(cardBoundingBoxes[1]).not.toBeNull();
    expect(cardBoundingBoxes[2]).not.toBeNull();

    // At 1440px width, all 3 cards should be on the same row (same Y position)
    // This confirms they're in a horizontal multi-column layout
    const yPositions = cardBoundingBoxes.map(box => box.y);
    const tolerance = 5; // Allow small tolerance for rounding

    // All cards should have roughly the same Y position (same row)
    expect(Math.abs(yPositions[0] - yPositions[1])).toBeLessThan(tolerance);
    expect(Math.abs(yPositions[1] - yPositions[2])).toBeLessThan(tolerance);

    // Cards should have different X positions (different columns)
    const xPositions = cardBoundingBoxes.map(box => box.x);
    expect(xPositions[0]).toBeLessThan(xPositions[1]);
    expect(xPositions[1]).toBeLessThan(xPositions[2]);

    // Each card should have reasonable width for a 3-column layout
    // At 1440px container with padding, each card should be roughly 300-400px wide
    const cardWidths = cardBoundingBoxes.map(box => box.width);
    cardWidths.forEach(width => {
      expect(width).toBeGreaterThanOrEqual(250);
      expect(width).toBeLessThanOrEqual(500);
    });
  });

  /**
   * Test Case 3: Load page at 2560px width
   * Input: Load page at 2560px width
   * Expected: Content remains centered with max-width, no stretching
   */
  test('TC3: Content remains centered with max-width at 2560px', async ({ page }) => {
    // Set viewport to very large desktop width
    await page.setViewportSize({ width: 2560, height: 1440 });
    await page.goto(`file://${indexPath}`);

    // Verify page loads successfully
    await expect(page.locator('body')).toBeVisible();

    // Check that container has max-width and is centered
    const container = page.locator('.container').first();
    await expect(container).toBeVisible();

    // Get container computed styles
    const containerStyles = await container.evaluate(el => {
      const styles = window.getComputedStyle(el);
      return {
        maxWidth: styles.maxWidth
      };
    });

    // Container should have a max-width set (1200px as per CSS variables)
    expect(containerStyles.maxWidth).not.toBe('none');
    const maxWidthValue = parseInt(containerStyles.maxWidth);
    expect(maxWidthValue).toBeLessThanOrEqual(1400);

    // Verify the container doesn't stretch to full viewport width
    const containerBoundingBox = await container.boundingBox();
    expect(containerBoundingBox).not.toBeNull();
    expect(containerBoundingBox.width).toBeLessThan(2560);
    expect(containerBoundingBox.width).toBeLessThanOrEqual(1300);

    // Verify content is centered - container should have equal margins on both sides
    // When margin is set to "auto", computed style returns actual pixel value
    // So we verify centering by checking that left and right margins are approximately equal
    const leftMargin = containerBoundingBox.x;
    const rightMargin = 2560 - (containerBoundingBox.x + containerBoundingBox.width);
    const marginDifference = Math.abs(leftMargin - rightMargin);
    // Allow 50px tolerance for potential scrollbar or rendering differences
    expect(marginDifference).toBeLessThan(50);

    // Verify both margins are substantial (container is centered, not left-aligned)
    expect(leftMargin).toBeGreaterThan(500);
    expect(rightMargin).toBeGreaterThan(500);

    // Also verify hero container is centered
    const heroContainer = page.locator('.hero-container');
    await expect(heroContainer).toBeVisible();
    const heroContainerBox = await heroContainer.boundingBox();
    expect(heroContainerBox).not.toBeNull();
    expect(heroContainerBox.width).toBeLessThan(2560);

    // Verify hero container is centered
    const heroLeftMargin = heroContainerBox.x;
    const heroRightMargin = 2560 - (heroContainerBox.x + heroContainerBox.width);
    expect(Math.abs(heroLeftMargin - heroRightMargin)).toBeLessThan(100);
  });

  /**
   * Test Case 4: Navigation at desktop width
   * Input: Check navigation at desktop width
   * Expected: Full navigation menu visible without hamburger
   */
  test('TC4: Full navigation menu visible without hamburger at desktop width', async ({ page }) => {
    // Set viewport to desktop width
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.goto(`file://${indexPath}`);

    // Verify navigation bar is visible
    const navbar = page.locator('[data-testid="navbar"]');
    await expect(navbar).toBeVisible();

    // Verify nav-links container is visible (not hidden)
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    // Verify nav-links has display flex (not hidden)
    const navLinksDisplay = await navLinks.evaluate(el =>
      window.getComputedStyle(el).display
    );
    expect(navLinksDisplay).toBe('flex');

    // Verify all navigation links are visible
    const featuresLink = page.locator('[data-testid="nav-features"]');
    const quickstartLink = page.locator('[data-testid="nav-quickstart"]');
    const commandsLink = page.locator('[data-testid="nav-commands"]');
    const configLink = page.locator('[data-testid="nav-configuration"]');
    const githubLink = page.locator('[data-testid="nav-github"]');

    await expect(featuresLink).toBeVisible();
    await expect(quickstartLink).toBeVisible();
    await expect(commandsLink).toBeVisible();
    await expect(configLink).toBeVisible();
    await expect(githubLink).toBeVisible();

    // Verify there is no hamburger menu button visible at desktop width
    // (hamburger menus typically have a class containing 'hamburger', 'menu-toggle', or 'mobile-menu')
    const hamburgerButton = page.locator('[class*="hamburger"], [class*="menu-toggle"], [class*="mobile-menu"], button[aria-label*="menu"]');
    // Either the hamburger doesn't exist or it's not visible at desktop width
    const hamburgerCount = await hamburgerButton.count();
    if (hamburgerCount > 0) {
      await expect(hamburgerButton.first()).not.toBeVisible();
    }

    // Verify brand/logo is visible
    const navBrand = page.locator('[data-testid="nav-brand"]');
    await expect(navBrand).toBeVisible();
    await expect(navBrand).toHaveText('MirDB');
  });

  /**
   * Additional test: Layout consistency at 1920px (common desktop resolution)
   */
  test('Layout displays correctly at 1920px width', async ({ page }) => {
    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.goto(`file://${indexPath}`);

    // Verify all main sections are visible
    await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('#quickstart')).toBeVisible();
    await expect(page.locator('#commands')).toBeVisible();
    await expect(page.locator('#configuration')).toBeVisible();

    // Verify navigation is fully visible
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();
    const navLinksDisplay = await navLinks.evaluate(el =>
      window.getComputedStyle(el).display
    );
    expect(navLinksDisplay).toBe('flex');

    // Verify feature cards are in horizontal layout
    const featureCards = page.locator('.feature-card');
    const cardBoxes = await Promise.all([
      featureCards.nth(0).boundingBox(),
      featureCards.nth(1).boundingBox(),
      featureCards.nth(2).boundingBox()
    ]);

    // All cards should be on the same row
    const yPositions = cardBoxes.map(box => box.y);
    expect(Math.abs(yPositions[0] - yPositions[1])).toBeLessThan(5);
    expect(Math.abs(yPositions[1] - yPositions[2])).toBeLessThan(5);
  });

  /**
   * Additional test: Tables display correctly at desktop width
   */
  test('Tables display correctly at desktop width', async ({ page }) => {
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.goto(`file://${indexPath}`);

    // Scroll to commands section
    await page.locator('#commands').scrollIntoViewIfNeeded();
    await page.waitForTimeout(100);

    // Verify tables are visible
    const tables = page.locator('.commands table');
    const tableCount = await tables.count();
    expect(tableCount).toBeGreaterThan(0);

    // Check first table is properly displayed
    const firstTable = tables.first();
    await expect(firstTable).toBeVisible();

    // Verify table has proper width (not overflowing)
    const tableBox = await firstTable.boundingBox();
    expect(tableBox).not.toBeNull();
    expect(tableBox.width).toBeLessThanOrEqual(1440);
    expect(tableBox.width).toBeGreaterThan(0);

    // Verify table wrapper exists and handles overflow
    const tableWrapper = page.locator('.table-wrapper').first();
    await expect(tableWrapper).toBeVisible();
    const overflowX = await tableWrapper.evaluate(el =>
      window.getComputedStyle(el).overflowX
    );
    expect(overflowX).toBe('auto');
  });

  /**
   * Additional test: Footer displays in multi-column layout at desktop
   */
  test('Footer displays in multi-column layout at desktop width', async ({ page }) => {
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.goto(`file://${indexPath}`);

    // Scroll to footer
    await page.locator('.footer').scrollIntoViewIfNeeded();
    await page.waitForTimeout(100);

    const footerContent = page.locator('.footer-content');
    await expect(footerContent).toBeVisible();

    // Check footer uses grid layout at desktop
    const footerDisplay = await footerContent.evaluate(el =>
      window.getComputedStyle(el).display
    );
    expect(footerDisplay).toBe('grid');

    // Verify footer columns are horizontal (different X positions)
    const footerBrand = page.locator('.footer-brand');
    const footerLinks = page.locator('.footer-links');

    const brandBox = await footerBrand.boundingBox();
    const linksBox = await footerLinks.first().boundingBox();

    expect(brandBox).not.toBeNull();
    expect(linksBox).not.toBeNull();

    // Brand and links should have different X positions (multi-column)
    expect(brandBox.x).not.toBe(linksBox.x);
  });
});
