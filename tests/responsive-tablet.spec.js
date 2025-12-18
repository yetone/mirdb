// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = path.resolve(__dirname, '../index.html');

/**
 * Responsive Design - Tablet Tests
 * Verify the landing page displays correctly on tablet devices (768px - 1024px)
 */
test.describe('Responsive Design - Tablet', () => {
  /**
   * Test Case 1: Layout adapts appropriately for tablet viewport at 768px
   * Input: Load page at 768px width
   * Expected: Layout adapts appropriately for tablet viewport
   */
  test('TC1: Layout adapts appropriately at 768px viewport width', async ({ page }) => {
    // Set viewport to tablet width (768px)
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto(`file://${indexPath}`);

    // Verify page loads and hero section is visible
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify hero title has appropriate font size for tablet
    const heroTitle = page.locator('[data-testid="hero-title"]');
    await expect(heroTitle).toBeVisible();
    const heroTitleFontSize = await heroTitle.evaluate(el => window.getComputedStyle(el).fontSize);
    const heroTitleSizePx = parseFloat(heroTitleFontSize);
    // Hero title should be readable but not as large as desktop (2.5rem = 40px at 768px or smaller)
    expect(heroTitleSizePx).toBeLessThanOrEqual(64); // 4rem
    expect(heroTitleSizePx).toBeGreaterThanOrEqual(32); // Minimum readable size

    // Verify content is properly contained within viewport
    const container = page.locator('.container').first();
    const containerBox = await container.boundingBox();
    expect(containerBox).not.toBeNull();
    expect(containerBox.width).toBeLessThanOrEqual(768);

    // Verify sections are visible and properly laid out
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();
  });

  /**
   * Test Case 2: Feature cards display in appropriate tablet layout
   * Input: Check feature cards at tablet width
   * Expected: Feature cards display in 2-column grid or appropriate tablet layout
   */
  test('TC2: Feature cards display in appropriate tablet layout at 768px', async ({ page }) => {
    // Set viewport to tablet width (768px)
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto(`file://${indexPath}`);

    // Scroll to features section
    await page.locator('#features').scrollIntoViewIfNeeded();
    await page.waitForTimeout(100);

    // Get all feature cards
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(3);

    // Verify features grid has appropriate layout
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Get grid computed style to check layout
    const gridStyle = await featuresGrid.evaluate(el => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns,
        gap: style.gap
      };
    });
    expect(gridStyle.display).toBe('grid');

    // Get positions of the first two cards to verify they are side by side or stacked
    const firstCard = featureCards.nth(0);
    const secondCard = featureCards.nth(1);
    const firstCardBox = await firstCard.boundingBox();
    const secondCardBox = await secondCard.boundingBox();

    expect(firstCardBox).not.toBeNull();
    expect(secondCardBox).not.toBeNull();

    // At 768px with minmax(300px, 1fr), we should have at least 2 columns
    // Check if cards are laid out properly (either 2 columns or single column depending on CSS)
    // The grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)) means:
    // At 768px, we can fit 2 columns of ~384px each OR fall back to appropriate layout

    // Verify cards have reasonable width for tablet viewing
    expect(firstCardBox.width).toBeGreaterThanOrEqual(280);
    expect(firstCardBox.width).toBeLessThanOrEqual(768);

    // Verify cards are not overflowing the viewport
    expect(firstCardBox.x + firstCardBox.width).toBeLessThanOrEqual(768 + 5); // Small tolerance
  });

  /**
   * Test Case 3: Layout remains appropriate at 1024px viewport
   * Input: Load page at 1024px width
   * Expected: Layout remains appropriate for larger tablet/small laptop
   */
  test('TC3: Layout remains appropriate at 1024px viewport width', async ({ page }) => {
    // Set viewport to larger tablet/small laptop width (1024px)
    await page.setViewportSize({ width: 1024, height: 768 });
    await page.goto(`file://${indexPath}`);

    // Verify page loads and hero section is visible
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify hero title is visible with appropriate styling
    const heroTitle = page.locator('[data-testid="hero-title"]');
    await expect(heroTitle).toBeVisible();
    const heroTitleFontSize = await heroTitle.evaluate(el => window.getComputedStyle(el).fontSize);
    const heroTitleSizePx = parseFloat(heroTitleFontSize);
    // At 1024px, hero title should be full desktop size (4rem = 64px)
    expect(heroTitleSizePx).toBe(64);

    // Verify navigation is visible (should show full menu at 1024px)
    const navLinks = page.locator('.nav-links');
    const navDisplay = await navLinks.evaluate(el => window.getComputedStyle(el).display);
    // At 1024px (above 768px breakpoint), nav-links should be visible
    expect(navDisplay).toBe('flex');

    // Verify feature cards can display in multi-column layout
    const featuresGrid = page.locator('.features-grid');
    await featuresGrid.scrollIntoViewIfNeeded();
    await page.waitForTimeout(100);

    const featureCards = page.locator('.feature-card');
    const firstCard = featureCards.nth(0);
    const secondCard = featureCards.nth(1);
    const thirdCard = featureCards.nth(2);

    const firstCardBox = await firstCard.boundingBox();
    const secondCardBox = await secondCard.boundingBox();
    const thirdCardBox = await thirdCard.boundingBox();

    expect(firstCardBox).not.toBeNull();
    expect(secondCardBox).not.toBeNull();
    expect(thirdCardBox).not.toBeNull();

    // At 1024px, all 3 cards should fit in one row (3 columns of ~333px each)
    // Verify cards are on the same row (same Y position with small tolerance)
    expect(Math.abs(firstCardBox.y - secondCardBox.y)).toBeLessThan(5);
    expect(Math.abs(secondCardBox.y - thirdCardBox.y)).toBeLessThan(5);

    // Verify footer displays in appropriate layout
    const footerContent = page.locator('.footer-content');
    await footerContent.scrollIntoViewIfNeeded();
    const footerStyle = await footerContent.evaluate(el => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns
      };
    });
    expect(footerStyle.display).toBe('grid');
    // At 1024px (above 768px), should have multi-column footer layout
    expect(footerStyle.gridTemplateColumns).not.toBe('1fr');
  });

  /**
   * Test Case 4: Navigation displays appropriately at tablet width
   * Input: Check navigation at tablet width
   * Expected: Navigation displays appropriately (full menu or tablet-optimized)
   */
  test('TC4: Navigation displays appropriately at tablet width', async ({ page }) => {
    // Set viewport to tablet width (768px)
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto(`file://${indexPath}`);

    // Verify navbar is visible
    const navbar = page.locator('[data-testid="navbar"]');
    await expect(navbar).toBeVisible();

    // Verify brand/logo is visible
    const navBrand = page.locator('[data-testid="nav-brand"]');
    await expect(navBrand).toBeVisible();

    // Check nav-links visibility at 768px
    // At exactly 768px (max-width: 768px media query), nav-links should be hidden
    const navLinks = page.locator('.nav-links');
    const navLinksDisplay = await navLinks.evaluate(el => window.getComputedStyle(el).display);

    // The CSS has @media (max-width: 768px) { .nav-links { display: none; } }
    // At exactly 768px, this should apply
    expect(navLinksDisplay).toBe('none');

    // Test at 769px - nav-links should be visible
    await page.setViewportSize({ width: 769, height: 1024 });
    await page.waitForTimeout(100);

    const navLinksDisplayAbove = await navLinks.evaluate(el => window.getComputedStyle(el).display);
    expect(navLinksDisplayAbove).toBe('flex');

    // Verify all navigation links are visible at 769px+
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
  });

  /**
   * Additional test: Text readability at tablet viewport
   * Verify text sizes and spacing are appropriate for tablet viewing
   */
  test('Text readability at tablet viewport', async ({ page }) => {
    // Set viewport to tablet width (800px - middle of tablet range)
    await page.setViewportSize({ width: 800, height: 1024 });
    await page.goto(`file://${indexPath}`);

    // Verify hero tagline has readable font size
    const heroTagline = page.locator('[data-testid="hero-tagline"]');
    await expect(heroTagline).toBeVisible();
    const taglineFontSize = await heroTagline.evaluate(el => window.getComputedStyle(el).fontSize);
    const taglineSizePx = parseFloat(taglineFontSize);
    // Tagline should be at least 16px for readability
    expect(taglineSizePx).toBeGreaterThanOrEqual(16);

    // Verify body text in feature cards is readable
    const featureCardText = page.locator('.feature-card p').first();
    await featureCardText.scrollIntoViewIfNeeded();
    const featureTextFontSize = await featureCardText.evaluate(el => window.getComputedStyle(el).fontSize);
    const featureTextSizePx = parseFloat(featureTextFontSize);
    // Feature text should be at least 14px for readability
    expect(featureTextSizePx).toBeGreaterThanOrEqual(14);

    // Verify line height is appropriate for readability
    const bodyLineHeight = await page.evaluate(() => window.getComputedStyle(document.body).lineHeight);
    // Line height should be at least 1.5 for good readability
    const lineHeightValue = parseFloat(bodyLineHeight);
    // If line-height is in px, it will be larger; if unitless ratio, check >= 1.5
    if (lineHeightValue < 20) {
      expect(lineHeightValue).toBeGreaterThanOrEqual(1.5);
    }
  });

  /**
   * Additional test: Code blocks are readable at tablet width
   */
  test('Code blocks are readable at tablet viewport', async ({ page }) => {
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto(`file://${indexPath}`);

    // Scroll to quickstart section
    const quickstartSection = page.locator('#quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();
    await page.waitForTimeout(100);

    // Verify code blocks are visible
    const codeBlock = page.locator('.code-block').first();
    await expect(codeBlock).toBeVisible();

    // Get code block dimensions
    const codeBlockBox = await codeBlock.boundingBox();
    expect(codeBlockBox).not.toBeNull();

    // Code block should fit within tablet viewport with some padding
    expect(codeBlockBox.width).toBeLessThanOrEqual(768 - 48); // Account for container padding

    // Verify code text is readable
    const codeText = page.locator('.code-block code').first();
    const codeFontSize = await codeText.evaluate(el => window.getComputedStyle(el).fontSize);
    const codeSizePx = parseFloat(codeFontSize);
    // Code should be at least 12px for readability on tablet
    expect(codeSizePx).toBeGreaterThanOrEqual(12);
  });

  /**
   * Additional test: Tables are accessible at tablet width
   */
  test('Tables are scrollable/accessible at tablet viewport', async ({ page }) => {
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto(`file://${indexPath}`);

    // Scroll to commands section
    const commandsSection = page.locator('#commands');
    await commandsSection.scrollIntoViewIfNeeded();
    await page.waitForTimeout(100);

    // Verify table wrapper has overflow handling
    const tableWrapper = page.locator('.table-wrapper').first();
    await expect(tableWrapper).toBeVisible();

    const overflowX = await tableWrapper.evaluate(el => window.getComputedStyle(el).overflowX);
    expect(overflowX).toBe('auto');

    // Verify table is visible
    const table = page.locator('table').first();
    await expect(table).toBeVisible();

    // Verify table cells have appropriate padding for touch
    const tableCell = page.locator('td').first();
    const cellPadding = await tableCell.evaluate(el => window.getComputedStyle(el).padding);
    // Cell should have some padding for readability
    expect(cellPadding).not.toBe('0px');
  });
});
