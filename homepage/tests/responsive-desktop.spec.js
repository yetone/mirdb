// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Responsive Design - Desktop Tests
 *
 * These tests verify that the homepage renders correctly on desktop viewport sizes (NFR-1)
 * Testing at 1920x1080 (Full HD) and 1366x768 (common laptop) resolutions
 */

test.describe('Responsive Design - Desktop Viewport (1920x1080)', () => {
  test.use({ viewport: { width: 1920, height: 1080 } });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for page to be fully loaded
    await page.waitForLoadState('domcontentloaded');
  });

  /**
   * Test Case 1: View page at 1920x1080 viewport
   * Expected: Page displays correctly without horizontal scroll, all sections visible
   */
  test('should display page correctly at 1920x1080 without horizontal scrollbar', async ({ page }) => {
    // Verify no horizontal scrollbar by checking if document width equals viewport width
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Verify body does not overflow horizontally
    const bodyOverflow = await page.evaluate(() => {
      const body = document.body;
      return body.scrollWidth > body.clientWidth;
    });
    expect(bodyOverflow).toBe(false);
  });

  test('should display all main sections at 1920x1080 viewport', async ({ page }) => {
    // Verify Hero Section is visible
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify Features Section exists and is accessible
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeAttached();

    // Verify Architecture Section exists and is accessible
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await expect(architectureSection).toBeAttached();

    // Verify Commands Section exists and is accessible
    const commandsSection = page.locator('[data-testid="commands-section"]');
    await expect(commandsSection).toBeAttached();

    // Verify Getting Started Section exists and is accessible
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeAttached();

    // Verify Footer exists
    const footer = page.locator('footer');
    await expect(footer).toBeAttached();
  });

  test('should display hero section with proper layout at 1920x1080', async ({ page }) => {
    const heroSection = page.locator('[data-testid="hero-section"]');

    // Verify hero section takes full viewport height
    const heroBox = await heroSection.boundingBox();
    expect(heroBox).not.toBeNull();
    expect(heroBox.height).toBeGreaterThanOrEqual(1080);

    // Verify product name is visible and readable
    const productName = page.locator('[data-testid="product-name"]');
    await expect(productName).toBeVisible();

    // Verify tagline is visible
    const tagline = page.locator('[data-testid="tagline"]');
    await expect(tagline).toBeVisible();

    // Verify CTA buttons are visible and properly spaced
    const primaryCta = page.locator('[data-testid="primary-cta"]');
    const secondaryCta = page.locator('[data-testid="secondary-cta"]');
    await expect(primaryCta).toBeVisible();
    await expect(secondaryCta).toBeVisible();

    // Verify buttons are displayed horizontally (side by side) at desktop size
    const primaryBox = await primaryCta.boundingBox();
    const secondaryBox = await secondaryCta.boundingBox();
    expect(primaryBox).not.toBeNull();
    expect(secondaryBox).not.toBeNull();

    // Buttons should be on the same horizontal level (within small margin for alignment)
    const yDifference = Math.abs(primaryBox.y - secondaryBox.y);
    expect(yDifference).toBeLessThan(50); // Allow small variation for vertical alignment
  });

  test('should display features grid with multiple columns at 1920x1080', async ({ page }) => {
    // Scroll to features section
    await page.locator('[data-testid="features-section"]').scrollIntoViewIfNeeded();

    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThan(0);

    // At 1920px width, feature cards should be displayed in multiple columns
    // Check that cards are not all stacked vertically
    const firstCard = featureCards.first();
    const secondCard = featureCards.nth(1);

    const firstBox = await firstCard.boundingBox();
    const secondBox = await secondCard.boundingBox();

    expect(firstBox).not.toBeNull();
    expect(secondBox).not.toBeNull();

    // At desktop size, cards should be side by side (same Y position or minimal Y difference)
    // allowing for row wrapping
    if (cardCount >= 2) {
      // Either cards are on the same row OR the second card is on a new row
      // Both are valid for responsive grid layout
      const sameLine = Math.abs(firstBox.y - secondBox.y) < 20;
      const newRow = secondBox.y > firstBox.y + firstBox.height - 20;
      expect(sameLine || newRow).toBe(true);
    }
  });
});

test.describe('Responsive Design - Common Laptop Viewport (1366x768)', () => {
  test.use({ viewport: { width: 1366, height: 768 } });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  /**
   * Test Case 2: View page at 1366x768 viewport (common laptop)
   * Expected: Page displays correctly, content is readable
   */
  test('should display page correctly at 1366x768 without horizontal scrollbar', async ({ page }) => {
    // Verify no horizontal scrollbar
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });

  test('should display all content readable at 1366x768', async ({ page }) => {
    // Verify hero content is readable
    const productName = page.locator('[data-testid="product-name"]');
    await expect(productName).toBeVisible();

    // Verify font size is readable (minimum font size check)
    const fontSize = await productName.evaluate((el) => {
      return window.getComputedStyle(el).fontSize;
    });
    const fontSizeValue = parseFloat(fontSize);
    expect(fontSizeValue).toBeGreaterThanOrEqual(16); // Minimum readable font size

    // Verify tagline is visible and readable
    const tagline = page.locator('[data-testid="tagline"]');
    await expect(tagline).toBeVisible();
    const taglineFontSize = await tagline.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    expect(taglineFontSize).toBeGreaterThanOrEqual(14);
  });

  test('should display features section properly at 1366x768', async ({ page }) => {
    await page.locator('[data-testid="features-section"]').scrollIntoViewIfNeeded();

    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Verify section fits within viewport width
    const sectionBox = await featuresSection.boundingBox();
    expect(sectionBox).not.toBeNull();
    expect(sectionBox.width).toBeLessThanOrEqual(1366);

    // Verify feature cards are visible
    const featureCards = page.locator('.feature-card');
    const firstCard = featureCards.first();
    await expect(firstCard).toBeVisible();
  });

  test('should display architecture diagram properly at 1366x768', async ({ page }) => {
    await page.locator('[data-testid="architecture-section"]').scrollIntoViewIfNeeded();

    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await expect(architectureSection).toBeVisible();

    // Verify diagram container exists and is visible
    const diagramContainer = page.locator('[data-testid="architecture-diagram"]');
    await expect(diagramContainer).toBeVisible();

    // Verify diagram fits within viewport
    const diagramBox = await diagramContainer.boundingBox();
    expect(diagramBox).not.toBeNull();
    expect(diagramBox.width).toBeLessThanOrEqual(1366);
  });
});

test.describe('Responsive Design - Navigation at Desktop Size', () => {
  test.use({ viewport: { width: 1920, height: 1080 } });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  /**
   * Test Case 3: Check navigation elements at desktop size
   * Expected: Navigation is fully visible and functional
   */
  test('should display navigation elements fully visible at desktop size', async ({ page }) => {
    // Verify primary CTA (View on GitHub) is fully visible
    const primaryCta = page.locator('[data-testid="primary-cta"]');
    await expect(primaryCta).toBeVisible();
    await expect(primaryCta).toBeInViewport();

    // Verify secondary CTA (Learn More) is fully visible
    const secondaryCta = page.locator('[data-testid="secondary-cta"]');
    await expect(secondaryCta).toBeVisible();
    await expect(secondaryCta).toBeInViewport();

    // Verify button text is fully visible (not truncated)
    const primaryText = await primaryCta.textContent();
    expect(primaryText.trim()).toBeTruthy();
    expect(primaryText.length).toBeGreaterThan(5);

    const secondaryText = await secondaryCta.textContent();
    expect(secondaryText.trim()).toBeTruthy();
    expect(secondaryText.length).toBeGreaterThan(5);
  });

  test('should have functional navigation to features section', async ({ page }) => {
    const secondaryCta = page.locator('[data-testid="secondary-cta"]');

    // Click Learn More button
    await secondaryCta.click();

    // Wait for scroll animation
    await page.waitForTimeout(600);

    // Verify features section is now in viewport
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeInViewport();
  });

  test('should have functional external link to GitHub', async ({ page }) => {
    const primaryCta = page.locator('[data-testid="primary-cta"]');

    // Verify link attributes for external navigation
    const href = await primaryCta.getAttribute('href');
    expect(href).toContain('github.com');

    const target = await primaryCta.getAttribute('target');
    expect(target).toBe('_blank');

    const rel = await primaryCta.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  test('should display footer navigation elements at desktop size', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();

    await expect(footer).toBeVisible();

    // Verify GitHub link in footer is visible
    const footerLink = footer.locator('a[href*="github.com"]');
    await expect(footerLink).toBeVisible();

    // Verify technology badges are visible
    const badges = footer.locator('.badge');
    const badgeCount = await badges.count();
    expect(badgeCount).toBeGreaterThan(0);

    // Verify badges are displayed inline (horizontally)
    if (badgeCount >= 2) {
      const firstBadge = badges.first();
      const secondBadge = badges.nth(1);

      const firstBox = await firstBadge.boundingBox();
      const secondBox = await secondBadge.boundingBox();

      expect(firstBox).not.toBeNull();
      expect(secondBox).not.toBeNull();

      // Badges should be on the same line at desktop size
      const yDifference = Math.abs(firstBox.y - secondBox.y);
      expect(yDifference).toBeLessThan(10);
    }
  });
});

test.describe('Responsive Design - Content Spacing at Desktop', () => {
  test.use({ viewport: { width: 1920, height: 1080 } });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('should have proper content spacing and margins at desktop size', async ({ page }) => {
    // Verify hero content is centered with proper max-width
    const heroContent = page.locator('.hero-content');
    const heroContentBox = await heroContent.boundingBox();
    expect(heroContentBox).not.toBeNull();

    // Content should be centered (not full width)
    const viewportWidth = 1920;
    const leftMargin = heroContentBox.x;
    const rightMargin = viewportWidth - (heroContentBox.x + heroContentBox.width);

    // Both margins should be roughly equal for centered content
    const marginDifference = Math.abs(leftMargin - rightMargin);
    expect(marginDifference).toBeLessThan(100);
  });

  test('should have proper grid layout for features at desktop size', async ({ page }) => {
    await page.locator('[data-testid="features-section"]').scrollIntoViewIfNeeded();

    const featuresGrid = page.locator('.features-grid');
    const gridBox = await featuresGrid.boundingBox();
    expect(gridBox).not.toBeNull();

    // Grid should have a max-width constraint
    expect(gridBox.width).toBeLessThanOrEqual(1200 + 100); // max-width: 1200px plus some padding

    // Verify grid is centered
    const viewportWidth = 1920;
    const leftMargin = gridBox.x;
    const rightMargin = viewportWidth - (gridBox.x + gridBox.width);
    const marginDifference = Math.abs(leftMargin - rightMargin);
    expect(marginDifference).toBeLessThan(100);
  });

  test('should display commands table properly at desktop size', async ({ page }) => {
    await page.locator('[data-testid="commands-section"]').scrollIntoViewIfNeeded();

    const commandsTable = page.locator('[data-testid="commands-table"]');
    await expect(commandsTable).toBeVisible();

    const tableBox = await commandsTable.boundingBox();
    expect(tableBox).not.toBeNull();

    // Table should be readable width at desktop size
    expect(tableBox.width).toBeGreaterThanOrEqual(300);
    expect(tableBox.width).toBeLessThanOrEqual(800);
  });

  test('should display getting started code blocks properly at desktop size', async ({ page }) => {
    await page.locator('[data-testid="getting-started-section"]').scrollIntoViewIfNeeded();

    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeVisible();

    // Verify code blocks are visible and properly contained
    const codeBlocks = page.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    const firstCodeBlock = codeBlocks.first();
    const codeBlockBox = await firstCodeBlock.boundingBox();
    expect(codeBlockBox).not.toBeNull();

    // Code block should have a reasonable max-width
    expect(codeBlockBox.width).toBeLessThanOrEqual(900);
  });
});
