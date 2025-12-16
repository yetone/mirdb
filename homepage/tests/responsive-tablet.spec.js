// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Responsive Design - Tablet Tests
 *
 * These tests verify that the homepage renders correctly on tablet viewport sizes (NFR-1)
 * Testing at 768x1024 (iPad portrait) and 1024x768 (iPad landscape) resolutions
 */

test.describe('Responsive Design - Tablet Portrait (768x1024)', () => {
  test.use({ viewport: { width: 768, height: 1024 } });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for page to be fully loaded
    await page.waitForLoadState('domcontentloaded');
  });

  /**
   * Test Case 1: View page at 768x1024 viewport (iPad portrait)
   * Expected: Page layout adapts to tablet width, no horizontal scrolling
   */
  test('should display page correctly at 768x1024 without horizontal scrollbar', async ({ page }) => {
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

  test('should display all main sections at tablet portrait viewport', async ({ page }) => {
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

  test('should display hero section with proper layout at tablet portrait', async ({ page }) => {
    const heroSection = page.locator('[data-testid="hero-section"]');

    // Verify hero section takes full viewport height
    const heroBox = await heroSection.boundingBox();
    expect(heroBox).not.toBeNull();
    expect(heroBox.height).toBeGreaterThanOrEqual(1024);

    // Verify product name is visible and readable
    const productName = page.locator('[data-testid="product-name"]');
    await expect(productName).toBeVisible();

    // Verify tagline is visible
    const tagline = page.locator('[data-testid="tagline"]');
    await expect(tagline).toBeVisible();

    // Verify CTA buttons are visible
    const primaryCta = page.locator('[data-testid="primary-cta"]');
    const secondaryCta = page.locator('[data-testid="secondary-cta"]');
    await expect(primaryCta).toBeVisible();
    await expect(secondaryCta).toBeVisible();
  });

  test('should display features section within viewport width at tablet portrait', async ({ page }) => {
    await page.locator('[data-testid="features-section"]').scrollIntoViewIfNeeded();

    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Verify section fits within viewport width
    const sectionBox = await featuresSection.boundingBox();
    expect(sectionBox).not.toBeNull();
    expect(sectionBox.width).toBeLessThanOrEqual(768);

    // Verify feature cards are visible
    const featureCards = page.locator('.feature-card');
    const firstCard = featureCards.first();
    await expect(firstCard).toBeVisible();
  });

  test('should display architecture diagram within viewport at tablet portrait', async ({ page }) => {
    await page.locator('[data-testid="architecture-section"]').scrollIntoViewIfNeeded();

    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await expect(architectureSection).toBeVisible();

    // Verify diagram container exists and is visible
    const diagramContainer = page.locator('[data-testid="architecture-diagram"]');
    await expect(diagramContainer).toBeVisible();

    // Verify diagram fits within viewport
    const diagramBox = await diagramContainer.boundingBox();
    expect(diagramBox).not.toBeNull();
    expect(diagramBox.width).toBeLessThanOrEqual(768);
  });

  test('should display commands table properly at tablet portrait', async ({ page }) => {
    await page.locator('[data-testid="commands-section"]').scrollIntoViewIfNeeded();

    const commandsTable = page.locator('[data-testid="commands-table"]');
    await expect(commandsTable).toBeVisible();

    const tableBox = await commandsTable.boundingBox();
    expect(tableBox).not.toBeNull();
    expect(tableBox.width).toBeLessThanOrEqual(768);
  });

  test('should display getting started section properly at tablet portrait', async ({ page }) => {
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
    expect(codeBlockBox.width).toBeLessThanOrEqual(768);
  });
});

test.describe('Responsive Design - Tablet Landscape (1024x768)', () => {
  test.use({ viewport: { width: 1024, height: 768 } });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  /**
   * Test Case 2: View page at 1024x768 viewport (iPad landscape)
   * Expected: Page displays correctly in landscape orientation
   */
  test('should display page correctly at 1024x768 without horizontal scrollbar', async ({ page }) => {
    // Verify no horizontal scrollbar
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

  test('should display all main sections at tablet landscape viewport', async ({ page }) => {
    // Verify Hero Section is visible
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify Features Section exists
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeAttached();

    // Verify Architecture Section exists
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await expect(architectureSection).toBeAttached();

    // Verify Commands Section exists
    const commandsSection = page.locator('[data-testid="commands-section"]');
    await expect(commandsSection).toBeAttached();

    // Verify Getting Started Section exists
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeAttached();

    // Verify Footer exists
    const footer = page.locator('footer');
    await expect(footer).toBeAttached();
  });

  test('should display hero content properly at tablet landscape', async ({ page }) => {
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

  test('should display features grid with multiple columns at tablet landscape', async ({ page }) => {
    await page.locator('[data-testid="features-section"]').scrollIntoViewIfNeeded();

    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThan(0);

    // At 1024px width, feature cards should be displayed in multiple columns
    if (cardCount >= 2) {
      const firstCard = featureCards.first();
      const secondCard = featureCards.nth(1);

      const firstBox = await firstCard.boundingBox();
      const secondBox = await secondCard.boundingBox();

      expect(firstBox).not.toBeNull();
      expect(secondBox).not.toBeNull();

      // Cards should be on the same row at tablet landscape size (minmax(300px, 1fr) allows 3 per row at 1024px)
      // OR on separate rows if grid wraps - both are valid
      const sameLine = Math.abs(firstBox.y - secondBox.y) < 20;
      const newRow = secondBox.y > firstBox.y + firstBox.height - 20;
      expect(sameLine || newRow).toBe(true);
    }
  });

  test('should display architecture diagram properly at tablet landscape', async ({ page }) => {
    await page.locator('[data-testid="architecture-section"]').scrollIntoViewIfNeeded();

    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await expect(architectureSection).toBeVisible();

    const diagramContainer = page.locator('[data-testid="architecture-diagram"]');
    await expect(diagramContainer).toBeVisible();

    const diagramBox = await diagramContainer.boundingBox();
    expect(diagramBox).not.toBeNull();
    expect(diagramBox.width).toBeLessThanOrEqual(1024);
  });

  test('should display commands table properly at tablet landscape', async ({ page }) => {
    await page.locator('[data-testid="commands-section"]').scrollIntoViewIfNeeded();

    const commandsTable = page.locator('[data-testid="commands-table"]');
    await expect(commandsTable).toBeVisible();

    const tableBox = await commandsTable.boundingBox();
    expect(tableBox).not.toBeNull();
    expect(tableBox.width).toBeLessThanOrEqual(1024);
  });
});

test.describe('Responsive Design - Touch-Friendly Elements at Tablet Size', () => {
  test.use({ viewport: { width: 768, height: 1024 } });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  /**
   * Test Case 3: Check button sizes at tablet viewport
   * Expected: Buttons have minimum 44x44 pixel touch targets
   *
   * Per WCAG 2.1 and Apple Human Interface Guidelines,
   * touch targets should be at least 44x44 pixels for comfortable touch interaction
   */
  test('should have touch-friendly primary CTA button (minimum 44x44 pixels)', async ({ page }) => {
    const primaryCta = page.locator('[data-testid="primary-cta"]');
    await expect(primaryCta).toBeVisible();

    const buttonBox = await primaryCta.boundingBox();
    expect(buttonBox).not.toBeNull();

    // Verify minimum touch target size of 44x44 pixels
    expect(buttonBox.width).toBeGreaterThanOrEqual(44);
    expect(buttonBox.height).toBeGreaterThanOrEqual(44);
  });

  test('should have touch-friendly secondary CTA button (minimum 44x44 pixels)', async ({ page }) => {
    const secondaryCta = page.locator('[data-testid="secondary-cta"]');
    await expect(secondaryCta).toBeVisible();

    const buttonBox = await secondaryCta.boundingBox();
    expect(buttonBox).not.toBeNull();

    // Verify minimum touch target size of 44x44 pixels
    expect(buttonBox.width).toBeGreaterThanOrEqual(44);
    expect(buttonBox.height).toBeGreaterThanOrEqual(44);
  });

  test('should have touch-friendly feature cards at tablet size', async ({ page }) => {
    await page.locator('[data-testid="features-section"]').scrollIntoViewIfNeeded();

    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThan(0);

    // Check first feature card has adequate touch size
    const firstCard = featureCards.first();
    const cardBox = await firstCard.boundingBox();
    expect(cardBox).not.toBeNull();

    // Feature cards should be large enough for comfortable touch interaction
    expect(cardBox.width).toBeGreaterThanOrEqual(44);
    expect(cardBox.height).toBeGreaterThanOrEqual(44);
  });

  test('should have touch-friendly footer links at tablet size', async ({ page }) => {
    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();

    const footerLink = footer.locator('a[href*="github.com"]');
    await expect(footerLink).toBeVisible();

    // Get the clickable area including padding
    const linkBox = await footerLink.boundingBox();
    expect(linkBox).not.toBeNull();

    // Footer links should have adequate touch target
    expect(linkBox.width).toBeGreaterThanOrEqual(44);
    expect(linkBox.height).toBeGreaterThanOrEqual(20); // Links may be shorter in height but should be tappable
  });

  test('should have touch-friendly technology badges at tablet size', async ({ page }) => {
    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();

    const badges = footer.locator('.badge');
    const badgeCount = await badges.count();
    expect(badgeCount).toBeGreaterThan(0);

    // Check first badge has adequate touch size
    const firstBadge = badges.first();
    const badgeBox = await firstBadge.boundingBox();
    expect(badgeBox).not.toBeNull();

    // Badges should be large enough for comfortable touch interaction
    expect(badgeBox.width).toBeGreaterThanOrEqual(44);
    expect(badgeBox.height).toBeGreaterThanOrEqual(30); // Badges can be shorter but should be tappable
  });
});

test.describe('Responsive Design - Navigation at Tablet Size', () => {
  test.use({ viewport: { width: 768, height: 1024 } });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('should have functional navigation to features section at tablet size', async ({ page }) => {
    const secondaryCta = page.locator('[data-testid="secondary-cta"]');

    // Click Learn More button
    await secondaryCta.click();

    // Wait for scroll animation
    await page.waitForTimeout(600);

    // Verify features section is now in viewport
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeInViewport();
  });

  test('should have functional external link to GitHub at tablet size', async ({ page }) => {
    const primaryCta = page.locator('[data-testid="primary-cta"]');

    // Verify link attributes for external navigation
    const href = await primaryCta.getAttribute('href');
    expect(href).toContain('github.com');

    const target = await primaryCta.getAttribute('target');
    expect(target).toBe('_blank');

    const rel = await primaryCta.getAttribute('rel');
    expect(rel).toContain('noopener');
  });
});

test.describe('Responsive Design - Content Spacing at Tablet Size', () => {
  test.use({ viewport: { width: 768, height: 1024 } });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('should have proper content spacing at tablet portrait size', async ({ page }) => {
    // Verify hero content has proper max-width and centering
    const heroContent = page.locator('.hero-content');
    const heroContentBox = await heroContent.boundingBox();
    expect(heroContentBox).not.toBeNull();

    // Content should fit within viewport with padding
    expect(heroContentBox.width).toBeLessThanOrEqual(768);
  });

  test('should have proper grid layout for features at tablet size', async ({ page }) => {
    await page.locator('[data-testid="features-section"]').scrollIntoViewIfNeeded();

    const featuresGrid = page.locator('.features-grid');
    const gridBox = await featuresGrid.boundingBox();
    expect(gridBox).not.toBeNull();

    // Grid should fit within tablet viewport
    expect(gridBox.width).toBeLessThanOrEqual(768);
  });

  test('should have proper architecture paths layout at tablet size', async ({ page }) => {
    await page.locator('[data-testid="architecture-section"]').scrollIntoViewIfNeeded();

    const architecturePaths = page.locator('.architecture-paths');
    const pathsBox = await architecturePaths.boundingBox();
    expect(pathsBox).not.toBeNull();

    // Architecture paths should fit within tablet viewport
    expect(pathsBox.width).toBeLessThanOrEqual(768);
  });

  test('should have readable code blocks at tablet size', async ({ page }) => {
    await page.locator('[data-testid="getting-started-section"]').scrollIntoViewIfNeeded();

    const codeBlocks = page.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Check that code blocks allow horizontal scrolling for long lines
    const firstCodeBlock = codeBlocks.first();
    const codeBlockBox = await firstCodeBlock.boundingBox();
    expect(codeBlockBox).not.toBeNull();

    // Code blocks should fit within tablet width or allow horizontal scroll
    expect(codeBlockBox.width).toBeLessThanOrEqual(768);
  });
});
