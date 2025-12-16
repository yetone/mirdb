// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Responsive Design - Mobile Tests
 *
 * These tests verify that the homepage renders correctly on mobile viewport sizes (NFR-1)
 * Testing at 375x667 (iPhone SE) and 320x568 (iPhone 5/SE small) resolutions
 */

test.describe('Responsive Design - Mobile Viewport (375x667 iPhone SE)', () => {
  test.use({ viewport: { width: 375, height: 667 } });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for page to be fully loaded
    await page.waitForLoadState('domcontentloaded');
  });

  /**
   * Test Case 1: View page at 375x667 viewport (iPhone SE)
   * Expected: Page layout adapts to mobile width with single-column layout
   */
  test('should display page correctly at 375x667 without horizontal scrollbar', async ({ page }) => {
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

  test('should display all main sections at mobile viewport', async ({ page }) => {
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

  test('should display hero section with proper layout at mobile viewport', async ({ page }) => {
    const heroSection = page.locator('[data-testid="hero-section"]');

    // Verify hero section takes full viewport height
    const heroBox = await heroSection.boundingBox();
    expect(heroBox).not.toBeNull();
    expect(heroBox.height).toBeGreaterThanOrEqual(667);

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

  test('should display features grid with single-column layout at mobile viewport', async ({ page }) => {
    await page.locator('[data-testid="features-section"]').scrollIntoViewIfNeeded();

    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThan(0);

    // At 375px width, feature cards should be displayed in single column (stacked vertically)
    if (cardCount >= 2) {
      const firstCard = featureCards.first();
      const secondCard = featureCards.nth(1);

      const firstBox = await firstCard.boundingBox();
      const secondBox = await secondCard.boundingBox();

      expect(firstBox).not.toBeNull();
      expect(secondBox).not.toBeNull();

      // Cards should be stacked vertically (second card below first card)
      expect(secondBox.y).toBeGreaterThan(firstBox.y + firstBox.height - 10);
    }
  });

  test('should display CTA buttons stacked vertically at mobile viewport', async ({ page }) => {
    const primaryCta = page.locator('[data-testid="primary-cta"]');
    const secondaryCta = page.locator('[data-testid="secondary-cta"]');

    const primaryBox = await primaryCta.boundingBox();
    const secondaryBox = await secondaryCta.boundingBox();

    expect(primaryBox).not.toBeNull();
    expect(secondaryBox).not.toBeNull();

    // At mobile size, buttons should be stacked vertically (not side by side)
    // The secondary CTA should be below the primary CTA
    expect(secondaryBox.y).toBeGreaterThan(primaryBox.y);
  });

  test('should display architecture diagram within viewport at mobile', async ({ page }) => {
    await page.locator('[data-testid="architecture-section"]').scrollIntoViewIfNeeded();

    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await expect(architectureSection).toBeVisible();

    // Verify diagram container exists and is visible
    const diagramContainer = page.locator('[data-testid="architecture-diagram"]');
    await expect(diagramContainer).toBeVisible();

    // Verify diagram fits within viewport (may allow horizontal scroll within container)
    const diagramBox = await diagramContainer.boundingBox();
    expect(diagramBox).not.toBeNull();
    expect(diagramBox.width).toBeLessThanOrEqual(375);
  });

  test('should display commands table properly at mobile viewport', async ({ page }) => {
    await page.locator('[data-testid="commands-section"]').scrollIntoViewIfNeeded();

    const commandsTable = page.locator('[data-testid="commands-table"]');
    await expect(commandsTable).toBeVisible();

    const tableBox = await commandsTable.boundingBox();
    expect(tableBox).not.toBeNull();
    expect(tableBox.width).toBeLessThanOrEqual(375);
  });

  test('should display getting started section properly at mobile viewport', async ({ page }) => {
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
    expect(codeBlockBox.width).toBeLessThanOrEqual(375);
  });
});

test.describe('Responsive Design - Smallest Mobile Viewport (320x568 iPhone 5/SE small)', () => {
  test.use({ viewport: { width: 320, height: 568 } });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  /**
   * Test Case 2: View page at 320x568 viewport (iPhone 5/SE small)
   * Expected: Page remains functional on smallest common mobile size
   */
  test('should display page correctly at 320x568 without horizontal scrollbar', async ({ page }) => {
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

  test('should display all main sections at smallest mobile viewport', async ({ page }) => {
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

  test('should display hero content properly at smallest mobile viewport', async ({ page }) => {
    // Verify product name is visible and readable
    const productName = page.locator('[data-testid="product-name"]');
    await expect(productName).toBeVisible();

    // Verify tagline is visible and readable
    const tagline = page.locator('[data-testid="tagline"]');
    await expect(tagline).toBeVisible();

    // Verify CTA buttons are visible
    const primaryCta = page.locator('[data-testid="primary-cta"]');
    const secondaryCta = page.locator('[data-testid="secondary-cta"]');
    await expect(primaryCta).toBeVisible();
    await expect(secondaryCta).toBeVisible();
  });

  test('should display features section within viewport at smallest mobile', async ({ page }) => {
    await page.locator('[data-testid="features-section"]').scrollIntoViewIfNeeded();

    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Verify section fits within viewport width
    const sectionBox = await featuresSection.boundingBox();
    expect(sectionBox).not.toBeNull();
    expect(sectionBox.width).toBeLessThanOrEqual(320);

    // Verify feature cards are visible
    const featureCards = page.locator('.feature-card');
    const firstCard = featureCards.first();
    await expect(firstCard).toBeVisible();
  });

  test('should display architecture diagram within viewport at smallest mobile', async ({ page }) => {
    await page.locator('[data-testid="architecture-section"]').scrollIntoViewIfNeeded();

    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await expect(architectureSection).toBeVisible();

    const diagramContainer = page.locator('[data-testid="architecture-diagram"]');
    await expect(diagramContainer).toBeVisible();

    const diagramBox = await diagramContainer.boundingBox();
    expect(diagramBox).not.toBeNull();
    expect(diagramBox.width).toBeLessThanOrEqual(320);
  });

  test('should display commands table at smallest mobile viewport', async ({ page }) => {
    await page.locator('[data-testid="commands-section"]').scrollIntoViewIfNeeded();

    const commandsTable = page.locator('[data-testid="commands-table"]');
    await expect(commandsTable).toBeVisible();

    const tableBox = await commandsTable.boundingBox();
    expect(tableBox).not.toBeNull();
    expect(tableBox.width).toBeLessThanOrEqual(320);
  });

  test('should display getting started section at smallest mobile viewport', async ({ page }) => {
    await page.locator('[data-testid="getting-started-section"]').scrollIntoViewIfNeeded();

    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeVisible();

    // Verify code blocks are visible
    const codeBlocks = page.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    const firstCodeBlock = codeBlocks.first();
    const codeBlockBox = await firstCodeBlock.boundingBox();
    expect(codeBlockBox).not.toBeNull();
    expect(codeBlockBox.width).toBeLessThanOrEqual(320);
  });
});

test.describe('Responsive Design - Mobile Typography', () => {
  test.use({ viewport: { width: 375, height: 667 } });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  /**
   * Test Case 3: Check text size at mobile viewport
   * Expected: Body text is at least 16px, readable without zooming
   */
  test('should have readable body text size of at least 16px', async ({ page }) => {
    // Check body font size
    const bodyFontSize = await page.evaluate(() => {
      return parseFloat(window.getComputedStyle(document.body).fontSize);
    });
    // Body font size should be at least 16px for readability on mobile
    expect(bodyFontSize).toBeGreaterThanOrEqual(16);
  });

  test('should have readable tagline text at mobile viewport', async ({ page }) => {
    const tagline = page.locator('[data-testid="tagline"]');
    await expect(tagline).toBeVisible();

    // Check tagline font size
    const taglineFontSize = await tagline.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // Tagline should be at least 16px for readability
    expect(taglineFontSize).toBeGreaterThanOrEqual(16);
  });

  test('should have readable feature card text at mobile viewport', async ({ page }) => {
    await page.locator('[data-testid="features-section"]').scrollIntoViewIfNeeded();

    const featureCard = page.locator('.feature-card').first();
    const featureText = featureCard.locator('p');

    const fontSize = await featureText.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // Feature card text should be at least 14px for readability
    expect(fontSize).toBeGreaterThanOrEqual(14);
  });

  test('should have readable code block text at mobile viewport', async ({ page }) => {
    await page.locator('[data-testid="getting-started-section"]').scrollIntoViewIfNeeded();

    const codeBlock = page.locator('.code-block code').first();
    await expect(codeBlock).toBeVisible();

    const fontSize = await codeBlock.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // Code block text should be at least 14px for readability
    expect(fontSize).toBeGreaterThanOrEqual(14);
  });

  test('should have proper line height for readability at mobile viewport', async ({ page }) => {
    // Check body line height
    const lineHeight = await page.evaluate(() => {
      const style = window.getComputedStyle(document.body);
      const fontSize = parseFloat(style.fontSize);
      const lineHeightValue = style.lineHeight;
      // If line-height is normal or a number, convert to ratio
      if (lineHeightValue === 'normal') {
        return 1.2; // normal is typically around 1.2
      }
      const lineHeightPx = parseFloat(lineHeightValue);
      return lineHeightPx / fontSize;
    });
    // Line height should be at least 1.4 for comfortable reading
    expect(lineHeight).toBeGreaterThanOrEqual(1.4);
  });
});

test.describe('Responsive Design - Mobile Navigation', () => {
  test.use({ viewport: { width: 375, height: 667 } });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  /**
   * Test Case 4: Test mobile navigation interaction
   * Expected: Navigation is accessible via hamburger menu or visible links
   */
  test('should have accessible primary CTA navigation at mobile', async ({ page }) => {
    const primaryCta = page.locator('[data-testid="primary-cta"]');
    await expect(primaryCta).toBeVisible();

    // Verify button is clickable/tappable with sufficient size
    const buttonBox = await primaryCta.boundingBox();
    expect(buttonBox).not.toBeNull();
    expect(buttonBox.width).toBeGreaterThanOrEqual(44); // Minimum touch target
    expect(buttonBox.height).toBeGreaterThanOrEqual(44);

    // Verify link attributes for external navigation
    const href = await primaryCta.getAttribute('href');
    expect(href).toContain('github.com');
  });

  test('should have accessible secondary CTA navigation at mobile', async ({ page }) => {
    const secondaryCta = page.locator('[data-testid="secondary-cta"]');
    await expect(secondaryCta).toBeVisible();

    // Verify button is clickable/tappable with sufficient size
    const buttonBox = await secondaryCta.boundingBox();
    expect(buttonBox).not.toBeNull();
    expect(buttonBox.width).toBeGreaterThanOrEqual(44); // Minimum touch target
    expect(buttonBox.height).toBeGreaterThanOrEqual(44);

    // Verify it links to features section
    const href = await secondaryCta.getAttribute('href');
    expect(href).toBe('#features');
  });

  test('should have functional smooth scroll navigation at mobile', async ({ page }) => {
    const secondaryCta = page.locator('[data-testid="secondary-cta"]');

    // Click Learn More button
    await secondaryCta.click();

    // Wait for scroll animation
    await page.waitForTimeout(600);

    // Verify features section is now in viewport
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeInViewport();
  });

  test('should have accessible footer navigation at mobile', async ({ page }) => {
    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();

    await expect(footer).toBeVisible();

    // Verify GitHub link in footer is visible and accessible
    const footerLink = footer.locator('a[href*="github.com"]');
    await expect(footerLink).toBeVisible();

    // Check touch target size
    const linkBox = await footerLink.boundingBox();
    expect(linkBox).not.toBeNull();
    expect(linkBox.width).toBeGreaterThanOrEqual(44);
  });

  test('should display technology badges accessibly at mobile', async ({ page }) => {
    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();

    const badges = footer.locator('.badge');
    const badgeCount = await badges.count();
    expect(badgeCount).toBeGreaterThan(0);

    // Verify badges are visible and wrapped properly for mobile
    const firstBadge = badges.first();
    await expect(firstBadge).toBeVisible();
  });
});

test.describe('Responsive Design - Mobile Touch Targets', () => {
  test.use({ viewport: { width: 375, height: 667 } });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  /**
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

  test('should have touch-friendly feature cards at mobile size', async ({ page }) => {
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

  test('should have touch-friendly footer links at mobile size', async ({ page }) => {
    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();

    const footerLink = footer.locator('a[href*="github.com"]');
    await expect(footerLink).toBeVisible();

    // Get the clickable area including padding
    const linkBox = await footerLink.boundingBox();
    expect(linkBox).not.toBeNull();

    // Footer links should have adequate touch target
    expect(linkBox.width).toBeGreaterThanOrEqual(44);
    expect(linkBox.height).toBeGreaterThanOrEqual(20);
  });
});

test.describe('Responsive Design - Mobile Content Spacing', () => {
  test.use({ viewport: { width: 375, height: 667 } });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('should have proper content padding at mobile viewport', async ({ page }) => {
    // Verify hero content fits within viewport with padding
    const heroContent = page.locator('.hero-content');
    const heroContentBox = await heroContent.boundingBox();
    expect(heroContentBox).not.toBeNull();

    // Content should fit within viewport width
    expect(heroContentBox.width).toBeLessThanOrEqual(375);
  });

  test('should have proper section padding at mobile viewport', async ({ page }) => {
    await page.locator('[data-testid="features-section"]').scrollIntoViewIfNeeded();

    const featuresSection = page.locator('[data-testid="features-section"]');
    const sectionBox = await featuresSection.boundingBox();
    expect(sectionBox).not.toBeNull();

    // Section should fit within viewport
    expect(sectionBox.width).toBeLessThanOrEqual(375);
  });

  test('should have proper grid layout for features at mobile size', async ({ page }) => {
    await page.locator('[data-testid="features-section"]').scrollIntoViewIfNeeded();

    const featuresGrid = page.locator('.features-grid');
    const gridBox = await featuresGrid.boundingBox();
    expect(gridBox).not.toBeNull();

    // Grid should fit within mobile viewport
    expect(gridBox.width).toBeLessThanOrEqual(375);
  });

  test('should have readable code blocks at mobile size', async ({ page }) => {
    await page.locator('[data-testid="getting-started-section"]').scrollIntoViewIfNeeded();

    const codeBlocks = page.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Check that code blocks allow horizontal scrolling for long lines
    const firstCodeBlock = codeBlocks.first();
    const codeBlockBox = await firstCodeBlock.boundingBox();
    expect(codeBlockBox).not.toBeNull();

    // Code blocks should fit within mobile width or allow horizontal scroll
    expect(codeBlockBox.width).toBeLessThanOrEqual(375);
  });

  test('should have proper architecture paths layout at mobile size', async ({ page }) => {
    await page.locator('[data-testid="architecture-section"]').scrollIntoViewIfNeeded();

    const architecturePaths = page.locator('.architecture-paths');
    const pathsBox = await architecturePaths.boundingBox();
    expect(pathsBox).not.toBeNull();

    // Architecture paths should fit within mobile viewport
    expect(pathsBox.width).toBeLessThanOrEqual(375);
  });
});
