// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Mobile Responsive Design (NFR-2)
 * Verify homepage is responsive across phone, tablet, and desktop breakpoints
 */

// Viewport configurations for different devices
const VIEWPORTS = {
  mobile: { width: 375, height: 667 },    // iPhone SE
  tablet: { width: 768, height: 1024 },   // iPad
  desktop: { width: 1440, height: 900 }   // Desktop
};

test.describe('Mobile Responsive Design (NFR-2)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Mobile viewport (375px width)
   * Verify page renders correctly without horizontal overflow
   */
  test('TC1: Page renders correctly at mobile viewport (375px) without horizontal overflow', async ({ page }) => {
    // Set viewport to mobile width
    await page.setViewportSize(VIEWPORTS.mobile);

    // Wait for page to fully render
    await page.waitForLoadState('domcontentloaded');

    // Check that the page renders - body should be visible
    const body = page.locator('body');
    await expect(body).toBeVisible();

    // Check for horizontal overflow by comparing scroll width vs viewport width
    const hasHorizontalOverflow = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalOverflow).toBe(false);

    // Verify hero section is visible
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify hero content fits within viewport
    const heroBoundingBox = await heroSection.boundingBox();
    expect(heroBoundingBox).not.toBeNull();
    expect(heroBoundingBox.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width);
  });

  /**
   * Test Case 2: Tablet viewport (768px width)
   * Verify page renders correctly with tablet layout
   */
  test('TC2: Page renders correctly at tablet viewport (768px) with tablet layout', async ({ page }) => {
    // Set viewport to tablet width
    await page.setViewportSize(VIEWPORTS.tablet);

    // Wait for page to fully render
    await page.waitForLoadState('domcontentloaded');

    // Check that the page renders
    const body = page.locator('body');
    await expect(body).toBeVisible();

    // Check for horizontal overflow
    const hasHorizontalOverflow = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalOverflow).toBe(false);

    // Verify all major sections are visible
    await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();
    await expect(page.locator('[data-testid="comparison-table"]')).toBeVisible();
    await expect(page.locator('[data-testid="quick-start-section"]')).toBeVisible();
    await expect(page.locator('[data-testid="architecture-section"]')).toBeVisible();

    // Verify navigation is visible
    await expect(page.locator('[data-testid="navigation"]')).toBeVisible();

    // Verify table container scrolls horizontally if needed
    const tableContainer = page.locator('.table-container');
    await expect(tableContainer).toBeVisible();
  });

  /**
   * Test Case 3: Desktop viewport (1440px width)
   * Verify page renders correctly with full desktop layout
   */
  test('TC3: Page renders correctly at desktop viewport (1440px) with full desktop layout', async ({ page }) => {
    // Set viewport to desktop width
    await page.setViewportSize(VIEWPORTS.desktop);

    // Wait for page to fully render
    await page.waitForLoadState('domcontentloaded');

    // Check that the page renders
    const body = page.locator('body');
    await expect(body).toBeVisible();

    // Check for horizontal overflow
    const hasHorizontalOverflow = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalOverflow).toBe(false);

    // Verify all major sections are visible
    await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();
    await expect(page.locator('[data-testid="comparison-table"]')).toBeVisible();
    await expect(page.locator('[data-testid="quick-start-section"]')).toBeVisible();
    await expect(page.locator('[data-testid="architecture-section"]')).toBeVisible();

    // Verify navigation links are visible (not collapsed)
    await expect(page.locator('[data-testid="nav-links"]')).toBeVisible();
    const navLinks = page.locator('[data-testid="nav-links"] li');
    const linkCount = await navLinks.count();
    expect(linkCount).toBeGreaterThanOrEqual(3);

    // Verify CTA buttons are displayed side by side
    const ctaButtons = page.locator('.hero-cta-buttons');
    const ctaStyle = await ctaButtons.evaluate(el => {
      const style = window.getComputedStyle(el);
      return {
        flexDirection: style.flexDirection,
        display: style.display
      };
    });
    expect(ctaStyle.display).toBe('flex');
    expect(ctaStyle.flexDirection).toBe('row');
  });

  /**
   * Test Case 4: Navigation on mobile
   * Verify navigation is accessible (hamburger menu or adapted layout)
   */
  test('TC4: Navigation is accessible on mobile', async ({ page }) => {
    // Set viewport to mobile width
    await page.setViewportSize(VIEWPORTS.mobile);

    // Wait for page to fully render
    await page.waitForLoadState('domcontentloaded');

    // Verify navigation header is visible
    const navigation = page.locator('[data-testid="navigation"]');
    await expect(navigation).toBeVisible();

    // Verify navigation brand is visible
    const navBrand = page.locator('[data-testid="nav-brand"]');
    await expect(navBrand).toBeVisible();

    // Verify nav links section exists (adapted for mobile)
    const navLinks = page.locator('[data-testid="nav-links"]');
    await expect(navLinks).toBeVisible();

    // Verify links are still accessible and clickable
    const gettingStartedLink = page.locator('[data-testid="nav-getting-started"]');
    await expect(gettingStartedLink).toBeVisible();
    await expect(gettingStartedLink).toBeEnabled();

    // Verify navigation fits within mobile viewport (no overflow)
    const navContainer = page.locator('.nav-container');
    const navBoundingBox = await navContainer.boundingBox();
    expect(navBoundingBox).not.toBeNull();
    expect(navBoundingBox.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width);
  });

  /**
   * Test Case 5: Feature cards on mobile
   * Verify feature cards stack vertically on mobile
   * Note: Feature cards are represented in comparison table and architecture sections
   */
  test('TC5: Feature cards and content sections stack vertically on mobile', async ({ page }) => {
    // Set viewport to mobile width
    await page.setViewportSize(VIEWPORTS.mobile);

    // Wait for page to fully render
    await page.waitForLoadState('domcontentloaded');

    // Verify CTA buttons stack vertically on mobile
    const ctaButtons = page.locator('.hero-cta-buttons');
    await expect(ctaButtons).toBeVisible();

    const ctaStyle = await ctaButtons.evaluate(el => {
      const style = window.getComputedStyle(el);
      return {
        flexDirection: style.flexDirection,
        display: style.display
      };
    });
    expect(ctaStyle.display).toBe('flex');
    expect(ctaStyle.flexDirection).toBe('column');

    // Verify code blocks are contained within viewport
    const installCodeBlock = page.locator('[data-testid="install-code-block"]');
    await installCodeBlock.scrollIntoViewIfNeeded();
    await expect(installCodeBlock).toBeVisible();

    const codeBlockBoundingBox = await installCodeBlock.boundingBox();
    expect(codeBlockBoundingBox).not.toBeNull();
    expect(codeBlockBoundingBox.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width);

    // Verify architecture section adapts to mobile
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await architectureSection.scrollIntoViewIfNeeded();
    await expect(architectureSection).toBeVisible();
  });

  /**
   * Test Case 6: Images on mobile
   * Verify images scale appropriately on mobile devices
   */
  test('TC6: Images scale appropriately on mobile devices', async ({ page }) => {
    // Set viewport to mobile width
    await page.setViewportSize(VIEWPORTS.mobile);

    // Wait for page to fully render
    await page.waitForLoadState('domcontentloaded');

    // Verify hero logo scales appropriately
    const heroLogo = page.locator('[data-testid="hero-logo"]');
    await expect(heroLogo).toBeVisible();

    const logoBoundingBox = await heroLogo.boundingBox();
    expect(logoBoundingBox).not.toBeNull();
    // Logo should fit within mobile viewport width
    expect(logoBoundingBox.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width);
    // Logo should be reasonably sized (not too large, not too small)
    expect(logoBoundingBox.width).toBeLessThanOrEqual(150);
    expect(logoBoundingBox.width).toBeGreaterThanOrEqual(50);

    // Verify architecture diagram scales appropriately
    const architectureDiagram = page.locator('[data-testid="architecture-diagram"]');
    await architectureDiagram.scrollIntoViewIfNeeded();
    await expect(architectureDiagram).toBeVisible();

    const diagramBoundingBox = await architectureDiagram.boundingBox();
    expect(diagramBoundingBox).not.toBeNull();
    // Diagram should fit within mobile viewport
    expect(diagramBoundingBox.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width);

    // Verify badge images scale appropriately
    const badges = page.locator('[data-testid="status-badges"] img');
    const badgeCount = await badges.count();
    expect(badgeCount).toBeGreaterThan(0);

    for (let i = 0; i < badgeCount; i++) {
      const badge = badges.nth(i);
      await expect(badge).toBeVisible();
      const badgeBoundingBox = await badge.boundingBox();
      expect(badgeBoundingBox).not.toBeNull();
      // Badges should be reasonably sized on mobile
      expect(badgeBoundingBox.height).toBeLessThanOrEqual(30);
    }
  });
});

test.describe('Responsive Design - No Horizontal Scroll at Any Breakpoint', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Additional test: Verify no horizontal scroll at various breakpoints
   */
  test('No horizontal scroll at various breakpoints', async ({ page }) => {
    const breakpoints = [
      { name: 'Small mobile', width: 320, height: 568 },
      { name: 'Mobile', width: 375, height: 667 },
      { name: 'Large mobile', width: 428, height: 926 },
      { name: 'Small tablet', width: 640, height: 960 },
      { name: 'Tablet', width: 768, height: 1024 },
      { name: 'Large tablet', width: 1024, height: 768 },
      { name: 'Desktop', width: 1440, height: 900 },
      { name: 'Large desktop', width: 1920, height: 1080 }
    ];

    for (const breakpoint of breakpoints) {
      await page.setViewportSize({ width: breakpoint.width, height: breakpoint.height });
      await page.waitForLoadState('domcontentloaded');

      const hasHorizontalOverflow = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      expect(hasHorizontalOverflow, `Horizontal overflow detected at ${breakpoint.name} (${breakpoint.width}px)`).toBe(false);
    }
  });

  /**
   * Test text readability at mobile viewport
   */
  test('Text remains readable at mobile viewport', async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.mobile);
    await page.waitForLoadState('domcontentloaded');

    // Verify hero title is visible and has appropriate font size
    const heroTitle = page.locator('[data-testid="hero-title"]');
    await expect(heroTitle).toBeVisible();

    const titleFontSize = await heroTitle.evaluate(el => {
      return parseInt(window.getComputedStyle(el).fontSize);
    });
    // Font size should be at least 16px for readability on mobile
    expect(titleFontSize).toBeGreaterThanOrEqual(16);

    // Verify tagline is readable
    const heroTagline = page.locator('[data-testid="hero-tagline"]');
    await expect(heroTagline).toBeVisible();

    const taglineFontSize = await heroTagline.evaluate(el => {
      return parseInt(window.getComputedStyle(el).fontSize);
    });
    expect(taglineFontSize).toBeGreaterThanOrEqual(14);
  });
});
