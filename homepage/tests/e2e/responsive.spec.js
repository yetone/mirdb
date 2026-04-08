/**
 * Responsive Design E2E Tests
 * Owner: Scenario 8 (mobile), Scenario 9 (tablet)
 *
 * Test cases:
 * - Mobile (320px): Single column layout, hamburger menu, no overflow
 * - Tablet (768px): Two column grid, appropriate navigation
 * - All sections render correctly at each breakpoint
 */

const { test, expect } = require('@playwright/test');

// Mobile viewport size (smallest supported size per NFR-2)
const MOBILE_VIEWPORT = { width: 320, height: 568 };

test.describe('Mobile Responsive Design (320px)', () => {
  test.beforeEach(async ({ page }) => {
    // Set mobile viewport before each test
    await page.setViewportSize(MOBILE_VIEWPORT);
    await page.goto('/');
    // Wait for page to fully load
    await page.waitForLoadState('networkidle');
  });

  test('TC1: Hero section stacks vertically with full-width CTAs', async ({ page }) => {
    // Check hero section exists
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Check that hero CTAs container uses flex-direction column
    const heroCtas = page.locator('.hero-ctas');
    await expect(heroCtas).toBeVisible();

    // Verify flex-direction is column on mobile
    const ctasFlexDirection = await heroCtas.evaluate(el =>
      window.getComputedStyle(el).flexDirection
    );
    expect(ctasFlexDirection).toBe('column');

    // Check that CTAs span full width or nearly full width
    const primaryCta = page.locator('.hero-cta-primary');
    const secondaryCta = page.locator('.hero-cta-secondary');

    await expect(primaryCta).toBeVisible();
    await expect(secondaryCta).toBeVisible();

    const primaryCtaBox = await primaryCta.boundingBox();
    const secondaryCtaBox = await secondaryCta.boundingBox();

    // CTAs should be stacked (not side by side)
    expect(secondaryCtaBox.y).toBeGreaterThan(primaryCtaBox.y);

    // CTAs should be at least 80% of viewport width (full-width or appropriately sized)
    const viewportWidth = MOBILE_VIEWPORT.width;
    const ctaMinWidth = viewportWidth * 0.7; // Allow for padding
    expect(primaryCtaBox.width).toBeGreaterThanOrEqual(ctaMinWidth);
  });

  test('TC2: Features grid displays in single column on mobile', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Get the features grid
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Verify grid has single column layout
    const gridTemplateColumns = await featuresGrid.evaluate(el =>
      window.getComputedStyle(el).gridTemplateColumns
    );

    // Should be a single column (1fr or similar)
    // Grid template columns should resolve to a single value or "1fr"
    const columnCount = gridTemplateColumns.split(' ').filter(col => col && col !== 'none').length;
    expect(columnCount).toBeLessThanOrEqual(1);

    // Alternatively, check that all feature cards are stacked vertically
    const featureCards = page.locator('.features-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThan(0);

    // Get bounding boxes of first two cards
    if (cardCount >= 2) {
      const firstCardBox = await featureCards.nth(0).boundingBox();
      const secondCardBox = await featureCards.nth(1).boundingBox();

      // Cards should be stacked vertically (second card is below first)
      expect(secondCardBox.y).toBeGreaterThan(firstCardBox.y);

      // Cards should NOT be side by side (should have overlapping or same x positions)
      expect(Math.abs(firstCardBox.x - secondCardBox.x)).toBeLessThan(50);
    }
  });

  test('TC3: Navigation collapses to hamburger menu on mobile', async ({ page }) => {
    // Check hamburger menu toggle button is visible
    const menuToggle = page.locator('.nav-menu-toggle');
    await expect(menuToggle).toBeVisible();

    // Verify the hamburger button has correct ARIA attributes
    await expect(menuToggle).toHaveAttribute('aria-label', 'Toggle navigation menu');
    await expect(menuToggle).toHaveAttribute('aria-expanded', 'false');

    // Verify nav-links is hidden by default on mobile
    const navLinks = page.locator('.nav-links');
    const navLinksDisplay = await navLinks.evaluate(el =>
      window.getComputedStyle(el).display
    );
    expect(navLinksDisplay).toBe('none');

    // Click hamburger menu to open
    await menuToggle.click();

    // Verify aria-expanded is now true
    await expect(menuToggle).toHaveAttribute('aria-expanded', 'true');

    // Verify nav-links is now visible
    await expect(navLinks).toHaveClass(/nav-open/);
    const navLinksDisplayOpen = await navLinks.evaluate(el =>
      window.getComputedStyle(el).display
    );
    expect(navLinksDisplayOpen).not.toBe('none');

    // Click hamburger menu to close
    await menuToggle.click();

    // Verify aria-expanded is back to false
    await expect(menuToggle).toHaveAttribute('aria-expanded', 'false');
  });

  test('TC4: Code examples are horizontally scrollable', async ({ page }) => {
    // Navigate to code examples section
    const codeSection = page.locator('#code-examples');
    await codeSection.scrollIntoViewIfNeeded();
    await expect(codeSection).toBeVisible();

    // Check code blocks exist
    const codeBlocks = page.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Check that code block pre elements have overflow-x auto
    const firstCodeBlock = codeBlocks.first();
    const preElement = firstCodeBlock.locator('pre');
    await expect(preElement).toBeVisible();

    const overflowX = await preElement.evaluate(el =>
      window.getComputedStyle(el).overflowX
    );

    // Should be 'auto' or 'scroll' to enable horizontal scrolling
    expect(['auto', 'scroll']).toContain(overflowX);

    // Verify the code content doesn't cause horizontal page overflow
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = MOBILE_VIEWPORT.width;
    expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth + 1); // Allow 1px tolerance
  });

  test('TC5: No horizontal scrollbar appears on mobile viewport', async ({ page }) => {
    // Wait for all content to load
    await page.waitForLoadState('networkidle');

    // Check that body doesn't have horizontal overflow
    const hasHorizontalOverflow = await page.evaluate(() => {
      return document.body.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalOverflow).toBe(false);

    // Scroll through all sections to ensure no overflow
    const sections = ['#hero', '#features', '#code-examples', '#architecture', '#installation', '#status'];

    for (const sectionId of sections) {
      const section = page.locator(sectionId);
      if (await section.isVisible()) {
        await section.scrollIntoViewIfNeeded();

        // Wait a moment for any dynamic content
        await page.waitForTimeout(100);

        // Re-check overflow after scrolling to each section
        const overflowAfterScroll = await page.evaluate(() => {
          return document.body.scrollWidth > document.documentElement.clientWidth;
        });
        expect(overflowAfterScroll).toBe(false);
      }
    }

    // Verify html and body overflow-x is hidden or auto (not visible)
    const htmlOverflow = await page.evaluate(() =>
      window.getComputedStyle(document.documentElement).overflowX
    );
    const bodyOverflow = await page.evaluate(() =>
      window.getComputedStyle(document.body).overflowX
    );

    // Either hidden, auto, or scroll but not 'visible' which would allow overflow
    expect(['hidden', 'auto', 'scroll', 'clip']).toContain(htmlOverflow);
    expect(['hidden', 'auto', 'scroll', 'clip']).toContain(bodyOverflow);
  });

  test('All sections render correctly on mobile', async ({ page }) => {
    // Hero section
    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();

    // Features section
    const features = page.locator('#features');
    await features.scrollIntoViewIfNeeded();
    await expect(features).toBeVisible();

    // Code examples section
    const codeExamples = page.locator('#code-examples');
    await codeExamples.scrollIntoViewIfNeeded();
    await expect(codeExamples).toBeVisible();

    // Architecture section
    const architecture = page.locator('#architecture');
    await architecture.scrollIntoViewIfNeeded();
    await expect(architecture).toBeVisible();

    // Installation section
    const installation = page.locator('#installation');
    await installation.scrollIntoViewIfNeeded();
    await expect(installation).toBeVisible();

    // Status section
    const status = page.locator('#status');
    await status.scrollIntoViewIfNeeded();
    await expect(status).toBeVisible();

    // Footer
    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();
  });

  test('Installation methods display in single column on mobile', async ({ page }) => {
    const installSection = page.locator('#installation');
    await installSection.scrollIntoViewIfNeeded();
    await expect(installSection).toBeVisible();

    const installMethods = page.locator('.install-methods');
    const gridTemplateColumns = await installMethods.evaluate(el =>
      window.getComputedStyle(el).gridTemplateColumns
    );

    // Should be single column
    const columnCount = gridTemplateColumns.split(' ').filter(col => col && col !== 'none').length;
    expect(columnCount).toBeLessThanOrEqual(1);
  });

  test('Architecture components display in single column on mobile', async ({ page }) => {
    const archSection = page.locator('#architecture');
    await archSection.scrollIntoViewIfNeeded();
    await expect(archSection).toBeVisible();

    const archGrid = page.locator('.arch-components-grid');
    const gridTemplateColumns = await archGrid.evaluate(el =>
      window.getComputedStyle(el).gridTemplateColumns
    );

    // Should be single column
    const columnCount = gridTemplateColumns.split(' ').filter(col => col && col !== 'none').length;
    expect(columnCount).toBeLessThanOrEqual(1);
  });

  test('Status grid displays in single column on mobile', async ({ page }) => {
    const statusSection = page.locator('#status');
    await statusSection.scrollIntoViewIfNeeded();
    await expect(statusSection).toBeVisible();

    const statusGrid = page.locator('.status-grid');
    const gridTemplateColumns = await statusGrid.evaluate(el =>
      window.getComputedStyle(el).gridTemplateColumns
    );

    // Should be single column
    const columnCount = gridTemplateColumns.split(' ').filter(col => col && col !== 'none').length;
    expect(columnCount).toBeLessThanOrEqual(1);
  });
});
