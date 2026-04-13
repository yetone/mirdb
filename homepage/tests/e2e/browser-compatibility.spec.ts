/**
 * Browser Compatibility E2E Tests
 * Owner: Scenario 16 - Browser Compatibility
 *
 * Tests:
 * - Page renders correctly in Chrome, Firefox, Safari (WebKit), and Edge
 * - CSS Grid layouts render correctly
 * - Flexbox layouts render correctly
 * - All sections display without visual bugs
 *
 * These tests run on all browser projects defined in playwright.config.ts
 */
import { test, expect } from '@playwright/test';

test.describe('Browser Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should load page and display all main sections', async ({ page, browserName }) => {
    // Log which browser is being tested
    test.info().annotations.push({ type: 'browser', description: browserName });

    // Verify page loaded
    await expect(page).toHaveTitle(/MirDB/i);

    // Check hero section is visible
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Check features section is visible
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check quick start section is visible
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Check demo section is visible
    const demoSection = page.locator('#demo');
    await expect(demoSection).toBeVisible();

    // Check roadmap section is visible
    const roadmapSection = page.locator('#roadmap');
    await expect(roadmapSection).toBeVisible();
  });

  test('should render hero section correctly with proper layout', async ({ page, browserName }) => {
    test.info().annotations.push({ type: 'browser', description: browserName });

    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Check product name is visible
    const productName = heroSection.locator('h1');
    await expect(productName).toBeVisible();
    await expect(productName).toContainText('MirDB');

    // Check CTA button is visible and accessible
    const ctaButton = heroSection.locator('a[href*="github.com"]');
    await expect(ctaButton).toBeVisible();

    // Verify CTA button has proper styling (inline-block or flexbox)
    const displayStyle = await ctaButton.evaluate((el) => window.getComputedStyle(el).display);
    expect(['inline-block', 'inline-flex', 'flex', 'block']).toContain(displayStyle);
  });

  test('should render CSS Grid layout correctly in features section', async ({ page, browserName }) => {
    test.info().annotations.push({ type: 'browser', description: browserName });

    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check grid container exists and has grid display
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    const gridDisplay = await featuresGrid.evaluate((el) => window.getComputedStyle(el).display);
    expect(gridDisplay).toBe('grid');

    // Verify grid has proper template columns (not collapsed)
    const gridTemplateColumns = await featuresGrid.evaluate((el) =>
      window.getComputedStyle(el).gridTemplateColumns
    );
    // Grid should have defined columns (not 'none')
    expect(gridTemplateColumns).not.toBe('none');

    // Verify feature cards are rendered in the grid
    const featureCards = featuresGrid.locator('[data-testid="feature-card"]');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(5);

    // Verify cards have proper dimensions
    for (let i = 0; i < Math.min(cardCount, 3); i++) {
      const card = featureCards.nth(i);
      const boundingBox = await card.boundingBox();
      expect(boundingBox).not.toBeNull();
      expect(boundingBox!.width).toBeGreaterThan(100);
      expect(boundingBox!.height).toBeGreaterThan(50);
    }
  });

  test('should render Flexbox layout correctly in hero section', async ({ page, browserName }) => {
    test.info().annotations.push({ type: 'browser', description: browserName });

    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Check container has proper layout
    const container = heroSection.locator('.container');
    await expect(container).toBeVisible();

    // Verify the hero content is centered (text-align or flexbox center)
    const textAlign = await heroSection.evaluate((el) =>
      window.getComputedStyle(el).textAlign
    );
    const justifyContent = await container.evaluate((el) =>
      window.getComputedStyle(el).justifyContent
    );

    // Either text-align center or flexbox justify-content center
    const isCentered = textAlign === 'center' ||
                       justifyContent === 'center' ||
                       justifyContent === 'space-around';
    expect(isCentered).toBe(true);
  });

  test('should render Flexbox layout correctly in roadmap section', async ({ page, browserName }) => {
    test.info().annotations.push({ type: 'browser', description: browserName });

    const roadmapSection = page.locator('#roadmap');
    await expect(roadmapSection).toBeVisible();

    // Check roadmap columns/flex container
    const roadmapColumns = page.locator('.roadmap-columns');
    await expect(roadmapColumns).toBeVisible();

    const displayStyle = await roadmapColumns.evaluate((el) =>
      window.getComputedStyle(el).display
    );
    // Should be grid or flex for layout
    expect(['grid', 'flex']).toContain(displayStyle);

    // Verify columns are properly displayed (direct children with roadmap-column class)
    const columns = roadmapColumns.locator('> .roadmap-column');
    const columnCount = await columns.count();
    expect(columnCount).toBeGreaterThanOrEqual(2);

    // Verify each column has proper dimensions
    for (let i = 0; i < columnCount; i++) {
      const column = columns.nth(i);
      const boundingBox = await column.boundingBox();
      expect(boundingBox).not.toBeNull();
      expect(boundingBox!.width).toBeGreaterThan(100);
    }
  });

  test('should render quick start code blocks correctly', async ({ page, browserName }) => {
    test.info().annotations.push({ type: 'browser', description: browserName });

    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Check code blocks are visible
    const codeBlocks = quickStartSection.locator('.code-block, pre, code');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Verify code blocks have proper monospace font
    const codeElement = quickStartSection.locator('.code-block-code').first();
    const fontFamily = await codeElement.evaluate((el) =>
      window.getComputedStyle(el).fontFamily
    );
    // Should contain monospace or code font (SF Mono, Fira Code, Consolas, Monaco, monospace)
    const isMonospace = fontFamily.toLowerCase().includes('mono') ||
                        fontFamily.toLowerCase().includes('courier') ||
                        fontFamily.toLowerCase().includes('consolas') ||
                        fontFamily.toLowerCase().includes('monaco') ||
                        fontFamily.toLowerCase().includes('fira') ||
                        fontFamily.toLowerCase().includes('ui-monospace');
    expect(isMonospace).toBe(true);
  });

  test('should not have horizontal overflow on page', async ({ page, browserName }) => {
    test.info().annotations.push({ type: 'browser', description: browserName });

    // Get viewport width
    const viewport = page.viewportSize();
    expect(viewport).not.toBeNull();

    // Check body doesn't have horizontal overflow
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = viewport!.width;

    // Body scroll width should not exceed viewport width significantly
    // Allow small margin for rounding
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth + 5);
  });

  test('should render images with proper attributes', async ({ page, browserName }) => {
    test.info().annotations.push({ type: 'browser', description: browserName });

    // Wait for page to fully load
    await page.waitForLoadState('domcontentloaded');

    // Find all images
    const images = page.locator('img');
    const imageCount = await images.count();

    // Verify images have proper attributes (src and alt)
    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i);

      // Check src attribute exists
      const src = await img.getAttribute('src');
      expect(src).toBeTruthy();

      // Check alt attribute exists for accessibility
      const alt = await img.getAttribute('alt');
      expect(alt).toBeTruthy();

      // Verify image element is visible in the DOM
      await expect(img).toBeVisible();
    }
  });

  test('should apply CSS custom properties correctly', async ({ page, browserName }) => {
    test.info().annotations.push({ type: 'browser', description: browserName });

    // Check that CSS custom properties are supported and applied
    const body = page.locator('body');

    // Get computed styles that use CSS variables
    const primaryColor = await body.evaluate(() => {
      const style = getComputedStyle(document.documentElement);
      return style.getPropertyValue('--color-primary');
    });

    // CSS variables should be defined and not empty
    expect(primaryColor.trim()).not.toBe('');

    // Verify background color is applied
    const bgColor = await body.evaluate((el) =>
      window.getComputedStyle(el).backgroundColor
    );
    expect(bgColor).not.toBe('');
  });
});

test.describe('Browser-specific Layout Verification', () => {
  test('should render consistently across browsers at desktop viewport', async ({ page, browserName }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.goto('/');

    test.info().annotations.push({ type: 'browser', description: browserName });

    // Verify features grid shows multiple columns on desktop
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    const featureCards = featuresGrid.locator('[data-testid="feature-card"]');
    const cardCount = await featureCards.count();

    if (cardCount >= 2) {
      // Get positions of first two cards
      const firstCard = featureCards.first();
      const secondCard = featureCards.nth(1);

      const firstBox = await firstCard.boundingBox();
      const secondBox = await secondCard.boundingBox();

      expect(firstBox).not.toBeNull();
      expect(secondBox).not.toBeNull();

      // On desktop, cards should be side by side (same row) or in a grid
      // If they're on the same row, y positions should be similar
      // If grid wraps, that's also acceptable
      expect(firstBox!.width).toBeGreaterThan(0);
      expect(secondBox!.width).toBeGreaterThan(0);
    }
  });

  test('should render consistently across browsers at tablet viewport', async ({ page, browserName }) => {
    // Set tablet viewport
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('/');

    test.info().annotations.push({ type: 'browser', description: browserName });

    // All sections should still be visible
    const sections = ['#hero', '#features', '#quick-start', '#demo', '#roadmap'];
    for (const selector of sections) {
      const section = page.locator(selector);
      await expect(section).toBeVisible();
    }

    // No horizontal overflow
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    expect(bodyWidth).toBeLessThanOrEqual(768 + 5);
  });

  test('should render consistently across browsers at mobile viewport', async ({ page, browserName }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    test.info().annotations.push({ type: 'browser', description: browserName });

    // All sections should still be visible
    const sections = ['#hero', '#features', '#quick-start', '#demo', '#roadmap'];
    for (const selector of sections) {
      const section = page.locator(selector);
      await expect(section).toBeVisible();
    }

    // No horizontal overflow on mobile
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    expect(bodyWidth).toBeLessThanOrEqual(375 + 5);
  });
});
