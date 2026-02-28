// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * MirDB Homepage Browser Compatibility Tests
 *
 * Owner: Scenario 12 - Browser Compatibility
 *
 * Tests that the homepage renders correctly in:
 * - Chrome (latest)
 * - Firefox (latest)
 * - Safari/WebKit (latest)
 * - Edge (latest)
 *
 * Each test verifies:
 * 1. All sections render correctly
 * 2. No console errors
 * 3. Theme toggle works
 */

test.describe('MirDB Homepage Browser Compatibility', () => {
  // Track console errors during tests
  let consoleErrors = [];

  test.beforeEach(async ({ page }) => {
    consoleErrors = [];

    // Listen for console errors
    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text());
      }
    });

    // Listen for page errors (uncaught exceptions)
    page.on('pageerror', (error) => {
      consoleErrors.push(error.message);
    });
  });

  test('should render all homepage sections correctly', async ({ page, browserName }) => {
    await page.goto('/');

    // Wait for page to fully load
    await page.waitForLoadState('networkidle');

    // Verify header section
    const header = page.locator('.header');
    await expect(header).toBeVisible();
    await expect(page.locator('.logo-text')).toHaveText('MirDB');

    // Verify navigation links
    const nav = page.locator('.nav');
    await expect(nav).toBeVisible();
    await expect(nav.locator('a[href="#features"]')).toBeVisible();
    await expect(nav.locator('a[href="#quick-start"]')).toBeVisible();

    // Verify theme toggle button
    const themeToggle = page.locator('#theme-toggle');
    await expect(themeToggle).toBeVisible();

    // Verify hero section
    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();
    await expect(hero.locator('h1')).toHaveText('MirDB');
    await expect(hero.locator('.tagline')).toContainText('key-value store');

    // Verify features section
    const features = page.locator('#features');
    await expect(features).toBeVisible();
    await expect(features.locator('h2')).toHaveText('Features');

    // Verify feature cards
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(3);
    await expect(featureCards.nth(0).locator('h3')).toContainText('Memcached Protocol');
    await expect(featureCards.nth(1).locator('h3')).toContainText('LSM Tree');
    await expect(featureCards.nth(2).locator('h3')).toContainText('Persistent Storage');

    // Verify metrics section
    const metrics = page.locator('#metrics');
    await expect(metrics).toBeVisible();
    await expect(metrics.locator('h2')).toHaveText('Server Metrics');

    // Verify config section
    const config = page.locator('#config');
    await expect(config).toBeVisible();
    await expect(config.locator('h2')).toHaveText('Configuration');

    // Verify quick start section
    const quickStart = page.locator('#quick-start');
    await expect(quickStart).toBeVisible();
    await expect(quickStart.locator('h2')).toHaveText('Quick Start');

    // Verify code examples are visible
    const codeExamples = page.locator('.code-examples pre');
    await expect(codeExamples).toHaveCount(3);

    // Verify footer
    const footer = page.locator('.footer');
    await expect(footer).toBeVisible();
    await expect(footer.locator('.footer-links a')).toHaveCount(3);

    // Check for no console errors (filter out network/API errors)
    const criticalErrors = consoleErrors.filter(
      (err) => !err.includes('fetch') && !err.includes('api')
    );
    expect(criticalErrors).toHaveLength(0);

    console.log(`✓ All sections rendered correctly in ${browserName}`);
  });

  test('should have no CSS layout issues', async ({ page, browserName }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Verify no horizontal overflow
    const body = page.locator('body');
    const bodyBox = await body.boundingBox();
    const viewportSize = page.viewportSize();

    expect(bodyBox.width).toBeLessThanOrEqual(viewportSize.width + 1);

    // Verify flexbox layout is working (header)
    const headerContent = page.locator('.header-content');
    const headerBox = await headerContent.boundingBox();
    expect(headerBox.width).toBeGreaterThan(0);
    expect(headerBox.height).toBeGreaterThan(0);

    // Verify grid layout is working (features)
    const featuresGrid = page.locator('.features-grid');
    const gridBox = await featuresGrid.boundingBox();
    expect(gridBox.width).toBeGreaterThan(0);
    expect(gridBox.height).toBeGreaterThan(0);

    // Verify feature cards are visible and laid out properly
    const featureCards = page.locator('.feature-card');
    const count = await featureCards.count();
    expect(count).toBe(3);

    for (let i = 0; i < count; i++) {
      const cardBox = await featureCards.nth(i).boundingBox();
      expect(cardBox.width).toBeGreaterThan(0);
      expect(cardBox.height).toBeGreaterThan(0);
    }

    console.log(`✓ No CSS layout issues in ${browserName}`);
  });

  test('should toggle theme correctly', async ({ page, browserName }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    const themeToggle = page.locator('#theme-toggle');
    const html = page.locator('html');

    // Verify initial state (light theme - no data-theme attribute or light)
    const initialTheme = await html.getAttribute('data-theme');
    const isInitiallyLight = !initialTheme || initialTheme !== 'dark';

    // Click theme toggle to switch theme
    await themeToggle.click();

    // Wait for transition
    await page.waitForTimeout(400);

    // Verify theme changed
    const newTheme = await html.getAttribute('data-theme');
    if (isInitiallyLight) {
      expect(newTheme).toBe('dark');
    } else {
      expect(newTheme).toBeNull();
    }

    // Toggle back
    await themeToggle.click();
    await page.waitForTimeout(400);

    // Verify theme changed back
    const finalTheme = await html.getAttribute('data-theme');
    if (isInitiallyLight) {
      expect(finalTheme).toBeNull();
    } else {
      expect(finalTheme).toBe('dark');
    }

    console.log(`✓ Theme toggle works correctly in ${browserName}`);
  });

  test('should persist theme preference', async ({ page, browserName, context }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    const themeToggle = page.locator('#theme-toggle');

    // Set to dark theme
    const html = page.locator('html');
    const initialTheme = await html.getAttribute('data-theme');
    if (initialTheme !== 'dark') {
      await themeToggle.click();
      await page.waitForTimeout(400);
    }

    // Verify it's now dark
    await expect(html).toHaveAttribute('data-theme', 'dark');

    // Reload the page
    await page.reload();
    await page.waitForLoadState('networkidle');

    // Verify theme persisted
    await expect(html).toHaveAttribute('data-theme', 'dark');

    console.log(`✓ Theme persistence works correctly in ${browserName}`);
  });

  test('should apply CSS variables correctly', async ({ page, browserName }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Get computed styles for light theme
    const body = page.locator('body');
    const lightBgColor = await body.evaluate((el) =>
      getComputedStyle(el).backgroundColor
    );

    // Verify background color is not transparent or undefined
    expect(lightBgColor).not.toBe('');
    expect(lightBgColor).not.toBe('transparent');
    expect(lightBgColor).not.toBe('rgba(0, 0, 0, 0)');

    // Switch to dark theme
    const themeToggle = page.locator('#theme-toggle');
    await themeToggle.click();
    await page.waitForTimeout(400);

    // Get computed styles for dark theme
    const darkBgColor = await body.evaluate((el) =>
      getComputedStyle(el).backgroundColor
    );

    // Verify background color changed
    expect(darkBgColor).not.toBe(lightBgColor);
    expect(darkBgColor).not.toBe('');
    expect(darkBgColor).not.toBe('transparent');

    console.log(`✓ CSS variables work correctly in ${browserName}`);
  });

  test('should render code blocks with proper styling', async ({ page, browserName }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Verify code blocks exist and have proper styling
    const codeBlocks = page.locator('pre');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const codeBlock = codeBlocks.nth(i);
      await expect(codeBlock).toBeVisible();

      // Check styling
      const fontFamily = await codeBlock.evaluate((el) =>
        getComputedStyle(el).fontFamily
      );
      // Should use monospace font
      expect(fontFamily.toLowerCase()).toMatch(/mono|courier|consolas|menlo|monaco/i);

      // Check overflow handling
      const overflow = await codeBlock.evaluate((el) =>
        getComputedStyle(el).overflowX
      );
      expect(overflow).toBe('auto');
    }

    console.log(`✓ Code blocks render correctly in ${browserName}`);
  });

  test('should handle navigation links', async ({ page, browserName }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Test internal anchor links
    const featuresLink = page.locator('.nav a[href="#features"]');
    await featuresLink.click();

    // Wait for scroll
    await page.waitForTimeout(600);

    // Verify features section is in view
    const features = page.locator('#features');
    const featuresBox = await features.boundingBox();
    const viewportHeight = page.viewportSize().height;

    // Features section should be near the top of the viewport
    expect(featuresBox.y).toBeLessThan(viewportHeight / 2);

    console.log(`✓ Navigation works correctly in ${browserName}`);
  });

  test('should have accessible theme toggle', async ({ page, browserName }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    const themeToggle = page.locator('#theme-toggle');

    // Verify button has aria-label
    const ariaLabel = await themeToggle.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel.length).toBeGreaterThan(0);

    // Click and verify aria-label updates
    await themeToggle.click();
    await page.waitForTimeout(400);

    const newAriaLabel = await themeToggle.getAttribute('aria-label');
    expect(newAriaLabel).toBeTruthy();
    expect(newAriaLabel).not.toBe(ariaLabel);

    console.log(`✓ Accessibility features work correctly in ${browserName}`);
  });

  test('should handle status indicator styling', async ({ page, browserName }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Find status indicator
    const statusIndicator = page.locator('.status-indicator');
    await expect(statusIndicator).toBeVisible();

    // Verify it has border-radius (circular)
    const borderRadius = await statusIndicator.evaluate((el) =>
      getComputedStyle(el).borderRadius
    );
    expect(borderRadius).toBe('50%');

    // Verify it has a background color
    const bgColor = await statusIndicator.evaluate((el) =>
      getComputedStyle(el).backgroundColor
    );
    expect(bgColor).not.toBe('');
    expect(bgColor).not.toBe('transparent');

    console.log(`✓ Status indicator renders correctly in ${browserName}`);
  });
});
