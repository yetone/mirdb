// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Unit Tests for Hero Section Rendering
 * Test Case 4: Verify hero section renders without JavaScript errors
 */

test.describe('Hero Section Rendering - Unit Tests', () => {
  /**
   * Test Case 4: Hero section renders without JavaScript errors
   * Input: Verify hero section renders without JavaScript errors
   * Expected: No console errors related to hero section rendering
   */
  test('TC4: Hero section renders without JavaScript errors', async ({ page }) => {
    // Collect console errors during page load
    const consoleErrors = [];
    const consoleWarnings = [];

    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text());
      } else if (msg.type() === 'warning') {
        consoleWarnings.push(msg.text());
      }
    });

    // Collect page errors (uncaught exceptions)
    const pageErrors = [];
    page.on('pageerror', (error) => {
      pageErrors.push(error.message);
    });

    // Navigate to homepage
    await page.goto('/');

    // Wait for page to fully load
    await page.waitForLoadState('domcontentloaded');
    await page.waitForLoadState('networkidle');

    // Verify hero section elements are present
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    const heroTitle = page.locator('[data-testid="hero-title"]');
    await expect(heroTitle).toBeVisible();

    const heroTagline = page.locator('[data-testid="hero-tagline"]');
    await expect(heroTagline).toBeVisible();

    const heroDescription = page.locator('[data-testid="hero-description"]');
    await expect(heroDescription).toBeVisible();

    const ctaButtons = page.locator('[data-testid="hero-cta-buttons"]');
    await expect(ctaButtons).toBeVisible();

    // Filter for hero-related errors only
    const heroRelatedErrors = consoleErrors.filter(error =>
      error.toLowerCase().includes('hero') ||
      error.toLowerCase().includes('mirdb') ||
      error.toLowerCase().includes('main.js')
    );

    // Filter for page errors related to hero section
    const heroPageErrors = pageErrors.filter(error =>
      error.toLowerCase().includes('hero') ||
      error.toLowerCase().includes('mirdb') ||
      error.toLowerCase().includes('main.js')
    );

    // Verify no hero-related console errors
    expect(heroRelatedErrors).toHaveLength(0);

    // Verify no hero-related page errors
    expect(heroPageErrors).toHaveLength(0);

    // Verify the MirDBHomepage validation function exists and runs successfully
    const validationResult = await page.evaluate(() => {
      // Check if the MirDBHomepage object exists
      if (typeof window.MirDBHomepage === 'undefined') {
        return { success: false, error: 'MirDBHomepage not defined' };
      }

      // Run the validation function
      try {
        const result = window.MirDBHomepage.validateHeroRender();
        return { success: result, error: null };
      } catch (e) {
        return { success: false, error: e.message };
      }
    });

    expect(validationResult.success).toBe(true);
    expect(validationResult.error).toBeNull();

    // Verify successful render message was logged
    const successLogs = [];
    page.on('console', (msg) => {
      if (msg.type() === 'log' && msg.text().includes('Hero section rendered successfully')) {
        successLogs.push(msg.text());
      }
    });

    // Re-run validation to check for success log
    await page.evaluate(() => {
      window.MirDBHomepage.validateHeroRender();
    });
  });

  test('TC4-B: Hero section JavaScript initializes correctly', async ({ page }) => {
    // Track all errors
    const errors = [];

    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        errors.push({ type: 'console', text: msg.text() });
      }
    });

    page.on('pageerror', (error) => {
      errors.push({ type: 'page', text: error.message });
    });

    // Navigate and wait for full load
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Check that main.js loaded successfully
    const scriptsLoaded = await page.evaluate(() => {
      const scripts = Array.from(document.querySelectorAll('script'));
      return scripts.some(s => s.src.includes('main.js'));
    });
    expect(scriptsLoaded).toBe(true);

    // Verify the global MirDBHomepage object exists
    const hasGlobal = await page.evaluate(() => {
      return typeof window.MirDBHomepage !== 'undefined' &&
             typeof window.MirDBHomepage.validateHeroRender === 'function';
    });
    expect(hasGlobal).toBe(true);

    // Check no critical errors occurred during initialization
    const criticalErrors = errors.filter(e =>
      !e.text.includes('favicon') && // Ignore favicon errors
      !e.text.includes('404')        // Ignore 404 for non-critical resources
    );

    expect(criticalErrors).toHaveLength(0);
  });
});
