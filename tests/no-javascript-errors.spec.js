// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * No JavaScript Errors Tests
 * Scenario: Verify that the page loads without JavaScript errors
 *
 * Test Cases:
 * 1. Load page and check console for errors - No JavaScript errors in console on page load
 * 2. Click all CTA buttons and check console - No JavaScript errors occur when interacting with buttons
 * 3. Scroll through entire page and check console - No JavaScript errors during scrolling or lazy loading
 */

test.describe('No JavaScript Errors', () => {
  let consoleErrors = [];
  let pageErrors = [];

  test.beforeEach(async ({ page }) => {
    consoleErrors = [];
    pageErrors = [];

    // Listen for console errors
    page.on('console', msg => {
      if (msg.type() === 'error') {
        // Filter out expected errors (e.g., favicon, external resource loading issues)
        const text = msg.text();
        if (!text.includes('favicon.ico') && !text.includes('Failed to load resource')) {
          consoleErrors.push(text);
        }
      }
    });

    // Listen for page errors (uncaught exceptions)
    page.on('pageerror', error => {
      pageErrors.push(error.message);
    });
  });

  test('TC1: No JavaScript errors in console on page load', async ({ page }) => {
    // Navigate to the landing page
    const response = await page.goto('/');

    // Verify successful HTTP response
    expect(response).not.toBeNull();
    expect(response.status()).toBe(200);

    // Wait for page to be fully loaded
    await page.waitForLoadState('networkidle');

    // Additional wait for any deferred scripts
    await page.waitForTimeout(500);

    // Verify no JavaScript console errors occurred during page load
    expect(consoleErrors).toEqual([]);

    // Verify no uncaught page errors
    expect(pageErrors).toEqual([]);
  });

  test('TC2: No JavaScript errors occur when interacting with buttons', async ({ page }) => {
    // Navigate to the landing page
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Clear any initial errors from page load (should be none)
    const loadErrors = [...consoleErrors, ...pageErrors];
    consoleErrors = [];
    pageErrors = [];

    // Find all CTA buttons on the page
    const primaryCta = page.locator('.hero .btn-primary');
    const secondaryCta = page.locator('.hero .btn-secondary');
    const githubFooterLink = page.locator('.github-link');

    // Click the "Learn More" button (secondary CTA - internal anchor link)
    await expect(secondaryCta).toBeVisible();
    await secondaryCta.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(500);

    // Verify features section is now in view
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();

    // Verify no errors from secondary CTA click
    expect(consoleErrors).toEqual([]);
    expect(pageErrors).toEqual([]);

    // Test clicking feature cards (hover interaction)
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();

    for (let i = 0; i < cardCount; i++) {
      await featureCards.nth(i).hover();
      await page.waitForTimeout(100);
    }

    // Verify no errors from feature card interactions
    expect(consoleErrors).toEqual([]);
    expect(pageErrors).toEqual([]);

    // Test roadmap items hover
    const roadmapItems = page.locator('.roadmap-item');
    const roadmapCount = await roadmapItems.count();

    for (let i = 0; i < roadmapCount; i++) {
      await roadmapItems.nth(i).hover();
      await page.waitForTimeout(100);
    }

    // Verify no errors from roadmap item interactions
    expect(consoleErrors).toEqual([]);
    expect(pageErrors).toEqual([]);

    // Note: External links (GitHub, CircleCI) are tested via href validation,
    // not actual click navigation, to avoid network-dependent test failures
    await expect(primaryCta).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    await expect(primaryCta).toHaveAttribute('target', '_blank');
    await expect(githubFooterLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

    // Final check - no errors occurred during any button/interaction testing
    expect(consoleErrors).toEqual([]);
    expect(pageErrors).toEqual([]);
  });

  test('TC3: No JavaScript errors during scrolling or lazy loading', async ({ page }) => {
    // Navigate to the landing page
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Clear any initial errors from page load
    consoleErrors = [];
    pageErrors = [];

    // Get all main sections
    const sections = [
      page.locator('[data-testid="hero"]'),
      page.locator('[data-testid="features"]'),
      page.locator('[data-testid="usage"]'),
      page.locator('[data-testid="roadmap"]'),
      page.locator('[data-testid="footer"]')
    ];

    // Scroll to each section progressively
    for (const section of sections) {
      await section.scrollIntoViewIfNeeded();
      // Wait for any lazy-loaded content or animations
      await page.waitForTimeout(300);

      // Check for errors after each scroll
      expect(consoleErrors).toEqual([]);
      expect(pageErrors).toEqual([]);
    }

    // Perform a full page scroll from top to bottom
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.waitForTimeout(200);

    // Scroll to bottom of page
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(500);

    // Check for errors after full scroll
    expect(consoleErrors).toEqual([]);
    expect(pageErrors).toEqual([]);

    // Scroll back to top
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.waitForTimeout(300);

    // Perform incremental scroll through entire page
    const scrollHeight = await page.evaluate(() => document.body.scrollHeight);
    const viewportHeight = await page.evaluate(() => window.innerHeight);
    const scrollSteps = Math.ceil(scrollHeight / (viewportHeight * 0.5));
    const scrollAmount = viewportHeight * 0.5;

    for (let i = 0; i < scrollSteps; i++) {
      await page.evaluate((amount) => {
        window.scrollBy(0, amount);
      }, scrollAmount);

      // Short wait for any scroll-triggered events
      await page.waitForTimeout(100);
    }

    // Final comprehensive check - no JavaScript errors during any scrolling
    expect(consoleErrors).toEqual([]);
    expect(pageErrors).toEqual([]);
  });

  test('page has no inline script errors', async ({ page }) => {
    // Navigate to the landing page
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Verify the page doesn't have any inline JavaScript that throws errors
    // by checking that page content loaded correctly
    const hero = page.locator('[data-testid="hero"]');
    const features = page.locator('[data-testid="features"]');
    const usage = page.locator('[data-testid="usage"]');
    const roadmap = page.locator('[data-testid="roadmap"]');
    const footer = page.locator('[data-testid="footer"]');

    // All sections should be present and visible
    await expect(hero).toBeVisible();
    await expect(features).toBeVisible();
    await expect(usage).toBeVisible();
    await expect(roadmap).toBeVisible();
    await expect(footer).toBeVisible();

    // No errors should have occurred
    expect(consoleErrors).toEqual([]);
    expect(pageErrors).toEqual([]);
  });
});
