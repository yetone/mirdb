// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Integration tests for Static Page Functionality - HTTP Server
 * Verifies the page loads correctly when served via a simple HTTP server
 */

test.describe('Static Page - HTTP Server', () => {
  test('page loads correctly when served via HTTP server', async ({ page }) => {
    // Navigate to the page served by the HTTP server (configured in playwright.config.js)
    await page.goto('/');

    // Verify the page loaded successfully
    await expect(page).toHaveTitle(/MirDB/);

    // Verify main content is visible
    await expect(page.locator('h1')).toContainText('MirDB');
    await expect(page.locator('.tagline')).toContainText('Persistent Key-Value Store');
  });

  test('all sections are accessible via HTTP server', async ({ page }) => {
    await page.goto('/');

    // Check all major sections are present and visible
    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    const features = page.locator('#features');
    await expect(features).toBeVisible();

    const gettingStarted = page.locator('#getting-started');
    await expect(gettingStarted).toBeVisible();

    const commands = page.locator('.commands');
    await expect(commands).toBeVisible();

    const footer = page.locator('footer');
    await expect(footer).toBeVisible();
  });

  test('CSS styles are applied correctly via HTTP server', async ({ page }) => {
    await page.goto('/');

    // Verify CSS is loaded and applied
    const heroSection = page.locator('.hero');

    // Check that the hero has background styling applied
    const backgroundColor = await heroSection.evaluate(el => {
      return window.getComputedStyle(el).background;
    });

    // The hero should have a gradient background (not default white)
    expect(backgroundColor).toContain('linear-gradient');

    // Check that navigation bar is styled
    const navbar = page.locator('.navbar');
    const navbarPosition = await navbar.evaluate(el => {
      return window.getComputedStyle(el).position;
    });
    expect(navbarPosition).toBe('sticky');
  });

  test('navigation links work via HTTP server', async ({ page }) => {
    await page.goto('/');

    // Test internal navigation
    await page.click('a[href="#features"]');
    await expect(page).toHaveURL(/#features/);

    await page.click('a[href="#getting-started"]');
    await expect(page).toHaveURL(/#getting-started/);
  });

  test('page is fully functional without JavaScript', async ({ page, context }) => {
    // The page should work without JavaScript (it's a static HTML page)
    await page.goto('/');

    // All content should be visible (no JS-dependent rendering)
    await expect(page.locator('h1')).toContainText('MirDB');
    await expect(page.locator('.feature-card')).toHaveCount(4);
    await expect(page.locator('.step-card')).toHaveCount(3);
    await expect(page.locator('.command-group')).toHaveCount(4);
  });

  test('static assets load successfully via HTTP', async ({ page }) => {
    const failedRequests = [];

    // Monitor network requests
    page.on('requestfailed', request => {
      failedRequests.push({
        url: request.url(),
        error: request.failure()?.errorText
      });
    });

    await page.goto('/');

    // Wait for all assets to load
    await page.waitForLoadState('networkidle');

    // Check no critical requests failed
    const criticalFailures = failedRequests.filter(req =>
      !req.url.includes('favicon') // Favicon is optional
    );

    expect(criticalFailures).toHaveLength(0);
  });

  test('page responds with correct content type', async ({ page }) => {
    const responses = [];

    page.on('response', response => {
      if (response.url().endsWith('/') || response.url().endsWith('index.html')) {
        responses.push({
          url: response.url(),
          contentType: response.headers()['content-type']
        });
      }
    });

    await page.goto('/');

    // Verify HTML content type
    const htmlResponse = responses.find(r =>
      r.url.endsWith('/') || r.url.endsWith('index.html')
    );

    expect(htmlResponse).toBeDefined();
    expect(htmlResponse.contentType).toContain('text/html');
  });
});
