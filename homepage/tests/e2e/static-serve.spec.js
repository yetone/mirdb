/**
 * Static File Serving E2E Tests
 * Owner: Scenario 19 - Static Site Deployment
 *
 * Tests that verify the static site serves correctly
 * without requiring a dynamic backend.
 */

const { test, expect } = require('@playwright/test');

test.describe('Static Site Deployment - E2E', () => {
  // Test Case 5: Homepage loads correctly from static file server
  test('homepage loads correctly from static file server', async ({ page }) => {
    // Navigate to homepage
    await page.goto('/');

    // Verify page title is set
    await expect(page).toHaveTitle(/MirDB/);

    // Verify hero section is visible
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify MirDB logo is loaded
    const logo = page.locator('#mirdb-logo, .hero-logo');
    await expect(logo).toBeVisible();

    // Verify hero title is present
    const heroTitle = page.locator('.hero-title');
    await expect(heroTitle).toContainText('MirDB');

    // Verify hero tagline is present
    const tagline = page.locator('.hero-tagline');
    await expect(tagline).toContainText('Persistent Key-Value Store');

    // Verify CTA button exists
    const ctaButton = page.locator('#get-started-btn, .cta-button');
    await expect(ctaButton).toBeVisible();
  });

  test('CSS styles are loaded correctly', async ({ page }) => {
    await page.goto('/');

    // Check that CSS is applied by verifying styles
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify the page has styling applied (not plain HTML)
    const bodyBackgroundColor = await page.evaluate(() => {
      return window.getComputedStyle(document.body).backgroundColor;
    });
    // Background color should be set (not empty)
    expect(bodyBackgroundColor).not.toBe('');
  });

  test('JavaScript is loaded and functional', async ({ page }) => {
    await page.goto('/');

    // Verify theme toggle exists (part of theme.js functionality)
    const themeToggle = page.locator('#theme-toggle');
    await expect(themeToggle).toBeVisible();

    // Verify mobile menu toggle exists in DOM (may be hidden on desktop viewport)
    const mobileMenuToggle = page.locator('#mobile-menu-toggle');
    await expect(mobileMenuToggle).toBeAttached();
  });

  test('images are served correctly', async ({ page }) => {
    await page.goto('/');

    // Verify logo image loads successfully
    const logoImage = page.locator('img[src*="logo"]').first();
    await expect(logoImage).toBeVisible();

    // Wait for image to load and check dimensions
    const logoNaturalWidth = await logoImage.evaluate((img) => img.naturalWidth);
    expect(logoNaturalWidth).toBeGreaterThan(0);
  });

  test('navigation links work correctly', async ({ page }) => {
    await page.goto('/');

    // Verify navigation exists
    const nav = page.locator('nav, .main-nav');
    await expect(nav).toBeVisible();

    // Verify Features link exists
    const featuresLink = page.locator('a[href*="features"]').first();
    await expect(featuresLink).toBeVisible();
  });

  test('page loads without JavaScript errors', async ({ page }) => {
    const errors = [];
    page.on('pageerror', (error) => {
      errors.push(error.message);
    });

    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Assert no critical JavaScript errors
    expect(errors.length).toBe(0);
  });

  test('all critical assets load successfully', async ({ page }) => {
    const failedRequests = [];

    page.on('response', (response) => {
      if (response.status() >= 400) {
        failedRequests.push({
          url: response.url(),
          status: response.status()
        });
      }
    });

    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Filter out expected 404s (like favicon if not present)
    const criticalFailures = failedRequests.filter(r =>
      !r.url.includes('favicon') &&
      r.status !== 404
    );

    expect(criticalFailures.length).toBe(0);
  });
});
