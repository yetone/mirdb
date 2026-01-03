// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for 404 Error Page Handling
 *
 * Scenario: Verify appropriate handling when users access non-existent pages
 *
 * Test Cases:
 * 1. Navigate to /nonexistent-page - User sees 404 page or is redirected to homepage
 * 2. Check 404 page has navigation back - 404 page provides link back to homepage
 */

test.describe('Error Page Handling - 404', () => {
  /**
   * Test Case 1: Navigate to a non-existent page
   * Expected: User sees 404 page or is redirected to homepage
   */
  test('TC1: navigating to non-existent page shows 404 page', async ({ page }) => {
    // Navigate to a non-existent page
    const response = await page.goto('/nonexistent-page');

    // The serve package should return 404 status and serve 404.html
    expect(response).not.toBeNull();
    expect(response.status()).toBe(404);

    // Wait for page to load
    await page.waitForLoadState('domcontentloaded');

    // Verify the 404 page content is displayed
    // Check for the error code "404"
    const errorCode = page.locator('.error-code');
    await expect(errorCode).toBeVisible();
    await expect(errorCode).toHaveText('404');

    // Check for the error title
    const errorTitle = page.locator('.error-title');
    await expect(errorTitle).toBeVisible();
    await expect(errorTitle).toHaveText('Page Not Found');

    // Check for the error message
    const errorMessage = page.locator('.error-message');
    await expect(errorMessage).toBeVisible();
    await expect(errorMessage).toContainText('page you are looking for');
  });

  /**
   * Test Case 2: Check 404 page has navigation back to homepage
   * Expected: 404 page provides link back to homepage
   */
  test('TC2: 404 page provides link back to homepage', async ({ page }) => {
    // Navigate to a non-existent page
    await page.goto('/nonexistent-page');
    await page.waitForLoadState('domcontentloaded');

    // Verify the "Back to Homepage" link exists
    const homeLink = page.locator('#home-link');
    await expect(homeLink).toBeVisible();
    await expect(homeLink).toHaveText('Back to Homepage');
    await expect(homeLink).toHaveAttribute('href', '/');

    // Click the home link and verify navigation
    await homeLink.click();

    // Wait for navigation to complete
    await page.waitForLoadState('domcontentloaded');

    // Verify we're on the homepage
    await expect(page).toHaveURL(/\/$/);

    // Verify homepage content is visible
    const heroTitle = page.locator('.hero h1');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toHaveText('MirDB');
  });

  /**
   * Additional test: Verify 404 page maintains site branding and styling
   */
  test('TC3: 404 page maintains consistent branding with main site', async ({ page }) => {
    await page.goto('/nonexistent-page');
    await page.waitForLoadState('domcontentloaded');

    // Verify the page title includes MirDB branding
    await expect(page).toHaveTitle(/MirDB/);

    // Verify the footer is present with copyright information
    const footer = page.locator('.footer');
    await expect(footer).toBeVisible();

    const copyright = page.locator('.footer p');
    await expect(copyright).toContainText('MirDB Project');

    // Verify footer links are present
    const footerLinks = page.locator('.footer-links a');
    const linkCount = await footerLinks.count();
    expect(linkCount).toBeGreaterThanOrEqual(3);
  });

  /**
   * Additional test: Verify 404 page is accessible
   */
  test('TC4: 404 page is accessible and has proper semantic structure', async ({ page }) => {
    await page.goto('/nonexistent-page');
    await page.waitForLoadState('domcontentloaded');

    // Verify main content has proper role
    const main = page.locator('main[role="main"]');
    await expect(main).toBeVisible();

    // Verify h1 heading exists for the page
    const h1 = page.locator('h1');
    await expect(h1).toBeVisible();

    // Verify links have proper attributes
    const homeLink = page.locator('#home-link');
    await expect(homeLink).toHaveAttribute('href');

    // External links should have rel="noopener" for security
    const githubLink = page.locator('a[href*="github.com"]').first();
    await expect(githubLink).toHaveAttribute('rel', /noopener/);
    await expect(githubLink).toHaveAttribute('target', '_blank');
  });

  /**
   * Additional test: Verify 404 page works with different invalid paths
   */
  test('TC5: 404 page works for various invalid paths', async ({ page }) => {
    const invalidPaths = [
      '/does-not-exist',
      '/random/nested/path',
      '/some-file.txt',
      '/api/fake-endpoint'
    ];

    for (const path of invalidPaths) {
      const response = await page.goto(path);

      // Should return 404 status
      expect(response).not.toBeNull();
      expect(response.status()).toBe(404);

      // Should show 404 page content
      await page.waitForLoadState('domcontentloaded');
      const errorCode = page.locator('.error-code');
      await expect(errorCode).toBeVisible();
      await expect(errorCode).toHaveText('404');
    }
  });

  /**
   * Additional test: Verify 404 page is responsive
   */
  test('TC6: 404 page is responsive on mobile viewport', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });

    await page.goto('/nonexistent-page');
    await page.waitForLoadState('domcontentloaded');

    // Verify all essential elements are visible on mobile
    const errorCode = page.locator('.error-code');
    await expect(errorCode).toBeVisible();

    const errorTitle = page.locator('.error-title');
    await expect(errorTitle).toBeVisible();

    const homeLink = page.locator('#home-link');
    await expect(homeLink).toBeVisible();

    // Verify the home link is clickable and navigates correctly
    await homeLink.click();
    await page.waitForLoadState('domcontentloaded');

    // Should navigate to homepage
    const heroTitle = page.locator('.hero h1');
    await expect(heroTitle).toBeVisible();
  });
});
