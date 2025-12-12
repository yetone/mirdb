import { test, expect } from '@playwright/test';

/**
 * Error Handling - 404 Page E2E Tests
 *
 * This test suite verifies that the website handles non-existent pages gracefully
 * by displaying a user-friendly 404 error page with navigation back to homepage.
 */

test.describe('Error Handling - 404 Page', () => {

  /**
   * Test Case 1: 404 error page is displayed for non-existent routes
   * Input: Navigate to /nonexistent-page
   * Expected: 404 error page is displayed with user-friendly message
   */
  test('should display 404 error page with user-friendly message for non-existent route', async ({ page }) => {
    // Navigate to a non-existent page
    const response = await page.goto('/nonexistent-page');

    // Verify the response status is 404
    expect(response?.status()).toBe(404);

    // Verify the 404 page container is visible
    const errorPage = page.locator('[data-testid="error-404-page"]');
    await expect(errorPage).toBeVisible();

    // Verify 404 error code is displayed
    const errorCode = page.locator('[data-testid="error-404-code"]');
    await expect(errorCode).toBeVisible();
    await expect(errorCode).toHaveText('404');

    // Verify user-friendly error message is displayed
    const errorTitle = page.locator('[data-testid="error-404-title"]');
    await expect(errorTitle).toBeVisible();
    await expect(errorTitle).toContainText(/page not found|not found/i);

    // Verify descriptive message is shown
    const errorDescription = page.locator('[data-testid="error-404-description"]');
    await expect(errorDescription).toBeVisible();
  });

  /**
   * Test Case 2: 404 page includes link to return to homepage
   * Input: Check 404 page has navigation
   * Expected: 404 page includes link to return to homepage
   */
  test('should include link to return to homepage on 404 page', async ({ page }) => {
    // Navigate to a non-existent page
    await page.goto('/nonexistent-page');

    // Verify the home link is visible
    const homeLink = page.locator('[data-testid="error-404-home-link"]');
    await expect(homeLink).toBeVisible();

    // Verify the link text indicates returning home
    await expect(homeLink).toContainText(/home|back|return/i);

    // Verify the link href points to homepage
    const href = await homeLink.getAttribute('href');
    expect(href).toBe('/');

    // Click the home link and verify navigation to homepage
    await homeLink.click();
    await page.waitForURL('/');

    // Verify we're on the homepage
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();
  });

  /**
   * Additional test: 404 page is accessible and has proper structure
   */
  test('should have accessible 404 page with proper semantic structure', async ({ page }) => {
    await page.goto('/nonexistent-page');

    // Verify the page has a main heading
    const mainHeading = page.locator('[data-testid="error-404-title"]');
    const tagName = await mainHeading.evaluate((el) => el.tagName.toLowerCase());
    expect(['h1', 'h2']).toContain(tagName);

    // Verify the page title contains 404
    const pageTitle = await page.title();
    expect(pageTitle.toLowerCase()).toContain('404');
  });

  /**
   * Additional test: 404 page works on mobile viewport
   */
  test('should display 404 page correctly on mobile viewport', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });

    // Navigate to non-existent page
    await page.goto('/another-nonexistent-page');

    // Verify 404 page elements are visible on mobile
    const errorPage = page.locator('[data-testid="error-404-page"]');
    await expect(errorPage).toBeVisible();

    const errorCode = page.locator('[data-testid="error-404-code"]');
    await expect(errorCode).toBeVisible();

    const homeLink = page.locator('[data-testid="error-404-home-link"]');
    await expect(homeLink).toBeVisible();
  });

  /**
   * Additional test: Multiple non-existent routes all show 404
   */
  test('should show 404 for various non-existent paths', async ({ page }) => {
    const nonExistentPaths = [
      '/foo',
      '/bar/baz',
      '/this-page-does-not-exist',
      '/random-123'
    ];

    for (const path of nonExistentPaths) {
      const response = await page.goto(path);
      expect(response?.status()).toBe(404);

      const errorPage = page.locator('[data-testid="error-404-page"]');
      await expect(errorPage).toBeVisible();
    }
  });

});
