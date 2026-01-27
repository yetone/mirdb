/**
 * E2E tests for 404 Error Page.
 * Owner: Scenario 17 - Error Handling - 404 Page
 *
 * Tests:
 * - 404 page displays for non-existent routes
 * - 404 page shows helpful error message
 * - 404 page has link back to homepage
 * - Link back to homepage works correctly
 */

import { test, expect } from '@playwright/test';

test.describe('404 Error Page', () => {
  // Test Case 1: Navigate to /nonexistent-page and verify 404 page is displayed
  test('should display 404 page when navigating to non-existent route', async ({ page }) => {
    // Navigate to a non-existent page
    const response = await page.goto('/nonexistent-page');

    // Verify 404 content is displayed
    await expect(page.locator('h1.error-code')).toContainText('404');
    await expect(page.locator('h2.error-title')).toContainText('Page Not Found');

    // Verify the page title indicates 404
    await expect(page).toHaveTitle(/404/);
  });

  test('should display helpful error message on 404 page', async ({ page }) => {
    // Navigate to non-existent page
    await page.goto('/nonexistent-page');

    // Verify error code is displayed
    const errorCode = page.locator('.error-code');
    await expect(errorCode).toHaveText('404');

    // Verify "Page Not Found" title is displayed
    const errorTitle = page.locator('.error-title');
    await expect(errorTitle).toHaveText('Page Not Found');

    // Verify helpful message is displayed
    const errorMessage = page.locator('.error-message');
    await expect(errorMessage).toBeVisible();
    const messageText = await errorMessage.textContent();
    expect(messageText?.toLowerCase()).toContain("doesn't exist");
  });

  // Test Case 2: Verify 404 page has link to homepage
  test('should have link back to homepage on 404 page', async ({ page }) => {
    // Navigate to non-existent page
    await page.goto('/nonexistent-page');

    // Verify link to homepage exists
    const homeLink = page.locator('a.home-link');
    await expect(homeLink).toBeVisible();
    await expect(homeLink).toHaveAttribute('href', '/');
    await expect(homeLink).toContainText('Back to Homepage');
  });

  test('should navigate to homepage when clicking back link', async ({ page }) => {
    // Navigate to non-existent page
    await page.goto('/some-random-page-that-does-not-exist');

    // Verify we're on the 404 page
    await expect(page.locator('h1.error-code')).toContainText('404');

    // Click the home link
    const homeLink = page.locator('a.home-link');
    await homeLink.click();

    // Verify we're now on the homepage
    await expect(page).toHaveURL('/');
  });

  test('should display 404 page for various non-existent routes', async ({ page }) => {
    const nonExistentRoutes = [
      '/about',
      '/contact',
      '/foo/bar/baz',
      '/123456',
      '/docs/something',
    ];

    for (const route of nonExistentRoutes) {
      await page.goto(route);

      // Verify 404 content is displayed
      const errorCode = page.locator('.error-code');
      await expect(errorCode).toHaveText('404');
    }
  });

  test('should have consistent styling with rest of site', async ({ page }) => {
    // Navigate to 404 page
    await page.goto('/nonexistent-page');

    // Verify the error page uses the container class for consistent styling
    await expect(page.locator('.container')).toBeVisible();
    await expect(page.locator('.error-content')).toBeVisible();

    // Verify button has expected styling (checking it's not unstyled)
    const homeLink = page.locator('.home-link, .btn-primary').first();
    await expect(homeLink).toBeVisible();
    await expect(homeLink).toHaveClass(/btn-primary/);
  });

  test('should be accessible with keyboard navigation', async ({ page }) => {
    // Navigate to 404 page
    await page.goto('/nonexistent-page');

    // Tab to the home link
    await page.keyboard.press('Tab'); // Skip link (if exists)
    await page.keyboard.press('Tab'); // Home link

    // Verify home link is focusable
    const focusedElement = page.locator(':focus');
    const href = await focusedElement.getAttribute('href');
    expect(href).toBe('/');

    // Press Enter to navigate
    await page.keyboard.press('Enter');
    await page.waitForURL('/');

    // Verify navigation worked
    expect(page.url()).toMatch(/\/$/);
  });

  test('should display correctly on mobile viewport', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });

    // Navigate to 404 page
    await page.goto('/nonexistent-page');

    // Verify 404 content is visible on mobile
    const errorCode = page.locator('.error-code');
    await expect(errorCode).toBeVisible();

    // Verify home link is visible and tappable
    const homeLink = page.locator('.home-link, a[href="/"]').first();
    await expect(homeLink).toBeVisible();

    // Verify minimum tap target size (44x44)
    const box = await homeLink.boundingBox();
    expect(box).not.toBeNull();
    if (box) {
      expect(box.height).toBeGreaterThanOrEqual(44);
    }
  });

  test('should have proper meta tags on 404 page', async ({ page }) => {
    // Navigate to 404 page
    await page.goto('/nonexistent-page');

    // Verify page title
    const title = await page.title();
    expect(title).toContain('404');
    expect(title).toContain('MirDB');

    // Verify meta description
    const metaDescription = await page.locator('meta[name="description"]').getAttribute('content');
    expect(metaDescription?.toLowerCase()).toContain("doesn't exist");
  });
});
