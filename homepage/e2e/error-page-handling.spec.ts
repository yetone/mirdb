import { test, expect } from '@playwright/test';

/**
 * Error Page Handling Tests
 * Scenario: Verify appropriate 404 error page exists for invalid routes
 *
 * Test Case 1: Navigate to /nonexistent-page and verify 404 error page is displayed
 * Test Case 2: Check 404 page for navigation back - 404 page contains link to return to homepage
 */

test.describe('Error Page Handling', () => {
  /**
   * Test Case 1: 404 error page is displayed for non-existent pages
   * Navigate to a URL path that doesn't exist and verify the 404 page appears
   */
  test('TC1: navigating to non-existent page displays 404 error page', async ({ page }) => {
    // Navigate to a non-existent page
    await page.goto('/nonexistent-page');

    // Verify the 404 page is displayed
    const notFoundPage = page.locator('[data-testid="not-found-page"]');
    await expect(notFoundPage).toBeVisible();

    // Verify the 404 title is shown
    const title = page.locator('h1');
    await expect(title).toBeVisible();
    await expect(title).toHaveText('404');

    // Verify "Page Not Found" subtitle is displayed
    const subtitle = page.locator('h2');
    await expect(subtitle).toBeVisible();
    await expect(subtitle).toContainText('Page Not Found');

    // Verify descriptive message is present
    const message = page.locator('.not-found-message');
    await expect(message).toBeVisible();
    await expect(message).toContainText("page you're looking for doesn't exist");
  });

  /**
   * Test Case 2: 404 page contains link to return to homepage
   * Verify the 404 page provides navigation back to the main site
   */
  test('TC2: 404 page contains link to return to homepage', async ({ page }) => {
    // Navigate to a non-existent page
    await page.goto('/nonexistent-page');

    // Verify the home link is visible
    const homeLink = page.locator('[data-testid="home-link"]');
    await expect(homeLink).toBeVisible();

    // Verify the link text guides users back to homepage
    await expect(homeLink).toContainText('Return to Homepage');

    // Verify the link points to the homepage
    await expect(homeLink).toHaveAttribute('href', '/');

    // Click the link and verify we're taken to the homepage
    await homeLink.click();
    await page.waitForURL('/');

    // Verify we're on the homepage by checking for MirDB heading
    const heading = page.locator('h1');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('MirDB');
  });

  /**
   * Additional test: 404 page displays for various invalid routes
   */
  test('TC1-additional: 404 page displays for various invalid routes', async ({ page }) => {
    const invalidRoutes = [
      '/some-random-page',
      '/about',
      '/docs',
      '/api/v1/test',
      '/this/path/does/not/exist',
    ];

    for (const route of invalidRoutes) {
      await page.goto(route);

      // Verify 404 page is shown for each invalid route
      const notFoundPage = page.locator('[data-testid="not-found-page"]');
      await expect(notFoundPage).toBeVisible();

      const title = page.locator('h1');
      await expect(title).toHaveText('404');
    }
  });

  /**
   * Additional test: 404 page is accessible and user-friendly
   */
  test('TC2-additional: 404 page is accessible and styled correctly', async ({ page }) => {
    await page.goto('/nonexistent-page');

    // Verify the page has proper background styling (dark theme)
    const notFoundPage = page.locator('[data-testid="not-found-page"]');
    const backgroundColor = await notFoundPage.evaluate((el) => {
      return window.getComputedStyle(el).backgroundImage;
    });

    // Should have gradient background
    expect(backgroundColor).toContain('gradient');

    // Verify home link is keyboard accessible
    const homeLink = page.locator('[data-testid="home-link"]');
    await homeLink.focus();

    // Verify focus is on the link
    const focusedElement = await page.evaluate(() => document.activeElement?.getAttribute('data-testid'));
    expect(focusedElement).toBe('home-link');
  });
});
