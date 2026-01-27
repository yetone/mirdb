/**
 * E2E tests for 404 error page
 * Owner: Scenario 17 - Error Handling - 404 Page
 *
 * Tests:
 * - Navigation to non-existent page displays 404 content
 * - 404 page has link back to homepage
 * - User can navigate back to homepage from 404 page
 */

import { test, expect } from '@playwright/test';

test.describe('404 Error Page', () => {
  test('navigating to non-existent page displays 404 content', async ({ page }) => {
    // Navigate to a non-existent page
    const response = await page.goto('/nonexistent-page');

    // Verify the response indicates a 404 or the page contains 404 content
    // Astro dev server may return 200 for custom 404 pages
    await expect(page.locator('h1.error-code')).toContainText('404');
    await expect(page.locator('h2.error-title')).toContainText('Page Not Found');
  });

  test('404 page has link to homepage', async ({ page }) => {
    // Navigate to a non-existent page
    await page.goto('/nonexistent-page');

    // Verify the 404 page has a link back to homepage
    const homeLink = page.locator('a.home-link');
    await expect(homeLink).toBeVisible();
    await expect(homeLink).toHaveAttribute('href', '/');
    await expect(homeLink).toContainText('Back to Homepage');
  });

  test('user can navigate back to homepage from 404 page', async ({ page }) => {
    // Navigate to a non-existent page
    await page.goto('/nonexistent-page');

    // Verify we're on the 404 page
    await expect(page.locator('h1.error-code')).toContainText('404');

    // Click the home link
    const homeLink = page.locator('a.home-link');
    await homeLink.click();

    // Verify we're now on the homepage
    await expect(page).toHaveURL('/');
  });

  test('404 page displays helpful error message', async ({ page }) => {
    await page.goto('/some-random-nonexistent-url');

    // Check that a helpful message is displayed
    const errorMessage = page.locator('.error-message');
    await expect(errorMessage).toBeVisible();
    await expect(errorMessage).toContainText("doesn't exist");
  });

  test('404 page has proper page title', async ({ page }) => {
    await page.goto('/nonexistent-page');

    // Verify the page title contains 404
    await expect(page).toHaveTitle(/404/);
  });

  test('404 page maintains site styling', async ({ page }) => {
    await page.goto('/nonexistent-page');

    // Verify the error page uses the container class for consistent styling
    await expect(page.locator('.container')).toBeVisible();
    await expect(page.locator('.error-content')).toBeVisible();
  });

  test('home link button has accessible styling', async ({ page }) => {
    await page.goto('/nonexistent-page');

    // Verify the home link has the btn-primary class for visibility
    const homeLink = page.locator('a.home-link');
    await expect(homeLink).toHaveClass(/btn-primary/);
  });
});
