/**
 * Navigation E2E Tests
 * Owner: Scenario 3 - Navigation Menu
 *
 * End-to-end tests for navigation functionality:
 * - Click Login link navigates to /login
 * - Click Register link navigates to /register
 * - Navigation bar remains visible (sticky) when scrolling
 */

import { test, expect } from '@playwright/test';

test.describe('Navigation Menu', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should display the navigation bar', async ({ page }) => {
    const navbar = page.getByTestId('navbar');
    await expect(navbar).toBeVisible();
  });

  test('should navigate to /login when clicking Login link', async ({ page }) => {
    const loginLink = page.getByTestId('navbar-login');
    await expect(loginLink).toBeVisible();
    await loginLink.click();
    await expect(page).toHaveURL('/login');
  });

  test('should navigate to /register when clicking Register link', async ({ page }) => {
    const registerLink = page.getByTestId('navbar-register');
    await expect(registerLink).toBeVisible();
    await registerLink.click();
    await expect(page).toHaveURL('/register');
  });

  test('should keep navigation bar visible when scrolling (sticky positioning)', async ({ page }) => {
    // First ensure the page has scrollable content
    await page.setViewportSize({ width: 1280, height: 200 });

    // Verify navbar is visible initially
    const navbar = page.getByTestId('navbar');
    await expect(navbar).toBeVisible();

    // Get the initial bounding box
    const initialBox = await navbar.boundingBox();
    expect(initialBox).toBeTruthy();

    // Scroll down the page
    await page.evaluate(() => window.scrollTo(0, 500));

    // Wait for scroll to complete
    await page.waitForTimeout(100);

    // Verify navbar is still visible and at the top
    await expect(navbar).toBeVisible();
    const afterScrollBox = await navbar.boundingBox();
    expect(afterScrollBox).toBeTruthy();

    // The navbar should remain at the top of the viewport (y close to 0)
    expect(afterScrollBox!.y).toBeLessThanOrEqual(10);
  });

  test('should display theme toggle button', async ({ page }) => {
    const themeToggle = page.getByTestId('theme-toggle');
    await expect(themeToggle).toBeVisible();
  });

  test('should display logo with correct link', async ({ page }) => {
    const logo = page.getByTestId('navbar-logo');
    await expect(logo).toBeVisible();
    await expect(logo).toHaveText('URL Shortener');
  });
});
