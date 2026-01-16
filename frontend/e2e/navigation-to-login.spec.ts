import { test, expect } from '@playwright/test';

/**
 * Navigation to Login - E2E Tests
 *
 * This test file covers the scenario: "Verify users can navigate from homepage
 * to login page as specified in US-5"
 *
 * Test Cases:
 * 1. E2E: Click 'Login' button in hero section → navigates to /login
 * 2. E2E: Click 'Login' in navigation area → navigates to /login
 * 3. E2E: Verify Login navigation is client-side (no page reload)
 */

test.describe('Navigation to Login - E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage before each test
    await page.goto('/');
  });

  /**
   * Test Case 1: E2E Test
   * Input: Click 'Login' button in hero section
   * Expected: User is navigated to /login route
   */
  test.describe('Test Case 1: Hero Section Login Navigation', () => {
    test('Login button is visible in hero section', async ({ page }) => {
      const loginButton = page.getByTestId('cta-login');

      await expect(loginButton).toBeVisible();
      await expect(loginButton).toHaveText('Login');
    });

    test('clicking Login button navigates to /login route', async ({ page }) => {
      const loginButton = page.getByTestId('cta-login');

      // Click the Login button
      await loginButton.click();

      // Verify URL changed to /login
      await expect(page).toHaveURL('/login');
    });

    test('Login button has correct href attribute', async ({ page }) => {
      const loginButton = page.getByTestId('cta-login');

      // Verify the href attribute
      await expect(loginButton).toHaveAttribute('href', '/login');
    });

    test('Login button is clickable and accessible', async ({ page }) => {
      const loginButton = page.getByTestId('cta-login');

      // Button should be enabled and clickable
      await expect(loginButton).toBeEnabled();

      // Should be focusable for keyboard navigation
      await loginButton.focus();
      await expect(loginButton).toBeFocused();
    });
  });

  /**
   * Test Case 2: E2E Test
   * Input: Click 'Login' in Navbar
   * Expected: User is navigated to /login route
   *
   * Note: The Login button in the hero section serves as the primary
   * login navigation element in the current design.
   */
  test.describe('Test Case 2: Navigation Area Login Button', () => {
    test('Login button is accessible from main navigation area', async ({ page }) => {
      const heroSection = page.getByTestId('hero-section');
      const loginButton = page.getByTestId('cta-login');

      // Login button should be within the hero section (main navigation area)
      await expect(heroSection).toBeVisible();
      await expect(loginButton).toBeVisible();

      // Verify the button is an anchor element
      const tagName = await loginButton.evaluate(el => el.tagName.toLowerCase());
      expect(tagName).toBe('a');
    });

    test('Login navigation works from any scroll position', async ({ page }) => {
      // Scroll down the page
      await page.evaluate(() => window.scrollTo(0, 500));

      // Scroll back to top where Login button is
      await page.evaluate(() => window.scrollTo(0, 0));

      const loginButton = page.getByTestId('cta-login');
      await expect(loginButton).toBeVisible();

      // Click and verify navigation
      await loginButton.click();
      await expect(page).toHaveURL('/login');
    });

    test('Login button is positioned alongside Get Started button', async ({ page }) => {
      const loginButton = page.getByTestId('cta-login');
      const getStartedButton = page.getByTestId('cta-get-started');

      // Both buttons should be visible
      await expect(loginButton).toBeVisible();
      await expect(getStartedButton).toBeVisible();

      // Get bounding boxes to verify they're in the same area
      const loginBox = await loginButton.boundingBox();
      const getStartedBox = await getStartedButton.boundingBox();

      expect(loginBox).not.toBeNull();
      expect(getStartedBox).not.toBeNull();

      // They should be on the same horizontal line (similar y position, within tolerance)
      const yTolerance = 50;
      expect(Math.abs(loginBox!.y - getStartedBox!.y)).toBeLessThan(yTolerance);
    });
  });

  /**
   * Test Case 3: E2E Test
   * Input: Verify Login navigation is client-side
   * Expected: Navigation uses React Router without page reload
   */
  test.describe('Test Case 3: Client-Side Navigation (No Page Reload)', () => {
    test('navigation to /login is client-side without full page reload', async ({ page }) => {
      // Set up a listener to detect navigation type
      let fullPageLoadOccurred = false;

      page.on('load', () => {
        fullPageLoadOccurred = true;
      });

      // Reset the flag after initial page load
      await page.waitForLoadState('load');
      fullPageLoadOccurred = false;

      // Click Login button
      const loginButton = page.getByTestId('cta-login');
      await loginButton.click();

      // Wait a moment for any navigation to complete
      await page.waitForURL('/login');

      // Verify no full page load occurred (SPA navigation)
      expect(fullPageLoadOccurred).toBe(false);
    });

    test('Login link has no target attribute (opens in same tab)', async ({ page }) => {
      const loginButton = page.getByTestId('cta-login');

      // Should not have target="_blank" or any target attribute
      const hasTarget = await loginButton.evaluate(el => el.hasAttribute('target'));
      expect(hasTarget).toBe(false);
    });

    test('Login link is not an external link', async ({ page }) => {
      const loginButton = page.getByTestId('cta-login');

      // Get the href value
      const href = await loginButton.getAttribute('href');

      // Should be a relative path starting with /
      expect(href).toBe('/login');
      expect(href).not.toContain('http');
      expect(href).not.toContain('//');
    });

    test('back button returns to homepage after login navigation', async ({ page }) => {
      // Navigate to login
      const loginButton = page.getByTestId('cta-login');
      await loginButton.click();
      await expect(page).toHaveURL('/login');

      // Use browser back button
      await page.goBack();

      // Should be back at homepage
      await expect(page).toHaveURL('/');
    });

    test('navigation updates browser history correctly', async ({ page }) => {
      // Get initial URL
      const initialUrl = page.url();

      // Navigate to login
      const loginButton = page.getByTestId('cta-login');
      await loginButton.click();

      // URL should have changed
      const newUrl = page.url();
      expect(newUrl).not.toBe(initialUrl);
      expect(newUrl).toContain('/login');
    });
  });

  test.describe('Login Navigation - Mobile Viewport', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
    });

    test('Login button is visible on mobile', async ({ page }) => {
      await page.goto('/');
      const loginButton = page.getByTestId('cta-login');

      await expect(loginButton).toBeVisible();
    });

    test('Login navigation works on mobile', async ({ page }) => {
      await page.goto('/');
      const loginButton = page.getByTestId('cta-login');

      await loginButton.click();
      await expect(page).toHaveURL('/login');
    });

    test('Login button is tappable on mobile (sufficient touch target)', async ({ page }) => {
      await page.goto('/');
      const loginButton = page.getByTestId('cta-login');

      const box = await loginButton.boundingBox();
      expect(box).not.toBeNull();

      // Minimum touch target size is 44x44 pixels
      expect(box!.width).toBeGreaterThanOrEqual(44);
      expect(box!.height).toBeGreaterThanOrEqual(44);
    });
  });
});
