import { test, expect } from '@playwright/test';

/**
 * Navigation to Login - E2E Tests
 *
 * This test file covers the scenario: "Verify users can navigate from homepage
 * to login page as specified in US-5"
 *
 * Test Cases:
 * 1. E2E: Click 'Login' button in hero section → navigates to /login
 * 2. E2E: Click 'Login' in Navbar → navigates to /login
 * 3. E2E: Verify Login navigation is client-side (no page reload)
 */

test.describe('Navigation to Login - E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
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

      await loginButton.click();

      await expect(page).toHaveURL(/\/login$/);
    });

    test('Login button has correct href attribute', async ({ page }) => {
      const loginButton = page.getByTestId('cta-login');

      await expect(loginButton).toHaveAttribute('href', '/login');
    });

    test('Login button is clickable and accessible', async ({ page }) => {
      const loginButton = page.getByTestId('cta-login');

      await expect(loginButton).toBeEnabled();
      await loginButton.focus();
      await expect(loginButton).toBeFocused();
    });
  });

  /**
   * Test Case 2: E2E Test
   * Input: Click 'Login' in Navbar
   * Expected: User is navigated to /login route
   */
  test.describe('Test Case 2: Navbar Login Navigation', () => {
    test('Login link is visible in Navbar', async ({ page }) => {
      const navbarLogin = page.getByTestId('navbar-login');

      await expect(navbarLogin).toBeVisible();
      await expect(navbarLogin).toHaveText('Login');
    });

    test('clicking Login in Navbar navigates to /login route', async ({ page }) => {
      const navbarLogin = page.getByTestId('navbar-login');

      await navbarLogin.click();

      await expect(page).toHaveURL(/\/login$/);
    });

    test('Navbar Login has correct href attribute', async ({ page }) => {
      const navbarLogin = page.getByTestId('navbar-login');

      await expect(navbarLogin).toHaveAttribute('href', '/login');
    });

    test('both hero and navbar Login buttons are accessible on homepage', async ({ page }) => {
      const heroLogin = page.getByTestId('cta-login');
      const navbarLogin = page.getByTestId('navbar-login');

      await expect(heroLogin).toBeVisible();
      await expect(navbarLogin).toBeVisible();

      await expect(heroLogin).toHaveAttribute('href', '/login');
      await expect(navbarLogin).toHaveAttribute('href', '/login');
    });

    test('Login button is positioned alongside Get Started button', async ({ page }) => {
      const loginButton = page.getByTestId('cta-login');
      const getStartedButton = page.getByTestId('cta-get-started');

      await expect(loginButton).toBeVisible();
      await expect(getStartedButton).toBeVisible();

      const loginBox = await loginButton.boundingBox();
      const getStartedBox = await getStartedButton.boundingBox();

      expect(loginBox).not.toBeNull();
      expect(getStartedBox).not.toBeNull();

      const yTolerance = 50;
      expect(Math.abs(loginBox!.y - getStartedBox!.y)).toBeLessThan(yTolerance);
    });
  });

  /**
   * Test Case 3: Integration Test
   * Input: Verify Login navigation is client-side
   * Expected: Navigation uses React Router without page reload
   */
  test.describe('Test Case 3: Client-Side Navigation (No Page Reload)', () => {
    test('navigation to /login is client-side without full page reload', async ({ page }) => {
      let fullPageLoadOccurred = false;

      page.on('load', () => {
        fullPageLoadOccurred = true;
      });

      await page.waitForLoadState('load');
      fullPageLoadOccurred = false;

      const loginButton = page.getByTestId('cta-login');
      await loginButton.click();

      await page.waitForURL(/\/login$/);

      expect(fullPageLoadOccurred).toBe(false);
    });

    test('Login link has no target attribute (opens in same tab)', async ({ page }) => {
      const loginButton = page.getByTestId('cta-login');

      const hasTarget = await loginButton.evaluate(el => el.hasAttribute('target'));
      expect(hasTarget).toBe(false);
    });

    test('Login link is not an external link', async ({ page }) => {
      const loginButton = page.getByTestId('cta-login');

      const href = await loginButton.getAttribute('href');

      expect(href).toBe('/login');
      expect(href).not.toContain('http');
      expect(href).not.toContain('//');
    });

    test('back button returns to homepage after login navigation', async ({ page }) => {
      const loginButton = page.getByTestId('cta-login');
      await loginButton.click();
      await expect(page).toHaveURL(/\/login$/);

      await page.goBack();

      await expect(page).toHaveURL('/');
    });

    test('navigation updates browser history correctly', async ({ page }) => {
      const initialUrl = page.url();

      const loginButton = page.getByTestId('cta-login');
      await loginButton.click();

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
      await expect(page).toHaveURL(/\/login$/);
    });

    test('Login button is tappable on mobile (sufficient touch target)', async ({ page }) => {
      await page.goto('/');
      const loginButton = page.getByTestId('cta-login');

      const box = await loginButton.boundingBox();
      expect(box).not.toBeNull();

      expect(box!.width).toBeGreaterThanOrEqual(44);
      expect(box!.height).toBeGreaterThanOrEqual(44);
    });
  });
});
