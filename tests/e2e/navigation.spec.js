/**
 * Navigation Links E2E Tests
 * Owner: Scenario 4 - Navigation Links
 *
 * Tests:
 * - Navigation element exists with expected links
 * - Unauthenticated: Login and Sign Up visible; Dashboard hidden
 * - Authenticated: Dashboard visible; Login/Sign Up hidden
 * - Navigation link routing (login, register, dashboard)
 * - Mobile hamburger menu toggle
 */

const { test, expect } = require('@playwright/test');

test.describe('Navigation Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForSelector('nav', { state: 'visible' });
  });

  test.describe('Navigation Element', () => {
    test('should render navigation element with role and aria-label', async ({ page }) => {
      const nav = page.locator('nav');
      await expect(nav).toBeVisible();

      const role = await nav.getAttribute('role');
      expect(role).toBe('navigation');

      const ariaLabel = await nav.getAttribute('aria-label');
      expect(ariaLabel).toBe('Main navigation');
    });

    test('should render brand link pointing to home', async ({ page }) => {
      const brand = page.locator('[data-testid="nav-brand"]');
      await expect(brand).toBeVisible();

      const href = await brand.getAttribute('href');
      expect(href).toBe('/');

      const text = await brand.textContent();
      expect(text).toBe('MirDB');
    });
  });

  test.describe('Unauthenticated Navigation', () => {
    test('should show Login and Sign Up links for unauthenticated users', async ({ page }) => {
      // Use desktop viewport so desktop nav links are visible
      await page.setViewportSize({ width: 1280, height: 800 });
      // Ensure unauthenticated state
      await page.evaluate(() => {
        localStorage.setItem('auth_state', 'unauthenticated');
        window.location.reload();
      });
      await page.waitForSelector('nav', { state: 'visible' });

      const loginLink = page.locator('[data-testid="nav-login"]');
      const signupLink = page.locator('[data-testid="nav-signup"]');

      await expect(loginLink).toBeVisible();
      await expect(signupLink).toBeVisible();

      // Verify link text
      expect(await loginLink.textContent()).toContain('Log In');
      expect(await signupLink.textContent()).toContain('Sign Up');
    });

    test('should hide Dashboard link for unauthenticated users', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 800 });
      await page.evaluate(() => {
        localStorage.setItem('auth_state', 'unauthenticated');
        window.location.reload();
      });
      await page.waitForSelector('nav', { state: 'visible' });

      const dashboardLink = page.locator('[data-testid="nav-dashboard"]');
      await expect(dashboardLink).toBeHidden();
    });

    test('should show unauthenticated mobile links', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 812 });
      await page.evaluate(() => {
        localStorage.setItem('auth_state', 'unauthenticated');
        window.location.reload();
      });
      await page.waitForSelector('nav', { state: 'visible' });

      // Open mobile menu
      const toggle = page.locator('[data-testid="mobile-menu-toggle"]');
      await toggle.click();

      const mobileLogin = page.locator('[data-testid="mobile-login"]');
      const mobileSignup = page.locator('[data-testid="mobile-signup"]');

      await expect(mobileLogin).toBeVisible();
      await expect(mobileSignup).toBeVisible();
    });
  });

  test.describe('Authenticated Navigation', () => {
    test('should show Dashboard link for authenticated users', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 800 });
      await page.evaluate(() => {
        localStorage.setItem('auth_state', 'authenticated');
        window.location.reload();
      });
      await page.waitForSelector('nav', { state: 'visible' });

      const dashboardLink = page.locator('[data-testid="nav-dashboard"]');
      await expect(dashboardLink).toBeVisible();

      const href = await dashboardLink.getAttribute('href');
      expect(href).toBe('/dashboard');

      const text = await dashboardLink.textContent();
      expect(text).toContain('Dashboard');
    });

    test('should hide Login and Sign Up links for authenticated users', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 800 });
      await page.evaluate(() => {
        localStorage.setItem('auth_state', 'authenticated');
        window.location.reload();
      });
      await page.waitForSelector('nav', { state: 'visible' });

      const loginLink = page.locator('[data-testid="nav-login"]');
      const signupLink = page.locator('[data-testid="nav-signup"]');

      await expect(loginLink).toBeHidden();
      await expect(signupLink).toBeHidden();
    });

    test('should show authenticated mobile links', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 812 });
      await page.evaluate(() => {
        localStorage.setItem('auth_state', 'authenticated');
        window.location.reload();
      });
      await page.waitForSelector('nav', { state: 'visible' });

      // Open mobile menu
      const toggle = page.locator('[data-testid="mobile-menu-toggle"]');
      await toggle.click();

      const mobileDashboard = page.locator('[data-testid="mobile-dashboard"]');
      await expect(mobileDashboard).toBeVisible();

      const mobileLogin = page.locator('[data-testid="mobile-login"]');
      await expect(mobileLogin).toBeHidden();
    });
  });

  test.describe('Navigation Link Routing', () => {
    test('should redirect to /login when clicking Login link', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 800 });
      await page.evaluate(() => {
        localStorage.setItem('auth_state', 'unauthenticated');
        window.location.reload();
      });
      await page.waitForSelector('nav', { state: 'visible' });

      const loginLink = page.locator('[data-testid="nav-login"]');
      await expect(loginLink).toBeVisible();

      const href = await loginLink.getAttribute('href');
      expect(href).toBe('/login');
    });

    test('should redirect to /register when clicking Sign Up link', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 800 });
      await page.evaluate(() => {
        localStorage.setItem('auth_state', 'unauthenticated');
        window.location.reload();
      });
      await page.waitForSelector('nav', { state: 'visible' });

      const signupLink = page.locator('[data-testid="nav-signup"]');
      await expect(signupLink).toBeVisible();

      const href = await signupLink.getAttribute('href');
      expect(href).toBe('/register');
    });

    test('should redirect to /dashboard when clicking Dashboard link', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 800 });
      await page.evaluate(() => {
        localStorage.setItem('auth_state', 'authenticated');
        window.location.reload();
      });
      await page.waitForSelector('nav', { state: 'visible' });

      const dashboardLink = page.locator('[data-testid="nav-dashboard"]');
      await expect(dashboardLink).toBeVisible();

      const href = await dashboardLink.getAttribute('href');
      expect(href).toBe('/dashboard');
    });
  });

  test.describe('Mobile Navigation Menu', () => {
    test('should toggle mobile menu on hamburger button click', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 812 });
      await page.evaluate(() => {
        localStorage.setItem('auth_state', 'unauthenticated');
        window.location.reload();
      });
      await page.waitForSelector('nav', { state: 'visible' });

      const toggle = page.locator('[data-testid="mobile-menu-toggle"]');
      const menu = page.locator('[data-testid="mobile-menu"]');

      // Menu should initially be hidden
      await expect(menu).toHaveAttribute('aria-hidden', 'true');
      await expect(menu).toBeHidden();

      // Click toggle to open
      await toggle.click();
      await expect(menu).toBeVisible();
      await expect(toggle).toHaveAttribute('aria-expanded', 'true');
      await expect(menu).toHaveAttribute('aria-hidden', 'false');

      // Click toggle again to close
      await toggle.click();
      await expect(menu).toBeHidden();
      await expect(toggle).toHaveAttribute('aria-expanded', 'false');
      await expect(menu).toHaveAttribute('aria-hidden', 'true');
    });

    test('should reveal navigation links in mobile menu', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 812 });
      await page.evaluate(() => {
        localStorage.setItem('auth_state', 'unauthenticated');
        window.location.reload();
      });
      await page.waitForSelector('nav', { state: 'visible' });

      // Desktop nav should be hidden on mobile
      const desktopNav = page.locator('[data-testid="desktop-nav"]');
      await expect(desktopNav).toBeHidden();

      // Open mobile menu
      const toggle = page.locator('[data-testid="mobile-menu-toggle"]');
      await toggle.click();

      // Mobile links should be visible
      const mobileLogin = page.locator('[data-testid="mobile-login"]');
      const mobileSignup = page.locator('[data-testid="mobile-signup"]');

      await expect(mobileLogin).toBeVisible();
      await expect(mobileSignup).toBeVisible();

      // Verify hrefs
      expect(await mobileLogin.getAttribute('href')).toBe('/login');
      expect(await mobileSignup.getAttribute('href')).toBe('/register');
    });

    test('should have accessible hamburger button with aria attributes', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 812 });

      const toggle = page.locator('[data-testid="mobile-menu-toggle"]');
      await expect(toggle).toBeVisible();

      const ariaLabel = await toggle.getAttribute('aria-label');
      expect(ariaLabel).toBe('Toggle navigation menu');

      const ariaExpanded = await toggle.getAttribute('aria-expanded');
      expect(ariaExpanded).toBe('false');

      const ariaControls = await toggle.getAttribute('aria-controls');
      expect(ariaControls).toBe('mobile-menu');
    });
  });
});
