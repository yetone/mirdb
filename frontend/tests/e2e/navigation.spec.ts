import { test, expect } from '@playwright/test';

test.describe('Navigation and Routing', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Click Login link from homepage navigates to /login with login form', async ({ page }) => {
    const loginLink = page.getByTestId('nav-login');
    await expect(loginLink).toBeVisible();

    await loginLink.click();

    await expect(page).toHaveURL(/.*\/login/);
    await expect(page.getByTestId('login-page')).toBeVisible();
    await expect(page.getByTestId('login-form')).toBeVisible();
    await expect(page.getByTestId('login-username')).toBeVisible();
    await expect(page.getByTestId('login-password')).toBeVisible();
  });

  test('TC2: Click Register link from homepage navigates to /register with registration form', async ({ page }) => {
    const registerLink = page.getByTestId('nav-register');
    await expect(registerLink).toBeVisible();

    await registerLink.click();

    await expect(page).toHaveURL(/.*\/register/);
    await expect(page.getByTestId('register-page')).toBeVisible();
    await expect(page.getByTestId('register-form')).toBeVisible();
    await expect(page.getByTestId('register-username')).toBeVisible();
    await expect(page.getByTestId('register-email')).toBeVisible();
    await expect(page.getByTestId('register-password')).toBeVisible();
  });

  test('TC3: Click Get Started CTA button in hero navigates to /register', async ({ page }) => {
    const ctaButton = page.getByTestId('hero-cta-button');
    await expect(ctaButton).toBeVisible();

    await ctaButton.click();

    await expect(page).toHaveURL(/.*\/register/);
    await expect(page.getByTestId('register-page')).toBeVisible();
  });

  test('TC6: Click Home/Logo link from another page navigates back to homepage', async ({ page }) => {
    await page.goto('/login');
    await expect(page.getByTestId('login-page')).toBeVisible();

    const homeLink = page.getByTestId('nav-home-link');
    await expect(homeLink).toBeVisible();

    await homeLink.click();

    await expect(page).toHaveURL('/');
    await expect(page.getByTestId('home-page')).toBeVisible();
  });

  test('TC4: Authenticated user sees Dashboard link and can navigate to /dashboard', async ({ page }) => {
    await page.goto('/login');

    await page.route('/api/token', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ access_token: 'mock-token', token_type: 'bearer' }),
      });
    });

    await page.route('/api/users/me', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ id: 1, username: 'testuser', email: 'test@example.com', is_admin: 0 }),
      });
    });

    await page.getByTestId('login-username').fill('testuser');
    await page.getByTestId('login-password').fill('password123');
    await page.getByTestId('login-submit').click();

    await expect(page).toHaveURL('/dashboard');

    await page.goto('/');

    const dashboardLink = page.getByTestId('nav-dashboard');
    await expect(dashboardLink).toBeVisible();

    await dashboardLink.click();

    await expect(page).toHaveURL('/dashboard');
    await expect(page.getByTestId('dashboard-page')).toBeVisible();
  });
});
