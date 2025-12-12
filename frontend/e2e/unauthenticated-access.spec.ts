import { test, expect } from '@playwright/test';

test.describe('Unauthenticated User Access - REQ-6', () => {
  // Test Case 1: Access / without authentication token
  test('TC1: Homepage loads successfully without authentication token', async ({ page, context }) => {
    // Clear all cookies and storage to simulate fresh/unauthenticated state
    await context.clearCookies();

    // Navigate to homepage without any authentication
    const response = await page.goto('/');

    // Verify response is successful (not a redirect to login)
    expect(response).not.toBeNull();
    expect(response!.status()).toBe(200);

    // Verify the final URL is still the homepage (no redirect to login)
    expect(page.url()).toMatch(/\/$/);

    // Verify main content is visible
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify headline is present and visible
    const headline = page.locator('[data-testid="hero-headline"]');
    await expect(headline).toBeVisible();
    await expect(headline).toHaveText(/Welcome to MirDB/);
  });

  // Test Case 2: Access / in incognito/private browsing mode
  test('TC2: All homepage content renders correctly without auth cookies', async ({ page, context }) => {
    // Clear all cookies to simulate incognito-like fresh state
    await context.clearCookies();

    // Navigate first, then clear storage (localStorage requires a page context)
    await page.goto('/');

    // Clear storage after page load to simulate fresh state for subsequent checks
    await page.evaluate(() => {
      localStorage.clear();
      sessionStorage.clear();
    });

    // Reload to test with cleared storage
    await page.reload();

    // Verify main Navigation renders (use aria-label to be specific)
    const navigation = page.getByRole('navigation', { name: 'Main navigation' });
    await expect(navigation).toBeVisible();

    // Verify Hero section renders with all content
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    const headline = page.locator('[data-testid="hero-headline"]');
    await expect(headline).toBeVisible();

    const subheadline = page.locator('[data-testid="hero-subheadline"]');
    await expect(subheadline).toBeVisible();

    const ctaButton = page.locator('[data-testid="hero-cta"]');
    await expect(ctaButton).toBeVisible();
    await expect(ctaButton).toBeEnabled();

    // Verify Footer renders
    const footer = page.locator('footer, [data-testid="footer"]');
    await expect(footer).toBeVisible();
  });

  // Test Case 3: Verify no authentication API calls on initial load
  test('TC3: Page does not require auth check to display content', async ({ page, context }) => {
    // Clear all cookies and storage
    await context.clearCookies();

    // Track auth-related API calls (not source file requests)
    const authRequests: string[] = [];

    page.on('request', (request) => {
      const url = request.url().toLowerCase();
      // Only check for actual API calls, not source file requests
      // Source files contain .ts, .tsx, .js, .css, .map extensions
      const isSourceFile = /\.(tsx?|jsx?|css|map)($|\?)/.test(url);
      if (!isSourceFile) {
        // Check for common auth-related API endpoints
        if (
          url.includes('/api/auth') ||
          url.includes('/api/login') ||
          url.includes('/api/session') ||
          url.includes('/api/token') ||
          url.includes('/api/user') ||
          url.includes('/api/me') ||
          url.includes('/api/whoami') ||
          url.includes('/api/verify')
        ) {
          authRequests.push(url);
        }
      }
    });

    // Navigate to homepage
    await page.goto('/');

    // Wait for page to fully load
    await page.waitForLoadState('networkidle');

    // Verify no authentication-related API calls were made
    expect(authRequests).toHaveLength(0);

    // Verify page content is visible (proving no auth was needed)
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();
  });

  // Additional test: Verify no login redirect occurs
  test('Homepage does not redirect to login page', async ({ page, context }) => {
    await context.clearCookies();

    // Set up navigation listener to catch any redirects
    const navigationUrls: string[] = [];
    page.on('framenavigated', (frame) => {
      if (frame === page.mainFrame()) {
        navigationUrls.push(frame.url());
      }
    });

    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Verify no redirect to login-related pages occurred
    const loginRedirect = navigationUrls.some(url =>
      url.includes('/login') ||
      url.includes('/signin') ||
      url.includes('/auth') ||
      url.includes('/authenticate')
    );

    expect(loginRedirect).toBe(false);

    // Verify final URL is still the homepage
    expect(page.url()).toMatch(/\/(#.*)?$/);
  });

  // Test: Verify homepage is accessible without session cookies
  test('Homepage works without session cookies', async ({ page, context }) => {
    // Explicitly clear all cookies
    await context.clearCookies();

    // Navigate to homepage
    await page.goto('/');

    // Verify page loads successfully
    await expect(page).toHaveTitle(/.*/);

    // Verify main content is present
    const mainContent = page.locator('main');
    await expect(mainContent).toBeVisible();

    // Verify hero content loads
    const headline = page.locator('[data-testid="hero-headline"]');
    await expect(headline).toBeVisible();
  });
});
