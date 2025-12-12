import { test, expect } from '@playwright/test';

/**
 * New Visitor Journey E2E Tests
 * Scenario: User Journey - New Visitor Flow (US-1)
 *
 * Tests the complete user journey for new visitors:
 * 1. Entry via direct URL
 * 2. First impression (hero section)
 * 3. Orientation (navigation and page structure)
 * 4. Engagement (CTA interaction)
 */
test.describe('New Visitor Journey - US-1', () => {
  test.beforeEach(async ({ page, context }) => {
    // Simulate new visitor: clear all cookies and storage
    await context.clearCookies();

    // Navigate to the page, then clear storage for a true "new visitor" state
    await page.goto('/');
    await page.evaluate(() => {
      localStorage.clear();
      sessionStorage.clear();
    });
  });

  /**
   * Test Case 1 (E2E): Complete new visitor journey from entry to CTA click
   * Expected: User can successfully navigate from homepage to conversion action
   */
  test('TC1: Complete new visitor journey from entry to CTA click', async ({ page, context }) => {
    // Clear cookies to simulate new visitor
    await context.clearCookies();

    // Step 1: Entry via direct URL
    const response = await page.goto('/');
    expect(response).not.toBeNull();
    expect(response!.status()).toBe(200);

    // Verify we're on the homepage (no redirect)
    expect(page.url()).toMatch(/\/$/);

    // Step 2: First impression - Hero section with messaging and visual identity
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify headline is visible and contains welcoming message
    const headline = page.locator('[data-testid="hero-headline"]');
    await expect(headline).toBeVisible();
    await expect(headline).toContainText('Welcome');

    // Verify subheadline (value proposition) is visible
    const subheadline = page.locator('[data-testid="hero-subheadline"]');
    await expect(subheadline).toBeVisible();
    await expect(subheadline).toHaveText(/./); // Has content

    // Step 3: Orientation - Scan navigation and page structure
    const navigation = page.getByRole('navigation', { name: 'Main navigation' });
    await expect(navigation).toBeVisible();

    // Verify main navigation links are present
    await expect(page.locator('[data-testid="navigation-link-home"]')).toBeVisible();
    await expect(page.locator('[data-testid="navigation-link-features"]')).toBeVisible();
    await expect(page.locator('[data-testid="navigation-link-about"]')).toBeVisible();
    await expect(page.locator('[data-testid="navigation-link-contact"]')).toBeVisible();

    // Verify footer is present for complete page structure
    const footer = page.locator('footer, [data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Step 4: Engagement - Interact with primary CTA
    const ctaButton = page.locator('[data-testid="hero-cta"]');
    await expect(ctaButton).toBeVisible();
    await expect(ctaButton).toBeEnabled();

    // Verify CTA has meaningful text (conversion action)
    const ctaText = await ctaButton.textContent();
    expect(ctaText).toBeTruthy();
    expect(ctaText!.length).toBeGreaterThan(0);

    // Click the CTA to complete the journey
    await ctaButton.click();

    // Verify the CTA navigated to the expected destination
    // The CTA should have an href attribute pointing to the next step
    const href = await ctaButton.getAttribute('href');
    expect(href).toBeTruthy();
  });

  /**
   * Test Case 2 (Manual): Value proposition is clear within 5 seconds
   * Expected: Value proposition is clear within 5 seconds
   *
   * This test validates that key messaging elements load quickly
   * and are immediately visible to new visitors.
   */
  test('TC2: Value proposition is clear within 5 seconds', async ({ page, context }) => {
    // Clear cookies to simulate new visitor
    await context.clearCookies();

    // Start timing
    const startTime = Date.now();

    // Navigate to homepage
    await page.goto('/');

    // Wait for hero section to be visible (this contains the value proposition)
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible({ timeout: 5000 });

    // Verify headline is visible (primary value proposition)
    const headline = page.locator('[data-testid="hero-headline"]');
    await expect(headline).toBeVisible({ timeout: 5000 });

    // Verify subheadline is visible (supporting value proposition)
    const subheadline = page.locator('[data-testid="hero-subheadline"]');
    await expect(subheadline).toBeVisible({ timeout: 5000 });

    // Verify CTA is visible (clear action path)
    const ctaButton = page.locator('[data-testid="hero-cta"]');
    await expect(ctaButton).toBeVisible({ timeout: 5000 });

    // Calculate total time
    const endTime = Date.now();
    const loadTime = endTime - startTime;

    // Verify all key elements loaded within 5 seconds
    expect(loadTime).toBeLessThanOrEqual(5000);

    // Verify the value proposition content is meaningful
    const headlineText = await headline.textContent();
    const subheadlineText = await subheadline.textContent();

    // Headline should contain welcoming/descriptive text
    expect(headlineText).toBeTruthy();
    expect(headlineText!.length).toBeGreaterThan(5);

    // Subheadline should contain product description
    expect(subheadlineText).toBeTruthy();
    expect(subheadlineText!.length).toBeGreaterThan(10);
  });

  /**
   * Test Case 3 (E2E): CTA button click action
   * Expected: CTA navigates to appropriate next step (signup, learn more, etc.)
   */
  test('TC3: CTA button click navigates to appropriate next step', async ({ page, context }) => {
    // Clear cookies to simulate new visitor
    await context.clearCookies();

    // Navigate to homepage
    await page.goto('/');

    // Locate the CTA button
    const ctaButton = page.locator('[data-testid="hero-cta"]');
    await expect(ctaButton).toBeVisible();
    await expect(ctaButton).toBeEnabled();

    // Get the CTA href before clicking
    const href = await ctaButton.getAttribute('href');
    expect(href).toBeTruthy();

    // Verify CTA has meaningful call-to-action text
    const ctaText = await ctaButton.textContent();
    expect(ctaText).toMatch(/Get Started|Learn More|Sign Up|Try|Start|Explore/i);

    // Store current URL
    const initialUrl = page.url();

    // Click the CTA
    await ctaButton.click();

    // Wait for potential navigation
    await page.waitForLoadState('domcontentloaded');

    // The CTA should either:
    // 1. Navigate to a new page (URL changes)
    // 2. Navigate to an anchor on the same page (URL has hash)
    // 3. Stay on page but trigger some action (modal, scroll, etc.)

    const finalUrl = page.url();

    // Verify the CTA performed an action
    if (href!.startsWith('#')) {
      // Anchor link - URL should contain the hash
      expect(finalUrl).toContain(href);
    } else if (href!.startsWith('/')) {
      // Internal link - should navigate
      expect(finalUrl).toContain(href);
    } else {
      // External link or other - verify it has a valid destination
      expect(href).toMatch(/^(https?:\/\/|\/|#)/);
    }

    // Verify the page is still functional after CTA click
    await expect(page.locator('body')).toBeVisible();
  });

  /**
   * Additional test: Verify complete page structure for new visitors
   */
  test('Complete page structure is visible for new visitors', async ({ page, context }) => {
    // Clear all state for new visitor simulation
    await context.clearCookies();

    await page.goto('/');
    await page.evaluate(() => {
      localStorage.clear();
      sessionStorage.clear();
    });
    await page.reload();

    // Verify Navigation is present
    const navigation = page.locator('[data-testid="main-navigation"]');
    await expect(navigation).toBeVisible();

    // Verify logo/brand is visible
    const logo = page.locator('[data-testid="navigation-logo"]');
    await expect(logo).toBeVisible();

    // Verify Hero section
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify Hero is in unauthenticated state for new visitors
    const isAuthenticated = await heroSection.getAttribute('data-authenticated');
    expect(isAuthenticated).toBe('false');

    // Verify headline shows welcome message (not authenticated message)
    const headline = page.locator('[data-testid="hero-headline"]');
    await expect(headline).toContainText('Welcome to MirDB');

    // Verify Footer is present
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Verify main content area exists
    const main = page.locator('main');
    await expect(main).toBeVisible();
  });

  /**
   * Test: Verify no authentication is required for homepage access
   */
  test('Homepage is accessible without authentication', async ({ page, context }) => {
    // Ensure completely clean state
    await context.clearCookies();

    // Track auth-related API calls (not source file requests)
    const authApiCalls: string[] = [];
    page.on('request', (request) => {
      const url = request.url().toLowerCase();
      // Only check for actual API calls, not source file requests
      // Source files contain .ts, .tsx, .js extensions
      const isSourceFile = /\.(tsx?|jsx?|css|map)($|\?)/.test(url);
      if (!isSourceFile) {
        if (
          url.includes('/api/auth') ||
          url.includes('/api/login') ||
          url.includes('/api/session') ||
          url.includes('/api/token') ||
          url.includes('/api/user') ||
          url.includes('/api/me') ||
          url.includes('/api/verify')
        ) {
          authApiCalls.push(url);
        }
      }
    });

    // Navigate to homepage
    const response = await page.goto('/');

    // Verify successful response
    expect(response).not.toBeNull();
    expect(response!.status()).toBe(200);

    // Verify no redirect to login
    expect(page.url()).toMatch(/\/(#.*)?$/);

    // Wait for page to settle
    await page.waitForLoadState('networkidle');

    // Verify no authentication API calls were made
    expect(authApiCalls).toHaveLength(0);

    // Verify all homepage content is visible
    await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();
    await expect(page.locator('[data-testid="hero-headline"]')).toBeVisible();
    await expect(page.locator('[data-testid="hero-cta"]')).toBeVisible();
  });
});
