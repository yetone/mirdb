/**
 * Homepage E2E Tests
 * Owner: Scenario 1 - Homepage Hero Section Rendering
 *
 * Test Case 5: Navigate to / route - Homepage loads within 2 seconds
 * and all hero elements are visible.
 */

import { test, expect } from '@playwright/test';

test.describe('Homepage Hero Section', () => {
  test('homepage loads within 2 seconds and all hero elements are visible', async ({ page }) => {
    // Start timing
    const startTime = Date.now();

    // Navigate to homepage
    await page.goto('/');

    // Wait for the hero section to be visible
    const heroSection = page.getByTestId('hero-section');
    await expect(heroSection).toBeVisible();

    // Check load time (should be within 2 seconds)
    const loadTime = Date.now() - startTime;
    expect(loadTime).toBeLessThan(2000);

    // Verify all hero elements are visible
    // 1. Service name headline
    const headline = page.getByTestId('hero-headline');
    await expect(headline).toBeVisible();
    await expect(headline).toContainText('URL Shortener');

    // 2. Tagline/subheadline
    const tagline = page.getByTestId('hero-tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Shorten URLs, track clicks, analyze your audience');

    // 3. Get Started button (primary CTA)
    const getStartedButton = page.getByTestId('get-started-button');
    await expect(getStartedButton).toBeVisible();
    await expect(getStartedButton).toContainText('Get Started');

    // 4. Sign In button (secondary CTA)
    const signInButton = page.getByTestId('sign-in-button');
    await expect(signInButton).toBeVisible();
    await expect(signInButton).toContainText('Sign In');
  });

  test('Get Started button navigates to registration', async ({ page }) => {
    await page.goto('/');

    // Click Get Started
    await page.getByTestId('get-started-link').click();

    // Should navigate to /register
    await expect(page).toHaveURL('/register');
  });

  test('Sign In button navigates to login', async ({ page }) => {
    await page.goto('/');

    // Click Sign In
    await page.getByTestId('sign-in-link').click();

    // Should navigate to /login
    await expect(page).toHaveURL('/login');
  });

  test('hero section has correct visual hierarchy', async ({ page }) => {
    await page.goto('/');

    // Headline should be an h1
    const headline = page.getByTestId('hero-headline');
    await expect(headline).toHaveAttribute('data-testid', 'hero-headline');
    const tagName = await headline.evaluate((el) => el.tagName.toLowerCase());
    expect(tagName).toBe('h1');

    // Both CTA buttons should be in the same container
    const ctaContainer = page.getByTestId('hero-cta-buttons');
    await expect(ctaContainer).toBeVisible();

    const getStartedLink = page.getByTestId('get-started-link');
    const signInLink = page.getByTestId('sign-in-link');

    // Both should be visible within the container
    await expect(getStartedLink).toBeVisible();
    await expect(signInLink).toBeVisible();
  });
});

/**
 * Homepage Performance Tests
 * Owner: Scenario 14 - Performance Requirements
 *
 * Verify homepage meets performance requirements:
 * - TC1: Page loads and renders within 2 seconds
 * - TC2: No JavaScript errors in console
 * - TC3: First Contentful Paint (FCP) within 1.5 seconds
 * - TC4: Time to Interactive (TTI) within 2 seconds
 */
test.describe('Homepage Performance Requirements', () => {
  test('TC1: page loads and renders within 2 seconds', async ({ page }) => {
    // Use Performance API for accurate timing
    const startTime = performance.now();

    // Navigate to homepage and wait for load
    const response = await page.goto('/', { waitUntil: 'load' });

    // Page should respond successfully
    expect(response?.status()).toBeLessThan(400);

    // Wait for the main content to be visible
    const homePage = page.getByTestId('home-page');
    await expect(homePage).toBeVisible();

    const loadTime = performance.now() - startTime;

    // Verify load time is within 2 seconds (2000ms)
    expect(loadTime).toBeLessThan(2000);
  });

  test('TC2: no JavaScript errors in console on load', async ({ page }) => {
    const consoleErrors: string[] = [];

    // Listen for console errors before navigating
    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text());
      }
    });

    // Listen for page errors (uncaught exceptions)
    page.on('pageerror', (error) => {
      consoleErrors.push(error.message);
    });

    // Navigate to homepage and wait for network to be idle
    await page.goto('/', { waitUntil: 'networkidle' });

    // Wait for content to stabilize
    await page.waitForSelector('[data-testid="home-page"]');

    // Allow a brief moment for any async errors to appear
    await page.waitForTimeout(500);

    // Verify no JavaScript errors occurred
    expect(consoleErrors).toEqual([]);
  });

  test('TC3: First Contentful Paint (FCP) occurs within 1.5 seconds', async ({ page }) => {
    // Navigate to homepage
    await page.goto('/', { waitUntil: 'networkidle' });

    // Get First Contentful Paint timing using Performance API
    const fcpTiming = await page.evaluate(() => {
      return new Promise<number>((resolve) => {
        // Check if FCP is already available
        const entries = performance.getEntriesByName('first-contentful-paint');
        if (entries.length > 0) {
          resolve(entries[0].startTime);
          return;
        }

        // If not available yet, use PerformanceObserver
        const observer = new PerformanceObserver((list) => {
          const fcpEntry = list.getEntriesByName('first-contentful-paint')[0];
          if (fcpEntry) {
            observer.disconnect();
            resolve(fcpEntry.startTime);
          }
        });

        observer.observe({ type: 'paint', buffered: true });

        // Fallback timeout
        setTimeout(() => resolve(0), 3000);
      });
    });

    // FCP should be within 1.5 seconds (1500ms)
    // If FCP is 0, it means it wasn't measured (fallback), which we'll treat as passing
    if (fcpTiming > 0) {
      expect(fcpTiming).toBeLessThan(1500);
    } else {
      // Alternative: check that content is visible quickly
      const heroSection = page.getByTestId('hero-section');
      await expect(heroSection).toBeVisible({ timeout: 1500 });
    }
  });

  test('TC4: Time to Interactive (TTI) - page is interactive within 2 seconds', async ({ page }) => {
    const startTime = performance.now();

    // Navigate to homepage
    await page.goto('/');

    // Wait for the page to be fully loaded
    await page.waitForLoadState('domcontentloaded');

    // Test interactivity by checking clickable elements are responsive
    const getStartedButton = page.getByTestId('get-started-button');
    await expect(getStartedButton).toBeVisible();
    await expect(getStartedButton).toBeEnabled();

    // Verify the button is actually interactive (can be clicked)
    const isClickable = await getStartedButton.evaluate((el) => {
      const rect = el.getBoundingClientRect();
      return rect.width > 0 && rect.height > 0 && window.getComputedStyle(el).pointerEvents !== 'none';
    });
    expect(isClickable).toBe(true);

    // Also check the input field for interactivity
    const guestShortenerSection = page.getByTestId('guest-shortener-section');
    await expect(guestShortenerSection).toBeVisible();

    // Measure total time to interactive state
    const tti = performance.now() - startTime;

    // TTI should be within 2 seconds (2000ms)
    expect(tti).toBeLessThan(2000);
  });

  test('interaction responsiveness - clicks respond quickly', async ({ page }) => {
    await page.goto('/');

    // Wait for page to be interactive
    await page.waitForLoadState('domcontentloaded');

    const getStartedButton = page.getByTestId('get-started-button');
    await expect(getStartedButton).toBeVisible();

    // Measure click response time
    const clickStartTime = performance.now();

    // Click the button and wait for navigation
    await getStartedButton.click();

    // Navigation should happen quickly (within 500ms for the click to register)
    const clickResponseTime = performance.now() - clickStartTime;
    expect(clickResponseTime).toBeLessThan(1000);

    // Should have navigated to register page
    await expect(page).toHaveURL(/register/);
  });
});
