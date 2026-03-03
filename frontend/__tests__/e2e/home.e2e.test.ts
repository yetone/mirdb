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
