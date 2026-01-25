/**
 * E2E tests for homepage - Hero Section Display
 * Scenario 1 - Test Case 4: Navigate to homepage as unauthenticated user
 *
 * Tests that the hero section loads within 2 seconds with all elements visible
 */

import { test, expect, measurePageLoadTime, waitForHeroSection } from './fixtures';

test.describe('Hero Section Display - E2E', () => {
  test.describe('Test Case 4: Navigate to homepage as unauthenticated user', () => {
    test('should load hero section within 2 seconds with all elements visible', async ({ page }) => {
      const startTime = Date.now();

      // Navigate to homepage
      await page.goto('/');

      // Wait for hero section to be visible
      await waitForHeroSection(page);

      // Measure total load time
      const loadTime = Date.now() - startTime;

      // Verify load time is under 2 seconds (2000ms)
      expect(loadTime).toBeLessThan(2000);

      // Verify all hero section elements are visible
      // Headline
      const headline = page.locator('h1');
      await expect(headline).toBeVisible();
      await expect(headline).toContainText(/shorten.*url/i);

      // Primary CTA button
      const ctaButton = page.getByTestId('hero-cta');
      await expect(ctaButton).toBeVisible();
      await expect(ctaButton).toContainText(/get started/i);

      // Login link
      const loginLink = page.getByTestId('hero-login-link');
      await expect(loginLink).toBeVisible();
      await expect(loginLink).toContainText(/login/i);
    });

    test('should display headline with URL shortening messaging', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      const headline = page.locator('h1');
      await expect(headline).toBeVisible();
      await expect(headline).toContainText('Shorten URLs. Track Every Click.');
    });

    test('should display subheadline with value proposition', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      // Check for subheadline text
      const subheadline = page.locator('text=Transform long, unwieldy URLs');
      await expect(subheadline).toBeVisible();
    });

    test('should have clickable Get Started CTA that navigates to registration', async ({
      page,
    }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      const ctaButton = page.getByTestId('hero-cta');
      await expect(ctaButton).toBeVisible();

      // Click the CTA button
      await ctaButton.click();

      // Verify navigation to registration page
      await expect(page).toHaveURL(/\/register/);
    });

    test('should have clickable Login link that navigates to login page', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      const loginLink = page.getByTestId('hero-login-link');
      await expect(loginLink).toBeVisible();

      // Click the login link
      await loginLink.click();

      // Verify navigation to login page
      await expect(page).toHaveURL(/\/login/);
    });

    test('should display hero section with proper visual hierarchy', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      // Check hero section exists
      const heroSection = page.locator('.hero');
      await expect(heroSection).toBeVisible();

      // Check h1 is present
      const h1 = page.locator('h1');
      await expect(h1).toBeVisible();

      // Check CTA buttons container
      const ctaContainer = page.locator('.flex.flex-col.sm\\:flex-row');
      await expect(ctaContainer).toBeVisible();
    });
  });
});
