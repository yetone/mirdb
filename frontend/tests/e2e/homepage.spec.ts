/**
 * E2E homepage tests for basic functionality and performance verification.
 * Owner: Scenario 10 - Performance and Loading
 *
 * These tests verify the homepage loads correctly and all sections are present.
 */

import { test, expect } from '@playwright/test';

test.describe('Homepage E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('homepage loads successfully and displays all sections', async ({ page }) => {
    // Verify page loads
    await expect(page).toHaveTitle(/URL Shortener|Home/i);

    // Verify main page container
    await expect(page.locator('[data-testid="home-page"]')).toBeVisible();

    // Verify Navbar is present
    await expect(page.locator('[data-testid="navbar"]')).toBeVisible();

    // Verify Hero section is present
    await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();
    await expect(page.locator('[data-testid="hero-headline"]')).toBeVisible();
    await expect(page.locator('[data-testid="hero-subheadline"]')).toBeVisible();

    // Verify CTA buttons are present and clickable
    await expect(page.locator('[data-testid="cta-get-started"]')).toBeVisible();
    await expect(page.locator('[data-testid="cta-sign-in"]')).toBeVisible();

    // Verify Features section is present
    await expect(page.locator('[data-testid="features-section"]')).toBeVisible();

    // Verify Footer is present
    await expect(page.locator('[data-testid="footer"]')).toBeVisible();

    // Verify Background effect is present
    await expect(page.locator('[data-testid="background-effect"]')).toBeVisible();
  });

  test('CTA buttons navigate to correct routes', async ({ page }) => {
    // Click Get Started and verify navigation to register
    const getStartedButton = page.locator('[data-testid="cta-get-started"]');
    await getStartedButton.click();
    await expect(page).toHaveURL(/\/register/);

    // Go back to homepage
    await page.goto('/');

    // Click Sign In and verify navigation to login
    const signInButton = page.locator('[data-testid="cta-sign-in"]');
    await signInButton.click();
    await expect(page).toHaveURL(/\/login/);
  });

  test('page is accessible without authentication', async ({ page }) => {
    // Verify page loads without redirecting to login
    await expect(page).not.toHaveURL(/\/login/);
    await expect(page).not.toHaveURL(/\/register/);

    // Verify content is visible
    await expect(page.locator('[data-testid="hero-headline"]')).toContainText(/shorten/i);
  });

  test('responsive layout works on mobile viewport', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });

    // Reload to apply mobile styles
    await page.reload();

    // Verify key elements are still visible
    await expect(page.locator('[data-testid="home-page"]')).toBeVisible();
    await expect(page.locator('[data-testid="hero-headline"]')).toBeVisible();
    await expect(page.locator('[data-testid="cta-get-started"]')).toBeVisible();

    // Check that content doesn't overflow horizontally
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth + 1); // Allow 1px tolerance
  });
});
