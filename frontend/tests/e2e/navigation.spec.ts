/**
 * E2E Navigation Tests
 * Owner: Scenario 3 - Primary CTA Redirect to Registration
 *
 * Tests for CTA navigation functionality:
 * - Primary CTA redirects to registration page within 1 second
 *
 * Requirements: REQ-3, US-2
 */
import { test, expect } from '@playwright/test';

test.describe('Homepage CTA Navigation', () => {
  /**
   * Test Case 4: Navigate to homepage and click 'Get Started' button
   * Input: Navigate to homepage and click 'Get Started' button
   * Expected: User is redirected to registration page within 1 second
   */
  test('should redirect to registration page within 1 second when primary CTA is clicked', async ({ page }) => {
    // Navigate to homepage
    await page.goto('/');

    // Wait for the page to load
    await page.waitForLoadState('domcontentloaded');

    // Find and click the primary CTA (Get Started Free button)
    const primaryCTA = page.getByRole('link', { name: /get started/i });
    await expect(primaryCTA).toBeVisible();

    // Record start time
    const startTime = Date.now();

    // Click the CTA
    await primaryCTA.click();

    // Wait for navigation to complete
    await page.waitForURL('/register', { timeout: 1000 });

    // Record end time
    const endTime = Date.now();

    // Verify redirect completed within 1 second
    const redirectTime = endTime - startTime;
    expect(redirectTime).toBeLessThan(1000);

    // Verify we're on the registration page
    expect(page.url()).toContain('/register');
  });

  test('should load registration page content after redirect', async ({ page }) => {
    // Navigate to homepage and click CTA
    await page.goto('/');
    await page.getByRole('link', { name: /get started/i }).click();

    // Verify registration page is loaded
    await expect(page.getByTestId('register-page')).toBeVisible();
  });

  test('should display primary CTA on homepage', async ({ page }) => {
    await page.goto('/');

    // Primary CTA should be visible
    const primaryCTA = page.getByRole('link', { name: /get started/i });
    await expect(primaryCTA).toBeVisible();

    // Should have correct href
    await expect(primaryCTA).toHaveAttribute('href', '/register');
  });

  test('should display secondary CTA (View Demo) on homepage', async ({ page }) => {
    await page.goto('/');

    // Secondary CTA should be visible
    const secondaryCTA = page.getByRole('link', { name: /view demo/i });
    await expect(secondaryCTA).toBeVisible();

    // Should have correct href
    await expect(secondaryCTA).toHaveAttribute('href', '/demo');
  });

  test('should navigate to demo page when secondary CTA is clicked', async ({ page }) => {
    await page.goto('/');

    // Click the secondary CTA
    const secondaryCTA = page.getByRole('link', { name: /view demo/i });
    await secondaryCTA.click();

    // Wait for navigation
    await page.waitForURL('/demo');

    // Verify we're on the demo page
    expect(page.url()).toContain('/demo');
    await expect(page.getByTestId('demo-page')).toBeVisible();
  });

  test('should not have any form inputs on homepage', async ({ page }) => {
    await page.goto('/');

    // Check for absence of form inputs
    const textInputs = page.locator('input[type="text"]');
    const emailInputs = page.locator('input[type="email"]');
    const passwordInputs = page.locator('input[type="password"]');
    const forms = page.locator('form');

    await expect(textInputs).toHaveCount(0);
    await expect(emailInputs).toHaveCount(0);
    await expect(passwordInputs).toHaveCount(0);
    await expect(forms).toHaveCount(0);
  });

  test('should not require any information before redirect', async ({ page }) => {
    // Navigate to homepage
    await page.goto('/');

    // Check that no required inputs exist
    const requiredInputs = await page.locator('[required]').count();
    expect(requiredInputs).toBe(0);

    // Verify user can click CTA without entering any info
    await page.getByRole('link', { name: /get started/i }).click();

    // Should navigate successfully without any form submission
    await expect(page).toHaveURL(/.*\/register/);
  });
});
