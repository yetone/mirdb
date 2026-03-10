/**
 * E2E tests for Post-Shortening Registration Prompt
 * Owner: Scenario 11 - Post-Shortening Registration Prompt
 *
 * Tests the registration prompt that appears after anonymous users
 * successfully shorten a URL, encouraging them to register for analytics.
 */

import { test, expect } from '@playwright/test';

test.describe('Post-Shortening Registration Prompt', () => {
  test.beforeEach(async ({ page }) => {
    // Mock the API response for URL shortening
    await page.route('**/api/urls/anonymous', async (route) => {
      const request = route.request();
      const postData = request.postDataJSON();

      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          short_url: 'http://localhost:8000/abc123',
          short_code: 'abc123',
          original_url: postData?.url || 'https://example.com',
        }),
      });
    });

    await page.goto('/');
  });

  test('TC1: Registration prompt appears after successful anonymous URL shortening', async ({ page }) => {
    // Test case 1: Anonymous user successfully shortens URL → Registration prompt appears
    const urlInput = page.getByTestId('url-input');
    const shortenButton = page.getByTestId('shorten-button');

    // Enter a valid URL as anonymous user (not logged in)
    await urlInput.fill('https://example.com/very/long/path/to/resource');

    // Click shorten button
    await shortenButton.click();

    // Wait for result to appear
    await expect(page.getByTestId('result-container')).toBeVisible();

    // Verify registration prompt appears with message about analytics
    const registrationPrompt = page.getByTestId('registration-prompt');
    await expect(registrationPrompt).toBeVisible();

    const promptMessage = page.getByTestId('registration-prompt-message');
    await expect(promptMessage).toContainText(/analytics/i);
  });

  test('TC2: Registration prompt displays correct content about tracking and analytics', async ({ page }) => {
    // Test case 2: Registration prompt content → Shows message about tracking and analytics
    const urlInput = page.getByTestId('url-input');
    const shortenButton = page.getByTestId('shorten-button');

    // Shorten a URL
    await urlInput.fill('https://example.com/path');
    await shortenButton.click();

    // Wait for result
    await expect(page.getByTestId('result-container')).toBeVisible();

    // Verify prompt message contains text about tracking clicks and analytics
    const promptMessage = page.getByTestId('registration-prompt-message');
    await expect(promptMessage).toBeVisible();

    // Should contain "Create an account to track clicks and view detailed analytics" or similar
    const messageText = await promptMessage.textContent();
    expect(messageText?.toLowerCase()).toContain('track');
    expect(messageText?.toLowerCase()).toContain('analytics');
  });

  test('TC3: Short URL and copy button remain visible alongside registration prompt', async ({ page }) => {
    // Test case 3: Short URL visibility after prompt → Short URL and copy button remain visible
    const urlInput = page.getByTestId('url-input');
    const shortenButton = page.getByTestId('shorten-button');

    // Shorten a URL
    await urlInput.fill('https://example.com/path');
    await shortenButton.click();

    // Wait for result container
    await expect(page.getByTestId('result-container')).toBeVisible();

    // Verify short URL is still visible
    const shortUrlDisplay = page.getByTestId('short-url-display');
    await expect(shortUrlDisplay).toBeVisible();
    await expect(shortUrlDisplay).toContainText('http://localhost:8000/abc123');

    // Verify copy button is still visible
    const copyButton = page.getByTestId('copy-button');
    await expect(copyButton).toBeVisible();

    // Verify registration prompt is also visible (non-blocking)
    const registrationPrompt = page.getByTestId('registration-prompt');
    await expect(registrationPrompt).toBeVisible();

    // Both should be in the DOM simultaneously (registration prompt doesn't hide the result)
    await expect(shortUrlDisplay).toBeVisible();
    await expect(registrationPrompt).toBeVisible();
  });

  test('TC4: Clicking Sign Up button navigates to /register page', async ({ page }) => {
    // Test case 4: Click Sign Up button in prompt → Navigates to /register page
    const urlInput = page.getByTestId('url-input');
    const shortenButton = page.getByTestId('shorten-button');

    // Shorten a URL
    await urlInput.fill('https://example.com/path');
    await shortenButton.click();

    // Wait for result and registration prompt
    await expect(page.getByTestId('result-container')).toBeVisible();
    await expect(page.getByTestId('registration-prompt')).toBeVisible();

    // Click the Sign Up button in the registration prompt
    const signUpButton = page.getByTestId('registration-prompt-signup-button');
    await expect(signUpButton).toBeVisible();
    await signUpButton.click();

    // Verify navigation to /register page
    await expect(page).toHaveURL(/\/register/);

    // Verify we're on the register page
    await expect(page.getByRole('heading', { name: /register/i })).toBeVisible();
  });

  test('Registration prompt has proper accessibility attributes', async ({ page }) => {
    // Accessibility test for the registration prompt
    const urlInput = page.getByTestId('url-input');
    const shortenButton = page.getByTestId('shorten-button');

    // Shorten a URL
    await urlInput.fill('https://example.com/path');
    await shortenButton.click();

    // Wait for result
    await expect(page.getByTestId('result-container')).toBeVisible();

    // Verify registration prompt has proper aria attributes
    const registrationPrompt = page.getByTestId('registration-prompt');
    await expect(registrationPrompt).toHaveAttribute('role', 'complementary');
    await expect(registrationPrompt).toHaveAttribute('aria-label', 'Registration prompt');

    // Verify Sign Up button is a proper link
    const signUpButton = page.getByTestId('registration-prompt-signup-button');
    await expect(signUpButton).toHaveAttribute('href', '/register');
  });
});
