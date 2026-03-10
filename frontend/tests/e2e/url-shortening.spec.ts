/**
 * E2E tests for URL Shortening in Hero Section
 * Owner: Scenario 1 - Hero Section URL Shortening
 *
 * Tests the anonymous URL shortening flow including form submission,
 * loading states, result display, and copy functionality.
 */

import { test, expect } from '@playwright/test';

test.describe('Hero Section URL Shortening', () => {
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

  test('should display hero section with URL input and shorten button', async ({ page }) => {
    // Verify hero section elements are present
    await expect(page.getByRole('heading', { level: 1 })).toContainText('Shorten URLs, Track Performance');
    await expect(page.getByTestId('url-input')).toBeVisible();
    await expect(page.getByTestId('shorten-button')).toBeVisible();
  });

  test('should shorten a valid URL and display the result', async ({ page }) => {
    // Test case 1: Valid URL shortening
    const urlInput = page.getByTestId('url-input');
    const shortenButton = page.getByTestId('shorten-button');

    // Enter a valid URL
    await urlInput.fill('https://example.com/very/long/path/to/some/resource?param=value');

    // Click shorten button
    await shortenButton.click();

    // Wait for and verify result display
    await expect(page.getByTestId('result-container')).toBeVisible();
    await expect(page.getByTestId('short-url-display')).toContainText('http://localhost:8000/abc123');

    // Verify copy button is visible
    await expect(page.getByTestId('copy-button')).toBeVisible();
  });

  test('should submit form when Enter key is pressed', async ({ page }) => {
    // Test case 2: Enter key submission
    const urlInput = page.getByTestId('url-input');

    // Enter a valid URL
    await urlInput.fill('https://example.com/path');

    // Press Enter to submit
    await urlInput.press('Enter');

    // Wait for result
    await expect(page.getByTestId('result-container')).toBeVisible();
    await expect(page.getByTestId('short-url-display')).toContainText('http://localhost:8000/abc123');
  });

  test('should copy short URL to clipboard when copy button is clicked', async ({ page, context }) => {
    // Test case 3: Copy button functionality
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const urlInput = page.getByTestId('url-input');
    const shortenButton = page.getByTestId('shorten-button');

    // First shorten a URL
    await urlInput.fill('https://example.com/path');
    await shortenButton.click();

    // Wait for result
    await expect(page.getByTestId('result-container')).toBeVisible();

    // Click copy button
    const copyButton = page.getByTestId('copy-button');
    await copyButton.click();

    // Verify visual confirmation (button text changes to "Copied!")
    await expect(copyButton).toContainText('Copied!');

    // Verify clipboard content
    const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardContent).toBe('http://localhost:8000/abc123');
  });

  test('should show loading state during form submission', async ({ page }) => {
    // Add delay to API response to test loading state
    await page.route('**/api/urls/anonymous', async (route) => {
      await new Promise(resolve => setTimeout(resolve, 500));
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          short_url: 'http://localhost:8000/abc123',
          short_code: 'abc123',
          original_url: 'https://example.com',
        }),
      });
    });

    const urlInput = page.getByTestId('url-input');
    const shortenButton = page.getByTestId('shorten-button');

    // Enter a valid URL
    await urlInput.fill('https://example.com/path');

    // Click and immediately check for loading state
    await shortenButton.click();

    // Verify loading indicator appears
    await expect(page.getByText('Shortening...')).toBeVisible();

    // Wait for completion
    await expect(page.getByTestId('result-container')).toBeVisible();
  });

  test('should show error when submitting empty URL', async ({ page }) => {
    const shortenButton = page.getByTestId('shorten-button');

    // Click without entering URL
    await shortenButton.click();

    // Verify error message
    await expect(page.getByTestId('error-message')).toBeVisible();
    await expect(page.getByTestId('error-message')).toContainText('Please enter a URL');
  });

  test('should have proper accessibility attributes on form elements', async ({ page }) => {
    // Test case 4: Accessibility attributes
    const urlInput = page.getByTestId('url-input');
    const shortenButton = page.getByTestId('shorten-button');

    // Verify input has proper placeholder
    await expect(urlInput).toHaveAttribute('placeholder', 'Paste your long URL here...');

    // Verify input has aria-label
    await expect(urlInput).toHaveAttribute('aria-label', 'URL to shorten');

    // Verify button has aria-label
    await expect(shortenButton).toHaveAttribute('aria-label');
  });
});
