/**
 * Homepage E2E Tests
 * Owner: Scenario 1 - Homepage Hero Section Rendering (primary)
 *        Extended by Scenarios 13, 14
 *
 * End-to-end tests for homepage rendering and interactions.
 */

import { test, expect } from '@playwright/test';

test.describe('Homepage - URL Shortening Result and Copy (Scenario 13)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC-2: click copy button copies shortened URL to clipboard and shows visual feedback', async ({ page, context }) => {
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    await page.route('/api/urls/shorten', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ short_url: 'https://short.link/abc123' }),
      });
    });

    const input = page.getByTestId('url-input');
    const submitButton = page.getByTestId('shorten-button');

    await input.fill('https://example.com/very-long-url-path');
    await submitButton.click();

    await expect(page.getByTestId('result')).toBeVisible();
    await expect(page.getByTestId('result-url')).toHaveText('https://short.link/abc123');

    const copyButton = page.getByTestId('copy-button');
    await expect(copyButton).toBeVisible();
    await expect(copyButton).toHaveText('Copy');

    await copyButton.click();

    await expect(copyButton).toHaveText('Copied!');
    await expect(page.getByTestId('copy-check-icon')).toBeVisible();

    const clipboardText = await page.evaluate(async () => {
      return await navigator.clipboard.readText();
    });
    expect(clipboardText).toBe('https://short.link/abc123');
  });

  test('TC-3: insecure context shows fallback message for manual copy', async ({ page, context }) => {
    await page.route('/api/urls/shorten', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ short_url: 'https://short.link/abc123' }),
      });
    });

    await page.addInitScript(() => {
      Object.defineProperty(window, 'isSecureContext', {
        value: false,
        configurable: true,
      });
      Object.defineProperty(navigator, 'clipboard', {
        value: undefined,
        configurable: true,
      });
    });

    const input = page.getByTestId('url-input');
    const submitButton = page.getByTestId('shorten-button');

    await input.fill('https://example.com/very-long-url-path');
    await submitButton.click();

    await expect(page.getByTestId('result')).toBeVisible();

    const copyButton = page.getByTestId('copy-button');
    await copyButton.click();

    const fallbackMessage = page.getByTestId('copy-fallback-message');
    await expect(fallbackMessage).toBeVisible();
    await expect(fallbackMessage).toHaveText('URL selected for manual copying. Press Ctrl+C to copy.');
    await expect(fallbackMessage).toHaveAttribute('role', 'status');
  });

  test('TC-4: submitting a new URL replaces the previous result', async ({ page }) => {
    let requestCount = 0;
    await page.route('/api/urls/shorten', async (route) => {
      requestCount++;
      const shortUrl = requestCount === 1 ? 'https://short.link/first123' : 'https://short.link/second456';
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ short_url: shortUrl }),
      });
    });

    const input = page.getByTestId('url-input');
    const submitButton = page.getByTestId('shorten-button');

    await input.fill('https://example.com/first-url');
    await submitButton.click();

    await expect(page.getByTestId('result-url')).toHaveText('https://short.link/first123');

    await input.fill('https://example.com/second-url');
    await submitButton.click();

    await expect(page.getByTestId('result-url')).toHaveText('https://short.link/second456');
    await expect(page.locator('[data-testid="result-url"]')).toHaveCount(1);
    await expect(page.locator('text=https://short.link/first123')).not.toBeVisible();
  });

  test('result remains visible while interacting with other page elements', async ({ page }) => {
    await page.route('/api/urls/shorten', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ short_url: 'https://short.link/abc123' }),
      });
    });

    const input = page.getByTestId('url-input');
    const submitButton = page.getByTestId('shorten-button');

    await input.fill('https://example.com/very-long-url-path');
    await submitButton.click();

    await expect(page.getByTestId('result')).toBeVisible();

    await page.getByTestId('hero-cta-button').hover();

    await expect(page.getByTestId('result')).toBeVisible();
    await expect(page.getByTestId('result-url')).toHaveText('https://short.link/abc123');
  });
});
