/**
 * End-to-end tests for KV Explorer GET operations.
 * Covers REQ-7 (read operations).
 */

import { test, expect } from '@playwright/test';

test.describe('KV Explorer - GET Operations', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('renders the Key-Value Explorer section', async ({ page }) => {
    await expect(page.getByTestId('kv-explorer')).toBeVisible();
  });

  test('renders the key input field', async ({ page }) => {
    const input = page.getByTestId('kv-key-input');
    await expect(input).toBeVisible();
    await expect(input).toHaveAttribute('placeholder', 'Enter key...');
  });

  test('renders the Get button', async ({ page }) => {
    const button = page.getByTestId('kv-get-button');
    await expect(button).toBeVisible();
    await expect(button).toHaveTextContent('Get');
  });

  test('shows validation error when Get is clicked with empty key', async ({ page }) => {
    await page.getByTestId('kv-get-button').click();
    await expect(page.getByTestId('kv-key-error')).toBeVisible();
    await expect(page.getByTestId('kv-key-error')).toHaveTextContent('Key is required');
  });

  test('clears validation error when user types in key input', async ({ page }) => {
    await page.getByTestId('kv-get-button').click();
    await expect(page.getByTestId('kv-key-error')).toBeVisible();

    await page.getByTestId('kv-key-input').fill('testkey');
    await expect(page.getByTestId('kv-key-error')).not.toBeVisible();
  });

  test('shows loading state during GET request', async ({ page }) => {
    await page.route('**/api/operation', async (route) => {
      await new Promise((resolve) => setTimeout(resolve, 500));
      await route.fulfill({
        status: 200,
        body: JSON.stringify({ status: 'ok', value: 'testvalue' }),
      });
    });

    await page.getByTestId('kv-key-input').fill('testkey');
    await page.getByTestId('kv-get-button').click();

    await expect(page.getByTestId('kv-loading')).toBeVisible();
  });

  test('displays value when GET returns existing key', async ({ page }) => {
    await page.route('**/api/operation', async (route) => {
      const postData = JSON.parse(route.request().postData() || '{}');
      if (postData.op === 'get' && postData.key === 'hello') {
        await route.fulfill({
          status: 200,
          body: JSON.stringify({ status: 'ok', value: 'world' }),
        });
      } else {
        await route.fulfill({
          status: 200,
          body: JSON.stringify({ status: 'not_found', message: 'Key not found' }),
        });
      }
    });

    await page.getByTestId('kv-key-input').fill('hello');
    await page.getByTestId('kv-get-button').click();

    await expect(page.getByTestId('kv-result-value')).toBeVisible();
    await expect(page.getByTestId('kv-result-text')).toHaveTextContent('world');
  });

  test('displays Key not found message for non-existing key', async ({ page }) => {
    await page.route('**/api/operation', async (route) => {
      await route.fulfill({
        status: 200,
        body: JSON.stringify({ status: 'not_found', message: 'Key not found' }),
      });
    });

    await page.getByTestId('kv-key-input').fill('nonexistent');
    await page.getByTestId('kv-get-button').click();

    await expect(page.getByTestId('kv-not-found')).toBeVisible();
    await expect(page.getByTestId('kv-not-found')).toHaveTextContent('Key not found');
  });

  test('triggers Get on Enter key press', async ({ page }) => {
    await page.route('**/api/operation', async (route) => {
      await route.fulfill({
        status: 200,
        body: JSON.stringify({ status: 'ok', value: 'testvalue' }),
      });
    });

    await page.getByTestId('kv-key-input').fill('testkey');
    await page.getByTestId('kv-key-input').press('Enter');

    await expect(page.getByTestId('kv-result-value')).toBeVisible();
  });
});
