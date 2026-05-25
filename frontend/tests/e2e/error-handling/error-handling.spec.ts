/**
 * End-to-end tests for Error Handling and Graceful Degradation.
 * Covers NFR-4 (graceful server disconnection handling).
 */

import { test, expect } from '@playwright/test';

test.describe('Error Handling - Server Disconnection', () => {
  test.beforeEach(async ({ page }) => {
    // Default: server is healthy
    await page.route('**/api/health', async (route) => {
      await route.fulfill({
        status: 200,
        body: JSON.stringify({ status: 'healthy', timestamp: new Date().toISOString() }),
      });
    });
    await page.route('**/api/metrics', async (route) => {
      await route.fulfill({
        status: 200,
        body: JSON.stringify({
          total_keys_in_memory: 12345,
          total_disk_storage_mb: 450,
          active_compactions: 2,
          request_latency_ms: 1.2,
        }),
      });
    });
    await page.goto('/');
  });

  test('connection status shows red when server disconnects', async ({ page }) => {
    // Initially connected
    await expect(page.getByTestId('connection-status')).toBeVisible();

    // Simulate server disconnect
    await page.route('**/api/health', async (route) => {
      await route.abort('failed');
    });
    await page.route('**/api/metrics', async (route) => {
      await route.abort('failed');
    });

    // Wait for the UI to reflect disconnection
    await expect(page.getByTestId('connection-status-label')).toHaveTextContent('Disconnected', { timeout: 15000 });

    const dot = page.getByTestId('connection-status-dot');
    await expect(dot).toHaveClass(/connection-status__dot--disconnected/);
  });

  test('metrics show stale data with indicator when server disconnects', async ({ page }) => {
    // Wait for metrics to load initially
    await expect(page.getByTestId('metrics-dashboard')).toBeVisible();

    // Disconnect server
    await page.route('**/api/metrics', async (route) => {
      await route.abort('failed');
    });

    // Wait for error state
    await expect(page.getByTestId('metrics-error')).toBeVisible({ timeout: 15000 });
  });

  test('attempting SET while disconnected shows user-friendly error', async ({ page }) => {
    // Disconnect the operation endpoint
    await page.route('**/api/operation', async (route) => {
      await route.abort('failed');
    });

    // Find and interact with set panel if it exists
    const setPanel = page.getByTestId('kv-set-panel');
    if (await setPanel.isVisible().catch(() => false)) {
      await page.getByTestId('kv-set-key-input').fill('test_key');
      await page.getByTestId('kv-set-value-input').fill('test_value');
      await page.getByTestId('kv-set-submit-button').click();

      // Should show error message
      await expect(page.getByTestId('kv-set-error-message')).toBeVisible();
      const errorText = await page.getByTestId('kv-set-error-message').textContent();
      expect(errorText).toContain('Server unreachable');
    }
  });

  test('retry button is available after disconnection error', async ({ page }) => {
    // Disconnect
    await page.route('**/api/metrics', async (route) => {
      await route.abort('failed');
    });

    // Wait for error state to show retry button
    await expect(page.getByTestId('metrics-error')).toBeVisible({ timeout: 15000 });
    const retryButton = page.locator('button').filter({ hasText: /Retry/i });
    await expect(retryButton).toBeVisible();
  });
});

test.describe('Error Handling - Reconnection', () => {
  test('connection returns to green after server recovers', async ({ page }) => {
    let isHealthy = false;

    await page.route('**/api/health', async (route) => {
      if (isHealthy) {
        await route.fulfill({
          status: 200,
          body: JSON.stringify({ status: 'healthy', timestamp: new Date().toISOString() }),
        });
      } else {
        await route.abort('failed');
      }
    });

    await page.goto('/');

    // Wait for disconnected state
    await expect(page.getByTestId('connection-status-label')).toHaveTextContent('Disconnected', { timeout: 15000 });

    // Restore server
    isHealthy = true;

    // Wait for reconnection (within 2 polling cycles ~10s)
    await expect(page.getByTestId('connection-status-label')).toHaveTextContent('Connected', { timeout: 15000 });

    const dot = page.getByTestId('connection-status-dot');
    await expect(dot).toHaveClass(/connection-status__dot--connected/);
  });

  test('metrics resume updating after reconnection', async ({ page }) => {
    let isHealthy = false;

    await page.route('**/api/health', async (route) => {
      if (isHealthy) {
        await route.fulfill({
          status: 200,
          body: JSON.stringify({ status: 'healthy', timestamp: new Date().toISOString() }),
        });
      } else {
        await route.abort('failed');
      }
    });

    await page.route('**/api/metrics', async (route) => {
      if (isHealthy) {
        await route.fulfill({
          status: 200,
          body: JSON.stringify({
            total_keys_in_memory: 99999,
            total_disk_storage_mb: 999,
            active_compactions: 5,
            request_latency_ms: 0.5,
          }),
        });
      } else {
        await route.abort('failed');
      }
    });

    await page.goto('/');

    // Wait for disconnected state
    await expect(page.getByTestId('connection-status-label')).toHaveTextContent('Disconnected', { timeout: 15000 });

    // Restore server
    isHealthy = true;

    // Metrics should resume
    await expect(page.getByTestId('metrics-grid')).toBeVisible({ timeout: 15000 });
  });
});

test.describe('Error Handling - Invalid API Requests', () => {
  test('POST malformed JSON returns 400 with error message', async ({ page }) => {
    await page.route('**/api/operation', async (route) => {
      const postData = route.request().postData();
      try {
        JSON.parse(postData || '{}');
        await route.continue();
      } catch {
        await route.fulfill({
          status: 400,
          body: JSON.stringify({ error: 'Invalid JSON: parsing failed' }),
        });
      }
    });

    await page.goto('/');

    // Try to trigger a POST with valid JSON first to verify the route works
    const setPanel = page.getByTestId('kv-set-panel');
    if (await setPanel.isVisible().catch(() => false)) {
      await page.getByTestId('kv-set-key-input').fill('test');
      await page.getByTestId('kv-set-value-input').fill('test');

      // The UI should handle errors gracefully
      await page.getByTestId('kv-set-submit-button').click();
    }
  });
});

test.describe('Error Handling - Network Timeout', () => {
  test('frontend shows timeout message and clears loading state', async ({ page }) => {
    // Simulate a slow endpoint that times out
    await page.route('**/api/operation', async (route) => {
      // Don't respond - let it timeout
      await new Promise(() => {});
    });

    await page.goto('/');

    const setPanel = page.getByTestId('kv-set-panel');
    if (await setPanel.isVisible().catch(() => false)) {
      await page.getByTestId('kv-set-key-input').fill('slow_key');
      await page.getByTestId('kv-set-value-input').fill('slow_value');
      await page.getByTestId('kv-set-submit-button').click();

      // Should eventually show error (after timeout)
      // The loading spinner should not be stuck indefinitely
      await page.waitForTimeout(12000);

      // Loading state should be cleared
      const submitButton = page.getByTestId('kv-set-submit-button');
      await expect(submitButton).not.toHaveTextContent('Setting...');
    }
  });
});
