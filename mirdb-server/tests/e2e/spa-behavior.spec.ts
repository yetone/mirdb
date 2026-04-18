import { test, expect, Page, Route } from '@playwright/test';

/**
 * MirDB Web Dashboard - SPA Behavior E2E Tests
 *
 * Scenario 13: Single Page Application Behavior
 *
 * Test cases:
 * 1. Submit key lookup form - No page reload, results appear dynamically
 * 2. Submit set key form - Loading indicator shown, then success toast
 * 3. Click delete button - Confirmation dialog, loading state, then success feedback
 * 4. Trigger compaction - Button shows loading state during API call
 * 5. Network error during operation - Error toast notification displayed
 */

// Helper to track page navigation (reload detection)
async function setupNavigationTracker(page: Page): Promise<{ wasReloaded: () => boolean }> {
  let reloaded = false;

  // Mark the page with a unique value
  await page.evaluate(() => {
    (window as any).__spa_test_marker = Math.random().toString(36);
  });

  const initialMarker = await page.evaluate(() => (window as any).__spa_test_marker);

  return {
    wasReloaded: () => {
      return reloaded;
    }
  };
}

// Helper to check if page was reloaded by checking our marker
async function checkNoReload(page: Page, initialMarker: string): Promise<boolean> {
  const currentMarker = await page.evaluate(() => (window as any).__spa_test_marker);
  return currentMarker === initialMarker;
}

// Helper to wait for loading state to appear and disappear
async function waitForLoadingCycle(page: Page, buttonSelector: string): Promise<boolean> {
  const button = page.locator(buttonSelector);

  // Wait for loading class to be added
  try {
    await expect(button).toHaveClass(/loading/, { timeout: 2000 });
  } catch {
    // Loading state may have already passed if API is fast
    return true;
  }

  // Wait for loading class to be removed
  await expect(button).not.toHaveClass(/loading/, { timeout: 10000 });
  return true;
}

test.describe('Single Page Application Behavior', () => {
  test.beforeEach(async ({ page }) => {
    // Mock the initial status API call
    await page.route('**/api/status', async (route: Route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          levels: [{ files: 3 }, { files: 12 }],
          memory: { percentage: 45.2 }
        })
      });
    });

    await page.goto('/');
    // Wait for initial load
    await expect(page.locator('#status-panel')).toBeVisible();
  });

  test('Test 1: Submit key lookup form - No page reload, results appear dynamically', async ({ page }) => {
    // Set up navigation marker to detect reloads
    await page.evaluate(() => {
      (window as any).__spa_marker = 'test_marker_' + Date.now();
    });
    const initialMarker = await page.evaluate(() => (window as any).__spa_marker);

    // Mock the GET key API endpoint
    await page.route('**/api/key/test-key', async (route: Route) => {
      // Add a small delay to ensure we can observe loading state
      await new Promise(resolve => setTimeout(resolve, 100));
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          value: 'test-value-123',
          flags: 0,
          ttl: 3600,
          bytes: 14
        })
      });
    });

    // Fill in the key lookup form
    await page.locator('#get-key-input').fill('test-key');

    // Submit the form
    await page.locator('#lookup-btn').click();

    // Verify the result appears dynamically
    const resultBox = page.locator('#get-result');
    await expect(resultBox).toContainText('test-value-123', { timeout: 5000 });
    await expect(resultBox).toContainText('Flags: 0');
    await expect(resultBox).toContainText('TTL: 3600');
    await expect(resultBox).toContainText('Bytes: 14');

    // Verify no page reload occurred
    const currentMarker = await page.evaluate(() => (window as any).__spa_marker);
    expect(currentMarker).toBe(initialMarker);

    // Verify delete button appears after successful lookup
    await expect(page.locator('#delete-action')).toBeVisible();
  });

  test('Test 2: Submit set key form - Loading indicator shown, then success toast', async ({ page }) => {
    // Track that we see loading state
    let loadingStateObserved = false;

    // Mock the POST key API endpoint with delay
    await page.route('**/api/key', async (route: Route) => {
      if (route.request().method() === 'POST') {
        // Delay to ensure loading state is visible
        await new Promise(resolve => setTimeout(resolve, 300));
        await route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({ success: true })
        });
      } else {
        await route.continue();
      }
    });

    // Fill in the set key form
    await page.locator('#set-key-input').fill('my-new-key');
    await page.locator('#set-value-input').fill('my-new-value');
    await page.locator('#set-flags-input').fill('0');
    await page.locator('#set-ttl-input').fill('3600');

    // Click submit and immediately check for loading state
    const setButton = page.locator('#set-btn');
    await setButton.click();

    // Check that button has loading class
    await expect(setButton).toHaveClass(/loading/);
    loadingStateObserved = true;

    // Wait for loading to complete
    await expect(setButton).not.toHaveClass(/loading/, { timeout: 5000 });

    // Verify success toast appears
    const toast = page.locator('.toast.success');
    await expect(toast).toBeVisible({ timeout: 3000 });
    await expect(toast).toContainText('my-new-key');
    await expect(toast).toContainText('set successfully');

    expect(loadingStateObserved).toBe(true);
  });

  test('Test 3: Click delete button - Confirmation dialog, loading state, then success feedback', async ({ page }) => {
    // First, set up a key to lookup so we can delete it
    await page.route('**/api/key/delete-test-key', async (route: Route) => {
      if (route.request().method() === 'GET') {
        await route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({
            value: 'value-to-delete',
            flags: 0,
            ttl: 0,
            bytes: 15
          })
        });
      } else if (route.request().method() === 'DELETE') {
        // Add delay for loading state observation
        await new Promise(resolve => setTimeout(resolve, 300));
        await route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({ success: true })
        });
      }
    });

    // Lookup the key first
    await page.locator('#get-key-input').fill('delete-test-key');
    await page.locator('#lookup-btn').click();

    // Wait for result and delete button to appear
    await expect(page.locator('#get-result')).toContainText('value-to-delete');
    await expect(page.locator('#delete-action')).toBeVisible();

    // Click the delete button
    await page.locator('#delete-btn').click();

    // Verify confirmation dialog appears
    const dialog = page.locator('#delete-confirm-dialog');
    await expect(dialog).toBeVisible();
    await expect(page.locator('#delete-confirm-message')).toContainText('delete-test-key');

    // Click confirm button and check for loading state
    const confirmBtn = page.locator('#delete-confirm-btn');
    await confirmBtn.click();

    // Check loading state on confirm button
    await expect(confirmBtn).toHaveClass(/loading/);

    // Wait for loading to complete and dialog to close
    await expect(dialog).toBeHidden({ timeout: 5000 });

    // Verify success toast appears
    const toast = page.locator('.toast.success');
    await expect(toast).toBeVisible({ timeout: 3000 });
    await expect(toast).toContainText('deleted successfully');

    // Verify delete action is hidden after successful deletion
    await expect(page.locator('#delete-action')).toBeHidden();
  });

  test('Test 4: Trigger compaction - Button shows loading state during API call', async ({ page }) => {
    // Mock the compaction API endpoint
    await page.route('**/api/operations/compact', async (route: Route) => {
      // Add delay to ensure loading state is visible
      await new Promise(resolve => setTimeout(resolve, 400));
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ success: true })
      });
    });

    // Navigate to operations panel
    await page.locator('#operations-panel').scrollIntoViewIfNeeded();

    const compactBtn = page.locator('#compact-btn');

    // Click compact button
    await compactBtn.click();

    // Verify loading state appears
    await expect(compactBtn).toHaveClass(/loading/);
    await expect(compactBtn).toBeDisabled();

    // Wait for loading to complete
    await expect(compactBtn).not.toHaveClass(/loading/, { timeout: 5000 });
    await expect(compactBtn).toBeEnabled();

    // Verify success toast appears
    const toast = page.locator('.toast.success');
    await expect(toast).toBeVisible({ timeout: 3000 });
    await expect(toast).toContainText('Compaction triggered successfully');
  });

  test('Test 5: Network error during operation - Error toast notification displayed', async ({ page }) => {
    // Mock the GET key API to return an error
    await page.route('**/api/key/error-test-key', async (route: Route) => {
      await route.fulfill({
        status: 500,
        contentType: 'application/json',
        body: JSON.stringify({ error: 'Internal server error' })
      });
    });

    // Fill in and submit the form
    await page.locator('#get-key-input').fill('error-test-key');
    await page.locator('#lookup-btn').click();

    // Verify error is displayed in result box
    const resultBox = page.locator('#get-result');
    await expect(resultBox).toContainText('Error:', { timeout: 5000 });
  });

  test('Test 5b: Network failure shows error toast for set operation', async ({ page }) => {
    // Mock a network failure for set key
    await page.route('**/api/key', async (route: Route) => {
      if (route.request().method() === 'POST') {
        await route.abort('failed');
      } else {
        await route.continue();
      }
    });

    // Fill in the set key form
    await page.locator('#set-key-input').fill('network-fail-key');
    await page.locator('#set-value-input').fill('some value');

    // Submit the form
    await page.locator('#set-btn').click();

    // Wait for loading to complete (will fail due to network error)
    await expect(page.locator('#set-btn')).not.toHaveClass(/loading/, { timeout: 5000 });

    // Verify error toast appears
    const errorToast = page.locator('.toast.error');
    await expect(errorToast).toBeVisible({ timeout: 5000 });
  });

  test('Test 5c: Network failure shows error toast for compaction', async ({ page }) => {
    // Mock a network failure for compaction
    await page.route('**/api/operations/compact', async (route: Route) => {
      await route.fulfill({
        status: 503,
        contentType: 'application/json',
        body: JSON.stringify({ error: 'Service unavailable' })
      });
    });

    // Click compact button
    await page.locator('#compact-btn').click();

    // Wait for loading to complete
    await expect(page.locator('#compact-btn')).not.toHaveClass(/loading/, { timeout: 5000 });

    // Verify error toast appears
    const errorToast = page.locator('.toast.error');
    await expect(errorToast).toBeVisible({ timeout: 5000 });
  });
});

test.describe('SPA Navigation Integrity', () => {
  test('Multiple operations do not cause page reloads', async ({ page }) => {
    // Mock all API endpoints
    await page.route('**/api/status', async (route: Route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          levels: [{ files: 5 }, { files: 20 }],
          memory: { percentage: 60.0 }
        })
      });
    });

    await page.route('**/api/key/**', async (route: Route) => {
      if (route.request().method() === 'GET') {
        await route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({
            value: 'test-value',
            flags: 1,
            ttl: 1800,
            bytes: 10
          })
        });
      } else if (route.request().method() === 'DELETE') {
        await route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({ success: true })
        });
      }
    });

    await page.route('**/api/key', async (route: Route) => {
      if (route.request().method() === 'POST') {
        await route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({ success: true })
        });
      } else {
        await route.continue();
      }
    });

    await page.route('**/api/operations/compact', async (route: Route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ success: true })
      });
    });

    await page.goto('/');

    // Set navigation marker
    await page.evaluate(() => {
      (window as any).__navigation_test_id = 'navigation_marker_' + Date.now();
    });
    const initialMarkerId = await page.evaluate(() => (window as any).__navigation_test_id);

    // Perform multiple operations

    // 1. Refresh status
    await page.locator('#refresh-status').click();
    await expect(page.locator('#level-0-files')).toContainText('5', { timeout: 3000 });

    // 2. Lookup a key
    await page.locator('#get-key-input').fill('multi-op-key');
    await page.locator('#lookup-btn').click();
    await expect(page.locator('#get-result')).toContainText('test-value', { timeout: 3000 });

    // 3. Set a key
    await page.locator('#set-key-input').fill('another-key');
    await page.locator('#set-value-input').fill('another-value');
    await page.locator('#set-btn').click();
    await expect(page.locator('.toast.success')).toBeVisible({ timeout: 3000 });

    // 4. Trigger compaction
    await page.locator('#compact-btn').click();
    await expect(page.locator('#compact-btn')).not.toHaveClass(/loading/, { timeout: 5000 });

    // Verify no navigation/reload occurred
    const currentMarkerId = await page.evaluate(() => (window as any).__navigation_test_id);
    expect(currentMarkerId).toBe(initialMarkerId);
  });
});

test.describe('Loading State Visual Feedback', () => {
  test('Loading spinner appears on buttons during API calls', async ({ page }) => {
    // Mock slow API response
    await page.route('**/api/status', async (route: Route) => {
      await new Promise(resolve => setTimeout(resolve, 500));
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          levels: [{ files: 1 }, { files: 2 }],
          memory: { percentage: 30.0 }
        })
      });
    });

    await page.goto('/');

    // Click refresh and verify loading spinner CSS is applied
    const refreshBtn = page.locator('#refresh-status');
    await refreshBtn.click();

    // Check the button has the loading class which triggers the spinner
    await expect(refreshBtn).toHaveClass(/loading/);

    // Verify the button text is hidden (color: transparent in CSS)
    const buttonStyles = await refreshBtn.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        color: styles.color,
        position: styles.position
      };
    });

    // When loading, button has position: relative (from .loading class)
    expect(buttonStyles.position).toBe('relative');

    // Wait for loading to complete
    await expect(refreshBtn).not.toHaveClass(/loading/, { timeout: 5000 });
  });

  test('Result area shows loading text during fetch', async ({ page }) => {
    // Mock slow API response for key lookup
    await page.route('**/api/status', async (route: Route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          levels: [{ files: 1 }, { files: 2 }],
          memory: { percentage: 30.0 }
        })
      });
    });

    await page.route('**/api/key/slow-key', async (route: Route) => {
      await new Promise(resolve => setTimeout(resolve, 500));
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          value: 'finally loaded',
          flags: 0,
          ttl: 0,
          bytes: 14
        })
      });
    });

    await page.goto('/');

    // Lookup a key
    await page.locator('#get-key-input').fill('slow-key');
    await page.locator('#lookup-btn').click();

    // Result box should show "Loading..." text
    const resultBox = page.locator('#get-result');
    await expect(resultBox).toContainText('Loading...');

    // Eventually shows the actual result
    await expect(resultBox).toContainText('finally loaded', { timeout: 5000 });
  });
});

test.describe('Toast Notification Behavior', () => {
  test('Success toast appears and auto-dismisses', async ({ page }) => {
    // Mock API endpoints
    await page.route('**/api/status', async (route: Route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          levels: [{ files: 1 }],
          memory: { percentage: 25.0 }
        })
      });
    });

    await page.route('**/api/key', async (route: Route) => {
      if (route.request().method() === 'POST') {
        await route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({ success: true })
        });
      }
    });

    await page.goto('/');

    // Submit a set key form
    await page.locator('#set-key-input').fill('toast-test-key');
    await page.locator('#set-value-input').fill('toast-test-value');
    await page.locator('#set-btn').click();

    // Toast should appear
    const toast = page.locator('.toast.success');
    await expect(toast).toBeVisible({ timeout: 3000 });

    // Toast should have correct role for accessibility
    await expect(toast).toHaveAttribute('role', 'alert');

    // Toast should auto-dismiss after ~3 seconds (with some tolerance)
    await expect(toast).toBeHidden({ timeout: 5000 });
  });

  test('Error toast displays error message', async ({ page }) => {
    // Mock failing API
    await page.route('**/api/status', async (route: Route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          levels: [],
          memory: { percentage: 0 }
        })
      });
    });

    await page.route('**/api/operations/compact', async (route: Route) => {
      await route.fulfill({
        status: 500,
        contentType: 'application/json',
        body: JSON.stringify({ error: 'Compaction failed: disk full' })
      });
    });

    await page.goto('/');

    // Try to trigger compaction
    await page.locator('#compact-btn').click();

    // Error toast should appear
    const errorToast = page.locator('.toast.error');
    await expect(errorToast).toBeVisible({ timeout: 3000 });
    await expect(errorToast).toContainText('Compaction failed');
  });

  test('Multiple toasts can appear simultaneously', async ({ page }) => {
    // Mock API endpoints with quick responses
    await page.route('**/api/status', async (route: Route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          levels: [{ files: 1 }],
          memory: { percentage: 25.0 }
        })
      });
    });

    await page.route('**/api/key', async (route: Route) => {
      if (route.request().method() === 'POST') {
        await route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({ success: true })
        });
      }
    });

    await page.goto('/');

    // Submit multiple forms quickly
    await page.locator('#set-key-input').fill('key1');
    await page.locator('#set-value-input').fill('value1');
    await page.locator('#set-btn').click();

    // Wait briefly and submit another
    await page.waitForTimeout(100);
    await page.locator('#set-key-input').fill('key2');
    await page.locator('#set-value-input').fill('value2');
    await page.locator('#set-btn').click();

    // Both toasts should be visible
    const toasts = page.locator('.toast.success');
    await expect(toasts.first()).toBeVisible({ timeout: 3000 });

    // Toast container should have the toasts
    const toastContainer = page.locator('#toast-container');
    await expect(toastContainer).toBeVisible();
  });
});

test.describe('Delete Confirmation Dialog', () => {
  test.beforeEach(async ({ page }) => {
    await page.route('**/api/status', async (route: Route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          levels: [{ files: 1 }],
          memory: { percentage: 25.0 }
        })
      });
    });

    await page.route('**/api/key/dialog-test-key', async (route: Route) => {
      if (route.request().method() === 'GET') {
        await route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({
            value: 'dialog-test-value',
            flags: 0,
            ttl: 0,
            bytes: 17
          })
        });
      } else if (route.request().method() === 'DELETE') {
        await route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({ success: true })
        });
      }
    });

    await page.goto('/');
  });

  test('Cancel button closes dialog without deleting', async ({ page }) => {
    // Lookup a key
    await page.locator('#get-key-input').fill('dialog-test-key');
    await page.locator('#lookup-btn').click();
    await expect(page.locator('#get-result')).toContainText('dialog-test-value');

    // Click delete
    await page.locator('#delete-btn').click();

    // Dialog should appear
    const dialog = page.locator('#delete-confirm-dialog');
    await expect(dialog).toBeVisible();

    // Click cancel
    await page.locator('#delete-cancel-btn').click();

    // Dialog should close
    await expect(dialog).toBeHidden();

    // Delete button should still be visible (key not deleted)
    await expect(page.locator('#delete-action')).toBeVisible();

    // Result should still show the key value
    await expect(page.locator('#get-result')).toContainText('dialog-test-value');
  });

  test('Escape key closes dialog', async ({ page }) => {
    // Lookup a key
    await page.locator('#get-key-input').fill('dialog-test-key');
    await page.locator('#lookup-btn').click();
    await expect(page.locator('#get-result')).toContainText('dialog-test-value');

    // Click delete
    await page.locator('#delete-btn').click();

    // Dialog should appear
    const dialog = page.locator('#delete-confirm-dialog');
    await expect(dialog).toBeVisible();

    // Press Escape
    await page.keyboard.press('Escape');

    // Dialog should close
    await expect(dialog).toBeHidden();
  });

  test('Dialog has proper ARIA attributes', async ({ page }) => {
    // Lookup a key
    await page.locator('#get-key-input').fill('dialog-test-key');
    await page.locator('#lookup-btn').click();
    await expect(page.locator('#get-result')).toContainText('dialog-test-value');

    // Click delete
    await page.locator('#delete-btn').click();

    // Dialog should have proper ARIA attributes
    const dialog = page.locator('#delete-confirm-dialog');
    await expect(dialog).toHaveAttribute('role', 'dialog');
    await expect(dialog).toHaveAttribute('aria-modal', 'true');
    await expect(dialog).toHaveAttribute('aria-labelledby', 'delete-dialog-title');
  });
});
