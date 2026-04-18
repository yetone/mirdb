import { test, expect, Page, Route } from '@playwright/test';

/**
 * MirDB Web Dashboard - Form Validation E2E Tests
 *
 * Scenario 14: Form Validation
 *
 * Test cases:
 * 1. Submit key lookup with empty field - Inline error message, form not submitted
 * 2. Submit set key form with empty key name - Validation error on key field, form not submitted
 * 3. Enter invalid TTL value (negative number) - Validation error or input constrained to valid values
 * 4. Enter invalid flags value (non-numeric) - Validation error or input constrained to numbers
 */

test.describe('Form Validation', () => {
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

  test('Test 1: Submit key lookup with empty field - Inline error message, form not submitted', async ({ page }) => {
    let apiCalled = false;

    // Track if API is called
    await page.route('**/api/key/**', async (route: Route) => {
      apiCalled = true;
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ value: 'test', flags: 0, ttl: 0, bytes: 4 })
      });
    });

    // Clear the input field to ensure it's empty
    const keyInput = page.locator('#get-key-input');
    await keyInput.clear();

    // Try to submit the empty form
    await page.locator('#lookup-btn').click();

    // Verify error message appears
    const errorMessage = page.locator('#get-key-error');
    await expect(errorMessage).toBeVisible();
    await expect(errorMessage).toContainText('Key is required');

    // Verify input has error styling
    await expect(keyInput).toHaveClass(/error/);

    // Verify aria-invalid is set for accessibility
    await expect(keyInput).toHaveAttribute('aria-invalid', 'true');

    // Verify form was not submitted (API not called)
    expect(apiCalled).toBe(false);

    // Verify result box is empty (no loading state triggered)
    const resultBox = page.locator('#get-result');
    await expect(resultBox).not.toContainText('Loading');
  });

  test('Test 2: Submit set key form with empty key name - Validation error on key field, form not submitted', async ({ page }) => {
    let apiCalled = false;

    // Track if API is called
    await page.route('**/api/key', async (route: Route) => {
      if (route.request().method() === 'POST') {
        apiCalled = true;
        await route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({ success: true })
        });
      } else {
        await route.continue();
      }
    });

    // Ensure key input is empty but value is filled
    const keyInput = page.locator('#set-key-input');
    await keyInput.clear();
    await page.locator('#set-value-input').fill('some value');
    await page.locator('#set-flags-input').fill('0');
    await page.locator('#set-ttl-input').fill('3600');

    // Try to submit the form
    await page.locator('#set-btn').click();

    // Verify error message appears on key field
    const errorMessage = page.locator('#set-key-error');
    await expect(errorMessage).toBeVisible();
    await expect(errorMessage).toContainText('Key is required');

    // Verify input has error styling
    await expect(keyInput).toHaveClass(/error/);

    // Verify aria-invalid is set for accessibility
    await expect(keyInput).toHaveAttribute('aria-invalid', 'true');

    // Verify form was not submitted (API not called)
    expect(apiCalled).toBe(false);

    // Verify no success toast appears
    const successToast = page.locator('.toast.success');
    await expect(successToast).toHaveCount(0);
  });

  test('Test 3: Enter invalid TTL value (negative number) - Validation error or input constrained to valid values', async ({ page }) => {
    let apiCalled = false;
    let requestBody: any = null;

    // Track API calls
    await page.route('**/api/key', async (route: Route) => {
      if (route.request().method() === 'POST') {
        apiCalled = true;
        requestBody = JSON.parse(route.request().postData() || '{}');
        await route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({ success: true })
        });
      } else {
        await route.continue();
      }
    });

    // Fill the form with valid key and value
    await page.locator('#set-key-input').fill('test-key');
    await page.locator('#set-value-input').fill('test-value');
    await page.locator('#set-flags-input').fill('0');

    // Try to enter a negative TTL value
    const ttlInput = page.locator('#set-ttl-input');
    await ttlInput.clear();
    await ttlInput.fill('-100');

    // Try to submit the form
    await page.locator('#set-btn').click();

    // Check if validation error appears OR if input was constrained
    const ttlError = page.locator('#set-ttl-error');
    const hasError = await ttlError.isVisible().catch(() => false);
    const errorText = hasError ? await ttlError.textContent() : '';

    if (hasError && errorText) {
      // Validation approach: error message is shown
      expect(errorText).toContain('TTL');
      await expect(ttlInput).toHaveAttribute('aria-invalid', 'true');
      expect(apiCalled).toBe(false);
    } else {
      // Constraint approach: browser constraints negative to 0 or validation normalizes it
      // The HTML input has min="0" which should prevent negative values
      const inputValue = await ttlInput.inputValue();
      const numericValue = parseInt(inputValue, 10) || 0;

      // If form submitted, verify TTL was constrained to valid value (>= 0)
      if (apiCalled && requestBody) {
        expect(requestBody.ttl).toBeGreaterThanOrEqual(0);
      } else {
        // Otherwise, input value should be non-negative
        expect(numericValue).toBeGreaterThanOrEqual(0);
      }
    }
  });

  test('Test 4: Enter invalid flags value (non-numeric) - Validation error or input constrained to numbers', async ({ page }) => {
    let apiCalled = false;
    let requestBody: any = null;

    // Track API calls
    await page.route('**/api/key', async (route: Route) => {
      if (route.request().method() === 'POST') {
        apiCalled = true;
        requestBody = JSON.parse(route.request().postData() || '{}');
        await route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({ success: true })
        });
      } else {
        await route.continue();
      }
    });

    // Fill the form with valid key and value
    await page.locator('#set-key-input').fill('test-key');
    await page.locator('#set-value-input').fill('test-value');
    await page.locator('#set-ttl-input').fill('3600');

    // Try to enter a non-numeric flags value
    const flagsInput = page.locator('#set-flags-input');
    await flagsInput.clear();

    // Type non-numeric characters - number inputs typically block these
    await flagsInput.type('abc');

    // Get the actual value in the input
    const inputValue = await flagsInput.inputValue();

    // Try to submit the form
    await page.locator('#set-btn').click();

    // Check if validation error appears
    const flagsError = page.locator('#set-flags-error');
    const hasError = await flagsError.isVisible().catch(() => false);
    const errorText = hasError ? await flagsError.textContent() : '';

    if (hasError && errorText) {
      // Validation approach: error message is shown
      expect(errorText.toLowerCase()).toContain('flag');
      await expect(flagsInput).toHaveAttribute('aria-invalid', 'true');
      expect(apiCalled).toBe(false);
    } else {
      // Constraint approach: browser input type="number" blocks non-numeric input
      // OR JavaScript validation normalizes it to 0
      // The input field should either be empty or contain a numeric value
      if (inputValue === '' || inputValue === undefined) {
        // Empty is acceptable - type="number" rejected the letters
        expect(true).toBe(true);
      } else {
        // If there's a value, it should be numeric
        const numericValue = parseInt(inputValue, 10);
        expect(isNaN(numericValue)).toBe(false);
      }

      // If form submitted, flags should be a valid number (>=0)
      if (apiCalled && requestBody) {
        expect(typeof requestBody.flags).toBe('number');
        expect(requestBody.flags).toBeGreaterThanOrEqual(0);
      }
    }
  });

  test('Error message clears when user starts typing', async ({ page }) => {
    // First trigger an error on get key form
    const keyInput = page.locator('#get-key-input');
    await keyInput.clear();
    await page.locator('#lookup-btn').click();

    // Verify error is shown
    const errorMessage = page.locator('#get-key-error');
    await expect(errorMessage).toContainText('Key is required');
    await expect(keyInput).toHaveClass(/error/);

    // Start typing
    await keyInput.type('n');

    // Error should be cleared
    await expect(errorMessage).toHaveText('');
    await expect(keyInput).not.toHaveClass(/error/);
    await expect(keyInput).toHaveAttribute('aria-invalid', 'false');
  });

  test('Error message clears when user starts typing in set form', async ({ page }) => {
    // First trigger an error on set key form
    const keyInput = page.locator('#set-key-input');
    await keyInput.clear();
    await page.locator('#set-btn').click();

    // Verify error is shown
    const errorMessage = page.locator('#set-key-error');
    await expect(errorMessage).toContainText('Key is required');
    await expect(keyInput).toHaveClass(/error/);

    // Start typing
    await keyInput.type('t');

    // Error should be cleared
    await expect(errorMessage).toHaveText('');
    await expect(keyInput).not.toHaveClass(/error/);
    await expect(keyInput).toHaveAttribute('aria-invalid', 'false');
  });

  test('Form submission succeeds with valid data after fixing validation error', async ({ page }) => {
    let apiCalled = false;

    await page.route('**/api/key', async (route: Route) => {
      if (route.request().method() === 'POST') {
        apiCalled = true;
        await route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({ success: true })
        });
      } else {
        await route.continue();
      }
    });

    // First trigger validation error
    const keyInput = page.locator('#set-key-input');
    await keyInput.clear();
    await page.locator('#set-btn').click();

    // Error should appear
    await expect(page.locator('#set-key-error')).toContainText('Key is required');

    // Now fill in valid data
    await keyInput.fill('valid-key');
    await page.locator('#set-value-input').fill('valid-value');

    // Submit again
    await page.locator('#set-btn').click();

    // Should succeed
    await expect(page.locator('.toast.success')).toBeVisible({ timeout: 5000 });
    expect(apiCalled).toBe(true);
  });
});

test.describe('TTL and Flags Validation Edge Cases', () => {
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

    await page.goto('/');
  });

  test('TTL input has min attribute to prevent negative values', async ({ page }) => {
    const ttlInput = page.locator('#set-ttl-input');
    await expect(ttlInput).toHaveAttribute('min', '0');
    await expect(ttlInput).toHaveAttribute('type', 'number');
  });

  test('Flags input has min attribute to prevent negative values', async ({ page }) => {
    const flagsInput = page.locator('#set-flags-input');
    await expect(flagsInput).toHaveAttribute('min', '0');
    await expect(flagsInput).toHaveAttribute('type', 'number');
  });

  test('Empty TTL defaults to 0 (no expiry)', async ({ page }) => {
    let requestBody: any = null;

    await page.route('**/api/key', async (route: Route) => {
      if (route.request().method() === 'POST') {
        requestBody = JSON.parse(route.request().postData() || '{}');
        await route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({ success: true })
        });
      } else {
        await route.continue();
      }
    });

    await page.locator('#set-key-input').fill('test-key');
    await page.locator('#set-value-input').fill('test-value');

    // Clear TTL
    const ttlInput = page.locator('#set-ttl-input');
    await ttlInput.clear();

    await page.locator('#set-btn').click();

    // Wait for success
    await expect(page.locator('.toast.success')).toBeVisible({ timeout: 5000 });

    // TTL should default to 0
    expect(requestBody.ttl).toBe(0);
  });

  test('Empty flags defaults to 0', async ({ page }) => {
    let requestBody: any = null;

    await page.route('**/api/key', async (route: Route) => {
      if (route.request().method() === 'POST') {
        requestBody = JSON.parse(route.request().postData() || '{}');
        await route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({ success: true })
        });
      } else {
        await route.continue();
      }
    });

    await page.locator('#set-key-input').fill('test-key');
    await page.locator('#set-value-input').fill('test-value');

    // Clear flags
    const flagsInput = page.locator('#set-flags-input');
    await flagsInput.clear();

    await page.locator('#set-btn').click();

    // Wait for success
    await expect(page.locator('.toast.success')).toBeVisible({ timeout: 5000 });

    // Flags should default to 0
    expect(requestBody.flags).toBe(0);
  });
});

test.describe('Accessibility for Form Validation', () => {
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

    await page.goto('/');
  });

  test('Error messages have proper ARIA attributes for screen readers', async ({ page }) => {
    // Get key error element should have role="alert" and aria-live
    const getKeyError = page.locator('#get-key-error');
    await expect(getKeyError).toHaveAttribute('role', 'alert');
    await expect(getKeyError).toHaveAttribute('aria-live', 'polite');

    // Set key error element should have role="alert" and aria-live
    const setKeyError = page.locator('#set-key-error');
    await expect(setKeyError).toHaveAttribute('role', 'alert');
    await expect(setKeyError).toHaveAttribute('aria-live', 'polite');
  });

  test('Form inputs have aria-required attribute', async ({ page }) => {
    await expect(page.locator('#get-key-input')).toHaveAttribute('aria-required', 'true');
    await expect(page.locator('#set-key-input')).toHaveAttribute('aria-required', 'true');
  });

  test('Invalid inputs have aria-invalid set to true', async ({ page }) => {
    // Trigger validation error
    await page.locator('#get-key-input').clear();
    await page.locator('#lookup-btn').click();

    // Check aria-invalid
    await expect(page.locator('#get-key-input')).toHaveAttribute('aria-invalid', 'true');
  });

  test('Valid inputs have aria-invalid set to false after correction', async ({ page }) => {
    // Trigger validation error first
    const keyInput = page.locator('#get-key-input');
    await keyInput.clear();
    await page.locator('#lookup-btn').click();

    // Verify aria-invalid is true
    await expect(keyInput).toHaveAttribute('aria-invalid', 'true');

    // Fix the error
    await keyInput.fill('valid-key');

    // aria-invalid should be false
    await expect(keyInput).toHaveAttribute('aria-invalid', 'false');
  });
});
