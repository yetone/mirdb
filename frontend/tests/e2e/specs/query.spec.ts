/**
 * E2E tests for Interactive Query Builder
 *
 * Owner: Scenario 6 - Interactive Query Builder
 *
 * Test Case 5: Execute command and check history
 * Expected: Command appears in history list and can be clicked to re-execute
 */

import { test, expect } from '@playwright/test';

test.describe('Query Builder', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the query builder page
    await page.goto('/query');
  });

  test('should display query builder interface', async ({ page }) => {
    // Check for main elements
    await expect(page.getByTestId('query-builder')).toBeVisible();
    await expect(page.getByTestId('query-command-input')).toBeVisible();
    await expect(page.getByTestId('query-execute-button')).toBeVisible();
    await expect(page.getByTestId('query-response')).toBeVisible();
    await expect(page.getByTestId('query-history')).toBeVisible();
  });

  test('should show empty state initially', async ({ page }) => {
    await expect(
      page.getByText(/enter a command above and click execute/i)
    ).toBeVisible();

    await expect(
      page.getByText(/no command history yet/i)
    ).toBeVisible();
  });

  test('should disable execute button when input is empty', async ({ page }) => {
    const executeButton = page.getByTestId('query-execute-button');
    await expect(executeButton).toBeDisabled();
  });

  test('should enable execute button when input has content', async ({ page }) => {
    const input = page.getByTestId('query-command-input');
    await input.fill('stats');

    const executeButton = page.getByTestId('query-execute-button');
    await expect(executeButton).toBeEnabled();
  });

  test('should execute command and show response', async ({ page }) => {
    // Mock the API response
    await page.route('**/api/query', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          response: 'STAT version 1.0.0\r\nSTAT uptime 86400\r\nEND',
          success: true,
          execution_time_ms: 5,
        }),
      });
    });

    const input = page.getByTestId('query-command-input');
    await input.fill('stats');

    const executeButton = page.getByTestId('query-execute-button');
    await executeButton.click();

    // Wait for response
    await expect(page.getByTestId('query-response-content')).toContainText('STAT version');
    await expect(page.getByTestId('query-response-status')).toContainText('Success');
  });

  test('should add executed command to history', async ({ page }) => {
    // Mock the API response
    await page.route('**/api/query', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          response: 'STORED',
          success: true,
          execution_time_ms: 3,
        }),
      });
    });

    const input = page.getByTestId('query-command-input');
    await input.fill('set mykey 0 3600 5\r\nhello');

    const executeButton = page.getByTestId('query-execute-button');
    await executeButton.click();

    // Wait for history to update
    const historyList = page.getByTestId('query-history-list');
    await expect(historyList).toBeVisible();
    await expect(historyList).toContainText('set mykey');
  });

  test('should re-execute command from history', async ({ page }) => {
    let callCount = 0;

    // Mock the API with different responses
    await page.route('**/api/query', async (route) => {
      callCount++;
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          response: `Response ${callCount}`,
          success: true,
          execution_time_ms: 2,
        }),
      });
    });

    // Execute first command
    const input = page.getByTestId('query-command-input');
    await input.fill('stats');

    const executeButton = page.getByTestId('query-execute-button');
    await executeButton.click();

    // Wait for first response
    await expect(page.getByTestId('query-response-content')).toContainText('Response 1');

    // Click history item to re-execute
    const historyList = page.getByTestId('query-history-list');
    await historyList.locator('button').first().click();

    // Wait for second response
    await expect(page.getByTestId('query-response-content')).toContainText('Response 2');
  });

  test('should show error for invalid command', async ({ page }) => {
    // Mock error response
    await page.route('**/api/query', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          response: '',
          success: false,
          error: 'Invalid command: invalid_cmd',
        }),
      });
    });

    const input = page.getByTestId('query-command-input');
    await input.fill('invalid_cmd');

    const executeButton = page.getByTestId('query-execute-button');
    await executeButton.click();

    // Check error state
    await expect(page.getByTestId('query-response-status')).toContainText('Error');
  });

  test('should execute with Ctrl+Enter keyboard shortcut', async ({ page }) => {
    // Mock the API response
    await page.route('**/api/query', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          response: 'END',
          success: true,
          execution_time_ms: 1,
        }),
      });
    });

    const input = page.getByTestId('query-command-input');
    await input.fill('get nonexistent');

    // Press Ctrl+Enter
    await input.press('Control+Enter');

    // Wait for response
    await expect(page.getByTestId('query-response-status')).toContainText('Success');
  });

  test('should copy response to clipboard', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-write', 'clipboard-read']);

    // Mock the API response
    await page.route('**/api/query', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          response: 'STORED',
          success: true,
          execution_time_ms: 2,
        }),
      });
    });

    const input = page.getByTestId('query-command-input');
    await input.fill('set key 0 0 5\r\nhello');

    await page.getByTestId('query-execute-button').click();

    // Wait for response
    await expect(page.getByTestId('query-response-content')).toBeVisible();

    // Click copy button
    await page.getByTestId('query-response-copy').click();

    // Check button text changes to "Copied!"
    await expect(page.getByTestId('query-response-copy')).toContainText('Copied!');
  });
});
