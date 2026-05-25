/**
 * End-to-end tests for Interactive Demo and Quick Try.
 * Covers REQ-3 (interactive demo) and REQ-14 (Quick Try).
 */

import { test, expect } from '@playwright/test';

test.describe('Interactive Demo', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('renders the interactive demo section', async ({ page }) => {
    await expect(page.getByTestId('interactive-demo')).toBeVisible();
  });

  test('renders the demo heading', async ({ page }) => {
    await expect(page.getByTestId('interactive-demo-heading')).toHaveTextContent('Interactive Demo');
  });

  test('renders the command input', async ({ page }) => {
    const input = page.getByTestId('command-input');
    await expect(input).toBeVisible();
    await expect(input).toHaveAttribute('placeholder', 'Enter a memcached command...');
  });

  test('renders the execute button', async ({ page }) => {
    await expect(page.getByTestId('execute-button')).toHaveTextContent('Execute');
  });

  test('renders the output terminal', async ({ page }) => {
    await expect(page.getByTestId('output-terminal')).toBeVisible();
  });

  test('shows placeholder when no commands have been run', async ({ page }) => {
    await expect(page.getByTestId('terminal-placeholder')).toHaveTextContent(
      'Type a command and press Enter to see the result...'
    );
  });

  test('executes set command and shows STORED response', async ({ page }) => {
    await page.route('**/api/operation', async (route) => {
      await route.fulfill({
        status: 200,
        body: JSON.stringify({ status: 'ok' }),
      });
    });

    await page.getByTestId('command-input').fill('set demo_key demo_value');
    await page.getByTestId('execute-button').click();

    await expect(page.locator('[data-testid="terminal-line-command"]').first()).toBeVisible();
    await expect(page.locator('[data-testid="terminal-line-response"]').filter({ hasText: 'STORED' })).toBeVisible();
  });

  test('preserves command history after multiple commands', async ({ page }) => {
    await page.route('**/api/operation', async (route) => {
      await route.fulfill({
        status: 200,
        body: JSON.stringify({ status: 'ok' }),
      });
    });

    await page.getByTestId('command-input').fill('set key1 value1');
    await page.getByTestId('execute-button').click();
    await expect(page.locator('[data-testid="terminal-line-response"]').filter({ hasText: 'STORED' }).first()).toBeVisible();

    await page.getByTestId('command-input').fill('set key2 value2');
    await page.getByTestId('execute-button').click();

    const commands = page.locator('[data-testid="terminal-line-command"]');
    await expect(commands).toHaveCount(2);
  });

  test('executes get and displays value', async ({ page }) => {
    await page.route('**/api/operation', async (route) => {
      const postData = JSON.parse(route.request().postData() || '{}');
      if (postData.op === 'get' && postData.key === 'demo_key') {
        await route.fulfill({
          status: 200,
          body: JSON.stringify({ status: 'ok', value: 'demo_value' }),
        });
      } else {
        await route.fulfill({
          status: 200,
          body: JSON.stringify({ status: 'not_found' }),
        });
      }
    });

    await page.getByTestId('command-input').fill('get demo_key');
    await page.getByTestId('execute-button').click();

    await expect(page.locator('[data-testid="terminal-line-response"]').filter({ hasText: 'demo_value' })).toBeVisible();
  });

  test('shows ERROR with hint for invalid command', async ({ page }) => {
    await page.getByTestId('command-input').fill('invalid_cmd');
    await page.getByTestId('execute-button').click();

    await expect(page.locator('[data-testid="terminal-line-error"]').filter({ hasText: 'ERROR' })).toBeVisible();
    await expect(page.locator('[data-testid="terminal-line-info"]').filter({ hasText: 'Hint' })).toBeVisible();
  });

  test('clears output on Ctrl+L and refocuses input', async ({ page }) => {
    await page.route('**/api/operation', async (route) => {
      await route.fulfill({
        status: 200,
        body: JSON.stringify({ status: 'ok' }),
      });
    });

    await page.getByTestId('command-input').fill('set hello world');
    await page.getByTestId('execute-button').click();
    await expect(page.locator('[data-testid="terminal-line-command"]')).toBeVisible();

    await page.getByTestId('command-input').press('Control+l');

    await expect(page.locator('[data-testid="terminal-line-command"]')).not.toBeVisible();
    await expect(page.getByTestId('terminal-placeholder')).toBeVisible();
    await expect(page.getByTestId('command-input')).toBeFocused();
  });

  test('executes command on Enter key press', async ({ page }) => {
    await page.route('**/api/operation', async (route) => {
      await route.fulfill({
        status: 200,
        body: JSON.stringify({ status: 'ok' }),
      });
    });

    await page.getByTestId('command-input').fill('set hello world');
    await page.getByTestId('command-input').press('Enter');

    await expect(page.locator('[data-testid="terminal-line-response"]').filter({ hasText: 'STORED' })).toBeVisible();
  });

  test('terminal has proper ARIA attributes', async ({ page }) => {
    const terminal = page.getByTestId('output-terminal');
    await expect(terminal).toHaveAttribute('role', 'log');
    await expect(terminal).toHaveAttribute('aria-live', 'polite');
  });

  test('shows connection error when server is unavailable', async ({ page }) => {
    await page.route('**/api/operation', async (route) => {
      await route.abort('failed');
    });

    await page.getByTestId('command-input').fill('get key');
    await page.getByTestId('execute-button').click();

    await expect(page.locator('[data-testid="terminal-line-error"]').first()).toBeVisible();
  });
});
