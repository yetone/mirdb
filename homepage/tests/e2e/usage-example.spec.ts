/**
 * E2E tests for Usage Example Section
 * Owner: Scenario 3 - Usage Example Section
 */
import { test, expect } from '@playwright/test';

test.describe('Usage Example Section E2E', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 4: Usage GIF loads and displays correctly', async ({ page }) => {
    // Navigate to usage section
    const usageSection = page.locator('#usage');
    await expect(usageSection).toBeVisible();

    // Check that the section has terminal display
    const terminalDisplay = page.getByTestId('terminal-display');
    await expect(terminalDisplay).toBeVisible();

    // Check GIF container is present
    const gifContainer = page.getByTestId('usage-gif-container');
    await expect(gifContainer).toBeVisible();

    // Check GIF image element exists
    const usageGif = page.getByTestId('usage-gif');
    await expect(usageGif).toBeVisible();

    // Verify GIF source attribute
    await expect(usageGif).toHaveAttribute('src', '/assets/images/usage.gif');

    // Verify alt text for accessibility
    await expect(usageGif).toHaveAttribute('alt', 'MirDB usage demonstration showing SET and GET commands');
  });

  test('Usage section displays SET command', async ({ page }) => {
    const setCommand = page.getByTestId('set-command');
    await expect(setCommand).toBeVisible();
    await expect(setCommand).toContainText('set mykey');
  });

  test('Usage section displays GET command', async ({ page }) => {
    const getCommand = page.getByTestId('get-command');
    await expect(getCommand).toBeVisible();
    await expect(getCommand).toContainText('get mykey');
  });

  test('Terminal display has proper styling', async ({ page }) => {
    const terminalDisplay = page.getByTestId('terminal-display');
    await expect(terminalDisplay).toBeVisible();

    // Check terminal buttons exist
    const closeButton = page.locator('.terminal-button.close');
    const minimizeButton = page.locator('.terminal-button.minimize');
    const maximizeButton = page.locator('.terminal-button.maximize');

    await expect(closeButton).toBeVisible();
    await expect(minimizeButton).toBeVisible();
    await expect(maximizeButton).toBeVisible();
  });

  test('Section is navigable via anchor link', async ({ page }) => {
    // Navigate directly via anchor
    await page.goto('/#usage');

    const usageSection = page.locator('#usage');
    await expect(usageSection).toBeInViewport();
  });

  test('Terminal displays complete command sequence', async ({ page }) => {
    const usageSection = page.locator('#usage');

    // Verify the complete terminal output sequence
    await expect(usageSection).toContainText('telnet 127.0.0.1 11211');
    await expect(usageSection).toContainText('Connected to localhost');
    await expect(usageSection).toContainText('set mykey');
    await expect(usageSection).toContainText('STORED');
    await expect(usageSection).toContainText('get mykey');
    await expect(usageSection).toContainText('VALUE mykey');
    await expect(usageSection).toContainText('END');
  });
});
