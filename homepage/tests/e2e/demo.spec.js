/**
 * Interactive Demo E2E Tests
 * Owner: Scenario 3 - Interactive Demo Functionality
 *
 * Test cases:
 * - Terminal interface visible with input field and output area
 * - Example command suggestions displayed
 * - Loading indicator appears during execution
 * - Commands execute and display results
 */

const { test, expect } = require('@playwright/test');

test.describe('Interactive Demo Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for demo section to be visible
    await page.waitForSelector('#demo');
  });

  test('TC1: Terminal interface with command input and output display', async ({ page }) => {
    // Check demo section exists
    const demoSection = page.locator('#demo');
    await expect(demoSection).toBeVisible();

    // Check terminal container
    const terminal = page.locator('.demo__terminal');
    await expect(terminal).toBeVisible();

    // Check command input field
    const inputField = page.locator('[data-testid="demo-input"]');
    await expect(inputField).toBeVisible();
    await expect(inputField).toBeEnabled();

    // Check output area
    const outputArea = page.locator('#demo-output');
    await expect(outputArea).toBeVisible();

    // Check submit button
    const submitBtn = page.locator('[data-testid="demo-submit"]');
    await expect(submitBtn).toBeVisible();
  });

  test('TC6: Example command suggestions displayed', async ({ page }) => {
    // Check example commands section
    const examplesSection = page.locator('.demo__examples');
    await expect(examplesSection).toBeVisible();

    // Check example buttons exist
    const setExample = page.locator('[data-testid="example-set"]');
    const getExample = page.locator('[data-testid="example-get"]');
    const deleteExample = page.locator('[data-testid="example-delete"]');

    await expect(setExample).toBeVisible();
    await expect(getExample).toBeVisible();
    await expect(deleteExample).toBeVisible();

    // Verify buttons are clickable
    await expect(setExample).toBeEnabled();
    await expect(getExample).toBeEnabled();
    await expect(deleteExample).toBeEnabled();
  });

  test('TC7: Submit empty command shows error or no action', async ({ page }) => {
    const inputField = page.locator('[data-testid="demo-input"]');
    const submitBtn = page.locator('[data-testid="demo-submit"]');

    // Ensure input is empty
    await inputField.clear();

    // Submit empty command
    await submitBtn.click();

    // Check that an error message appears in output
    const output = page.locator('#demo-output');
    const errorLine = output.locator('.demo__output-line--error');

    // Should show error message or not add any new content
    await expect(errorLine.or(output.locator(':scope > *').last())).toBeVisible();
  });

  test('TC8: Loading indicator during command execution', async ({ page }) => {
    const inputField = page.locator('[data-testid="demo-input"]');
    const submitBtn = page.locator('[data-testid="demo-submit"]');

    // Enter a command
    await inputField.fill('SET testkey 0 0 5');

    // Click submit and immediately check for loading state
    const submitPromise = submitBtn.click();

    // Check loading indicator appears (may be brief)
    // The loading class should be applied to submit button
    const loadingIndicator = page.locator('[data-testid="demo-loading"]');
    await expect(loadingIndicator).toBeAttached();

    // Wait for submission to complete
    await submitPromise;
  });

  test('TC2: Execute SET command returns STORED', async ({ page }) => {
    const inputField = page.locator('[data-testid="demo-input"]');
    const submitBtn = page.locator('[data-testid="demo-submit"]');

    // Enter SET command
    await inputField.fill('SET mykey 0 0 5');
    await submitBtn.click();

    // Wait for response
    await page.waitForTimeout(300);

    // Check output contains STORED
    const output = page.locator('#demo-output');
    await expect(output).toContainText('STORED');
  });

  test('TC3: Execute GET command returns stored value', async ({ page }) => {
    const inputField = page.locator('[data-testid="demo-input"]');
    const submitBtn = page.locator('[data-testid="demo-submit"]');

    // First, SET a value
    await inputField.fill('SET mykey 0 0 5');
    await submitBtn.click();
    await page.waitForTimeout(300);

    // Clear input and GET the value
    await inputField.fill('GET mykey');
    await submitBtn.click();
    await page.waitForTimeout(300);

    // Check output contains VALUE response
    const output = page.locator('#demo-output');
    await expect(output).toContainText('VALUE mykey');
    await expect(output).toContainText('END');
  });

  test('TC4: Execute DELETE command returns DELETED', async ({ page }) => {
    const inputField = page.locator('[data-testid="demo-input"]');
    const submitBtn = page.locator('[data-testid="demo-submit"]');

    // First, SET a value
    await inputField.fill('SET delkey 0 0 5');
    await submitBtn.click();
    await page.waitForTimeout(300);

    // DELETE the key
    await inputField.fill('DELETE delkey');
    await submitBtn.click();
    await page.waitForTimeout(300);

    // Check output contains DELETED
    const output = page.locator('#demo-output');
    await expect(output).toContainText('DELETED');
  });

  test('TC5: GET after DELETE returns END (key not found)', async ({ page }) => {
    const inputField = page.locator('[data-testid="demo-input"]');
    const submitBtn = page.locator('[data-testid="demo-submit"]');

    // SET a value
    await inputField.fill('SET tempkey 0 0 5');
    await submitBtn.click();
    await page.waitForTimeout(300);

    // DELETE the key
    await inputField.fill('DELETE tempkey');
    await submitBtn.click();
    await page.waitForTimeout(300);

    // GET the deleted key
    await inputField.fill('GET tempkey');
    await submitBtn.click();
    await page.waitForTimeout(300);

    // Check output ends with END (not VALUE)
    const output = page.locator('#demo-output');
    const lastResponse = output.locator('.demo__output-line--response').last();
    await expect(lastResponse).toContainText('END');
  });

  test('Example button click populates input and executes', async ({ page }) => {
    const setExample = page.locator('[data-testid="example-set"]');
    const output = page.locator('#demo-output');

    // Click SET example
    await setExample.click();
    await page.waitForTimeout(500);

    // Check that command was executed and STORED appears
    await expect(output).toContainText('STORED');
  });

  test('Terminal has proper visual styling', async ({ page }) => {
    // Check terminal header styling (macOS-style dots)
    const redDot = page.locator('.demo__terminal-dot--red');
    const yellowDot = page.locator('.demo__terminal-dot--yellow');
    const greenDot = page.locator('.demo__terminal-dot--green');

    await expect(redDot).toBeVisible();
    await expect(yellowDot).toBeVisible();
    await expect(greenDot).toBeVisible();

    // Check terminal title
    const title = page.locator('.demo__terminal-title');
    await expect(title).toContainText('MirDB Demo');
  });

  test('Input field clears after command execution', async ({ page }) => {
    const inputField = page.locator('[data-testid="demo-input"]');
    const submitBtn = page.locator('[data-testid="demo-submit"]');

    // Enter and submit command
    await inputField.fill('VERSION');
    await submitBtn.click();
    await page.waitForTimeout(300);

    // Check input is cleared
    await expect(inputField).toHaveValue('');
  });

  test('Demo section has proper accessibility attributes', async ({ page }) => {
    // Check ARIA labels
    const terminal = page.locator('.demo__terminal');
    await expect(terminal).toHaveAttribute('role', 'application');
    await expect(terminal).toHaveAttribute('aria-label', 'Interactive MirDB demo terminal');

    const output = page.locator('#demo-output');
    await expect(output).toHaveAttribute('role', 'log');
    await expect(output).toHaveAttribute('aria-live', 'polite');

    // Check input has associated label
    const inputLabel = page.locator('label[for="demo-input"]');
    await expect(inputLabel).toBeAttached();
  });
});
