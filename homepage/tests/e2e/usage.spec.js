/**
 * Usage and Code Examples Tests
 * Owner: Scenario 5 - Usage and Code Examples
 *
 * Tests:
 * - Usage section presence with id='usage'
 * - Code block presence with syntax highlighting
 * - SET/GET command examples
 * - ADD/REPLACE/APPEND/PREPEND/DELETE command examples
 * - Copy-to-clipboard functionality
 */

const { test, expect } = require('@playwright/test');
const { BASE_URL, SELECTORS, waitForPageLoad } = require('./test-utils');

test.describe('Usage and Code Examples Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);
  });

  test('TC1: Usage section exists with id="usage"', async ({ page }) => {
    // Test case 1: Check usage section element
    const usageSection = page.locator('#usage');
    await expect(usageSection).toBeVisible();
    await expect(usageSection).toHaveClass(/usage/);
  });

  test('TC2: Code blocks with syntax highlighting exist', async ({ page }) => {
    // Test case 2: Check for code block elements with syntax highlighting
    const codeBlocks = page.locator('#usage pre.usage__code');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThan(0);

    // Check that code blocks have the language class
    const firstCodeBlock = codeBlocks.first();
    await expect(firstCodeBlock).toHaveClass(/language-bash/);
  });

  test('TC3: SET command example is present', async ({ page }) => {
    // Test case 3: Search for 'set' command example
    const usageSection = page.locator('#usage');

    // Look for SET command in code examples
    const setExample = usageSection.locator('pre:has-text("set ")').first();
    await expect(setExample).toBeVisible();

    // Verify SET command syntax is shown
    const setCode = await setExample.textContent();
    expect(setCode.toLowerCase()).toContain('set');
  });

  test('TC4: GET command example is present', async ({ page }) => {
    // Test case 4: Search for 'get' command example
    const usageSection = page.locator('#usage');

    // Look for GET command in code examples
    const getExample = usageSection.locator('pre:has-text("get ")').first();
    await expect(getExample).toBeVisible();

    // Verify GET command syntax is shown
    const getCode = await getExample.textContent();
    expect(getCode.toLowerCase()).toContain('get');
  });

  test('TC5: Copy buttons exist on code blocks', async ({ page }) => {
    // Test case 5: Check for copy button on code blocks
    const usageSection = page.locator('#usage');
    const copyButtons = usageSection.locator('.usage__copy-btn');

    const count = await copyButtons.count();
    expect(count).toBeGreaterThan(0);

    // Check that copy buttons have the necessary attributes
    const firstCopyBtn = copyButtons.first();
    await expect(firstCopyBtn).toBeVisible();
    await expect(firstCopyBtn).toHaveAttribute('data-copy-target');

    // Verify button has aria-label for accessibility
    await expect(firstCopyBtn).toHaveAttribute('aria-label', 'Copy to clipboard');
  });

  test('TC6: Syntax highlighting is applied to code blocks', async ({ page }) => {
    // Test case 6: Verify syntax highlighting applied
    const usageSection = page.locator('#usage');

    // Check for code blocks with language class
    const highlightedBlocks = usageSection.locator('pre[class*="language-"]');
    const count = await highlightedBlocks.count();
    expect(count).toBeGreaterThan(0);

    // Wait for JavaScript to apply highlighting
    await page.waitForTimeout(500);

    // Check for highlighted class (added by prism.js after processing)
    const firstBlock = highlightedBlocks.first();
    await expect(firstBlock).toHaveClass(/highlighted/);
  });

  test('ADD command example is present', async ({ page }) => {
    // Additional test: Check for ADD command
    const usageSection = page.locator('#usage');
    const addExample = usageSection.locator('pre:has-text("add ")').first();
    await expect(addExample).toBeVisible();
  });

  test('REPLACE command example is present', async ({ page }) => {
    // Additional test: Check for REPLACE command
    const usageSection = page.locator('#usage');
    const replaceExample = usageSection.locator('pre:has-text("replace ")').first();
    await expect(replaceExample).toBeVisible();
  });

  test('APPEND command example is present', async ({ page }) => {
    // Additional test: Check for APPEND command
    const usageSection = page.locator('#usage');
    const appendExample = usageSection.locator('pre:has-text("append ")').first();
    await expect(appendExample).toBeVisible();
  });

  test('PREPEND command example is present', async ({ page }) => {
    // Additional test: Check for PREPEND command
    const usageSection = page.locator('#usage');
    const prependExample = usageSection.locator('pre:has-text("prepend ")').first();
    await expect(prependExample).toBeVisible();
  });

  test('DELETE command example is present', async ({ page }) => {
    // Additional test: Check for DELETE command
    const usageSection = page.locator('#usage');
    const deleteExample = usageSection.locator('pre:has-text("delete ")').first();
    await expect(deleteExample).toBeVisible();
  });

  test('Usage section has proper structure', async ({ page }) => {
    // Verify section structure
    const usageSection = page.locator('#usage');

    // Check for title
    const title = usageSection.locator('.usage__title');
    await expect(title).toBeVisible();
    await expect(title).toContainText('Usage');

    // Check for subtitle
    const subtitle = usageSection.locator('.usage__subtitle');
    await expect(subtitle).toBeVisible();

    // Check for section containers
    const sections = usageSection.locator('.usage__section');
    const sectionCount = await sections.count();
    expect(sectionCount).toBeGreaterThanOrEqual(2);
  });

  test('Copy button changes state on click', async ({ page, context }) => {
    // Grant clipboard permissions for the test
    await context.grantPermissions(['clipboard-write', 'clipboard-read']);

    // Test copy button interaction
    const usageSection = page.locator('#usage');
    const firstCopyBtn = usageSection.locator('.usage__copy-btn').first();

    // Click the copy button
    await firstCopyBtn.click();

    // Wait for state change
    await page.waitForTimeout(300);

    // Check that button text changed to indicate success or error state
    // (clipboard API may not work in all test environments)
    const buttonText = await firstCopyBtn.textContent();
    // Button should change state - either to "Copied!" or "Error" depending on clipboard permissions
    expect(buttonText).toMatch(/(Copied|Error)/);
  });

  test('Code blocks contain properly formatted examples', async ({ page }) => {
    // Verify code formatting
    const usageSection = page.locator('#usage');
    const codeBlocks = usageSection.locator('pre.usage__code code');

    const count = await codeBlocks.count();
    expect(count).toBeGreaterThan(0);

    // Each code block should have actual content
    for (let i = 0; i < count; i++) {
      const codeContent = await codeBlocks.nth(i).textContent();
      expect(codeContent.trim().length).toBeGreaterThan(0);
    }
  });

  test('Code header shows language label', async ({ page }) => {
    // Verify language labels
    const usageSection = page.locator('#usage');
    const langLabels = usageSection.locator('.usage__code-lang');

    const count = await langLabels.count();
    expect(count).toBeGreaterThan(0);

    // Check first label contains 'bash'
    const firstLabel = langLabels.first();
    await expect(firstLabel).toContainText('bash');
  });
});
