/**
 * Usage and Code Examples Tests
 * Owner: Scenario 5 - Usage and Code Examples
 *
 * Tests:
 * - Usage section presence
 * - Code block presence
 * - Syntax highlighting applied
 * - SET/GET command examples
 * - Additional command examples (ADD, REPLACE, APPEND, PREPEND, DELETE)
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
    // Test Case 1: Check usage section element
    const usageSection = page.locator('#usage');
    await expect(usageSection).toBeVisible();
    await expect(usageSection).toHaveClass(/usage/);
  });

  test('TC2: Code blocks exist with syntax highlighting class', async ({ page }) => {
    // Test Case 2: Check for code block elements with syntax highlighting
    const usageSection = page.locator('#usage');

    // Check for pre elements containing code blocks
    const codeBlocks = usageSection.locator('pre.usage__code-block');
    await expect(codeBlocks).toHaveCount(7); // 7 command examples

    // Check that code elements have language class for syntax highlighting
    const codeElements = usageSection.locator('code[class*="language-"]');
    const count = await codeElements.count();
    expect(count).toBeGreaterThanOrEqual(1);

    // Verify at least one code element has language-bash class
    const bashCode = usageSection.locator('code.language-bash').first();
    await expect(bashCode).toBeVisible();
  });

  test('TC3: SET command example is present', async ({ page }) => {
    // Test Case 3: Search for 'set' command example
    const usageSection = page.locator('#usage');

    // Check that SET command is shown in a code example
    const setExample = usageSection.locator('.usage__example').filter({ hasText: 'SET' }).first();
    await expect(setExample).toBeVisible();

    // Verify the code block contains the set command
    const setCodeBlock = setExample.locator('code');
    const codeText = await setCodeBlock.textContent();
    expect(codeText.toLowerCase()).toContain('set');
    expect(codeText).toContain('STORED');
  });

  test('TC4: GET command example is present', async ({ page }) => {
    // Test Case 4: Search for 'get' command example
    const usageSection = page.locator('#usage');

    // Check that GET command is shown in a code example
    const getExample = usageSection.locator('.usage__example').filter({ hasText: /GET.*Retrieve/i }).first();
    await expect(getExample).toBeVisible();

    // Verify the code block contains the get command
    const getCodeBlock = getExample.locator('code');
    const codeText = await getCodeBlock.textContent();
    expect(codeText.toLowerCase()).toContain('get');
    expect(codeText).toContain('VALUE');
    expect(codeText).toContain('END');
  });

  test('TC5: Copy button exists on code blocks', async ({ page }) => {
    // Test Case 5: Check for copy button on code blocks
    const usageSection = page.locator('#usage');

    // Check that each code wrapper has a copy button
    const copyButtons = usageSection.locator('.usage__copy-btn');
    const buttonCount = await copyButtons.count();
    expect(buttonCount).toBeGreaterThanOrEqual(1);

    // Verify the first copy button is visible and has correct attributes
    const firstCopyBtn = copyButtons.first();
    await expect(firstCopyBtn).toBeVisible();
    await expect(firstCopyBtn).toHaveAttribute('aria-label', 'Copy code to clipboard');

    // Check button contains "Copy" text
    const buttonText = await firstCopyBtn.textContent();
    expect(buttonText).toContain('Copy');
  });

  test('TC6: Syntax highlighting classes are applied', async ({ page }) => {
    // Test Case 6: Verify syntax highlighting applied
    const usageSection = page.locator('#usage');

    // Check for code elements with language classes
    const codeWithLanguage = usageSection.locator('code[class*="language-"]');
    const count = await codeWithLanguage.count();
    expect(count).toBeGreaterThanOrEqual(1);

    // Verify that code blocks have the language-bash class (our syntax highlighting)
    const bashCode = usageSection.locator('code.language-bash');
    const bashCount = await bashCode.count();
    expect(bashCount).toBeGreaterThanOrEqual(1);

    // Wait for Prism to highlight and check for highlighted tokens
    await page.waitForTimeout(500); // Allow time for JS to execute

    // Check that syntax highlighting has been applied (data-highlighted attribute)
    const highlightedCode = usageSection.locator('code[data-highlighted="true"]');
    const highlightedCount = await highlightedCode.count();
    expect(highlightedCount).toBeGreaterThanOrEqual(1);
  });

  test('Additional commands are documented: ADD, REPLACE, APPEND, PREPEND, DELETE', async ({ page }) => {
    // Additional test: Verify all command examples are present
    const usageSection = page.locator('#usage');

    // Check ADD command
    const addExample = usageSection.locator('.usage__example').filter({ hasText: /ADD.*Add Only/i });
    await expect(addExample).toBeVisible();

    // Check REPLACE command
    const replaceExample = usageSection.locator('.usage__example').filter({ hasText: /REPLACE.*Replace/i });
    await expect(replaceExample).toBeVisible();

    // Check APPEND command
    const appendExample = usageSection.locator('.usage__example').filter({ hasText: /APPEND.*Append/i });
    await expect(appendExample).toBeVisible();

    // Check PREPEND command
    const prependExample = usageSection.locator('.usage__example').filter({ hasText: /PREPEND.*Prepend/i });
    await expect(prependExample).toBeVisible();

    // Check DELETE command
    const deleteExample = usageSection.locator('.usage__example').filter({ hasText: /DELETE.*Remove/i });
    await expect(deleteExample).toBeVisible();
  });

  test('Copy button provides visual feedback on click', async ({ page }) => {
    // Test copy button functionality
    const usageSection = page.locator('#usage');
    const firstCopyBtn = usageSection.locator('.usage__copy-btn').first();

    // Grant clipboard permissions for the test
    await page.context().grantPermissions(['clipboard-read', 'clipboard-write']);

    // Click the copy button
    await firstCopyBtn.click();

    // Check that button text changes to show feedback
    await expect(firstCopyBtn).toContainText('Copied!');

    // Wait for feedback to reset
    await page.waitForTimeout(2500);
    await expect(firstCopyBtn).toContainText('Copy');
  });

  test('Usage section title and description are present', async ({ page }) => {
    const usageSection = page.locator('#usage');

    // Check title
    const title = usageSection.locator('.usage__title');
    await expect(title).toBeVisible();
    await expect(title).toContainText('Usage');

    // Check subtitle/description
    const subtitle = usageSection.locator('.usage__subtitle');
    await expect(subtitle).toBeVisible();
    await expect(subtitle).toContainText('memcached');
  });
});
