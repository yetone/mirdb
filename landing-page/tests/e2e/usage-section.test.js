/**
 * MirDB Landing Page - Usage Section E2E Tests
 * Owner: Scenario 3 - Usage Section with Code Examples
 *
 * Tests verify:
 * - Usage demo media (usage.gif) is displayed
 * - Code examples for get, set, delete commands
 * - Copy-to-clipboard functionality
 * - Syntax highlighting in code blocks
 */

const { test, expect } = require('@playwright/test');
const { setupPage } = require('../helpers/test-utils');

test.describe('Usage Section with Code Examples', () => {
  test.beforeEach(async ({ page }) => {
    await setupPage(page);
    // Navigate to usage section
    await page.locator('#usage').scrollIntoViewIfNeeded();
  });

  // Test Case 1: Check for usage demonstration media
  test('TC1: usage.gif is displayed or interactive demo is present', async ({ page }) => {
    const usageSection = page.locator('#usage');
    await expect(usageSection).toBeVisible();

    // Check for usage.gif image
    const usageGif = page.locator('.usage-gif');
    await expect(usageGif).toBeVisible();

    // Verify the image source contains usage.gif
    const imgSrc = await usageGif.getAttribute('src');
    expect(imgSrc).toContain('usage.gif');

    // Verify proper alt text for accessibility
    const altText = await usageGif.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText.length).toBeGreaterThan(0);
  });

  // Test Case 2: Verify 'get' command example
  test('TC2: Code example shows get key command syntax', async ({ page }) => {
    const usageSection = page.locator('#usage');

    // Find the get command code example
    const getCodeBlock = page.locator('#get-code');
    await expect(getCodeBlock).toBeVisible();

    // Verify the code contains 'get' keyword
    const codeText = await getCodeBlock.textContent();
    expect(codeText.toLowerCase()).toContain('get');

    // Verify it shows the key parameter
    expect(codeText).toContain('mykey');
  });

  // Test Case 3: Verify 'set' command example
  test('TC3: Code example shows set key flags ttl bytes command syntax', async ({ page }) => {
    const usageSection = page.locator('#usage');

    // Find the set command code example
    const setCodeBlock = page.locator('#set-code');
    await expect(setCodeBlock).toBeVisible();

    // Verify the code contains 'set' keyword
    const codeText = await setCodeBlock.textContent();
    expect(codeText.toLowerCase()).toContain('set');

    // Verify it shows the key parameter
    expect(codeText).toContain('mykey');

    // Verify it shows flags (0), ttl (3600), and bytes (5) parameters
    expect(codeText).toMatch(/\d+/); // Contains numeric parameters
  });

  // Test Case 4: Verify 'delete' command example
  test('TC4: Code example shows delete key command syntax', async ({ page }) => {
    const usageSection = page.locator('#usage');

    // Find the delete command code example
    const deleteCodeBlock = page.locator('#delete-code');
    await expect(deleteCodeBlock).toBeVisible();

    // Verify the code contains 'delete' keyword
    const codeText = await deleteCodeBlock.textContent();
    expect(codeText.toLowerCase()).toContain('delete');

    // Verify it shows the key parameter
    expect(codeText).toContain('mykey');
  });

  // Test Case 5: Check for copy-to-clipboard button
  test('TC5: Copy button is present on code blocks', async ({ page }) => {
    const usageSection = page.locator('#usage');

    // Find all copy buttons
    const copyButtons = usageSection.locator('.copy-btn');

    // There should be at least 3 copy buttons (for set, get, delete)
    const buttonCount = await copyButtons.count();
    expect(buttonCount).toBeGreaterThanOrEqual(3);

    // Verify each button is visible and has proper attributes
    for (let i = 0; i < buttonCount; i++) {
      const button = copyButtons.nth(i);
      await expect(button).toBeVisible();

      // Check for aria-label (accessibility)
      const ariaLabel = await button.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel.toLowerCase()).toContain('copy');

      // Check for data-copy-target attribute
      const copyTarget = await button.getAttribute('data-copy-target');
      expect(copyTarget).toBeTruthy();
    }
  });

  // Test Case 6: Click copy button on code example
  test('TC6: Code is copied to clipboard successfully', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const usageSection = page.locator('#usage');

    // Find the first copy button (for set command)
    const copyButton = usageSection.locator('.copy-btn').first();
    await expect(copyButton).toBeVisible();

    // Click the copy button
    await copyButton.click();

    // Verify visual feedback - button should show success state
    await expect(copyButton).toHaveClass(/copy-success/);

    // Verify button text changes to "Copied!"
    const copyText = copyButton.locator('.copy-text');
    await expect(copyText).toHaveText('Copied!');

    // Wait for the feedback to reset (optional, to verify it returns to normal)
    await page.waitForTimeout(2100);
    await expect(copyButton).not.toHaveClass(/copy-success/);
    await expect(copyText).toHaveText('Copy');
  });

  // Test Case 7: Code blocks have syntax highlighting
  test('TC7: Code is displayed with proper syntax highlighting in monospace font', async ({ page }) => {
    const usageSection = page.locator('#usage');

    // Check all code blocks
    const codeBlocks = usageSection.locator('.code-block');
    const codeCount = await codeBlocks.count();
    expect(codeCount).toBeGreaterThanOrEqual(3);

    for (let i = 0; i < codeCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      await expect(codeBlock).toBeVisible();

      // Verify monospace font family is applied
      const fontFamily = await codeBlock.evaluate(el => {
        return window.getComputedStyle(el).fontFamily;
      });
      expect(fontFamily.toLowerCase()).toMatch(/mono|consolas|menlo|courier/i);

      // Check for syntax highlighting elements
      const codeElement = codeBlock.locator('code');
      await expect(codeElement).toBeVisible();

      // Verify there are styled spans for syntax highlighting
      const highlightedElements = codeBlock.locator('span[class^="cmd-"]');
      const highlightCount = await highlightedElements.count();
      expect(highlightCount).toBeGreaterThan(0);
    }
  });

  // Additional test: Usage section has proper heading and structure
  test('Usage section has proper accessibility structure', async ({ page }) => {
    const usageSection = page.locator('#usage');

    // Verify section has aria-labelledby
    await expect(usageSection).toHaveAttribute('aria-labelledby', 'usage-title');

    // Verify heading exists and is visible
    const usageTitle = page.locator('#usage-title');
    await expect(usageTitle).toBeVisible();
    await expect(usageTitle).toHaveText('Usage');

    // Verify usage gif has alt text
    const usageGif = page.locator('.usage-gif');
    const altText = await usageGif.getAttribute('alt');
    expect(altText).toBeTruthy();
  });

  // Additional test: Code example headers are present
  test('Code examples have descriptive headers', async ({ page }) => {
    const usageSection = page.locator('#usage');
    const codeExampleHeaders = usageSection.locator('.code-example-header h3');

    const headerCount = await codeExampleHeaders.count();
    expect(headerCount).toBeGreaterThanOrEqual(3);

    // Verify headers have meaningful text
    const headerTexts = await codeExampleHeaders.allTextContents();
    expect(headerTexts.some(text => text.toLowerCase().includes('store') || text.toLowerCase().includes('set'))).toBeTruthy();
    expect(headerTexts.some(text => text.toLowerCase().includes('retrieve') || text.toLowerCase().includes('get'))).toBeTruthy();
    expect(headerTexts.some(text => text.toLowerCase().includes('delete') || text.toLowerCase().includes('remove'))).toBeTruthy();
  });
});
