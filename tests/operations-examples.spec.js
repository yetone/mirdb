// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Operations Examples Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Set operation example shows "set mykey 0 3600 5" followed by "hello"', async ({ page }) => {
    // Navigate to the quick-start section containing operations examples
    const quickStartSection = page.locator('#quick-start');
    await quickStartSection.scrollIntoViewIfNeeded();
    await expect(quickStartSection).toBeVisible();

    // Find the operations code block
    const operationsCode = page.locator('#operations-code');
    await expect(operationsCode).toBeVisible();

    // Get the text content
    const codeText = await operationsCode.textContent();

    // Verify the set operation example is present with correct format
    expect(codeText).toContain('set mykey 0 3600 5');
    expect(codeText).toContain('hello');

    // Verify the set command comes before hello (correct order)
    const setIndex = codeText.indexOf('set mykey 0 3600 5');
    const helloIndex = codeText.indexOf('hello');
    expect(setIndex).toBeLessThan(helloIndex);
    expect(setIndex).toBeGreaterThanOrEqual(0);
  });

  test('TC2: Get operation example shows "get mykey" command', async ({ page }) => {
    // Navigate to the quick-start section
    const quickStartSection = page.locator('#quick-start');
    await quickStartSection.scrollIntoViewIfNeeded();
    await expect(quickStartSection).toBeVisible();

    // Find the operations code block
    const operationsCode = page.locator('#operations-code');
    await expect(operationsCode).toBeVisible();

    // Get the text content
    const codeText = await operationsCode.textContent();

    // Verify the get operation example is present
    expect(codeText).toContain('get mykey');

    // Verify there's a comment explaining it
    expect(codeText).toContain('# Get a value');
  });

  test('TC3: Delete operation example shows "delete mykey" command', async ({ page }) => {
    // Navigate to the quick-start section
    const quickStartSection = page.locator('#quick-start');
    await quickStartSection.scrollIntoViewIfNeeded();
    await expect(quickStartSection).toBeVisible();

    // Find the operations code block
    const operationsCode = page.locator('#operations-code');
    await expect(operationsCode).toBeVisible();

    // Get the text content
    const codeText = await operationsCode.textContent();

    // Verify the delete operation example is present
    expect(codeText).toContain('delete mykey');

    // Verify there's a comment explaining it
    expect(codeText).toContain('# Delete a value');
  });

  test('TC4: Code examples have proper syntax highlighting for readability', async ({ page }) => {
    // Navigate to the quick-start section
    const quickStartSection = page.locator('#quick-start');
    await quickStartSection.scrollIntoViewIfNeeded();
    await expect(quickStartSection).toBeVisible();

    // Find the operations code block container
    const codeBlock = page.locator('.code-block').filter({ has: page.locator('#operations-code') });
    await expect(codeBlock).toBeVisible();

    // Verify the code block has proper styling for syntax highlighting
    // Check that the code block has a dark background (typical for code highlighting)
    const backgroundColor = await codeBlock.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // The code block should have a dark background (rgb(30, 41, 59) is var(--color-code-bg))
    expect(backgroundColor).toBeTruthy();

    // Verify the code uses monospace font for readability
    const codeElement = page.locator('#operations-code');
    const fontFamily = await codeElement.evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });

    // Should contain monospace font
    expect(fontFamily.toLowerCase()).toMatch(/mono|consolas|courier/);

    // Verify the code has light text color for contrast against dark background
    const textColor = await codeElement.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });
    expect(textColor).toBeTruthy();

    // Check that code block has proper header with title
    const codeHeader = codeBlock.locator('.code-header');
    await expect(codeHeader).toBeVisible();

    const codeTitle = codeBlock.locator('.code-title');
    await expect(codeTitle).toHaveText('Basic Operations');
  });

  test('Operations code block has copy functionality', async ({ page }) => {
    // Navigate to the quick-start section
    const quickStartSection = page.locator('#quick-start');
    await quickStartSection.scrollIntoViewIfNeeded();
    await expect(quickStartSection).toBeVisible();

    // Find the operations code block
    const codeBlock = page.locator('.code-block').filter({ has: page.locator('#operations-code') });
    await expect(codeBlock).toBeVisible();

    // Find the copy button
    const copyButton = codeBlock.locator('.copy-btn');
    await expect(copyButton).toBeVisible();
    await expect(copyButton).toHaveText('Copy');

    // Verify the copy button has proper aria-label for accessibility
    await expect(copyButton).toHaveAttribute('aria-label', 'Copy operation examples');

    // Verify the copy button has the correct data-copy attribute
    await expect(copyButton).toHaveAttribute('data-copy', 'operations');
  });

  test('All three operations (set, get, delete) are present in correct order', async ({ page }) => {
    // Navigate to the quick-start section
    const quickStartSection = page.locator('#quick-start');
    await quickStartSection.scrollIntoViewIfNeeded();
    await expect(quickStartSection).toBeVisible();

    // Find the operations code block
    const operationsCode = page.locator('#operations-code');
    await expect(operationsCode).toBeVisible();

    // Get the text content
    const codeText = await operationsCode.textContent();

    // Verify all three operations are present
    expect(codeText).toContain('set mykey');
    expect(codeText).toContain('get mykey');
    expect(codeText).toContain('delete mykey');

    // Verify the operations are in the correct order (set -> get -> delete)
    const setIndex = codeText.indexOf('set mykey');
    const getIndex = codeText.indexOf('get mykey');
    const deleteIndex = codeText.indexOf('delete mykey');

    expect(setIndex).toBeLessThan(getIndex);
    expect(getIndex).toBeLessThan(deleteIndex);
  });

  test('Operations examples section is accessible via navigation', async ({ page }) => {
    // Find the navigation link to Quick Start section
    const navLink = page.locator('.nav-links a[href="#quick-start"]');
    await expect(navLink).toBeVisible();
    await expect(navLink).toHaveText('Quick Start');

    // Click the navigation link
    await navLink.click();

    // Verify we navigated to the quick-start section
    await expect(page).toHaveURL(/#quick-start$/);

    // Verify the operations code is visible
    const operationsCode = page.locator('#operations-code');
    await expect(operationsCode).toBeVisible();
  });
});
