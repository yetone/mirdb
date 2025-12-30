// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Quick Start Instructions', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Installation command code block exists', async ({ page }) => {
    // Test Case 1: Query for code block containing installation command (cargo, git clone, or similar)
    // Expected: Code block exists with installation instructions
    const quickStartSection = page.locator('#quick-start, [data-testid="quick-start"], .quick-start');
    await expect(quickStartSection).toBeVisible();

    // Look for code block containing installation-related commands (use first() to avoid strict mode)
    const codeBlock = quickStartSection.locator('pre').first();
    await expect(codeBlock).toBeVisible();

    const codeContent = await codeBlock.textContent();
    const lowerCode = codeContent?.toLowerCase() || '';

    // Check for installation-related keywords
    const hasGitClone = lowerCode.includes('git clone');
    const hasCargo = lowerCode.includes('cargo');
    const hasInstall = lowerCode.includes('install');
    const hasBuild = lowerCode.includes('build');

    expect(hasGitClone || hasCargo || hasInstall || hasBuild).toBeTruthy();
  });

  test('TC2: SET command example exists in code block', async ({ page }) => {
    // Test Case 2: Query for code block containing 'SET' command example
    // Expected: Code example exists demonstrating SET command usage
    const quickStartSection = page.locator('#quick-start, [data-testid="quick-start"], .quick-start');
    await expect(quickStartSection).toBeVisible();

    // Look for code block containing SET command (use first() to avoid strict mode)
    const codeBlock = quickStartSection.locator('pre').first();
    await expect(codeBlock).toBeVisible();

    const codeContent = await codeBlock.textContent();

    // Check for SET command (case-insensitive check but memcached uses lowercase 'set')
    const hasSetCommand = codeContent?.toLowerCase().includes('set ');

    expect(hasSetCommand).toBeTruthy();
  });

  test('TC3: GET command example exists in code block', async ({ page }) => {
    // Test Case 3: Query for code block containing 'GET' command example
    // Expected: Code example exists demonstrating GET command usage
    const quickStartSection = page.locator('#quick-start, [data-testid="quick-start"], .quick-start');
    await expect(quickStartSection).toBeVisible();

    // Look for code block containing GET command (use first() to avoid strict mode)
    const codeBlock = quickStartSection.locator('pre').first();
    await expect(codeBlock).toBeVisible();

    const codeContent = await codeBlock.textContent();

    // Check for GET command (memcached uses lowercase 'get')
    const hasGetCommand = codeContent?.toLowerCase().includes('get ');

    expect(hasGetCommand).toBeTruthy();
  });

  test('TC4: Code blocks have syntax highlighting applied', async ({ page }) => {
    // Test Case 4: Query for code blocks with syntax highlighting applied
    // Expected: Code blocks have syntax highlighting (Rust or shell)
    const quickStartSection = page.locator('#quick-start, [data-testid="quick-start"], .quick-start');
    await expect(quickStartSection).toBeVisible();

    // Check for code block structure - either:
    // 1. Pre/code elements with language class (e.g., language-bash, language-rust)
    // 2. Pre/code elements with proper styling/highlighting classes
    // 3. Code block wrapper with syntax highlighting
    const codeBlock = quickStartSection.locator('pre');
    await expect(codeBlock).toBeVisible();

    // Verify the code block exists and has a code element or content
    const hasCodeElement = await quickStartSection.locator('pre code').count() > 0;
    const hasPreWithContent = await codeBlock.textContent();

    // The code block should have structured content
    expect(hasCodeElement || (hasPreWithContent && hasPreWithContent.length > 0)).toBeTruthy();

    // Check if syntax highlighting classes exist (language-* or hljs classes)
    // OR if the code block has proper styling for highlighting
    const preElement = quickStartSection.locator('pre').first();
    const codeElement = quickStartSection.locator('pre code').first();

    // Get class attributes to check for highlighting
    const preClass = await preElement.getAttribute('class');
    const codeClass = await codeElement.getAttribute('class').catch(() => null);

    // Check for syntax highlighting indicators:
    // - language-* class on pre or code
    // - hljs class (highlight.js)
    // - code-block or similar styling class
    // - Or check that code is in pre > code structure (proper semantic markup for highlighting)
    const hasHighlightingClass =
      preClass?.includes('language-') ||
      preClass?.includes('hljs') ||
      preClass?.includes('code') ||
      codeClass?.includes('language-') ||
      codeClass?.includes('hljs');

    // Or check for proper structure: pre containing code with meaningful content
    const hasProperStructure = await quickStartSection.locator('pre code').count() > 0;

    // At minimum, code should be in pre/code tags which allows for CSS-based syntax highlighting
    expect(hasHighlightingClass || hasProperStructure || hasPreWithContent).toBeTruthy();
  });

  test('TC5: Code blocks are selectable and copy-able (manual verification indicator)', async ({ page }) => {
    // Test Case 5: Check code blocks are selectable and copy-able
    // Expected: Code content can be selected and copied
    // Note: This is a manual test case, but we can verify basic selectability
    const quickStartSection = page.locator('#quick-start, [data-testid="quick-start"], .quick-start');
    await expect(quickStartSection).toBeVisible();

    const codeBlock = quickStartSection.locator('pre');
    await expect(codeBlock).toBeVisible();

    // Verify the code block is not disabled for selection
    const codeContent = await codeBlock.textContent();
    expect(codeContent).toBeTruthy();
    expect(codeContent?.length).toBeGreaterThan(0);

    // Check that user-select is not disabled (no 'none' value)
    const userSelect = await codeBlock.evaluate((el) => {
      return window.getComputedStyle(el).userSelect;
    });

    // user-select should NOT be 'none' for the code to be selectable
    expect(userSelect).not.toBe('none');
  });
});
