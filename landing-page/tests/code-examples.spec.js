// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

test.describe('Code Examples and Usage Section', () => {
  test.beforeEach(async ({ page }) => {
    const filePath = path.resolve(__dirname, '../index.html');
    await page.goto(`file://${filePath}`);
  });

  // Test Case 1: At least one code block element (pre/code tags) is present
  test('TC1: At least one code block element is present on the page', async ({ page }) => {
    // Look for code block elements - either pre or code tags
    const codeBlocks = page.locator('pre, code');
    const count = await codeBlocks.count();

    // Verify at least one code block exists
    expect(count).toBeGreaterThanOrEqual(1);

    // Verify at least one is visible
    const preElements = page.locator('pre');
    const preCount = await preElements.count();
    if (preCount > 0) {
      await expect(preElements.first()).toBeVisible();
    }
  });

  // Test Case 2: Code example includes SET command syntax
  test('TC2: Code example includes SET command syntax', async ({ page }) => {
    // Look for code blocks containing SET command
    const codeBlocks = page.locator('pre, code');
    const count = await codeBlocks.count();

    let foundSetCommand = false;
    for (let i = 0; i < count; i++) {
      const text = await codeBlocks.nth(i).textContent();
      if (text && text.toUpperCase().includes('SET')) {
        foundSetCommand = true;
        break;
      }
    }

    expect(foundSetCommand).toBeTruthy();
  });

  // Test Case 3: Code example includes GET command syntax
  test('TC3: Code example includes GET command syntax', async ({ page }) => {
    // Look for code blocks containing GET command
    const codeBlocks = page.locator('pre, code');
    const count = await codeBlocks.count();

    let foundGetCommand = false;
    for (let i = 0; i < count; i++) {
      const text = await codeBlocks.nth(i).textContent();
      if (text && text.toUpperCase().includes('GET')) {
        foundGetCommand = true;
        break;
      }
    }

    expect(foundGetCommand).toBeTruthy();
  });

  // Test Case 4: Code elements have syntax highlighting CSS applied
  test('TC4: Code blocks have syntax highlighting styles applied', async ({ page }) => {
    // Locate the code examples section
    const codeSection = page.locator('[data-testid="code-examples-section"]');
    await expect(codeSection).toBeVisible();

    // Check that code blocks have syntax highlighting classes or inline styles
    const codeBlocks = codeSection.locator('pre code, pre');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThan(0);

    // Verify code blocks have proper styling for syntax highlighting
    const firstCodeBlock = codeBlocks.first();
    const styles = await firstCodeBlock.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        fontFamily: computed.fontFamily,
        backgroundColor: computed.backgroundColor,
        hasHighlightClass: el.classList.contains('highlight') ||
                          el.classList.contains('code-block') ||
                          el.closest('.code-block') !== null ||
                          el.closest('.code-example') !== null
      };
    });

    // Verify monospace font is applied
    expect(styles.fontFamily.toLowerCase()).toMatch(/mono|courier|consolas/);

    // Verify the code block has some distinguishing background
    expect(styles.backgroundColor).not.toBe('rgba(0, 0, 0, 0)');
  });

  // Test Case 5: Page displays list of supported commands
  test('TC5: Page displays list of supported commands (SET, GET, DELETE, ADD, REPLACE, etc.)', async ({ page }) => {
    // Look for supported commands section
    const pageContent = await page.locator('body').textContent();

    // Check for required commands - at minimum SET and GET
    expect(pageContent.toUpperCase()).toContain('SET');
    expect(pageContent.toUpperCase()).toContain('GET');

    // Check for other supported commands as mentioned in PRD
    const hasDelete = pageContent.toUpperCase().includes('DELETE');
    const hasAdd = pageContent.toUpperCase().includes('ADD');
    const hasReplace = pageContent.toUpperCase().includes('REPLACE');

    // At least some of the additional commands should be present
    const additionalCommandsCount = [hasDelete, hasAdd, hasReplace].filter(Boolean).length;
    expect(additionalCommandsCount).toBeGreaterThanOrEqual(2);

    // Look for a commands list or supported commands section
    const commandsSection = page.locator('[data-testid="supported-commands"]');
    await expect(commandsSection).toBeVisible();
  });

  // Additional: Verify code blocks are properly formatted with monospace font
  test('Code blocks use monospace font styling', async ({ page }) => {
    const codeSection = page.locator('[data-testid="code-examples-section"]');
    await expect(codeSection).toBeVisible();

    const codeElements = codeSection.locator('pre, code');
    const count = await codeElements.count();
    expect(count).toBeGreaterThan(0);

    // Check the first code element for monospace font
    const firstCode = codeElements.first();
    const fontFamily = await firstCode.evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });

    // Font family should contain monospace indicators
    expect(fontFamily.toLowerCase()).toMatch(/mono|courier|consolas|menlo|'source code'|'fira code'/);
  });

  // Additional: Verify code section exists with proper test-id
  test('Code examples section exists with proper structure', async ({ page }) => {
    const codeSection = page.locator('[data-testid="code-examples-section"]');
    await expect(codeSection).toBeVisible();

    // Verify section has a title/heading
    const heading = codeSection.locator('h2, h3');
    await expect(heading.first()).toBeVisible();

    // Verify at least one code example is present
    const codeExamples = codeSection.locator('.code-example, pre');
    const count = await codeExamples.count();
    expect(count).toBeGreaterThan(0);
  });
});
