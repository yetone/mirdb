// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Code Examples Display E2E Tests
 *
 * Scenario: Verify code examples for common operations are properly displayed
 * with syntax highlighting
 *
 * Steps:
 * 1. Locate code example sections throughout the page
 * 2. Verify code formatting - monospace font, syntax highlighting if JS enabled
 */

test.describe('Code Examples Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Check code blocks exist
   * Input: Check code blocks exist
   * Expected: Multiple code blocks are present on the page
   */
  test('TC1: Multiple code blocks are present on the page', async ({ page }) => {
    // Locate all code blocks on the page
    const codeBlocks = page.locator('.code-block');

    // Verify multiple code blocks exist
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThanOrEqual(3);

    // Verify each code block contains pre and code elements
    for (let i = 0; i < count; i++) {
      const codeBlock = codeBlocks.nth(i);
      await expect(codeBlock).toBeVisible();

      const preElement = codeBlock.locator('pre');
      const codeElement = codeBlock.locator('code');

      await expect(preElement).toBeVisible();
      await expect(codeElement).toBeVisible();
    }

    // Verify code blocks are in the Quick Start section
    const quickStartSection = page.locator('#getting-started');
    const quickStartCodeBlocks = quickStartSection.locator('.code-block');
    const quickStartCount = await quickStartCodeBlocks.count();
    expect(quickStartCount).toBeGreaterThanOrEqual(3);
  });

  /**
   * Test Case 2: Verify code block styling
   * Input: Verify code block styling
   * Expected: Code blocks use monospace font and have distinct background
   */
  test('TC2: Code blocks use monospace font and have distinct background', async ({ page }) => {
    // Scroll to Quick Start section to ensure code blocks are visible
    await page.locator('#getting-started').scrollIntoViewIfNeeded();

    // Get the first code block
    const codeBlock = page.locator('.code-block').first();
    await expect(codeBlock).toBeVisible();

    // Verify code block has distinct background color
    const backgroundColor = await codeBlock.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    // Background should be a dark color (not white/transparent)
    expect(backgroundColor).toMatch(/rgb\(\d+, \d+, \d+\)/);
    // Verify it's a dark background (low RGB values)
    const rgbMatch = backgroundColor.match(/rgb\((\d+), (\d+), (\d+)\)/);
    if (rgbMatch) {
      const [, r, g, b] = rgbMatch.map(Number);
      // Dark theme means lower RGB values
      expect(r).toBeLessThan(100);
      expect(g).toBeLessThan(100);
      expect(b).toBeLessThan(100);
    }

    // Verify pre element uses monospace font
    const preElement = codeBlock.locator('pre');
    await expect(preElement).toBeVisible();

    const fontFamily = await preElement.evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });
    // Should contain a monospace font
    expect(fontFamily.toLowerCase()).toMatch(/monaco|menlo|monospace|consolas|courier/);

    // Verify code block has border-radius for rounded corners
    const borderRadius = await codeBlock.evaluate((el) => {
      return window.getComputedStyle(el).borderRadius;
    });
    expect(borderRadius).not.toBe('0px');

    // Verify code block header has different styling
    const codeBlockHeader = codeBlock.locator('.code-block-header');
    await expect(codeBlockHeader).toBeVisible();

    const headerBg = await codeBlockHeader.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    expect(headerBg).toMatch(/rgb\(\d+, \d+, \d+\)/);
  });

  /**
   * Test Case 3: Check for SET command example
   * Input: Check for SET command example
   * Expected: Example showing SET command usage is present
   */
  test('TC3: Example showing SET command usage is present', async ({ page }) => {
    // Scroll to Quick Start section
    await page.locator('#getting-started').scrollIntoViewIfNeeded();

    // Find the usage code block
    const usageCodeBlock = page.locator('[data-testid="usage-code-block"]');
    await expect(usageCodeBlock).toBeVisible();

    // Verify SET command is present in the code example
    const codeContent = usageCodeBlock.locator('code');
    await expect(codeContent).toContainText('set');

    // Verify the SET command example has proper format (command with key and value)
    const codeText = await codeContent.textContent();
    expect(codeText).toMatch(/set\s+\w+/i);

    // Verify STORED response is shown (indicating successful SET)
    await expect(codeContent).toContainText('STORED');

    // Also check that the commands section mentions SET
    const commandsSection = page.locator('#commands');
    const setCommand = commandsSection.locator('[data-command="set"]');
    await expect(setCommand).toBeVisible();
    await expect(setCommand.locator('.command-name')).toContainText('SET');
  });

  /**
   * Test Case 4: Check for GET command example
   * Input: Check for GET command example
   * Expected: Example showing GET command usage is present
   */
  test('TC4: Example showing GET command usage is present', async ({ page }) => {
    // Scroll to Quick Start section
    await page.locator('#getting-started').scrollIntoViewIfNeeded();

    // Find the usage code block
    const usageCodeBlock = page.locator('[data-testid="usage-code-block"]');
    await expect(usageCodeBlock).toBeVisible();

    // Verify GET command is present in the code example
    const codeContent = usageCodeBlock.locator('code');
    await expect(codeContent).toContainText('get');

    // Verify the GET command example has proper format
    const codeText = await codeContent.textContent();
    expect(codeText).toMatch(/get\s+\w+/i);

    // Verify VALUE response is shown (indicating successful GET)
    await expect(codeContent).toContainText('VALUE');

    // Verify END response is shown (end of GET response)
    await expect(codeContent).toContainText('END');

    // Also check that the commands section mentions GET
    const commandsSection = page.locator('#commands');
    const getCommand = commandsSection.locator('[data-command="get"]');
    await expect(getCommand).toBeVisible();
    await expect(getCommand.locator('.command-name')).toContainText('GET');
  });

  /**
   * Additional test: Verify syntax highlighting classes are applied
   */
  test('Code blocks have syntax highlighting classes', async ({ page }) => {
    // Scroll to Quick Start section
    await page.locator('#getting-started').scrollIntoViewIfNeeded();

    // Find the configuration code block which has syntax highlighting
    const configCodeBlock = page.locator('[data-testid="configuration-code-block"]');
    await expect(configCodeBlock).toBeVisible();

    // Verify syntax highlighting classes exist
    const codeContent = configCodeBlock.locator('code');

    // Check for comment highlighting
    const commentHighlight = codeContent.locator('.code-comment');
    const commentCount = await commentHighlight.count();
    expect(commentCount).toBeGreaterThan(0);

    // Check for string highlighting
    const stringHighlight = codeContent.locator('.code-string');
    const stringCount = await stringHighlight.count();
    expect(stringCount).toBeGreaterThan(0);

    // Check for number highlighting
    const numberHighlight = codeContent.locator('.code-number');
    const numberCount = await numberHighlight.count();
    expect(numberCount).toBeGreaterThan(0);

    // Verify the highlighting classes have distinct colors
    const commentColor = await commentHighlight.first().evaluate((el) => {
      return window.getComputedStyle(el).color;
    });
    const stringColor = await stringHighlight.first().evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // Colors should be different for different token types
    expect(commentColor).not.toBe(stringColor);
  });

  /**
   * Additional test: Verify usage example shows response highlighting
   */
  test('Usage example shows response highlighting', async ({ page }) => {
    // Scroll to Quick Start section
    await page.locator('#getting-started').scrollIntoViewIfNeeded();

    // Find the usage code block
    const usageCodeBlock = page.locator('[data-testid="usage-code-block"]');
    await expect(usageCodeBlock).toBeVisible();

    const codeContent = usageCodeBlock.locator('code');

    // Check for response highlighting (STORED, VALUE, END)
    const responseHighlight = codeContent.locator('.code-response');
    const responseCount = await responseHighlight.count();
    expect(responseCount).toBeGreaterThan(0);

    // Verify response text is highlighted
    const responseText = await responseHighlight.first().textContent();
    expect(responseText).toMatch(/STORED|VALUE|END/);
  });

  /**
   * Additional test: Verify code blocks have proper structure
   */
  test('Code blocks have proper structure with headers and titles', async ({ page }) => {
    // Scroll to Quick Start section
    await page.locator('#getting-started').scrollIntoViewIfNeeded();

    const codeBlocks = page.locator('#getting-started .code-block');
    const count = await codeBlocks.count();

    for (let i = 0; i < count; i++) {
      const codeBlock = codeBlocks.nth(i);

      // Verify header exists
      const header = codeBlock.locator('.code-block-header');
      await expect(header).toBeVisible();

      // Verify title exists
      const title = codeBlock.locator('.code-block-title');
      await expect(title).toBeVisible();
      const titleText = await title.textContent();
      expect(titleText.length).toBeGreaterThan(0);

      // Verify copy button exists
      const copyBtn = codeBlock.locator('.copy-btn');
      await expect(copyBtn).toBeVisible();
    }
  });
});
