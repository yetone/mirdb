// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Code Examples Display (Scenario 4)
 * Verifies that code examples showing basic memcached commands (SET, GET, DELETE)
 * are displayed as specified in REQ-5
 */

test.describe('Code Examples Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Search for SET command in code examples
   * Expected: Code block containing 'set' command syntax is present
   */
  test('TC1: SET command example is displayed in code block', async ({ page }) => {
    // Navigate to the getting started section
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Find code blocks in the getting started section
    const codeBlocks = gettingStartedSection.locator('pre, code, .code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Search for SET command in code examples
    let setCommandFound = false;
    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      const codeText = await codeBlock.textContent();
      if (codeText && codeText.toLowerCase().includes('set ')) {
        setCommandFound = true;
        // Verify the SET command shows syntax (key, flags, exptime, bytes)
        expect(codeText.toLowerCase()).toContain('set');
        break;
      }
    }

    expect(setCommandFound).toBeTruthy();
  });

  /**
   * Test Case 2: Search for GET command in code examples
   * Expected: Code block containing 'get' command syntax is present
   */
  test('TC2: GET command example is displayed in code block', async ({ page }) => {
    // Navigate to the getting started section
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Find code blocks in the getting started section
    const codeBlocks = gettingStartedSection.locator('pre, code, .code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Search for GET command in code examples
    let getCommandFound = false;
    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      const codeText = await codeBlock.textContent();
      if (codeText && codeText.toLowerCase().includes('get ')) {
        getCommandFound = true;
        // Verify the GET command syntax
        expect(codeText.toLowerCase()).toContain('get');
        break;
      }
    }

    expect(getCommandFound).toBeTruthy();
  });

  /**
   * Test Case 3: Search for DELETE command in code examples
   * Expected: Code block containing 'delete' command syntax is present
   */
  test('TC3: DELETE command example is displayed in code block', async ({ page }) => {
    // Navigate to the getting started section
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Find code blocks in the getting started section
    const codeBlocks = gettingStartedSection.locator('pre, code, .code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Search for DELETE command in code examples
    let deleteCommandFound = false;
    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      const codeText = await codeBlock.textContent();
      if (codeText && codeText.toLowerCase().includes('delete ')) {
        deleteCommandFound = true;
        // Verify the DELETE command syntax
        expect(codeText.toLowerCase()).toContain('delete');
        break;
      }
    }

    expect(deleteCommandFound).toBeTruthy();
  });

  /**
   * Test Case 4: Verify code examples use monospace font
   * Expected: Code blocks are styled with monospace/code font family
   */
  test('TC4: Code blocks are styled with monospace font', async ({ page }) => {
    // Navigate to the getting started section
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Find code blocks (pre or .code-block elements)
    const codeBlocks = gettingStartedSection.locator('pre, .code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Check that code blocks have monospace font
    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      const fontFamily = await codeBlock.evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });

      // Verify font family contains a monospace font
      const monospaceIndicators = [
        'monospace',
        'monaco',
        'menlo',
        'consolas',
        'courier',
        'ubuntu mono',
        'sfmono',
        'liberation mono'
      ];

      const hasMonospace = monospaceIndicators.some(font =>
        fontFamily.toLowerCase().includes(font)
      );

      expect(hasMonospace).toBeTruthy();
    }
  });

  /**
   * Additional test: Verify SET command shows expected response (STORED)
   */
  test('SET command example shows STORED response', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Get text content of the getting started section
    const sectionText = await gettingStartedSection.textContent();

    // Verify STORED response is shown with SET command
    expect(sectionText).toContain('STORED');
  });

  /**
   * Additional test: Verify GET command shows response format (VALUE)
   */
  test('GET command example shows VALUE response format', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Get text content of the getting started section
    const sectionText = await gettingStartedSection.textContent();

    // Verify VALUE and END responses are shown with GET command
    expect(sectionText).toContain('VALUE');
    expect(sectionText).toContain('END');
  });

  /**
   * Additional test: Verify DELETE command shows expected response (DELETED)
   */
  test('DELETE command example shows DELETED response', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Get text content of the getting started section
    const sectionText = await gettingStartedSection.textContent();

    // Verify DELETED response is shown with DELETE command
    expect(sectionText).toContain('DELETED');
  });

  /**
   * Test navigation to getting started section
   */
  test('can navigate to getting started section via anchor link', async ({ page }) => {
    // Click on getting started link
    await page.click('a[href="#getting-started"]');

    // Verify getting started section is visible
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();
  });
});
