import { test, expect } from '@playwright/test';

/**
 * E2E Tests for MirDB Homepage - Getting Started Section
 * Tests verify the getting started section provides clear installation
 * and basic usage instructions enabling users to run MirDB within 5 minutes.
 */

test.describe('Getting Started Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Section with 'getting started' heading or id exists
   * Input: Query DOM for getting started section
   * Expected: Section with 'getting started' heading or id exists
   */
  test('TC1: Getting started section exists with proper heading', async ({ page }) => {
    // Check for section with id="getting-started"
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Check for h2 heading with text "Getting Started"
    const heading = gettingStartedSection.locator('h2#getting-started-heading');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Getting Started');
  });

  /**
   * Test Case 2: At least 2 code blocks present (install + usage)
   * Input: Count code block elements in getting started section
   * Expected: At least 2 code blocks present (install + usage)
   */
  test('TC2: Getting started section contains at least 2 code blocks', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');

    // Count code blocks (pre elements with code inside) in the getting started section
    const codeBlocks = gettingStartedSection.locator('.code-block-wrapper pre code');
    const count = await codeBlocks.count();

    // Should have at least 2 code blocks (installation + usage)
    expect(count).toBeGreaterThanOrEqual(2);
  });

  /**
   * Test Case 3: Code block contains cargo, git clone, or similar install command
   * Input: Search for installation command content
   * Expected: Code block contains cargo, git clone, or similar install command
   */
  test('TC3: Installation command is present', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');

    // Get all code block text content
    const codeBlocks = gettingStartedSection.locator('.code-block-wrapper pre code');
    const codeContent = await codeBlocks.allTextContents();
    const allCode = codeContent.join(' ');

    // Check for installation commands (git clone or cargo)
    const hasGitClone = allCode.includes('git clone');
    const hasCargoCommand = allCode.includes('cargo');

    expect(hasGitClone || hasCargoCommand).toBe(true);
  });

  /**
   * Test Case 4: Example showing how to connect to MirDB (telnet, client library, etc.)
   * Input: Search for connection example
   * Expected: Example showing how to connect to MirDB (telnet, client library, etc.)
   */
  test('TC4: Connection example is present', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');

    // Get all code block text content
    const codeBlocks = gettingStartedSection.locator('.code-block-wrapper pre code');
    const codeContent = await codeBlocks.allTextContents();
    const allCode = codeContent.join(' ');

    // Check for connection examples (telnet, nc, or client connection)
    const hasTelnet = allCode.includes('telnet');
    const hasNc = allCode.includes('nc ');
    const hasLocalhost = allCode.includes('localhost') || allCode.includes('127.0.0.1');

    expect(hasTelnet || hasNc || hasLocalhost).toBe(true);
  });

  /**
   * Test Case 5: Examples demonstrating basic SET and GET commands
   * Input: Search for SET/GET operation examples
   * Expected: Examples demonstrating basic SET and GET commands
   */
  test('TC5: SET and GET operation examples are present', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');

    // Get all code block text content
    const codeBlocks = gettingStartedSection.locator('.code-block-wrapper pre code');
    const codeContent = await codeBlocks.allTextContents();
    const allCode = codeContent.join(' ').toLowerCase();

    // Check for SET and GET commands
    const hasSet = allCode.includes('set ');
    const hasGet = allCode.includes('get ');

    expect(hasSet).toBe(true);
    expect(hasGet).toBe(true);
  });

  /**
   * Test Case 6: Code elements have highlighting classes (Prism.js or similar)
   * Input: Verify code blocks have syntax highlighting
   * Expected: Code elements have highlighting classes (Prism.js or similar)
   */
  test('TC6: Code blocks have syntax highlighting', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');

    // Check that code elements have language class (Prism.js convention)
    const codeWithLanguageClass = gettingStartedSection.locator('code[class*="language-"]');
    const count = await codeWithLanguageClass.count();

    // Should have at least one code block with language class
    expect(count).toBeGreaterThanOrEqual(1);

    // Verify the first code block has the correct language class for bash/shell
    const firstCodeBlock = codeWithLanguageClass.first();
    const className = await firstCodeBlock.getAttribute('class');
    expect(className).toContain('language-');
  });

  /**
   * Test Case 7: Clicking copy button copies code content to clipboard
   * Input: Test copy button functionality on code block
   * Expected: Clicking copy button copies code content to clipboard
   */
  test('TC7: Copy button functionality works', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const gettingStartedSection = page.locator('#getting-started');

    // Find the first copy button
    const copyButton = gettingStartedSection.locator('.copy-btn').first();
    await expect(copyButton).toBeVisible();

    // Get the code content before clicking
    const codeBlock = gettingStartedSection.locator('.code-block-wrapper pre code').first();
    const expectedContent = await codeBlock.textContent();

    // Click the copy button
    await copyButton.click();

    // Wait for the button to show "Copied!" state
    const copyText = copyButton.locator('.copy-text');
    await expect(copyText).toHaveText('Copied!');

    // Verify the button has the 'copied' class
    await expect(copyButton).toHaveClass(/copied/);

    // Read from clipboard and verify content
    const clipboardContent = await page.evaluate(async () => {
      return await navigator.clipboard.readText();
    });

    expect(clipboardContent).toBe(expectedContent);
  });
});
