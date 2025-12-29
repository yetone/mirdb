// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Quick Start Section E2E Tests
 *
 * These tests verify the getting started section displays installation commands
 * and basic usage examples that users can copy.
 */

test.describe('Quick Start Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Locate quick start section on page
   * Expected: Quick Start or Getting Started section exists with clear heading
   */
  test('TC1: should have a Quick Start section with clear heading', async ({ page }) => {
    // Navigate to the quick start section
    const quickStartSection = page.locator('#getting-started');

    // Verify the section exists
    await expect(quickStartSection).toBeVisible();

    // Verify there's a clear heading
    const heading = quickStartSection.locator('.section-title');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText(/Quick Start|Getting Started/i);
  });

  /**
   * Test Case 2: Verify installation command presence
   * Expected: Cargo installation command is displayed in a code block
   */
  test('TC2: should display cargo installation command in a code block', async ({ page }) => {
    const quickStartSection = page.locator('#getting-started');

    // Find the installation code block
    const installationCodeBlock = quickStartSection.locator('[data-testid="installation-code-block"]');
    await expect(installationCodeBlock).toBeVisible();

    // Verify the cargo install command is present
    const codeContent = installationCodeBlock.locator('code');
    await expect(codeContent).toContainText('cargo install');

    // Verify it's in a pre/code block structure
    const preElement = installationCodeBlock.locator('pre');
    await expect(preElement).toBeVisible();
  });

  /**
   * Test Case 3: Verify configuration example
   * Expected: Basic configuration example is provided showing how to configure MirDB
   */
  test('TC3: should display basic configuration example', async ({ page }) => {
    const quickStartSection = page.locator('#getting-started');

    // Find the configuration code block
    const configCodeBlock = quickStartSection.locator('[data-testid="configuration-code-block"]');
    await expect(configCodeBlock).toBeVisible();

    // Verify configuration content is present
    const codeContent = configCodeBlock.locator('code');

    // Check for essential configuration options
    await expect(codeContent).toContainText('addr');
    await expect(codeContent).toContainText('work_dir');
    await expect(codeContent).toContainText('12333');

    // Verify it shows TOML format (mirdb.toml)
    const codeBlockTitle = configCodeBlock.locator('.code-block-title');
    await expect(codeBlockTitle).toContainText('toml');
  });

  /**
   * Test Case 4: Verify usage example with client
   * Expected: Example showing telnet or client usage with SET/GET commands is displayed
   */
  test('TC4: should display usage example with SET/GET commands', async ({ page }) => {
    const quickStartSection = page.locator('#getting-started');

    // Find the usage code block
    const usageCodeBlock = quickStartSection.locator('[data-testid="usage-code-block"]');
    await expect(usageCodeBlock).toBeVisible();

    // Verify telnet command is present
    const codeContent = usageCodeBlock.locator('code');
    await expect(codeContent).toContainText('telnet');

    // Verify SET command example is present
    await expect(codeContent).toContainText('set');

    // Verify GET command example is present
    await expect(codeContent).toContainText('get');

    // Verify response examples are shown
    await expect(codeContent).toContainText('STORED');
    await expect(codeContent).toContainText('VALUE');
  });

  /**
   * Test Case 5: Check code blocks are copyable
   * Expected: Code blocks have copy functionality or are selectable for copying
   */
  test('TC5: should have copyable code blocks with copy buttons', async ({ page }) => {
    const quickStartSection = page.locator('#getting-started');

    // Find all code blocks
    const codeBlocks = quickStartSection.locator('.code-block');

    // Verify there are multiple code blocks (at least 3: installation, config, usage)
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThanOrEqual(3);

    // Check each code block has a copy button
    for (let i = 0; i < count; i++) {
      const copyButton = codeBlocks.nth(i).locator('.copy-btn');
      await expect(copyButton).toBeVisible();
      await expect(copyButton).toHaveAttribute('data-copy');
    }

    // Test that clicking copy button changes text to "Copied!"
    const firstCopyButton = codeBlocks.first().locator('.copy-btn');

    // Grant clipboard permissions and click the copy button
    await page.context().grantPermissions(['clipboard-read', 'clipboard-write']);
    await firstCopyButton.click();

    // Verify button text changes to "Copied!"
    await expect(firstCopyButton).toContainText('Copied!');

    // After 2 seconds it should revert (we wait a bit to verify it changes back)
    await page.waitForTimeout(2500);
    await expect(firstCopyButton).toContainText('Copy');
  });

  /**
   * Additional test: Verify navigation to Quick Start section
   */
  test('should be navigable via Get Started button in hero', async ({ page }) => {
    // Click the Get Started button in hero
    const getStartedBtn = page.locator('.hero .btn-primary');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toHaveAttribute('href', '#getting-started');
  });

  /**
   * Additional test: Verify code blocks have proper structure
   */
  test('should have properly structured code blocks with headers', async ({ page }) => {
    const quickStartSection = page.locator('#getting-started');
    const codeBlocks = quickStartSection.locator('.code-block');

    const count = await codeBlocks.count();

    for (let i = 0; i < count; i++) {
      const codeBlock = codeBlocks.nth(i);

      // Verify each code block has a header
      const header = codeBlock.locator('.code-block-header');
      await expect(header).toBeVisible();

      // Verify each code block has a title
      const title = codeBlock.locator('.code-block-title');
      await expect(title).toBeVisible();

      // Verify each code block has pre and code elements
      const pre = codeBlock.locator('pre');
      const code = codeBlock.locator('code');
      await expect(pre).toBeVisible();
      await expect(code).toBeVisible();
    }
  });

  /**
   * Additional test: Verify server start command is present
   */
  test('should display server start command', async ({ page }) => {
    const quickStartSection = page.locator('#getting-started');

    // Find the start server code block
    const startServerCodeBlock = quickStartSection.locator('[data-testid="start-server-code-block"]');
    await expect(startServerCodeBlock).toBeVisible();

    // Verify the command is present
    const codeContent = startServerCodeBlock.locator('code');
    await expect(codeContent).toContainText('mirdb-server');
    await expect(codeContent).toContainText('toml');
  });
});
