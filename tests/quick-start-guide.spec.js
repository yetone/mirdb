// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: Quick Start Guide
 * Scenario: Verify the quick-start guide provides installation and basic usage
 * commands that enable users to run MirDB within 5 minutes
 */

test.describe('Quick Start Guide', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage before each test
    await page.goto('/');
  });

  /**
   * Test Case 1: Check quick-start for installation instructions
   * Input: Check quick-start for installation instructions
   * Expected: Installation command 'cargo install mirdb' or equivalent is displayed
   */
  test('TC1: Installation command cargo install mirdb is displayed', async ({ page }) => {
    // Navigate to quick-start section
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Check for the installation section
    const installSection = page.locator('#install-section');
    await expect(installSection).toBeVisible();

    // Verify the cargo install command is present
    const installCommand = page.locator('#install-command');
    await expect(installCommand).toBeVisible();

    const installText = await installCommand.textContent();
    expect(installText).toContain('cargo install mirdb');
  });

  /**
   * Test Case 2: Check quick-start for build from source instructions
   * Input: Check quick-start for build from source instructions
   * Expected: Git clone and cargo build commands are displayed
   */
  test('TC2: Build from source instructions are displayed', async ({ page }) => {
    // Navigate to quick-start section
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Check for the build from source section
    const buildSection = page.locator('#build-from-source-section');
    await expect(buildSection).toBeVisible();

    // Verify git clone command is present
    const buildCommands = page.locator('#build-commands');
    await expect(buildCommands).toBeVisible();

    const buildText = await buildCommands.textContent();
    expect(buildText).toContain('git clone');
    expect(buildText).toContain('github.com');
    expect(buildText).toContain('mirdb');
    expect(buildText).toContain('cargo build --release');
  });

  /**
   * Test Case 3: Check quick-start for run command
   * Input: Check quick-start for run command
   * Expected: Command to run MirDB binary is displayed
   */
  test('TC3: Run command for MirDB binary is displayed', async ({ page }) => {
    // Navigate to quick-start section
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Check for the run section
    const runSection = page.locator('#run-section');
    await expect(runSection).toBeVisible();

    // Verify run command is present
    const runCommand = page.locator('#run-command');
    await expect(runCommand).toBeVisible();

    const runText = await runCommand.textContent();
    expect(runText).toContain('./target/release/mirdb');
  });

  /**
   * Test Case 4: Check quick-start for client connection example
   * Input: Check quick-start for client connection example
   * Expected: Example showing how to connect with memcached client (telnet localhost 12333)
   */
  test('TC4: Client connection example with telnet localhost 12333 is displayed', async ({ page }) => {
    // Navigate to quick-start section
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Check for the connect section
    const connectSection = page.locator('#connect-section');
    await expect(connectSection).toBeVisible();

    // Verify connection example is present
    const connectExample = page.locator('#connect-example');
    await expect(connectExample).toBeVisible();

    const connectText = await connectExample.textContent();
    expect(connectText).toContain('telnet');
    expect(connectText).toContain('localhost');
    expect(connectText).toContain('12333');
  });

  /**
   * Test Case 5: Check quick-start for SET/GET example
   * Input: Check quick-start for SET/GET example
   * Expected: Example showing SET and GET commands with expected output
   */
  test('TC5: SET and GET commands with expected output are displayed', async ({ page }) => {
    // Navigate to quick-start section
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Check for the usage section
    const usageSection = page.locator('#usage-section');
    await expect(usageSection).toBeVisible();

    // Verify SET/GET example is present
    const usageExample = page.locator('#usage-example');
    await expect(usageExample).toBeVisible();

    const usageText = await usageExample.textContent();

    // Verify SET command and response
    expect(usageText.toLowerCase()).toContain('set');
    expect(usageText).toContain('STORED');

    // Verify GET command and response
    expect(usageText.toLowerCase()).toContain('get');
    expect(usageText).toContain('VALUE');
    expect(usageText).toContain('END');
  });

  /**
   * Test Case 6: Verify code blocks have syntax highlighting
   * Input: Verify code blocks have syntax highlighting
   * Expected: Code blocks display with proper syntax highlighting for bash/shell commands
   */
  test('TC6: Code blocks have syntax highlighting for bash/shell commands', async ({ page }) => {
    // Navigate to quick-start section
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Find all code blocks in the quick-start section
    const codeBlocks = quickStartSection.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();

    // Verify there are code blocks
    expect(codeBlockCount).toBeGreaterThan(0);

    // Check that code blocks have the language-bash class for syntax highlighting
    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);

      // Verify the code block has data-language attribute for visual indicator
      const dataLanguage = await codeBlock.getAttribute('data-language');
      expect(dataLanguage).toBe('bash');

      // Verify the code element has the language-bash class for syntax highlighting
      const codeElement = codeBlock.locator('code.language-bash');
      await expect(codeElement).toBeVisible();

      // Verify the code block uses monospace font (check computed style)
      const fontFamily = await codeElement.evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });

      // Check that font family contains monospace keywords
      const isMonospace = fontFamily.toLowerCase().includes('mono') ||
                         fontFamily.toLowerCase().includes('courier') ||
                         fontFamily.toLowerCase().includes('consolas');
      expect(isMonospace).toBe(true);
    }
  });

  /**
   * Additional test: Quick-start section is accessible from navigation
   */
  test('Quick-start section is accessible via navigation', async ({ page }) => {
    // Find the Quick Start link in navigation
    const navLink = page.locator('nav a[href="#quick-start"]');
    await expect(navLink).toBeVisible();
    await expect(navLink).toHaveText('Quick Start');

    // Click the navigation link
    await navLink.click();

    // Wait for scroll and verify the quick-start section is in view
    await page.waitForTimeout(500);

    const quickStartSection = page.locator('#quick-start');
    const isInViewport = await quickStartSection.evaluate((el) => {
      const rect = el.getBoundingClientRect();
      return rect.top >= -100 && rect.top <= window.innerHeight;
    });
    expect(isInViewport).toBe(true);
  });
});
