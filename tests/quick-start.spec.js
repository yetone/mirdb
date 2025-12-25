// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Quick Start Section - Scenario: Quick Start Section
 *
 * These tests verify that the quick start section provides installation
 * and usage instructions for MirDB.
 */

test.describe('Quick Start Section', () => {

  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage
    await page.goto('/');
    // Wait for the page to be fully loaded
    await page.waitForLoadState('domcontentloaded');
  });

  /**
   * Test Case 1: Installation command is visible with syntax highlighting
   *
   * Verifies that the quick start section contains installation instructions
   * using cargo or build instructions, with proper syntax highlighting.
   */
  test('should display installation command with syntax highlighting', async ({ page }) => {
    // Navigate to quick start section
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Find the installation step
    const installationStep = page.locator('[data-testid="installation-step"]');
    await expect(installationStep).toBeVisible();

    // Verify installation code block exists
    const installationCode = page.locator('[data-testid="installation-code"]');
    await expect(installationCode).toBeVisible();

    // Verify the code contains cargo build command
    const codeContent = await installationCode.locator('code').textContent();
    expect(codeContent).toContain('cargo build');
    expect(codeContent).toContain('git clone');

    // Verify syntax highlighting is applied (Prism adds language class)
    const codeElement = installationCode.locator('code');
    await expect(codeElement).toHaveClass(/language-bash/);
  });

  /**
   * Test Case 2: TOML configuration example is displayed
   *
   * Verifies that the quick start section shows a TOML configuration example
   * with addr, work_dir, and other parameters.
   */
  test('should display TOML configuration example with required parameters', async ({ page }) => {
    // Navigate to quick start section
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Find the configuration step
    const configurationStep = page.locator('[data-testid="configuration-step"]');
    await expect(configurationStep).toBeVisible();

    // Verify configuration code block exists
    const configurationCode = page.locator('[data-testid="configuration-code"]');
    await expect(configurationCode).toBeVisible();

    // Verify the code contains required TOML parameters
    const codeContent = await configurationCode.locator('code').textContent();
    expect(codeContent).toContain('addr');
    expect(codeContent).toContain('work_dir');
    expect(codeContent).toContain('0.0.0.0:12333');
    expect(codeContent).toContain('/tmp/mirdb');
    expect(codeContent).toContain('sst_max_size');
    expect(codeContent).toContain('mem_table_max_size');

    // Verify syntax highlighting is applied for TOML
    const codeElement = configurationCode.locator('code');
    await expect(codeElement).toHaveClass(/language-toml/);
  });

  /**
   * Test Case 3: Usage example with memcached client is present
   *
   * Verifies that a code snippet showing memcached client connection
   * and basic operations (set, get, delete) is present.
   */
  test('should display usage example with memcached client operations', async ({ page }) => {
    // Navigate to quick start section
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Find the usage step
    const usageStep = page.locator('[data-testid="usage-step"]');
    await expect(usageStep).toBeVisible();

    // Verify Python usage code block exists
    const usageCode = page.locator('[data-testid="usage-code"]');
    await expect(usageCode).toBeVisible();

    // Verify the code contains memcached client operations
    const codeContent = await usageCode.locator('code').textContent();
    expect(codeContent).toContain('client');
    expect(codeContent).toContain('set');
    expect(codeContent).toContain('get');
    expect(codeContent).toContain('delete');
    expect(codeContent).toContain('12333'); // Port number

    // Verify Python syntax highlighting is applied
    const codeElement = usageCode.locator('code');
    await expect(codeElement).toHaveClass(/language-python/);

    // Also verify telnet usage example exists
    const telnetUsageCode = page.locator('[data-testid="telnet-usage-code"]');
    await expect(telnetUsageCode).toBeVisible();

    const telnetContent = await telnetUsageCode.locator('code').textContent();
    expect(telnetContent).toContain('telnet');
    expect(telnetContent).toContain('localhost');
    expect(telnetContent).toContain('STORED');
  });

  /**
   * Test Case 4: Code snippets have proper syntax highlighting
   *
   * Verifies that Rust and shell commands have proper syntax highlighting
   * applied through Prism.js classes.
   */
  test('should have proper syntax highlighting for all code blocks', async ({ page }) => {
    // Navigate to quick start section
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Verify all code blocks have language-specific classes

    // Shell/Bash highlighting for installation
    const bashCode = page.locator('[data-testid="installation-code"] code');
    await expect(bashCode).toHaveClass(/language-bash/);

    // TOML highlighting for configuration
    const tomlCode = page.locator('[data-testid="configuration-code"] code');
    await expect(tomlCode).toHaveClass(/language-toml/);

    // Python highlighting for usage
    const pythonCode = page.locator('[data-testid="usage-code"] code');
    await expect(pythonCode).toHaveClass(/language-python/);

    // Rust highlighting for commands reference
    const rustCode = page.locator('[data-testid="commands-code"] code');
    await expect(rustCode).toHaveClass(/language-rust/);

    // Verify code blocks have proper formatting
    const allCodeBlocks = page.locator('#quick-start pre');
    const count = await allCodeBlocks.count();
    expect(count).toBeGreaterThanOrEqual(4); // At least 4 code blocks
  });

  /**
   * Additional test: Quick start section is accessible
   */
  test('should have accessible quick start section', async ({ page }) => {
    // Verify the quick start section exists
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Verify section title
    const sectionTitle = quickStartSection.locator('h2');
    await expect(sectionTitle).toContainText('Quick Start');
  });

  /**
   * Additional test: All quick start steps are present
   */
  test('should display all four quick start steps', async ({ page }) => {
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Verify all steps are present
    await expect(page.locator('[data-testid="installation-step"]')).toBeVisible();
    await expect(page.locator('[data-testid="configuration-step"]')).toBeVisible();
    await expect(page.locator('[data-testid="usage-step"]')).toBeVisible();
    await expect(page.locator('[data-testid="commands-step"]')).toBeVisible();

    // Verify step count
    const steps = page.locator('#quick-start .step');
    await expect(steps).toHaveCount(4);
  });
});
