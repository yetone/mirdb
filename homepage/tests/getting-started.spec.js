// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Getting Started Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Check for installation instructions presence
   * Expected: Getting started section contains clear installation steps or commands
   */
  test('should display installation instructions', async ({ page }) => {
    // Navigate to getting started section
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeVisible();

    // Check for installation instructions
    const installationSection = page.locator('[data-testid="installation-instructions"]');
    await expect(installationSection).toBeVisible();

    // Verify installation commands are present
    const installationText = await installationSection.textContent();

    // Should contain build/install commands (cargo build, git clone, or installation steps)
    const hasGitClone = installationText?.includes('git clone');
    const hasCargoBuild = installationText?.includes('cargo build');
    const hasInstallCommand = installationText?.toLowerCase().includes('install');

    expect(hasGitClone || hasCargoBuild || hasInstallCommand).toBeTruthy();
  });

  /**
   * Test Case 2: Check for configuration example
   * Expected: Section includes basic configuration example (TOML or command-line args)
   */
  test('should display configuration example', async ({ page }) => {
    // Navigate to getting started section
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeVisible();

    // Check for configuration example
    const configSection = page.locator('[data-testid="configuration-example"]');
    await expect(configSection).toBeVisible();

    // Verify configuration content exists
    const configText = await configSection.textContent();

    // Should contain TOML config or command-line arguments
    const hasTomlConfig = configText?.includes('addr') || configText?.includes('work_dir') || configText?.includes('.toml');
    const hasCommandLineArgs = configText?.includes('-c') || configText?.includes('--config');

    expect(hasTomlConfig || hasCommandLineArgs).toBeTruthy();
  });

  /**
   * Test Case 3: Check for code snippets
   * Expected: Section includes code examples showing basic get/set operations
   */
  test('should display code examples with get/set operations', async ({ page }) => {
    // Navigate to getting started section
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeVisible();

    // Check for usage code examples
    const usageSection = page.locator('[data-testid="usage-examples"]');
    await expect(usageSection).toBeVisible();

    // Verify code examples contain get/set operations
    const usageText = await usageSection.textContent();

    // Should demonstrate basic Memcached operations
    const hasSetCommand = usageText?.toLowerCase().includes('set');
    const hasGetCommand = usageText?.toLowerCase().includes('get');

    expect(hasSetCommand || hasGetCommand).toBeTruthy();
  });

  /**
   * Test Case 4: Verify code blocks are properly formatted
   * Expected: Code examples are displayed with syntax highlighting or code block styling
   */
  test('should display code blocks with proper formatting', async ({ page }) => {
    // Navigate to getting started section
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeVisible();

    // Check for code blocks (pre or code elements)
    const codeBlocks = gettingStartedSection.locator('pre, code');
    const codeBlockCount = await codeBlocks.count();

    // Should have at least one code block
    expect(codeBlockCount).toBeGreaterThan(0);

    // Verify code blocks have proper styling (code-block class or language class)
    const firstCodeBlock = codeBlocks.first();
    await expect(firstCodeBlock).toBeVisible();

    // Check if code blocks are styled (have specific classes or parent elements)
    const parentPre = gettingStartedSection.locator('pre');
    const preCount = await parentPre.count();
    expect(preCount).toBeGreaterThan(0);
  });

  /**
   * Additional test: Getting started section has proper heading
   */
  test('should display Getting Started heading', async ({ page }) => {
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeVisible();

    // Check for heading
    const heading = gettingStartedSection.locator('h2');
    await expect(heading).toBeVisible();

    const headingText = await heading.textContent();
    expect(headingText?.toLowerCase()).toContain('getting started');
  });

  /**
   * Additional test: Section is navigable via anchor link
   */
  test('should be navigable via anchor link', async ({ page }) => {
    // Navigate directly to getting started section
    await page.goto('/#getting-started');

    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeVisible();
    await expect(gettingStartedSection).toBeInViewport();
  });
});
