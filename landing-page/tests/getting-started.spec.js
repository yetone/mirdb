// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

test.describe('Getting Started Section', () => {
  test.beforeEach(async ({ page }) => {
    const filePath = path.resolve(__dirname, '../index.html');
    await page.goto(`file://${filePath}`);
  });

  // Test Case 1: Page contains a clearly labeled getting started section
  test('TC1: Page contains a clearly labeled getting started section', async ({ page }) => {
    // Look for a getting started section with data-testid or id
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"], #getting-started, section:has-text("Getting Started")');
    await expect(gettingStartedSection.first()).toBeVisible();

    // Verify section has a heading with "Getting Started" or "Installation" text
    const pageContent = await page.locator('body').textContent();
    const hasGettingStartedHeading =
      pageContent.toLowerCase().includes('getting started') ||
      pageContent.toLowerCase().includes('installation') ||
      pageContent.toLowerCase().includes('quick start');
    expect(hasGettingStartedHeading).toBeTruthy();
  });

  // Test Case 2: Installation instructions are presented as ordered steps
  test('TC2: Installation instructions are presented as ordered steps', async ({ page }) => {
    // Look for numbered/ordered installation steps
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeVisible();

    // Check for an ordered list (ol) or numbered steps within the section
    const orderedList = gettingStartedSection.locator('ol');
    const stepElements = gettingStartedSection.locator('.step, [data-testid*="step"], li');

    const olCount = await orderedList.count();
    const stepCount = await stepElements.count();

    // Either an ordered list exists OR step elements are present
    const hasOrderedSteps = olCount > 0 || stepCount >= 2;
    expect(hasOrderedSteps).toBeTruthy();

    // Verify steps contain actionable content
    if (olCount > 0) {
      const listItems = orderedList.locator('li');
      const itemCount = await listItems.count();
      expect(itemCount).toBeGreaterThanOrEqual(2);
    }
  });

  // Test Case 3: Page shows how to install MirDB via Cargo or download binary
  test('TC3: Page shows how to install MirDB via Cargo or download binary', async ({ page }) => {
    // Look for Cargo installation command or binary download instructions
    const pageContent = await page.locator('body').textContent();

    const hasCargoInstall =
      pageContent.toLowerCase().includes('cargo') ||
      pageContent.toLowerCase().includes('crates.io');

    const hasBinaryDownload =
      pageContent.toLowerCase().includes('download') ||
      pageContent.toLowerCase().includes('binary') ||
      pageContent.toLowerCase().includes('release');

    const hasGitClone =
      pageContent.toLowerCase().includes('git clone') ||
      pageContent.toLowerCase().includes('github');

    // At least one installation method should be documented
    expect(hasCargoInstall || hasBinaryDownload || hasGitClone).toBeTruthy();

    // Look for code blocks with installation commands
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeVisible();

    const codeBlocks = gettingStartedSection.locator('pre, code');
    const codeCount = await codeBlocks.count();
    expect(codeCount).toBeGreaterThanOrEqual(1);
  });

  // Test Case 4: Page mentions default port 12333 for connection
  test('TC4: Page mentions default port 12333 for connection', async ({ page }) => {
    // Look for port 12333 mentioned on the page
    const pageContent = await page.locator('body').textContent();

    // Check for the default port number
    expect(pageContent).toContain('12333');

    // Optionally verify it's in context of configuration or connection
    const hasPortContext =
      pageContent.toLowerCase().includes('port') ||
      pageContent.toLowerCase().includes('connect') ||
      pageContent.toLowerCase().includes('addr') ||
      pageContent.toLowerCase().includes('config');
    expect(hasPortContext).toBeTruthy();
  });

  // Additional test: Configuration example is provided
  test('Configuration example with TOML or basic setup is provided', async ({ page }) => {
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeVisible();

    // Check for configuration-related content
    const sectionContent = await gettingStartedSection.textContent();

    const hasConfigContent =
      sectionContent.toLowerCase().includes('config') ||
      sectionContent.toLowerCase().includes('toml') ||
      sectionContent.toLowerCase().includes('configuration') ||
      sectionContent.toLowerCase().includes('setup');

    expect(hasConfigContent).toBeTruthy();
  });

  // Additional test: Quick test/verification command is provided
  test('Quick test command to verify installation is provided', async ({ page }) => {
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeVisible();

    // Look for verification/test commands
    const sectionContent = await gettingStartedSection.textContent();

    const hasVerifyContent =
      sectionContent.toLowerCase().includes('verify') ||
      sectionContent.toLowerCase().includes('test') ||
      sectionContent.toLowerCase().includes('telnet') ||
      sectionContent.toLowerCase().includes('connect') ||
      sectionContent.toLowerCase().includes('run');

    expect(hasVerifyContent).toBeTruthy();

    // Should have a code block with the verification command
    const codeBlocks = gettingStartedSection.locator('pre, code');
    const codeCount = await codeBlocks.count();
    expect(codeCount).toBeGreaterThanOrEqual(1);
  });
});
