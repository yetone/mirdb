// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Installation Section E2E Tests
 * Owner: Scenario 3 - Installation Instructions
 *
 * Test cases:
 * - Cargo install or cargo build instructions are present
 * - Git clone command with repository URL is present
 * - Default configuration info (port 12333, work directory /tmp/mirdb) is present
 * - Copy buttons are present on code snippets
 */

test.describe('Installation Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: should contain cargo install or cargo build instructions', async ({ page }) => {
    // Navigate to installation section
    const installSection = page.locator('#installation');
    await expect(installSection).toBeVisible();

    // Check for cargo build command
    const cargoBuildCommand = installSection.locator('code', { hasText: 'cargo build' });
    const cargoInstallCommand = installSection.locator('code', { hasText: 'cargo install' });

    // At least one of cargo build or cargo install should be present
    const hasCargoBuild = await cargoBuildCommand.count() > 0;
    const hasCargoInstall = await cargoInstallCommand.count() > 0;

    expect(hasCargoBuild || hasCargoInstall).toBeTruthy();
  });

  test('TC2: should contain git clone command with repository URL', async ({ page }) => {
    const installSection = page.locator('#installation');
    await expect(installSection).toBeVisible();

    // Check for git clone command
    const gitCloneCommand = installSection.locator('code', { hasText: 'git clone' });
    await expect(gitCloneCommand.first()).toBeVisible();

    // Verify it contains a repository URL
    const gitCloneText = await gitCloneCommand.first().textContent();
    expect(gitCloneText).toMatch(/git clone\s+https?:\/\//);
  });

  test('TC3: should display default configuration info with port 12333 and work directory /tmp/mirdb', async ({ page }) => {
    const installSection = page.locator('#installation');
    await expect(installSection).toBeVisible();

    // Check for default port 12333
    const portInfo = installSection.locator('text=12333');
    await expect(portInfo.first()).toBeVisible();

    // Check for default work directory /tmp/mirdb
    const workDirInfo = installSection.getByText('/tmp/mirdb');
    await expect(workDirInfo.first()).toBeVisible();
  });

  test('TC4: should have copy buttons on installation code snippets', async ({ page }) => {
    const installSection = page.locator('#installation');
    await expect(installSection).toBeVisible();

    // Get all code blocks in the installation section
    const codeBlocks = installSection.locator('.install-code-block');
    const codeBlockCount = await codeBlocks.count();

    // Ensure there are code blocks
    expect(codeBlockCount).toBeGreaterThan(0);

    // Check that each code block has a copy button
    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      const copyButton = codeBlock.locator('.install-copy-btn');
      await expect(copyButton).toBeVisible();
      await expect(copyButton).toHaveAttribute('type', 'button');
    }
  });

  test('copy button changes state when clicked', async ({ page }) => {
    const installSection = page.locator('#installation');
    await expect(installSection).toBeVisible();

    // Get the first copy button
    const copyButton = installSection.locator('.install-copy-btn').first();
    await expect(copyButton).toBeVisible();

    // Click the copy button
    await copyButton.click();

    // Button should show copied state
    await expect(copyButton).toHaveClass(/copied/);

    // Button text should change to "Copied!"
    const buttonText = copyButton.locator('span');
    await expect(buttonText).toHaveText('Copied!');
  });

  test('installation section has proper heading', async ({ page }) => {
    const installSection = page.locator('#installation');
    await expect(installSection).toBeVisible();

    // Check for installation title
    const title = installSection.locator('h2#install-title');
    await expect(title).toBeVisible();
    await expect(title).toHaveText('Installation');
  });

  test('installation methods are displayed', async ({ page }) => {
    const installSection = page.locator('#installation');
    await expect(installSection).toBeVisible();

    // Check for installation methods
    const installMethods = installSection.locator('.install-method');
    const methodCount = await installMethods.count();

    // Should have at least 2 installation methods (Build from Source, Cargo Install)
    expect(methodCount).toBeGreaterThanOrEqual(2);
  });

  test('installation steps are numbered', async ({ page }) => {
    const installSection = page.locator('#installation');
    await expect(installSection).toBeVisible();

    // Check for numbered steps
    const installSteps = installSection.locator('.install-step');
    const stepCount = await installSteps.count();

    // Should have multiple steps
    expect(stepCount).toBeGreaterThan(0);
  });

  test('configuration table displays all required settings', async ({ page }) => {
    const installSection = page.locator('#installation');
    await expect(installSection).toBeVisible();

    // Check for configuration table
    const configTable = installSection.locator('.install-config-table');
    await expect(configTable).toBeVisible();

    // Check table has rows for Port, Work Directory, and Bind Address
    const portRow = configTable.locator('tr', { hasText: 'Port' });
    const workDirRow = configTable.locator('tr', { hasText: 'Work Directory' });
    const bindAddressRow = configTable.locator('tr', { hasText: 'Bind Address' });

    await expect(portRow).toBeVisible();
    await expect(workDirRow).toBeVisible();
    await expect(bindAddressRow).toBeVisible();
  });
});
