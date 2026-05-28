/**
 * E2E Tests: Installation Instructions
 * Owner: Scenario 4 - Installation Instructions
 *
 * Test coverage:
 * - Platform tabs for Linux and macOS are present
 * - Linux installation commands (cargo install) are present
 * - macOS installation commands (Homebrew, cargo) are present
 * - System requirements are documented
 * - Build-from-source instructions (cargo build --release) are present
 * - Configuration setup (mirdb.toml) is documented
 * - Copy buttons work for code blocks
 * - Tab switching works correctly
 */

const { test, expect } = require('@playwright/test');

test.describe('Installation Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForSelector('#installation', { state: 'visible' });
  });

  test('installation section has correct heading and structure', async ({ page }) => {
    const section = page.locator('#installation');
    await expect(section).toBeVisible();

    const heading = section.locator('h2');
    await expect(heading).toHaveText('Installation');

    const intro = section.locator('.section-intro');
    await expect(intro).toContainText('Get MirDB running');
  });

  test('system requirements are documented', async ({ page }) => {
    const section = page.locator('#installation');

    const requirementsHeading = section.locator('.install-requirements h3');
    await expect(requirementsHeading).toHaveText('System Requirements');

    const requirementsList = section.locator('.requirements-list');
    await expect(requirementsList).toBeVisible();

    const requirements = requirementsList.locator('li');
    await expect(requirements).toHaveCount(5);

    await expect(requirements.nth(0)).toContainText('Rust');
    await expect(requirements.nth(0)).toContainText('1.70');
    await expect(requirements.nth(1)).toContainText('Linux');
    await expect(requirements.nth(1)).toContainText('macOS');
    await expect(requirements.nth(2)).toContainText('Disk Space');
    await expect(requirements.nth(3)).toContainText('Memory');
    await expect(requirements.nth(4)).toContainText('Network');
    await expect(requirements.nth(4)).toContainText('12333');
  });

  test('platform tabs for Linux and macOS are present', async ({ page }) => {
    const section = page.locator('#installation');

    const tablist = section.locator('[role="tablist"]');
    await expect(tablist).toBeVisible();

    const tabs = tablist.locator('[role="tab"]');
    await expect(tabs).toHaveCount(2);

    const linuxTab = tabs.filter({ hasText: 'Linux' });
    const macosTab = tabs.filter({ hasText: 'macOS' });

    await expect(linuxTab).toBeVisible();
    await expect(macosTab).toBeVisible();

    await expect(linuxTab).toHaveAttribute('aria-selected', 'true');
    await expect(macosTab).toHaveAttribute('aria-selected', 'false');

    await expect(linuxTab).toHaveAttribute('aria-controls', 'install-linux');
    await expect(macosTab).toHaveAttribute('aria-controls', 'install-macos');
  });

  test('Linux installation commands include cargo install and binary download', async ({ page }) => {
    const linuxPanel = page.locator('#install-linux');
    await expect(linuxPanel).toBeVisible();

    // Check for cargo install
    const codeBlocks = linuxPanel.locator('pre code');
    let foundCargoInstall = false;
    let foundBinaryDownload = false;
    let foundBuildSource = false;

    const count = await codeBlocks.count();
    for (let i = 0; i < count; i++) {
      const text = await codeBlocks.nth(i).textContent();
      if (text.includes('cargo install mirdb-server')) foundCargoInstall = true;
      if (text.includes('curl -L -o mirdb-server')) foundBinaryDownload = true;
      if (text.includes('cargo build --release')) foundBuildSource = true;
    }

    expect(foundCargoInstall).toBe(true);
    expect(foundBinaryDownload).toBe(true);
    expect(foundBuildSource).toBe(true);
  });

  test('macOS installation commands include Homebrew and cargo install', async ({ page }) => {
    // Switch to macOS tab
    const macosTab = page.locator('#tab-macos');
    await macosTab.click();

    const macosPanel = page.locator('#install-macos');
    await expect(macosPanel).toBeVisible();

    const codeBlocks = macosPanel.locator('pre code');
    let foundHomebrew = false;
    let foundCargoInstall = false;
    let foundBuildSource = false;

    const count = await codeBlocks.count();
    for (let i = 0; i < count; i++) {
      const text = await codeBlocks.nth(i).textContent();
      if (text.includes('brew install mirdb')) foundHomebrew = true;
      if (text.includes('cargo install mirdb-server')) foundCargoInstall = true;
      if (text.includes('cargo build --release')) foundBuildSource = true;
    }

    expect(foundHomebrew).toBe(true);
    expect(foundCargoInstall).toBe(true);
    expect(foundBuildSource).toBe(true);
  });

  test('build-from-source instructions are documented', async ({ page }) => {
    const section = page.locator('#installation');

    const codeBlocks = section.locator('pre code');
    let foundGitClone = false;
    let foundCargoBuild = false;

    const count = await codeBlocks.count();
    for (let i = 0; i < count; i++) {
      const text = await codeBlocks.nth(i).textContent();
      if (text.includes('git clone')) foundGitClone = true;
      if (text.includes('cargo build --release')) foundCargoBuild = true;
    }

    expect(foundGitClone).toBe(true);
    expect(foundCargoBuild).toBe(true);
  });

  test('configuration setup with mirdb.toml is documented', async ({ page }) => {
    const section = page.locator('#installation');

    const configHeading = section.locator('.config-setup h3');
    await expect(configHeading).toHaveText('Configuration Setup');

    const configText = section.locator('.config-setup');
    await expect(configText).toContainText('mirdb.toml');

    const codeBlocks = section.locator('.config-setup pre code');
    await expect(codeBlocks).toHaveCount(1);

    const configCode = await codeBlocks.first().textContent();
    expect(configCode).toContain('addr');
    expect(configCode).toContain('work_dir');
    expect(configCode).toContain('mem_table_max_size');
    expect(configCode).toContain('sstable_max_size');
    expect(configCode).toContain('block_size');
    expect(configCode).toContain('max_level');

    const configNote = section.locator('.config-note');
    await expect(configNote).toContainText('default');
  });

  test('tab switching updates content correctly', async ({ page }) => {
    const section = page.locator('#installation');

    // Initially Linux should be active
    const linuxPanel = section.locator('#install-linux');
    const macosPanel = section.locator('#install-macos');

    await expect(linuxPanel).toHaveClass(/active/);
    await expect(macosPanel).not.toHaveClass(/active/);

    // Click macOS tab
    const macosTab = section.locator('#tab-macos');
    await macosTab.click();

    await expect(macosTab).toHaveAttribute('aria-selected', 'true');
    await expect(section.locator('#tab-linux')).toHaveAttribute('aria-selected', 'false');

    await expect(macosPanel).toHaveClass(/active/);
    await expect(linuxPanel).not.toHaveClass(/active/);

    // Click back to Linux
    const linuxTab = section.locator('#tab-linux');
    await linuxTab.click();

    await expect(linuxTab).toHaveAttribute('aria-selected', 'true');
    await expect(macosTab).toHaveAttribute('aria-selected', 'false');
  });

  test('copy buttons are present on code blocks', async ({ page }) => {
    const section = page.locator('#installation');

    // Check Linux panel (active by default) has visible copy buttons
    const linuxPanel = section.locator('#install-linux');
    const linuxButtons = linuxPanel.locator('.copy-btn');
    const linuxCount = await linuxButtons.count();
    expect(linuxCount).toBeGreaterThan(0);

    for (let i = 0; i < linuxCount; i++) {
      const button = linuxButtons.nth(i);
      await expect(button).toBeVisible();
      const dataCode = await button.getAttribute('data-code');
      expect(dataCode).toBeTruthy();
    }

    // Switch to macOS tab and check its copy buttons
    const macosTab = section.locator('#tab-macos');
    await macosTab.click();

    const macosPanel = section.locator('#install-macos');
    const macosButtons = macosPanel.locator('.copy-btn');
    const macosCount = await macosButtons.count();
    expect(macosCount).toBeGreaterThan(0);

    for (let i = 0; i < macosCount; i++) {
      const button = macosButtons.nth(i);
      await expect(button).toBeVisible();
      const dataCode = await button.getAttribute('data-code');
      expect(dataCode).toBeTruthy();
    }
  });

  test('installation section is accessible', async ({ page }) => {
    const section = page.locator('#installation');

    // Check section has a heading
    const heading = section.locator('h2');
    await expect(heading).toHaveCount(1);

    // Check tablist has proper ARIA
    const tablist = section.locator('[role="tablist"]');
    await expect(tablist).toHaveAttribute('aria-label', 'Installation platform selector');

    // Check panels have proper ARIA
    const panels = section.locator('[role="tabpanel"]');
    const panelCount = await panels.count();
    expect(panelCount).toBe(2);

    for (let i = 0; i < panelCount; i++) {
      const labelledBy = await panels.nth(i).getAttribute('aria-labelledby');
      expect(labelledBy).toBeTruthy();
    }
  });
});
