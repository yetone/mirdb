import { test, expect } from '@playwright/test';

/**
 * E2E Tests for MirDB Homepage Getting Started Section
 *
 * These tests verify that the Getting Started section provides:
 * - Installation instructions/command
 * - TOML configuration example with key parameters
 * - Link to full documentation
 * - Default configuration values (port 12333, work directory, etc.)
 */

test.describe('Getting Started Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Installation command is present and visible', async ({ page }) => {
    // Navigate to getting started section
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Check for installation step
    const installStep = gettingStartedSection.locator('[data-step="install"]');
    await expect(installStep).toBeVisible();

    // Verify installation command contains cargo install
    const installCode = installStep.locator('code');
    await expect(installCode).toBeVisible();
    await expect(installCode).toContainText('cargo install mirdb');

    // Verify the step has a proper heading
    const installHeading = installStep.locator('h3');
    await expect(installHeading).toContainText('Install');
  });

  test('TC2: TOML configuration example with key parameters is displayed', async ({ page }) => {
    // Navigate to getting started section
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Check for configuration step
    const configStep = gettingStartedSection.locator('[data-step="configure"]');
    await expect(configStep).toBeVisible();

    // Verify TOML configuration is present
    const configCode = configStep.locator('code.language-toml');
    await expect(configCode).toBeVisible();

    // Verify key configuration parameters are present
    const configText = await configCode.textContent();
    expect(configText).toContain('addr');
    expect(configText).toContain('12333');
    expect(configText).toContain('work_dir');
    expect(configText).toContain('max_level');

    // Verify the step has a proper heading
    const configHeading = configStep.locator('h3');
    await expect(configHeading).toContainText('Configure');
  });

  test('TC3: Link to full documentation is present and functional', async ({ page }) => {
    // Navigate to getting started section
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Find the documentation link
    const docsLink = gettingStartedSection.locator('.docs-link');
    await expect(docsLink).toBeVisible();

    // Verify link text mentions documentation
    await expect(docsLink).toContainText('documentation');

    // Verify link href points to documentation (GitHub README or docs site)
    const href = await docsLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toContain('github.com');

    // Verify link opens in new tab for external links
    const target = await docsLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify rel attribute for security
    const rel = await docsLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  test('TC4: Default configuration values are displayed (port 12333, work directory, etc.)', async ({ page }) => {
    // Navigate to getting started section
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Check for defaults table
    const defaultsTable = gettingStartedSection.locator('.defaults-table');
    await expect(defaultsTable).toBeVisible();

    // Verify the table has proper structure
    const tableHeaders = defaultsTable.locator('thead th');
    await expect(tableHeaders).toHaveCount(2);
    await expect(tableHeaders.first()).toContainText('Parameter');
    await expect(tableHeaders.last()).toContainText('Default Value');

    // Verify key default values are present
    const tableContent = await defaultsTable.textContent();

    // Check for port 12333
    expect(tableContent).toContain('12333');

    // Check for work directory
    expect(tableContent).toContain('/tmp/mirdb');

    // Check for Max LSM Levels
    expect(tableContent).toContain('Max LSM Levels');
    expect(tableContent).toContain('7');

    // Check for SSTable Max Size
    expect(tableContent).toContain('SSTable Max Size');
    expect(tableContent).toContain('100MB');

    // Check for Memtable Max Size
    expect(tableContent).toContain('Memtable Max Size');
    expect(tableContent).toContain('4MB');

    // Check for Block Size
    expect(tableContent).toContain('Block Size');
    expect(tableContent).toContain('4KB');
  });

  test('Getting Started section has proper heading', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    const heading = gettingStartedSection.locator('h2');
    await expect(heading).toHaveText('Getting Started');
  });

  test('Run step is present with mirdb command', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Check for run step
    const runStep = gettingStartedSection.locator('[data-step="run"]');
    await expect(runStep).toBeVisible();

    // Verify run command
    const runCode = runStep.locator('code');
    await expect(runCode).toContainText('mirdb -c config.toml');

    // Verify heading
    const runHeading = runStep.locator('h3');
    await expect(runHeading).toContainText('Run');
  });

  test('All three steps (Install, Configure, Run) are displayed in order', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Verify all steps exist
    const steps = gettingStartedSection.locator('.step');
    await expect(steps).toHaveCount(3);

    // Verify step order by checking headings
    const stepHeadings = gettingStartedSection.locator('.step h3');
    const headingTexts = await stepHeadings.allTextContents();

    expect(headingTexts[0]).toContain('Install');
    expect(headingTexts[1]).toContain('Configure');
    expect(headingTexts[2]).toContain('Run');
  });
});
