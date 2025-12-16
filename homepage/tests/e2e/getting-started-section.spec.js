// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Get Started Section with Installation Instructions
 * Scenario: Verify that the Get Started section provides clear installation instructions
 * and links to documentation as per REQ-3
 */

test.describe('Get Started Section with Installation Instructions', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage before each test
    await page.goto('/');
    // Scroll to getting started section
    await page.locator('#getting-started').scrollIntoViewIfNeeded();
  });

  /**
   * Test Case 1: Check for installation instructions section
   * Input: Check for installation instructions section
   * Expected: Section contains clear installation commands or steps for setting up MirDB
   */
  test('TC1: Installation instructions section contains clear setup commands', async ({ page }) => {
    // Verify getting started section is visible
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeVisible();

    // Verify section title
    const sectionTitle = page.locator('[data-testid="getting-started-title"]');
    await expect(sectionTitle).toBeVisible();
    await expect(sectionTitle).toContainText('Getting Started');

    // Verify installation subsection exists
    const installationSection = page.locator('[data-testid="installation-section"]');
    await expect(installationSection).toBeVisible();

    // Verify installation title
    const installationTitle = page.locator('[data-testid="installation-title"]');
    await expect(installationTitle).toBeVisible();
    await expect(installationTitle).toContainText('Installation');

    // Verify installation code block exists with commands
    const installationCode = page.locator('[data-testid="installation-code"]');
    await expect(installationCode).toBeVisible();

    // Verify the installation instructions contain the expected commands
    const codeContent = await installationCode.textContent();

    // Check for git clone command
    expect(codeContent).toContain('git clone');
    expect(codeContent).toContain('github.com');
    expect(codeContent).toContain('mirdb');

    // Check for build command
    expect(codeContent).toContain('cargo build');
    expect(codeContent).toContain('--release');

    // Check for run command
    expect(codeContent).toContain('mirdb-server');
  });

  /**
   * Test Case 2: Check for documentation link
   * Input: Check for documentation link
   * Expected: Link to full documentation is present and functional
   */
  test('TC2: Documentation link is present and functional', async ({ page }) => {
    // Verify documentation links section exists
    const docsLinksSection = page.locator('[data-testid="documentation-links"]');
    await expect(docsLinksSection).toBeVisible();

    // Verify primary documentation link exists
    const docsLink = page.locator('[data-testid="docs-link"]');
    await expect(docsLink).toBeVisible();
    await expect(docsLink).toContainText('Documentation');

    // Verify the link has a valid href pointing to documentation
    const href = await docsLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toContain('github.com');
    expect(href).toContain('mirdb');

    // Verify link opens in new tab for external documentation
    const target = await docsLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify security attributes for external links
    const rel = await docsLink.getAttribute('rel');
    expect(rel).toContain('noopener');

    // Verify secondary wiki link also exists
    const wikiLink = page.locator('[data-testid="wiki-link"]');
    await expect(wikiLink).toBeVisible();

    const wikiHref = await wikiLink.getAttribute('href');
    expect(wikiHref).toBeTruthy();
    expect(wikiHref).toContain('wiki');
  });

  /**
   * Test Case 3: Verify quick configuration example is shown
   * Input: Verify quick configuration example is shown
   * Expected: A configuration example (e.g., TOML config) is displayed with proper formatting
   */
  test('TC3: Configuration example is displayed with proper TOML formatting', async ({ page }) => {
    // Verify configuration section exists
    const configSection = page.locator('[data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    // Verify configuration title
    const configTitle = page.locator('[data-testid="configuration-title"]');
    await expect(configTitle).toBeVisible();
    await expect(configTitle).toContainText('Configuration');

    // Verify configuration description mentions TOML file
    const configDescription = page.locator('[data-testid="configuration-description"]');
    await expect(configDescription).toBeVisible();
    const descText = await configDescription.textContent();
    expect(descText).toContain('mirdb.toml');

    // Verify configuration code block exists
    const configCode = page.locator('[data-testid="configuration-code"]');
    await expect(configCode).toBeVisible();

    // Verify the config contains TOML-formatted content
    const codeContent = await configCode.textContent();

    // Check for TOML key-value pairs (addr configuration)
    expect(codeContent).toContain('addr');
    expect(codeContent).toContain('0.0.0.0:12333');

    // Check for work_dir setting
    expect(codeContent).toContain('work_dir');

    // Check for memory settings
    expect(codeContent).toContain('mem_table_max_size');

    // Check for storage settings
    expect(codeContent).toContain('sst_max_size');
    expect(codeContent).toContain('block_size');

    // Check for LSM-tree settings
    expect(codeContent).toContain('max_level');
    expect(codeContent).toContain('l0_compaction_trigger');

    // Verify code block has the language-toml class for syntax highlighting
    const codeElement = configCode.locator('code');
    const codeClass = await codeElement.getAttribute('class');
    expect(codeClass).toContain('language-toml');
  });

  /**
   * Test: Verify Get Started section is accessible via hero CTA button
   */
  test('TC-Navigation: Get Started button navigates to Getting Started section', async ({ page }) => {
    // Go back to the top of the page
    await page.goto('/');

    // Click the Get Started button in hero section
    const getStartedBtn = page.locator('[data-testid="get-started-btn"]');
    await expect(getStartedBtn).toBeVisible();
    await getStartedBtn.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(500);

    // Verify the getting started section is now in viewport
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeInViewport();
  });

  /**
   * Test: Verify all code blocks have proper dark background
   */
  test('TC-Styling: Code blocks have proper dark background', async ({ page }) => {
    // Check installation code block styling
    const installationCode = page.locator('[data-testid="installation-code"]');
    const installBgColor = await installationCode.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    // Should be dark background (#1e1e1e = rgb(30, 30, 30))
    expect(installBgColor).toMatch(/rgb\(30,\s*30,\s*30\)/);

    // Check configuration code block styling
    const configCode = page.locator('[data-testid="configuration-code"]');
    const configBgColor = await configCode.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    expect(configBgColor).toMatch(/rgb\(30,\s*30,\s*30\)/);
  });
});
