// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Getting Started Section
 * Scenario: Verify the quick-start section includes installation and basic usage commands
 * Related Requirements: REQ-3, US-2
 */

test.describe('Getting Started Section', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage before each test
    await page.goto('/');
  });

  /**
   * Test Case 1: Cargo installation command is displayed in a code block
   */
  test('TC1: should display cargo installation command in a code block', async ({ page }) => {
    // Navigate to getting started section
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Check for cargo install command
    const cargoInstallBlock = page.locator('[data-testid="cargo-install"]');
    await expect(cargoInstallBlock).toBeVisible();

    // Verify the code block contains the cargo install command
    const codeContent = await cargoInstallBlock.locator('code').textContent();
    expect(codeContent).toContain('cargo install mirdb');

    // Verify it's in a code block (pre > code structure)
    const preElement = cargoInstallBlock.locator('pre');
    await expect(preElement).toBeVisible();
    const codeElement = cargoInstallBlock.locator('pre code');
    await expect(codeElement).toBeVisible();
  });

  /**
   * Test Case 2: Build from source instructions with git clone and cargo build commands
   */
  test('TC2: should display build from source instructions with git clone and cargo build commands', async ({ page }) => {
    // Navigate to getting started section
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Check for build from source instructions
    const buildFromSourceBlock = page.locator('[data-testid="build-from-source"]');
    await expect(buildFromSourceBlock).toBeVisible();

    // Verify the code block contains git clone command
    const codeContent = await buildFromSourceBlock.locator('code').textContent();
    expect(codeContent).toContain('git clone');
    expect(codeContent).toContain('github.com/yetone/mirdb');

    // Verify cargo build command is present
    expect(codeContent).toContain('cargo build');

    // Verify it's in a code block
    const preElement = buildFromSourceBlock.locator('pre');
    await expect(preElement).toBeVisible();
  });

  /**
   * Test Case 3: Server startup command showing how to run MirDB
   */
  test('TC3: should display server startup command showing how to run MirDB', async ({ page }) => {
    // Navigate to getting started section
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Check for server startup command
    const serverStartupBlock = page.locator('[data-testid="server-startup"]');
    await expect(serverStartupBlock).toBeVisible();

    // Verify the code block contains the server startup command
    const codeContent = await serverStartupBlock.locator('code').textContent();
    expect(codeContent).toContain('mirdb-server');

    // Verify it shows how to run the MirDB server
    expect(codeContent).toMatch(/\.\/target\/release\/mirdb-server|mirdb-server/);

    // Verify it's in a code block
    const preElement = serverStartupBlock.locator('pre');
    await expect(preElement).toBeVisible();
  });

  /**
   * Test Case 4: Link to full documentation is present in the getting started section
   */
  test('TC4: should display link to full documentation in getting started section', async ({ page }) => {
    // Navigate to getting started section
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Check for documentation link
    const docLink = page.locator('[data-testid="documentation-link"]');
    await expect(docLink).toBeVisible();

    // Verify the link text mentions documentation
    const linkText = await docLink.textContent();
    expect(linkText.toLowerCase()).toContain('documentation');

    // Verify the link has a valid href
    const href = await docLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toContain('github.com/yetone/mirdb');
  });

  /**
   * Additional test: Navigation to getting started section works
   */
  test('should be able to navigate to getting started section via navigation', async ({ page }) => {
    // Click on Getting Started link in navigation
    await page.click('a[href="#getting-started"]');

    // Verify the section is visible
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeInViewport();
  });

  /**
   * Additional test: Section title is present
   */
  test('should have a section title for Getting Started', async ({ page }) => {
    // Navigate to getting started section
    const gettingStartedSection = page.locator('#getting-started');

    // Verify section title exists and contains "Getting Started"
    const sectionTitle = gettingStartedSection.locator('h2');
    await expect(sectionTitle).toBeVisible();
    const titleText = await sectionTitle.textContent();
    expect(titleText).toContain('Getting Started');
  });
});
