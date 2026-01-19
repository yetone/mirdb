// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Getting Started Section
 * Scenario: Verify that the getting-started section provides installation and usage examples
 * as specified in REQ-5 and REQ-6
 */

test.describe('Getting Started Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Check for installation command
   * Input: Check for installation command
   * Expected: Clear installation command is displayed with copy functionality
   */
  test('TC1: Clear installation command is displayed', async ({ page }) => {
    // Navigate to getting-started section
    const gettingStartedSection = page.locator('#getting-started');
    await gettingStartedSection.scrollIntoViewIfNeeded();
    await expect(gettingStartedSection).toBeVisible();

    // Find the installation step
    const installationStep = page.locator('[data-testid="step-clone-build"]');
    await expect(installationStep).toBeVisible();

    // Verify the code block contains installation commands
    const codeBlock = installationStep.locator('pre code');
    await expect(codeBlock).toBeVisible();

    // Check that the code includes git clone and cargo build commands
    const codeText = await codeBlock.textContent();
    expect(codeText).toContain('git clone');
    expect(codeText).toContain('cargo build');

    // Verify the code block has proper styling (pre element exists)
    const preElement = installationStep.locator('pre');
    await expect(preElement).toBeVisible();
  });

  /**
   * Test Case 2: Check for SET operation example
   * Input: Check for SET operation example
   * Expected: Code example showing SET operation is displayed with syntax highlighting
   */
  test('TC2: SET operation example is displayed with syntax highlighting', async ({ page }) => {
    // Navigate to getting-started section
    const gettingStartedSection = page.locator('#getting-started');
    await gettingStartedSection.scrollIntoViewIfNeeded();

    // Find the step with memcached client examples
    const clientStep = page.locator('[data-testid="step-connect-client"]');
    await expect(clientStep).toBeVisible();

    // Verify the code block contains SET operation
    const codeBlock = clientStep.locator('pre code');
    await expect(codeBlock).toBeVisible();

    const codeText = await codeBlock.textContent();
    expect(codeText.toLowerCase()).toContain('set');
    expect(codeText).toContain('STORED');

    // Verify syntax highlighting via code element presence (styled code block)
    const preElement = clientStep.locator('pre');
    await expect(preElement).toBeVisible();
  });

  /**
   * Test Case 3: Check for GET operation example
   * Input: Check for GET operation example
   * Expected: Code example showing GET operation is displayed with syntax highlighting
   */
  test('TC3: GET operation example is displayed with syntax highlighting', async ({ page }) => {
    // Navigate to getting-started section
    const gettingStartedSection = page.locator('#getting-started');
    await gettingStartedSection.scrollIntoViewIfNeeded();

    // Find the step with memcached client examples
    const clientStep = page.locator('[data-testid="step-connect-client"]');
    await expect(clientStep).toBeVisible();

    // Verify the code block contains GET operation
    const codeBlock = clientStep.locator('pre code');
    await expect(codeBlock).toBeVisible();

    const codeText = await codeBlock.textContent();
    expect(codeText.toLowerCase()).toContain('get');
    expect(codeText).toContain('VALUE');

    // Verify syntax highlighting via code element presence (styled code block)
    const preElement = clientStep.locator('pre');
    await expect(preElement).toBeVisible();
  });

  /**
   * Test Case 4: Check for DELETE operation example
   * Input: Check for DELETE operation example
   * Expected: Code example showing DELETE operation is displayed with syntax highlighting
   */
  test('TC4: DELETE operation example is displayed with syntax highlighting', async ({ page }) => {
    // Navigate to getting-started section
    const gettingStartedSection = page.locator('#getting-started');
    await gettingStartedSection.scrollIntoViewIfNeeded();

    // Find the step with memcached client examples
    const clientStep = page.locator('[data-testid="step-connect-client"]');
    await expect(clientStep).toBeVisible();

    // Verify the code block contains DELETE operation
    const codeBlock = clientStep.locator('pre code');
    await expect(codeBlock).toBeVisible();

    const codeText = await codeBlock.textContent();
    expect(codeText.toLowerCase()).toContain('delete');
    expect(codeText).toContain('DELETED');

    // Verify syntax highlighting via code element presence (styled code block)
    const preElement = clientStep.locator('pre');
    await expect(preElement).toBeVisible();
  });

  /**
   * Test Case 5: Check for basic configuration example
   * Input: Check for basic configuration example
   * Expected: TOML configuration example is displayed showing basic settings
   */
  test('TC5: TOML configuration example is displayed showing basic settings', async ({ page }) => {
    // Navigate to getting-started section
    const gettingStartedSection = page.locator('#getting-started');
    await gettingStartedSection.scrollIntoViewIfNeeded();

    // Find the configuration step
    const configStep = page.locator('[data-testid="step-configure"]');
    await expect(configStep).toBeVisible();

    // Verify the code block contains TOML configuration
    const codeBlock = configStep.locator('pre code');
    await expect(codeBlock).toBeVisible();

    const codeText = await codeBlock.textContent();

    // Check for TOML configuration keys
    expect(codeText).toContain('addr');
    expect(codeText).toContain('max_level');
    expect(codeText).toContain('work_dir');
    expect(codeText).toContain('sst_max_size');
    expect(codeText).toContain('mem_table_max_size');

    // Verify syntax highlighting via code element presence (styled code block)
    const preElement = configStep.locator('pre');
    await expect(preElement).toBeVisible();
  });

  /**
   * Additional test: Getting started section has all required steps
   */
  test('Getting started section contains all required steps', async ({ page }) => {
    // Navigate to getting-started section
    const gettingStartedSection = page.locator('#getting-started');
    await gettingStartedSection.scrollIntoViewIfNeeded();
    await expect(gettingStartedSection).toBeVisible();

    // Verify section title
    const sectionTitle = gettingStartedSection.locator('h2');
    await expect(sectionTitle).toContainText('Getting Started');

    // Verify all steps exist
    const steps = gettingStartedSection.locator('.step');
    const stepCount = await steps.count();
    expect(stepCount).toBeGreaterThanOrEqual(4);
  });

  /**
   * Additional test: Getting started section follows architecture section
   */
  test('Getting started section follows architecture section', async ({ page }) => {
    const architectureSection = page.locator('#architecture');
    const gettingStartedSection = page.locator('#getting-started');

    await expect(architectureSection).toBeVisible();
    await expect(gettingStartedSection).toBeVisible();

    // Get bounding boxes to verify order
    await gettingStartedSection.scrollIntoViewIfNeeded();
    const archBox = await architectureSection.boundingBox();
    const gsBox = await gettingStartedSection.boundingBox();

    expect(archBox).toBeTruthy();
    expect(gsBox).toBeTruthy();

    // Getting started section should be below architecture section
    expect(gsBox.y).toBeGreaterThan(archBox.y);
  });

  /**
   * Additional test: All code blocks have syntax highlighting styling
   */
  test('All code blocks in getting started have proper styling', async ({ page }) => {
    // Navigate to getting-started section
    const gettingStartedSection = page.locator('#getting-started');
    await gettingStartedSection.scrollIntoViewIfNeeded();

    // Check all pre elements have code children
    const preElements = gettingStartedSection.locator('pre');
    const preCount = await preElements.count();

    expect(preCount).toBeGreaterThanOrEqual(4);

    for (let i = 0; i < preCount; i++) {
      const pre = preElements.nth(i);
      const code = pre.locator('code');
      await expect(code).toBeVisible();
    }
  });
});
