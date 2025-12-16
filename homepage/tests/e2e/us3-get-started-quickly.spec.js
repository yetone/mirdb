// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for User Story US-3: Get Started Quickly
 *
 * User Story:
 * > As a Developer, I want to find installation and usage instructions,
 * > so that I can try MirDB in my development environment.
 *
 * Acceptance Criteria:
 * - Given I am interested in using MirDB
 * - When I look for getting started information
 * - Then I find clear installation commands or links to documentation
 * - And I see code examples showing basic Memcached protocol usage
 *
 * Related Requirements: REQ-3, REQ-4, REQ-6
 */

test.describe('US-3: Get Started Quickly', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage before each test
    await page.goto('/');
  });

  /**
   * Test Case 1: Check for clear installation commands
   * Input: Check for clear installation commands
   * Expected: Installation commands or clear link to installation docs is present
   */
  test('TC1: Installation commands or clear link to installation docs is present', async ({ page }) => {
    // Step 1: Locate getting started information
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');

    // Scroll to the Getting Started section
    await gettingStartedSection.scrollIntoViewIfNeeded();
    await expect(gettingStartedSection).toBeVisible();

    // Step 2: Follow installation flow - verify installation section exists
    const installationSection = page.locator('[data-testid="installation-section"]');
    await expect(installationSection).toBeVisible();

    // Verify installation title is present
    const installationTitle = page.locator('[data-testid="installation-title"]');
    await expect(installationTitle).toBeVisible();
    await expect(installationTitle).toContainText('Installation');

    // Verify installation code block with commands exists
    const installationCode = page.locator('[data-testid="installation-code"]');
    await expect(installationCode).toBeVisible();

    // Verify the installation instructions contain clear commands
    const codeContent = await installationCode.textContent();

    // Must have git clone command
    expect(codeContent).toContain('git clone');
    expect(codeContent).toContain('mirdb');

    // Must have build command
    expect(codeContent).toContain('cargo build');

    // Must have run command
    expect(codeContent).toContain('mirdb-server');

    // Verify documentation links are present as alternative/supplement
    const docsLinksSection = page.locator('[data-testid="documentation-links"]');
    await expect(docsLinksSection).toBeVisible();

    const docsLink = page.locator('[data-testid="docs-link"]');
    await expect(docsLink).toBeVisible();

    const href = await docsLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toContain('github.com');
    expect(href).toContain('mirdb');
  });

  /**
   * Test Case 2: Check for Memcached protocol usage examples
   * Input: Check for Memcached protocol usage examples
   * Expected: Code examples showing get, set, delete operations are present
   */
  test('TC2: Code examples showing get, set, delete operations are present', async ({ page }) => {
    // Step 3: Find code examples - navigate to code examples section
    const codeExamplesSection = page.locator('[data-testid="code-examples-section"]');
    await codeExamplesSection.scrollIntoViewIfNeeded();
    await expect(codeExamplesSection).toBeVisible();

    // Verify section title mentions Memcached
    const sectionTitle = page.locator('[data-testid="code-examples-title"]');
    await expect(sectionTitle).toBeVisible();
    const titleText = await sectionTitle.textContent();
    expect(titleText.toLowerCase()).toContain('memcached');

    // Verify GET command example is present
    const getCommandCard = page.locator('[data-testid="command-card-get"]');
    await expect(getCommandCard).toBeVisible();

    const getCommandTitle = page.locator('[data-testid="command-title-get"]');
    await expect(getCommandTitle).toBeVisible();
    const getTitleText = await getCommandTitle.textContent();
    expect(getTitleText.toLowerCase()).toContain('get');

    const getCode = page.locator('[data-testid="code-get"]');
    await expect(getCode).toBeVisible();
    const getCodeText = await getCode.textContent();
    expect(getCodeText).toContain('get');

    // Verify SET command example is present
    const setCommandCard = page.locator('[data-testid="command-card-set"]');
    await expect(setCommandCard).toBeVisible();

    const setCommandTitle = page.locator('[data-testid="command-title-set"]');
    await expect(setCommandTitle).toBeVisible();
    const setTitleText = await setCommandTitle.textContent();
    expect(setTitleText.toLowerCase()).toContain('set');

    const setCode = page.locator('[data-testid="code-set"]');
    await expect(setCode).toBeVisible();
    const setCodeText = await setCode.textContent();
    expect(setCodeText).toContain('set');

    // Verify DELETE command example is present
    const deleteCommandCard = page.locator('[data-testid="command-card-delete"]');
    await expect(deleteCommandCard).toBeVisible();

    const deleteCommandTitle = page.locator('[data-testid="command-title-delete"]');
    await expect(deleteCommandTitle).toBeVisible();
    const deleteTitleText = await deleteCommandTitle.textContent();
    expect(deleteTitleText.toLowerCase()).toContain('delete');

    const deleteCode = page.locator('[data-testid="code-delete"]');
    await expect(deleteCode).toBeVisible();
    const deleteCodeText = await deleteCode.textContent();
    expect(deleteCodeText).toContain('delete');

    // Verify descriptions provide context for each command
    const getDesc = page.locator('[data-testid="command-desc-get"]');
    await expect(getDesc).toBeVisible();
    expect(await getDesc.textContent()).toBeTruthy();

    const setDesc = page.locator('[data-testid="command-desc-set"]');
    await expect(setDesc).toBeVisible();
    expect(await setDesc.textContent()).toBeTruthy();

    const deleteDesc = page.locator('[data-testid="command-desc-delete"]');
    await expect(deleteDesc).toBeVisible();
    expect(await deleteDesc.textContent()).toBeTruthy();
  });

  /**
   * Test Case 3: Check 'Get Started' CTA functionality
   * Input: Check 'Get Started' CTA functionality
   * Expected: 'Get Started' button navigates to installation/documentation section
   */
  test('TC3: Get Started button navigates to installation/documentation section', async ({ page }) => {
    // Verify the Get Started button exists in the hero section
    const getStartedBtn = page.locator('[data-testid="get-started-btn"]');
    await expect(getStartedBtn).toBeVisible();

    // Verify button text
    await expect(getStartedBtn).toContainText('Get Started');

    // Verify the button links to the getting-started section
    const href = await getStartedBtn.getAttribute('href');
    expect(href).toBe('#getting-started');

    // Click the Get Started button
    await getStartedBtn.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(500);

    // Verify the getting started section is now in viewport
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeInViewport();

    // Verify the installation section is visible after navigation
    const installationSection = page.locator('[data-testid="installation-section"]');
    await expect(installationSection).toBeVisible();

    // Verify documentation links are visible after navigation
    const docsLinksSection = page.locator('[data-testid="documentation-links"]');
    await expect(docsLinksSection).toBeVisible();
  });
});
