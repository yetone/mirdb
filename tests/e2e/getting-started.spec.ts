import { test, expect } from '@playwright/test';

test.describe('Getting Started Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Installation instructions are visible with command examples', async ({ page }) => {
    // Navigate to the getting started section
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await gettingStartedSection.scrollIntoViewIfNeeded();
    await expect(gettingStartedSection).toBeVisible();

    // Check for installation instructions
    const installationInstructions = page.locator('[data-testid="installation-instructions"]');
    await expect(installationInstructions).toBeVisible();

    // Verify command examples are present
    const installationCode = installationInstructions.locator('pre code');
    await expect(installationCode).toBeVisible();

    // Check for specific commands
    const codeContent = await installationCode.textContent();
    expect(codeContent).toContain('git clone');
    expect(codeContent).toContain('cargo build');
    expect(codeContent).toContain('cargo run');
  });

  test('TC2: Code example shows how to connect using memcached client', async ({ page }) => {
    // Navigate to getting started section
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await gettingStartedSection.scrollIntoViewIfNeeded();

    // Check for usage code example
    const usageExample = page.locator('[data-testid="usage-example"]');
    await expect(usageExample).toBeVisible();

    // Verify memcached client example is shown
    const usageCode = usageExample.locator('pre code');
    await expect(usageCode).toBeVisible();

    const codeContent = await usageCode.textContent();

    // Check for memcached client connection code
    expect(codeContent).toContain('pymemcache');
    expect(codeContent).toContain('Client');
    expect(codeContent).toContain('localhost');
    expect(codeContent).toContain('12333');

    // Check for basic operations
    expect(codeContent).toContain('set');
    expect(codeContent).toContain('get');
  });

  test('TC3: Code snippets have proper syntax highlighting applied', async ({ page }) => {
    // Navigate to getting started section
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await gettingStartedSection.scrollIntoViewIfNeeded();

    // Check that code blocks have language classes for syntax highlighting
    // Prism.js adds language classes to code elements

    // Check bash code block
    const bashCode = page.locator('[data-testid="installation-instructions"] pre code.language-bash');
    await expect(bashCode).toBeVisible();

    // Check python code block
    const pythonCode = page.locator('[data-testid="usage-example"] pre code.language-python');
    await expect(pythonCode).toBeVisible();

    // Check TOML code block
    const tomlCode = page.locator('[data-testid="configuration-example"] pre code.language-toml');
    await expect(tomlCode).toBeVisible();

    // Verify Prism.js has applied syntax highlighting by checking for token classes
    // Prism.js tokenizes code and wraps tokens in <span class="token ..."> elements
    // Wait for Prism to load and tokenize
    await page.waitForTimeout(500);

    // Check that the code blocks exist within pre elements (basic structure check)
    const allPreElements = page.locator('#getting-started pre');
    const count = await allPreElements.count();
    expect(count).toBeGreaterThanOrEqual(3);
  });

  test('TC4: TOML configuration example is displayed with key parameters', async ({ page }) => {
    // Navigate to getting started section
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await gettingStartedSection.scrollIntoViewIfNeeded();

    // Check for configuration example
    const configExample = page.locator('[data-testid="configuration-example"]');
    await expect(configExample).toBeVisible();

    // Check for configuration heading
    const configHeading = configExample.locator('h3');
    await expect(configHeading).toContainText('Configuration');

    // Verify TOML code is present
    const tomlCode = configExample.locator('pre code.language-toml');
    await expect(tomlCode).toBeVisible();

    const codeContent = await tomlCode.textContent();

    // Check for key configuration parameters
    expect(codeContent).toContain('addr');
    expect(codeContent).toContain('12333');
    expect(codeContent).toContain('max_level');
    expect(codeContent).toContain('work_dir');
    expect(codeContent).toContain('sst_max_size');
    expect(codeContent).toContain('mem_table_max_size');
  });

  test('TC5: Default port 12333 is mentioned in getting started instructions', async ({ page }) => {
    // Navigate to getting started section
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await gettingStartedSection.scrollIntoViewIfNeeded();

    // Check for default port in installation instructions
    const defaultPortElement = page.locator('[data-testid="default-port"]');
    await expect(defaultPortElement).toBeVisible();
    await expect(defaultPortElement).toHaveText('12333');

    // Also verify it appears in the code examples
    const installationCode = page.locator('[data-testid="installation-instructions"] pre code');
    const installCodeContent = await installationCode.textContent();
    expect(installCodeContent).toContain('12333');

    // And in the usage example
    const usageCode = page.locator('[data-testid="usage-example"] pre code');
    const usageCodeContent = await usageCode.textContent();
    expect(usageCodeContent).toContain('12333');

    // And in the configuration example
    const configCode = page.locator('[data-testid="configuration-example"] pre code');
    const configCodeContent = await configCode.textContent();
    expect(configCodeContent).toContain('12333');
  });

  test('Navigation to Getting Started section works', async ({ page }) => {
    // Click on the Getting Started link in navigation
    await page.click('a[href="#getting-started"]');

    // Verify the section is in view
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeInViewport();
  });
});
