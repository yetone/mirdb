const { test, expect } = require('@playwright/test');

test.describe('Installation Instructions', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Cargo build --release command is displayed', async ({ page }) => {
    // Navigate to quick start section where installation instructions are
    const quickStartSection = page.locator('#quickstart');
    await quickStartSection.scrollIntoViewIfNeeded();
    await expect(quickStartSection).toBeVisible();

    // Find the Installation heading
    const installationHeading = quickStartSection.locator('h3', { hasText: 'Installation' });
    await expect(installationHeading).toBeVisible();

    // Find the code blocks in the quickstart section
    const codeBlocks = quickStartSection.locator('.code-block');
    const firstCodeBlock = codeBlocks.first();
    await expect(firstCodeBlock).toBeVisible();

    // Verify the cargo build --release command is present
    const codeText = await firstCodeBlock.textContent();
    expect(codeText).toContain('cargo build --release');
  });

  test('TC2: Run command ./mirdb -c /path/to/config.toml is displayed', async ({ page }) => {
    // Navigate to quick start section
    const quickStartSection = page.locator('#quickstart');
    await quickStartSection.scrollIntoViewIfNeeded();
    await expect(quickStartSection).toBeVisible();

    // Find the Running MirDB heading
    const runningHeading = quickStartSection.locator('h3', { hasText: 'Running MirDB' });
    await expect(runningHeading).toBeVisible();

    // Find all code blocks and look for the run command
    const codeBlocks = quickStartSection.locator('.code-block');
    let foundRunCommand = false;

    const count = await codeBlocks.count();
    for (let i = 0; i < count; i++) {
      const codeText = await codeBlocks.nth(i).textContent();
      if (codeText.includes('./mirdb -c /path/to/config.toml')) {
        foundRunCommand = true;
        break;
      }
    }

    expect(foundRunCommand).toBeTruthy();
  });

  test('TC3: Installation commands can be copied directly from the page', async ({ page }) => {
    // Navigate to quick start section
    const quickStartSection = page.locator('#quickstart');
    await quickStartSection.scrollIntoViewIfNeeded();
    await expect(quickStartSection).toBeVisible();

    // Get all code blocks in the quickstart section
    const codeBlocks = quickStartSection.locator('.code-block');
    const count = await codeBlocks.count();

    // Ensure there are code blocks present
    expect(count).toBeGreaterThan(0);

    // Check the first code block (installation command)
    const installCodeBlock = codeBlocks.first();
    await expect(installCodeBlock).toBeVisible();

    // Get the code element inside the block
    const codeElement = installCodeBlock.locator('code');
    await expect(codeElement).toBeVisible();

    // Verify the code block contains the cargo build command
    const codeText = await codeElement.textContent();
    expect(codeText).toContain('cargo build --release');

    // Verify the code block is properly styled for copy-paste
    // Check that the code block has monospace font
    const fontFamily = await installCodeBlock.evaluate(el =>
      window.getComputedStyle(el).fontFamily
    );
    // The code block should inherit monospace from the code element

    // Check that the code element uses monospace font
    const codeFontFamily = await codeElement.evaluate(el =>
      window.getComputedStyle(el).fontFamily
    );
    expect(codeFontFamily.toLowerCase()).toMatch(/monaco|consolas|monospace/);

    // Verify the code is selectable (not disabled or hidden)
    const userSelect = await codeElement.evaluate(el =>
      window.getComputedStyle(el).userSelect
    );
    // userSelect should not be 'none' to allow copying
    expect(userSelect).not.toBe('none');

    // Check the second code block (run command)
    const runCodeBlock = codeBlocks.nth(1);
    await expect(runCodeBlock).toBeVisible();
    const runCodeText = await runCodeBlock.textContent();
    expect(runCodeText).toContain('./mirdb -c /path/to/config.toml');
  });
});
