// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Getting Started Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: should display server startup example', async ({ page }) => {
    // Navigate to getting started section
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Check for server startup command example
    const codeBlocks = gettingStartedSection.locator('pre, code');
    const codeContent = await codeBlocks.allTextContents();
    const combinedContent = codeContent.join(' ');

    // Verify startup command is present (mirdb -c /path/to/config.toml or similar)
    expect(combinedContent).toMatch(/mirdb\s+-c\s+.*config\.toml|mirdb.*config/i);
  });

  test('TC2: should display client connection example', async ({ page }) => {
    // Navigate to getting started section
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Check for memcached client connection example
    const codeBlocks = gettingStartedSection.locator('pre, code');
    const codeContent = await codeBlocks.allTextContents();
    const combinedContent = codeContent.join(' ');

    // Verify telnet localhost 12333 or similar client connection is present
    expect(combinedContent).toMatch(/telnet\s+localhost\s+12333|memcached.*client|nc\s+localhost\s+12333/i);
  });

  test('TC3: should display SET/GET command examples', async ({ page }) => {
    // Navigate to getting started section
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Check for SET and GET command examples
    const codeBlocks = gettingStartedSection.locator('pre, code');
    const codeContent = await codeBlocks.allTextContents();
    const combinedContent = codeContent.join(' ');

    // Verify SET command is present
    expect(combinedContent).toMatch(/set\s+\w+/i);
    // Verify GET command is present
    expect(combinedContent).toMatch(/get\s+\w+/i);
    // Verify expected output like STORED or VALUE is present
    expect(combinedContent).toMatch(/STORED|VALUE/);
  });

  test('TC4: should have properly formatted code blocks', async ({ page }) => {
    // Navigate to getting started section
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Check that code blocks exist and have proper styling
    const codeBlocks = gettingStartedSection.locator('pre');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThan(0);

    // Verify code blocks have monospace font or code styling
    for (let i = 0; i < count; i++) {
      const block = codeBlocks.nth(i);
      const fontFamily = await block.evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });
      // Check for monospace font indicators
      expect(fontFamily.toLowerCase()).toMatch(/mono|courier|consolas|code/i);
    }
  });

  test('TC5: should display installation instructions', async ({ page }) => {
    // Navigate to getting started section
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Check for installation or build instructions
    const sectionContent = await gettingStartedSection.textContent();

    // Verify installation instructions are present (cargo build, git clone, or download instructions)
    const hasInstallation =
      /cargo\s+build|cargo\s+install|git\s+clone|download|install|build/i.test(sectionContent);

    expect(hasInstallation).toBe(true);
  });
});
