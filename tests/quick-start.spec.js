// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Quick Start Section E2E Tests (REQ-3)
 * Verifies installation and usage instructions with code examples
 */

test.describe('Quick Start Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Installation instructions are displayed with proper formatting', async ({ page }) => {
    // Navigate to Quick Start section
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Check for installation heading
    const installationHeading = quickStartSection.locator('h3').filter({ hasText: /install/i });
    await expect(installationHeading).toBeVisible();

    // Check for cargo install command or binary installation
    const installationCode = quickStartSection.locator('pre code').first();
    await expect(installationCode).toBeVisible();

    // Verify installation commands are present (cargo or binary method)
    const installText = await installationCode.textContent();
    expect(installText).toMatch(/cargo|install|mirdb/i);
  });

  test('TC2: TOML configuration example is displayed', async ({ page }) => {
    // Navigate to Quick Start section
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Look for TOML configuration example
    const configBlock = quickStartSection.locator('pre code').filter({ hasText: /addr\s*=|work_dir\s*=|toml/i });
    await expect(configBlock.first()).toBeVisible();

    // Verify TOML format with key configuration parameters
    const configText = await configBlock.first().textContent();
    expect(configText).toMatch(/addr\s*=/);
    expect(configText).toMatch(/work_dir\s*=/);
  });

  test('TC3: Server start command example is displayed', async ({ page }) => {
    // Navigate to Quick Start section
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Look for server start command
    const codeBlocks = quickStartSection.locator('pre code');
    const count = await codeBlocks.count();

    let foundServerCommand = false;
    for (let i = 0; i < count; i++) {
      const text = await codeBlocks.nth(i).textContent();
      if (text && (text.includes('mirdb') || text.includes('./mirdb') || text.includes('cargo run'))) {
        foundServerCommand = true;
        break;
      }
    }

    expect(foundServerCommand).toBe(true);
  });

  test('TC4: Client connection example is displayed', async ({ page }) => {
    // Navigate to Quick Start section
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Look for client connection example (telnet, nc, or memcached client)
    const codeBlocks = quickStartSection.locator('pre code');
    const count = await codeBlocks.count();

    let foundClientExample = false;
    for (let i = 0; i < count; i++) {
      const text = await codeBlocks.nth(i).textContent();
      if (text && (text.includes('telnet') || text.includes('nc ') || text.includes('12333') || text.includes('set ') || text.includes('get '))) {
        foundClientExample = true;
        break;
      }
    }

    expect(foundClientExample).toBe(true);
  });

  test('TC5: Code blocks have syntax highlighting', async ({ page }) => {
    // Navigate to Quick Start section
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Check that code blocks have syntax highlighting classes/styles
    const codeBlocks = quickStartSection.locator('pre code');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThan(0);

    // Check for syntax highlighting indicators (class or style)
    const firstCodeBlock = codeBlocks.first();
    await expect(firstCodeBlock).toBeVisible();

    // Check if parent pre or the code element has highlighting class
    const preElement = quickStartSection.locator('pre').first();
    const preClass = await preElement.getAttribute('class');
    const codeClass = await firstCodeBlock.getAttribute('class');

    // Syntax highlighting typically adds language-* or hljs classes
    const hasHighlighting =
      (preClass && (preClass.includes('language-') || preClass.includes('hljs') || preClass.includes('highlight'))) ||
      (codeClass && (codeClass.includes('language-') || codeClass.includes('hljs') || codeClass.includes('highlight')));

    expect(hasHighlighting).toBe(true);
  });

  test('Quick Start section is navigable from navigation menu', async ({ page }) => {
    // Click on Quick Start navigation link
    const navLink = page.locator('nav a[href="#quick-start"]');
    await expect(navLink).toBeVisible();
    await navLink.click();

    // Verify section is scrolled into view
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeInViewport();
  });
});
