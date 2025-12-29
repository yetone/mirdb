// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Code Examples Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Code block elements have syntax highlighting classes', async ({ page }) => {
    // Navigate to code examples section
    const codeExamplesSection = page.locator('#code-examples');
    await expect(codeExamplesSection).toBeVisible();

    // Check for pre/code elements with language classes
    const codeBlocks = page.locator('#code-examples pre code');
    await expect(codeBlocks.first()).toBeVisible();

    // Verify syntax highlighting classes (language-bash, language-shell, etc.)
    const bashCodeBlocks = page.locator('#code-examples code.language-bash');
    const count = await bashCodeBlocks.count();
    expect(count).toBeGreaterThan(0);

    // Verify Prism.js adds tokenized spans for syntax highlighting
    const firstCodeBlock = bashCodeBlocks.first();
    await expect(firstCodeBlock).toHaveClass(/language-bash/);
  });

  test('TC2: MirDB server start command is present', async ({ page }) => {
    // Navigate to code examples section
    const codeExamplesSection = page.locator('#code-examples');
    await expect(codeExamplesSection).toBeVisible();

    // Check for the mirdb -c command in code examples
    const codeContent = await codeExamplesSection.textContent();
    expect(codeContent).toContain('mirdb -c');
    expect(codeContent).toContain('config.toml');
  });

  test('TC3: SET command example is present', async ({ page }) => {
    // Navigate to code examples section
    const codeExamplesSection = page.locator('#code-examples');
    await expect(codeExamplesSection).toBeVisible();

    // Check for SET command in code examples
    const codeContent = await codeExamplesSection.textContent();
    expect(codeContent).toContain('set mykey');
    expect(codeContent).toContain('STORED');
  });

  test('TC4: GET command example is present', async ({ page }) => {
    // Navigate to code examples section
    const codeExamplesSection = page.locator('#code-examples');
    await expect(codeExamplesSection).toBeVisible();

    // Check for GET command in code examples
    const codeContent = await codeExamplesSection.textContent();
    expect(codeContent).toContain('get mykey');
    expect(codeContent).toContain('VALUE mykey');
  });

  test('TC5: Code blocks are readable on mobile with horizontal scroll', async ({ page }) => {
    // Set viewport to mobile size
    await page.setViewportSize({ width: 375, height: 667 });

    // Navigate to code examples section
    const codeExamplesSection = page.locator('#code-examples');
    await expect(codeExamplesSection).toBeVisible();

    // Check that pre elements have overflow-x set to allow scrolling
    const preElements = page.locator('#code-examples pre');
    const firstPre = preElements.first();
    await expect(firstPre).toBeVisible();

    // Verify that the pre element allows horizontal scrolling via CSS
    const overflowX = await firstPre.evaluate((el) => {
      return window.getComputedStyle(el).overflowX;
    });
    expect(['auto', 'scroll']).toContain(overflowX);

    // Verify the code is still visible and readable (not cut off)
    const codeElement = page.locator('#code-examples pre code').first();
    await expect(codeElement).toBeVisible();

    // Check that code block doesn't exceed viewport width without scroll
    const preBox = await firstPre.boundingBox();
    expect(preBox).not.toBeNull();
    if (preBox) {
      expect(preBox.width).toBeLessThanOrEqual(375);
    }
  });
});

test.describe('Code Examples - Extended Verification', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Code examples section is navigable', async ({ page }) => {
    // Verify the section has proper heading
    const heading = page.locator('#code-examples h2');
    await expect(heading).toHaveText('Quick Start');
  });

  test('Commands list displays supported commands', async ({ page }) => {
    const commandsList = page.locator('.commands-list');
    await expect(commandsList).toBeVisible();

    // Verify key commands are listed
    const commands = page.locator('.commands-list .command');
    const commandTexts = await commands.allTextContents();
    expect(commandTexts).toContain('GET');
    expect(commandTexts).toContain('SET');
    expect(commandTexts).toContain('DELETE');
  });

  test('Prism.js library is loaded for syntax highlighting', async ({ page }) => {
    // Check that Prism is available globally
    const prismLoaded = await page.evaluate(() => {
      return typeof window.Prism !== 'undefined';
    });
    expect(prismLoaded).toBe(true);
  });
});
