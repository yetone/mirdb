// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Quick Start Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: should display cargo install mirdb command', async ({ page }) => {
    // Navigate to quick start section
    const quickStartSection = page.locator('#quick-start');
    await quickStartSection.scrollIntoViewIfNeeded();

    // Check for installation command
    const codeBlocks = quickStartSection.locator('pre code');
    const allCode = await codeBlocks.allTextContents();
    const combinedCode = allCode.join('\n');

    expect(combinedCode).toContain('cargo install mirdb');
  });

  test('TC2: should display server start command', async ({ page }) => {
    // Navigate to quick start section
    const quickStartSection = page.locator('#quick-start');
    await quickStartSection.scrollIntoViewIfNeeded();

    // Check for server start command
    const codeBlocks = quickStartSection.locator('pre code');
    const allCode = await codeBlocks.allTextContents();
    const combinedCode = allCode.join('\n');

    expect(combinedCode).toContain('mirdb -c /path/to/config.toml');
  });

  test('TC3: should display SET command example', async ({ page }) => {
    // Navigate to quick start section
    const quickStartSection = page.locator('#quick-start');
    await quickStartSection.scrollIntoViewIfNeeded();

    // Check for SET command example
    const codeBlocks = quickStartSection.locator('pre code');
    const allCode = await codeBlocks.allTextContents();
    const combinedCode = allCode.join('\n');

    // Check for the SET command format: set mykey 0 0 5 followed by a value
    expect(combinedCode).toContain('set mykey 0 0 5');
    expect(combinedCode).toContain('hello');
  });

  test('TC4: should display GET command example', async ({ page }) => {
    // Navigate to quick start section
    const quickStartSection = page.locator('#quick-start');
    await quickStartSection.scrollIntoViewIfNeeded();

    // Check for GET command example
    const codeBlocks = quickStartSection.locator('pre code');
    const allCode = await codeBlocks.allTextContents();
    const combinedCode = allCode.join('\n');

    expect(combinedCode).toContain('get mykey');
  });

  test('TC5: should have syntax highlighting on code blocks', async ({ page }) => {
    // Navigate to quick start section
    const quickStartSection = page.locator('#quick-start');
    await quickStartSection.scrollIntoViewIfNeeded();

    // Wait for highlight.js to process the code blocks
    await page.waitForFunction(() => {
      const codeElements = document.querySelectorAll('#quick-start pre code');
      return codeElements.length > 0 && Array.from(codeElements).some(el => el.classList.contains('hljs'));
    }, { timeout: 5000 });

    // Check that code blocks have syntax highlighting classes applied
    const highlightedBlocks = quickStartSection.locator('pre code.hljs');
    const count = await highlightedBlocks.count();

    expect(count).toBeGreaterThan(0);

    // Verify at least one code block has language-specific class
    const codeBlock = highlightedBlocks.first();
    const classAttribute = await codeBlock.getAttribute('class');
    expect(classAttribute).toContain('hljs');
  });

  test('TC6: should have documentation link in quick start section', async ({ page }) => {
    // Navigate to quick start section
    const quickStartSection = page.locator('#quick-start');
    await quickStartSection.scrollIntoViewIfNeeded();

    // Check for documentation link
    const docsLink = quickStartSection.locator('a[href*="github.com/yetone/mirdb"]');
    await expect(docsLink.first()).toBeVisible();

    // Verify the link points to documentation
    const href = await docsLink.first().getAttribute('href');
    expect(href).toContain('github.com/yetone/mirdb');
  });

  test('navigation link to quick start should exist', async ({ page }) => {
    // Check that navigation has a link to quick start
    const navLink = page.locator('nav a[href="#quick-start"]');
    await expect(navLink).toBeVisible();
  });
});
