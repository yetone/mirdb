import { test, expect } from '@playwright/test';

test.describe('Quick Start Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Quick Start or Getting Started section exists on the page', async ({ page }) => {
    // Query for quick-start section heading
    // Expected: Quick Start or Getting Started section exists on the page
    const quickStartSection = page.locator('#quickstart, #quick-start, #getting-started, .quick-start-section, .quickstart-section');
    await expect(quickStartSection).toBeVisible();

    // Check for a heading that contains "Quick Start" or "Getting Started"
    const sectionHeading = quickStartSection.locator('h2, h3').first();
    await expect(sectionHeading).toBeVisible();

    const headingText = await sectionHeading.textContent();
    const hasQuickStartHeading = headingText?.toLowerCase().includes('quick start') ||
                                  headingText?.toLowerCase().includes('getting started');
    expect(hasQuickStartHeading).toBe(true);
  });

  test('TC2: Installation commands or instructions are visible', async ({ page }) => {
    // Check for installation instructions
    // Expected: Installation commands or instructions are visible
    const quickStartSection = page.locator('#quickstart, #quick-start, #getting-started, .quick-start-section, .quickstart-section');
    await expect(quickStartSection).toBeVisible();

    // Check for installation-related content
    const installationContent = quickStartSection.locator('text=/install|installation|cargo|clone|build/i');
    await expect(installationContent.first()).toBeVisible();

    // Check for code blocks containing installation commands
    const codeBlocks = quickStartSection.locator('pre, code');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);
  });

  test('TC3: Code blocks with syntax highlighting are present', async ({ page }) => {
    // Query for code block elements
    // Expected: Code blocks with syntax highlighting are present
    const quickStartSection = page.locator('#quickstart, #quick-start, #getting-started, .quick-start-section, .quickstart-section');
    await expect(quickStartSection).toBeVisible();

    // Check for code blocks
    const codeBlocks = quickStartSection.locator('pre code, pre.code-block, .code-block');
    await expect(codeBlocks.first()).toBeVisible();

    // Verify syntax highlighting is applied (code blocks should have specific classes or styled spans)
    const highlightedCode = quickStartSection.locator('pre code .keyword, pre code .string, pre code .comment, pre .token, code[class*="language"], pre[class*="language"], .syntax-highlight, .hljs');
    const hasHighlighting = await highlightedCode.count() > 0 ||
                            await quickStartSection.locator('pre code span[style], pre code span[class]').count() > 0;
    expect(hasHighlighting).toBe(true);
  });

  test('TC4: Basic usage examples with common Memcached commands (get, set) are shown', async ({ page }) => {
    // Check for Memcached command examples
    // Expected: Basic usage examples with common Memcached commands (get, set) are shown
    const quickStartSection = page.locator('#quickstart, #quick-start, #getting-started, .quick-start-section, .quickstart-section');
    await expect(quickStartSection).toBeVisible();

    // Check for 'set' command example
    const setCommandContent = await quickStartSection.textContent();
    expect(setCommandContent?.toLowerCase()).toContain('set');

    // Check for 'get' command example
    expect(setCommandContent?.toLowerCase()).toContain('get');

    // Verify commands are in code blocks
    const codeBlocks = quickStartSection.locator('pre, code');
    const codeContent = await codeBlocks.allTextContents();
    const combinedCode = codeContent.join(' ').toLowerCase();

    expect(combinedCode).toContain('set');
    expect(combinedCode).toContain('get');
  });

  test('TC5: Code blocks have syntax highlighting applied (different colors for keywords, strings, etc.)', async ({ page }) => {
    // Verify code block styling
    // Expected: Code blocks have syntax highlighting applied (different colors for keywords, strings, etc.)
    const quickStartSection = page.locator('#quickstart, #quick-start, #getting-started, .quick-start-section, .quickstart-section');
    await expect(quickStartSection).toBeVisible();

    // Find code blocks
    const codeBlocks = quickStartSection.locator('pre code');
    await expect(codeBlocks.first()).toBeVisible();

    // Check for styled spans within code blocks (syntax highlighting creates spans with different colors)
    const styledSpans = quickStartSection.locator('pre code span');
    const spanCount = await styledSpans.count();
    expect(spanCount).toBeGreaterThan(0);

    // Verify different colors are applied by checking for different classes or inline styles
    const firstSpan = styledSpans.first();
    const hasClass = await firstSpan.getAttribute('class');
    const hasStyle = await firstSpan.getAttribute('style');

    // At least one span should have a class or style attribute for syntax highlighting
    const hasHighlightingStyles = hasClass !== null || hasStyle !== null;
    expect(hasHighlightingStyles).toBe(true);
  });
});
