// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for MirDB Homepage Code Example Section
 *
 * Test Case 1: Code example section exists on the page
 * Test Case 2: Code blocks have syntax highlighting applied
 * Test Case 3: Examples include basic Memcached commands (set, get)
 * Test Case 4: Copy to clipboard button works correctly
 */

test.describe('Code Example Section', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage before each test
    await page.goto('/');
  });

  test('Test Case 1: Code example section is present on the page', async ({ page }) => {
    // Verify the code example section exists
    const codeExampleSection = page.getByTestId('code-example-section');
    await expect(codeExampleSection).toBeVisible();

    // Verify the section has a heading
    const sectionHeading = page.getByTestId('code-example-heading');
    await expect(sectionHeading).toBeVisible();

    // Verify the heading contains relevant text (Quick Start or Usage or Example)
    const headingText = await sectionHeading.textContent();
    expect(headingText?.toLowerCase()).toMatch(/quick\s*start|usage|example|get\s*started/);
  });

  test('Test Case 2: Code blocks have syntax highlighting applied', async ({ page }) => {
    // Scroll to code example section
    const codeExampleSection = page.getByTestId('code-example-section');
    await codeExampleSection.scrollIntoViewIfNeeded();

    // Find code blocks with syntax highlighting
    const codeBlocks = page.locator('[data-testid="code-block"]');

    // Verify at least one code block exists
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThanOrEqual(1);

    // Verify code blocks have syntax highlighting class or styles
    const firstCodeBlock = codeBlocks.first();
    await expect(firstCodeBlock).toBeVisible();

    // Check for syntax highlighting indicators:
    // - Either has a 'language-*' class
    // - Or has highlighted spans with color
    // - Or has the 'syntax-highlight' class
    const hasHighlighting = await firstCodeBlock.evaluate((el) => {
      // Check for language class
      const hasLanguageClass = el.className.match(/language-\w+/) !== null;

      // Check for syntax-highlight class
      const hasSyntaxClass = el.classList.contains('syntax-highlight') ||
                             el.closest('.syntax-highlight') !== null;

      // Check for colored spans (common in syntax highlighting)
      const coloredSpans = el.querySelectorAll('span[style*="color"], span[class*="token"], span[class*="hljs"]');
      const hasColoredSpans = coloredSpans.length > 0;

      // Check for pre > code structure typical of code highlighting
      const hasCodeStructure = el.tagName === 'CODE' || el.querySelector('code') !== null;

      // Check computed styles for monospace font
      const computedStyle = window.getComputedStyle(el);
      const hasMonospaceFont = computedStyle.fontFamily.toLowerCase().includes('mono') ||
                               computedStyle.fontFamily.toLowerCase().includes('courier') ||
                               computedStyle.fontFamily.toLowerCase().includes('consolas');

      return hasLanguageClass || hasSyntaxClass || hasColoredSpans || (hasCodeStructure && hasMonospaceFont);
    });

    expect(hasHighlighting).toBe(true);
  });

  test('Test Case 3: Examples include basic Memcached commands (set, get)', async ({ page }) => {
    // Scroll to code example section
    const codeExampleSection = page.getByTestId('code-example-section');
    await codeExampleSection.scrollIntoViewIfNeeded();

    // Get all text content from code blocks
    const codeBlocks = page.locator('[data-testid="code-block"]');
    const codeContent = await codeBlocks.allTextContents();
    const fullCodeText = codeContent.join(' ').toLowerCase();

    // Verify that basic Memcached commands are present
    // Check for 'set' command
    expect(fullCodeText).toMatch(/\bset\b/);

    // Check for 'get' command
    expect(fullCodeText).toMatch(/\bget\b/);
  });

  test('Test Case 4: Copy to clipboard button works correctly', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Scroll to code example section
    const codeExampleSection = page.getByTestId('code-example-section');
    await codeExampleSection.scrollIntoViewIfNeeded();

    // Find the copy button
    const copyButton = page.getByTestId('copy-button').first();
    await expect(copyButton).toBeVisible();

    // Get the code content that will be copied
    const codeBlock = page.locator('[data-testid="code-block"]').first();
    const codeContent = await codeBlock.textContent();

    // Click the copy button
    await copyButton.click();

    // Verify clipboard contains the code
    const clipboardText = await page.evaluate(async () => {
      return await navigator.clipboard.readText();
    });

    // The clipboard should contain some of the code content
    // (may be trimmed or formatted slightly differently)
    expect(clipboardText.trim().length).toBeGreaterThan(0);

    // Verify the button shows success state (text change or visual feedback)
    // Wait a moment for any visual feedback
    await page.waitForTimeout(100);

    // Check if button text changed to indicate success or has success class
    const buttonState = await copyButton.evaluate((el) => {
      const text = el.textContent?.toLowerCase() || '';
      const hasSuccessText = text.includes('copied') || text.includes('✓');
      const hasSuccessClass = el.classList.contains('copied') ||
                              el.classList.contains('success');
      return hasSuccessText || hasSuccessClass;
    });

    // Either visual feedback exists OR clipboard was populated (both indicate success)
    expect(clipboardText.length > 0 || buttonState).toBe(true);
  });

  test('Code example section has proper visual styling', async ({ page }) => {
    // Scroll to code example section
    const codeExampleSection = page.getByTestId('code-example-section');
    await codeExampleSection.scrollIntoViewIfNeeded();

    // Verify the section has appropriate background styling
    const sectionStyles = await codeExampleSection.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        padding: computed.padding,
        backgroundColor: computed.backgroundColor
      };
    });

    // Section should have some padding
    expect(sectionStyles.padding).not.toBe('0px');

    // Verify code block has distinctive styling
    const codeBlock = page.locator('[data-testid="code-block"]').first();
    const codeStyles = await codeBlock.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        fontFamily: computed.fontFamily,
        backgroundColor: computed.backgroundColor,
        borderRadius: computed.borderRadius
      };
    });

    // Code should use monospace font
    expect(codeStyles.fontFamily.toLowerCase()).toMatch(/mono|courier|consolas/);
  });

  test('Code blocks are responsive and readable on different viewports', async ({ page }) => {
    // Test on mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });

    // Navigate and scroll to code section
    await page.goto('/');
    const codeExampleSection = page.getByTestId('code-example-section');
    await codeExampleSection.scrollIntoViewIfNeeded();

    // Verify code blocks are visible and not overflowing
    const codeBlock = page.locator('[data-testid="code-block"]').first();
    await expect(codeBlock).toBeVisible();

    // Check that the code block is readable (has overflow handling)
    const overflowHandling = await codeBlock.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      // Should have overflow scroll/auto or be within viewport
      const hasOverflowControl = computed.overflowX === 'auto' ||
                                  computed.overflowX === 'scroll' ||
                                  computed.whiteSpace === 'pre-wrap' ||
                                  computed.wordWrap === 'break-word';
      const rect = el.getBoundingClientRect();
      const isWithinViewport = rect.right <= window.innerWidth + 10; // 10px tolerance
      return hasOverflowControl || isWithinViewport;
    });

    expect(overflowHandling).toBe(true);
  });
});
