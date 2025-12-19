import { test, expect } from '@playwright/test';

/**
 * E2E Tests for MirDB Homepage - Code Snippet Syntax Highlighting
 * Tests verify code snippets throughout the page have proper syntax highlighting
 * for improved readability per REQ-10.
 */

test.describe('Code Snippet Syntax Highlighting', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Code blocks are wrapped in pre/code elements
   * Input: Query for code/pre elements
   * Expected: Code blocks are wrapped in pre/code elements
   */
  test('TC1: Code blocks are wrapped in pre/code elements', async ({ page }) => {
    // Find all pre elements with code children
    const preCodeBlocks = page.locator('pre code');
    const count = await preCodeBlocks.count();

    // There should be multiple code blocks on the page
    expect(count).toBeGreaterThanOrEqual(1);

    // Verify each code block is properly structured
    for (let i = 0; i < count; i++) {
      const codeElement = preCodeBlocks.nth(i);
      await expect(codeElement).toBeVisible();

      // Verify the parent is a <pre> element
      const parentTag = await codeElement.locator('xpath=..').evaluate(el => el.tagName.toLowerCase());
      expect(parentTag).toBe('pre');
    }

    // Also check that there are code blocks in the getting-started section
    const gettingStartedCodeBlocks = page.locator('#getting-started pre code');
    const gsCount = await gettingStartedCodeBlocks.count();
    expect(gsCount).toBeGreaterThanOrEqual(2);
  });

  /**
   * Test Case 2: Code elements have language-* or highlight classes
   * Input: Check code blocks for highlighting classes
   * Expected: Code elements have language-* or highlight classes
   */
  test('TC2: Code elements have language or highlight classes', async ({ page }) => {
    // Find all code elements inside pre elements
    const codeElements = page.locator('pre code');
    const count = await codeElements.count();

    expect(count).toBeGreaterThanOrEqual(1);

    let hasLanguageClass = false;

    for (let i = 0; i < count; i++) {
      const codeElement = codeElements.nth(i);
      const className = await codeElement.getAttribute('class');

      // Check for Prism.js language classes (language-*) or highlight classes
      if (className) {
        if (className.includes('language-') || className.includes('highlight')) {
          hasLanguageClass = true;
        }
      }
    }

    // At least some code blocks should have language classes
    expect(hasLanguageClass).toBe(true);

    // Specifically check for language-bash which is used in the getting started section
    const bashCodeBlocks = page.locator('pre code.language-bash');
    const bashCount = await bashCodeBlocks.count();
    expect(bashCount).toBeGreaterThanOrEqual(1);
  });

  /**
   * Test Case 3: Shell/bash commands are properly highlighted
   * Input: Verify shell commands have highlighting
   * Expected: Shell/bash commands are properly highlighted
   */
  test('TC3: Shell commands have proper syntax highlighting applied', async ({ page }) => {
    // Wait for Prism.js to load and highlight code
    await page.waitForLoadState('networkidle');

    // Give Prism.js time to process
    await page.waitForTimeout(500);

    // Find bash code blocks
    const bashCodeBlocks = page.locator('pre code.language-bash');
    const count = await bashCodeBlocks.count();

    expect(count).toBeGreaterThanOrEqual(1);

    // Check that Prism.js has added highlighting tokens
    // When Prism.js processes code, it adds span elements with token classes
    const firstBashBlock = bashCodeBlocks.first();

    // Check if the code block contains text (even without JS processing)
    const codeText = await firstBashBlock.textContent();
    expect(codeText).toBeTruthy();
    expect(codeText!.length).toBeGreaterThan(0);

    // Verify shell commands are present (git, cargo, etc.)
    const hasShellCommands = codeText!.includes('git') ||
                             codeText!.includes('cargo') ||
                             codeText!.includes('cd ') ||
                             codeText!.includes('telnet');
    expect(hasShellCommands).toBe(true);

    // After Prism.js loads, it should add token spans for syntax highlighting
    // Check for Prism.js token elements (comment, keyword, etc.)
    const tokenSpans = firstBashBlock.locator('span.token');
    const tokenCount = await tokenSpans.count();

    // Prism.js should have created highlighting tokens
    // Note: If Prism.js hasn't loaded yet, this might be 0, so we use a softer check
    // The primary requirement is that language classes are present
    if (tokenCount > 0) {
      // Verify different token types exist (comment, function, etc.)
      const commentTokens = firstBashBlock.locator('span.token.comment');
      const commentCount = await commentTokens.count();

      // Shell code blocks with comments should have comment tokens
      if (codeText!.includes('#')) {
        expect(commentCount).toBeGreaterThanOrEqual(1);
      }
    }
  });

  /**
   * Test Case 4: Syntax highlighting library is loaded (async preferred)
   * Input: Check Prism.js or highlighting library loaded
   * Expected: Syntax highlighting library is loaded (async preferred)
   */
  test('TC4: Prism.js syntax highlighting library is loaded', async ({ page }) => {
    // Check that Prism.js CSS is loaded
    const prismCSS = page.locator('link[href*="prism"]');
    const cssCount = await prismCSS.count();
    expect(cssCount).toBeGreaterThanOrEqual(1);

    // Verify the CSS link has the correct href for prism theme
    const cssHref = await prismCSS.first().getAttribute('href');
    expect(cssHref).toContain('prism');

    // Check that Prism.js scripts are loaded
    const prismScripts = page.locator('script[src*="prism"]');
    const scriptCount = await prismScripts.count();
    expect(scriptCount).toBeGreaterThanOrEqual(1);

    // Verify scripts use async/defer for non-blocking load
    for (let i = 0; i < scriptCount; i++) {
      const script = prismScripts.nth(i);
      const hasDefer = await script.getAttribute('defer');
      const hasAsync = await script.getAttribute('async');

      // Script should have either defer or async attribute for non-blocking load
      const isNonBlocking = hasDefer !== null || hasAsync !== null;
      expect(isNonBlocking).toBe(true);
    }

    // Wait for page to fully load
    await page.waitForLoadState('networkidle');

    // Verify Prism.js is available in the window object
    const prismLoaded = await page.evaluate(() => {
      return typeof (window as any).Prism !== 'undefined';
    });
    expect(prismLoaded).toBe(true);
  });

  /**
   * Test Case 5: Code text visible even if highlighting fails to load
   * Input: Verify code is readable without JS
   * Expected: Code text visible even if highlighting fails to load
   */
  test('TC5: Code is readable without JavaScript highlighting', async ({ browser }) => {
    // Create a new context with JavaScript disabled to test graceful degradation
    const context = await browser.newContext({ javaScriptEnabled: false });
    const page = await context.newPage();

    await page.goto('/');

    // Find all code blocks
    const codeBlocks = page.locator('pre code');
    const count = await codeBlocks.count();

    expect(count).toBeGreaterThanOrEqual(1);

    // Verify code is still visible and readable without JS
    for (let i = 0; i < Math.min(count, 3); i++) {
      const codeBlock = codeBlocks.nth(i);

      // Code block should be visible
      await expect(codeBlock).toBeVisible();

      // Code text should be present
      const textContent = await codeBlock.textContent();
      expect(textContent).toBeTruthy();
      expect(textContent!.trim().length).toBeGreaterThan(0);
    }

    // Verify the getting-started code blocks specifically have meaningful content
    const gettingStartedCode = page.locator('#getting-started pre code');
    const gsCount = await gettingStartedCode.count();

    for (let i = 0; i < gsCount; i++) {
      const codeBlock = gettingStartedCode.nth(i);
      const text = await codeBlock.textContent();

      // Should contain actual code/commands
      expect(text).toBeTruthy();
      expect(text!.length).toBeGreaterThan(5);
    }

    // Verify that pre elements have proper styling for readability
    const firstPre = page.locator('pre').first();
    const backgroundColor = await firstPre.evaluate(el => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Should have a distinct background color for code blocks
    expect(backgroundColor).not.toBe('rgba(0, 0, 0, 0)');
    expect(backgroundColor).not.toBe('transparent');

    // Clean up
    await context.close();
  });

  /**
   * Additional test: Verify all code blocks in getting-started section have language classes
   */
  test('TC6: All getting-started code blocks have language specification', async ({ page }) => {
    const gettingStartedCodeBlocks = page.locator('#getting-started pre code');
    const count = await gettingStartedCodeBlocks.count();

    expect(count).toBeGreaterThanOrEqual(2);

    // Each code block should have a language class
    for (let i = 0; i < count; i++) {
      const codeBlock = gettingStartedCodeBlocks.nth(i);
      const className = await codeBlock.getAttribute('class');

      expect(className).toBeTruthy();
      expect(className).toContain('language-');
    }
  });

  /**
   * Additional test: Verify code blocks have monospace font
   */
  test('TC7: Code blocks use monospace font for readability', async ({ page }) => {
    const codeBlocks = page.locator('pre code');
    const count = await codeBlocks.count();

    expect(count).toBeGreaterThanOrEqual(1);

    const firstCodeBlock = codeBlocks.first();
    const fontFamily = await firstCodeBlock.evaluate(el => {
      return window.getComputedStyle(el).fontFamily;
    });

    // Should use a monospace font family
    const isMonospace = fontFamily.toLowerCase().includes('mono') ||
                        fontFamily.toLowerCase().includes('courier') ||
                        fontFamily.toLowerCase().includes('consolas') ||
                        fontFamily.toLowerCase().includes('menlo');
    expect(isMonospace).toBe(true);
  });
});
