import { test, expect } from '@playwright/test';

/**
 * Syntax Highlighting Functionality Tests
 *
 * These tests verify that Prism.js syntax highlighting is properly applied
 * to code examples in the documentation, ensuring:
 * - TOML configuration blocks have appropriate highlighting
 * - Shell/bash commands have appropriate highlighting
 * - Client code (Python) examples have appropriate highlighting
 * - Syntax highlighting colors provide good readability
 */
test.describe('Syntax Highlighting Functionality', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for page to fully load and Prism.js to initialize
    await page.waitForLoadState('networkidle');
    // Give Prism.js time to tokenize code blocks
    await page.waitForTimeout(500);
  });

  test('TC1: TOML configuration has appropriate syntax highlighting', async ({ page }) => {
    // Navigate to the getting started section where TOML config is displayed
    const configExample = page.locator('[data-testid="configuration-example"]');
    await configExample.scrollIntoViewIfNeeded();
    await expect(configExample).toBeVisible();

    // Verify TOML code block is present with correct language class
    const tomlCode = configExample.locator('pre code.language-toml');
    await expect(tomlCode).toBeVisible();

    // Wait for Prism.js to tokenize the code
    await page.waitForTimeout(300);

    // Verify Prism.js has applied syntax highlighting by checking for token spans
    // Prism.js wraps highlighted code elements in <span class="token ..."> elements
    const tokenElements = tomlCode.locator('span.token');
    const tokenCount = await tokenElements.count();

    // TOML should have multiple token types: comments, keys, strings, numbers, etc.
    expect(tokenCount).toBeGreaterThan(5);

    // Check for specific TOML token types that should be highlighted
    // Comment tokens (lines starting with #)
    const commentTokens = tomlCode.locator('span.token.comment');
    const commentCount = await commentTokens.count();
    expect(commentCount).toBeGreaterThanOrEqual(1);

    // String tokens (values like "0.0.0.0:12333", "/tmp/mirdb")
    const stringTokens = tomlCode.locator('span.token.string');
    const stringCount = await stringTokens.count();
    expect(stringCount).toBeGreaterThanOrEqual(1);

    // Verify key configuration values are present in the code
    const codeContent = await tomlCode.textContent();
    expect(codeContent).toContain('addr');
    expect(codeContent).toContain('max_level');
    expect(codeContent).toContain('sst_max_size');

    // Verify highlighted tokens have actual CSS styles applied (not just default text color)
    const firstComment = commentTokens.first();
    if (await commentTokens.count() > 0) {
      const commentColor = await firstComment.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });
      // Comments should have a distinct color (not the default white/light color of code blocks)
      // prism-tomorrow theme uses specific colors for different token types
      expect(commentColor).toBeTruthy();
      // The color should not be the same as the base code color
      expect(commentColor).not.toBe('rgb(0, 0, 0)');
    }
  });

  test('TC2: Shell/bash commands have appropriate syntax highlighting', async ({ page }) => {
    // Navigate to the installation instructions section where bash code is displayed
    const installationSection = page.locator('[data-testid="installation-instructions"]');
    await installationSection.scrollIntoViewIfNeeded();
    await expect(installationSection).toBeVisible();

    // Verify bash code block is present with correct language class
    const bashCode = installationSection.locator('pre code.language-bash');
    await expect(bashCode).toBeVisible();

    // Wait for Prism.js to tokenize the code
    await page.waitForTimeout(300);

    // Verify Prism.js has applied syntax highlighting
    const tokenElements = bashCode.locator('span.token');
    const tokenCount = await tokenElements.count();

    // Bash commands should have multiple highlighted tokens
    expect(tokenCount).toBeGreaterThan(3);

    // Check for bash-specific token types
    // Comment tokens (lines starting with #)
    const commentTokens = bashCode.locator('span.token.comment');
    const commentCount = await commentTokens.count();
    expect(commentCount).toBeGreaterThanOrEqual(1);

    // Verify shell command content is present
    const codeContent = await bashCode.textContent();
    expect(codeContent).toContain('git clone');
    expect(codeContent).toContain('cargo build');
    expect(codeContent).toContain('cargo run');

    // Check for function/command highlighting (varies by Prism version)
    // Prism.js bash language typically highlights commands as 'function' tokens
    const functionTokens = bashCode.locator('span.token.function');
    const functionCount = await functionTokens.count();
    // Should have some function tokens for commands like git, cargo, cd
    expect(functionCount).toBeGreaterThanOrEqual(1);

    // Verify comment tokens have distinct styling
    const firstComment = commentTokens.first();
    if (await commentTokens.count() > 0) {
      const commentColor = await firstComment.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });
      expect(commentColor).toBeTruthy();
      expect(commentColor).not.toBe('rgb(0, 0, 0)');
    }
  });

  test('TC3: Client code (Python) has language-appropriate syntax highlighting', async ({ page }) => {
    // Navigate to the usage example section where Python code is displayed
    const usageExample = page.locator('[data-testid="usage-example"]');
    await usageExample.scrollIntoViewIfNeeded();
    await expect(usageExample).toBeVisible();

    // Verify Python code block is present with correct language class
    const pythonCode = usageExample.locator('pre code.language-python');
    await expect(pythonCode).toBeVisible();

    // Wait for Prism.js to tokenize the code
    await page.waitForTimeout(300);

    // Verify Prism.js has applied syntax highlighting
    const tokenElements = pythonCode.locator('span.token');
    const tokenCount = await tokenElements.count();

    // Python code should have many highlighted tokens
    expect(tokenCount).toBeGreaterThan(10);

    // Check for Python-specific token types
    // Comment tokens (lines starting with #)
    const commentTokens = pythonCode.locator('span.token.comment');
    const commentCount = await commentTokens.count();
    expect(commentCount).toBeGreaterThanOrEqual(1);

    // String tokens (for imports, string values)
    const stringTokens = pythonCode.locator('span.token.string');
    const stringCount = await stringTokens.count();
    expect(stringCount).toBeGreaterThanOrEqual(1);

    // Keyword tokens (from, import, etc.)
    const keywordTokens = pythonCode.locator('span.token.keyword');
    const keywordCount = await keywordTokens.count();
    expect(keywordCount).toBeGreaterThanOrEqual(1);

    // Verify Python code content is present
    const codeContent = await pythonCode.textContent();
    expect(codeContent).toContain('from pymemcache');
    expect(codeContent).toContain('import');
    expect(codeContent).toContain('Client');
    expect(codeContent).toContain('localhost');
    expect(codeContent).toContain('12333');

    // Check for function/builtin highlighting
    const builtinTokens = pythonCode.locator('span.token.builtin');
    const punctuationTokens = pythonCode.locator('span.token.punctuation');

    // Should have some builtin or punctuation tokens
    const builtinCount = await builtinTokens.count();
    const punctuationCount = await punctuationTokens.count();
    expect(builtinCount + punctuationCount).toBeGreaterThan(0);

    // Verify keyword tokens have distinct styling
    const firstKeyword = keywordTokens.first();
    if (await keywordTokens.count() > 0) {
      const keywordColor = await firstKeyword.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });
      expect(keywordColor).toBeTruthy();
      // Keyword should have a distinct color
      expect(keywordColor).not.toBe('rgb(0, 0, 0)');
    }
  });

  test('Syntax highlighting colors provide good contrast and readability', async ({ page }) => {
    // Navigate to getting started section which has all three code block types
    const gettingStarted = page.locator('[data-testid="getting-started-section"]');
    await gettingStarted.scrollIntoViewIfNeeded();
    await expect(gettingStarted).toBeVisible();

    // Wait for Prism.js to tokenize all code blocks
    await page.waitForTimeout(500);

    // Get all code blocks within the getting started section
    const codeBlocks = gettingStarted.locator('pre code');
    const blockCount = await codeBlocks.count();
    expect(blockCount).toBeGreaterThanOrEqual(3);

    // Check each code block for proper styling
    for (let i = 0; i < blockCount; i++) {
      const codeBlock = codeBlocks.nth(i);

      // Get the pre element (parent) for background color
      const preElement = codeBlock.locator('..');
      const backgroundColor = await preElement.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Verify background color is set (prism-tomorrow theme uses dark background)
      expect(backgroundColor).toBeTruthy();
      // Should be a dark color (not white/light)
      // prism-tomorrow theme uses #1e293b or similar dark colors
      expect(backgroundColor).not.toBe('rgb(255, 255, 255)');

      // Get the font family to verify monospace font
      const fontFamily = await codeBlock.evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });

      // Should use a monospace font
      expect(fontFamily.toLowerCase()).toMatch(/mono|consolas|fira code|monaco|courier/);
    }

    // Verify token colors are readable against the dark background
    // Check a sample of token types across all code blocks
    const allComments = gettingStarted.locator('pre code span.token.comment');
    const allStrings = gettingStarted.locator('pre code span.token.string');
    const allKeywords = gettingStarted.locator('pre code span.token.keyword');

    // Comments should have distinct color
    if (await allComments.count() > 0) {
      const commentColor = await allComments.first().evaluate((el) => {
        return window.getComputedStyle(el).color;
      });
      expect(commentColor).toBeTruthy();
      // prism-tomorrow theme uses gray/green tones for comments
      // Ensure it's not the same as background
      expect(commentColor).not.toBe('rgb(30, 41, 59)'); // #1e293b
    }

    // Strings should have distinct color
    if (await allStrings.count() > 0) {
      const stringColor = await allStrings.first().evaluate((el) => {
        return window.getComputedStyle(el).color;
      });
      expect(stringColor).toBeTruthy();
      // prism-tomorrow theme uses green/yellow tones for strings
      expect(stringColor).not.toBe('rgb(30, 41, 59)');
    }

    // Keywords should have distinct color
    if (await allKeywords.count() > 0) {
      const keywordColor = await allKeywords.first().evaluate((el) => {
        return window.getComputedStyle(el).color;
      });
      expect(keywordColor).toBeTruthy();
      // prism-tomorrow theme uses purple/blue tones for keywords
      expect(keywordColor).not.toBe('rgb(30, 41, 59)');
    }
  });

  test('Prism.js libraries are properly loaded', async ({ page }) => {
    // Verify Prism.js CSS is loaded
    const prismCSS = page.locator('link[href*="prism"]');
    await expect(prismCSS).toHaveCount(1);

    // Get the href attribute and verify it points to prism-tomorrow theme
    const cssHref = await prismCSS.getAttribute('href');
    expect(cssHref).toContain('prism-tomorrow');

    // Verify Prism.js is loaded and available in the window object
    const prismLoaded = await page.evaluate(() => {
      return typeof (window as any).Prism !== 'undefined';
    });
    expect(prismLoaded).toBe(true);

    // Verify language support is loaded
    const languageSupport = await page.evaluate(() => {
      const prism = (window as any).Prism;
      return {
        bash: !!prism.languages.bash,
        python: !!prism.languages.python,
        toml: !!prism.languages.toml
      };
    });

    expect(languageSupport.bash).toBe(true);
    expect(languageSupport.python).toBe(true);
    expect(languageSupport.toml).toBe(true);
  });

  test('All code blocks have language classes applied', async ({ page }) => {
    // Navigate to getting started section
    const gettingStarted = page.locator('[data-testid="getting-started-section"]');
    await gettingStarted.scrollIntoViewIfNeeded();

    // Get all code elements within pre tags
    const codeElements = gettingStarted.locator('pre code');
    const count = await codeElements.count();
    expect(count).toBeGreaterThanOrEqual(3);

    // Verify each code element has a language class
    for (let i = 0; i < count; i++) {
      const codeElement = codeElements.nth(i);
      const className = await codeElement.getAttribute('class');

      // Should have a language- class
      expect(className).toMatch(/language-(bash|python|toml|javascript|rust|go)/);
    }
  });
});
