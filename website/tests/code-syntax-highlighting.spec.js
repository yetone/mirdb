// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

// Base URL for the static HTML file
const BASE_URL = 'file://' + path.resolve(__dirname, '../dist/index.html');

test.describe('Code Syntax Highlighting', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(BASE_URL);
    // Wait for highlight.js to load and initialize
    await page.waitForFunction(() => typeof hljs !== 'undefined', { timeout: 10000 });
    // Give time for highlighting to apply
    await page.waitForTimeout(500);
  });

  test('Test Case 1: Rust code blocks display with proper syntax highlighting', async ({ page }) => {
    // Navigate to the rust code example
    const rustCodeExample = page.locator('[data-testid="rust-code-example"]');
    await expect(rustCodeExample).toBeVisible();

    // Check for the rust code block
    const rustCodeBlock = page.locator('#rust-code');
    await expect(rustCodeBlock).toBeVisible();

    // Verify the code block has the language-rust class
    await expect(rustCodeBlock).toHaveClass(/language-rust/);

    // Check that highlight.js has processed it (should have hljs class)
    await expect(rustCodeBlock).toHaveClass(/hljs/);

    // Check that syntax highlighting elements exist (keywords like pub, struct, impl, fn)
    const highlightedElements = rustCodeBlock.locator('.hljs-keyword, .hljs-type, .hljs-string, .hljs-comment, .hljs-function, .hljs-title');
    const count = await highlightedElements.count();
    expect(count).toBeGreaterThan(0);

    // Verify Rust-specific keywords are highlighted
    const codeContent = await rustCodeBlock.innerHTML();
    // Check for common Rust syntax highlighting classes
    const hasRustHighlighting =
      codeContent.includes('hljs-keyword') || // pub, struct, impl, fn
      codeContent.includes('hljs-type') || // Arc, RwLock, Vec, Option
      codeContent.includes('hljs-built_in'); // self

    expect(hasRustHighlighting).toBe(true);

    // Verify the language label is correct
    const languageLabel = rustCodeExample.locator('[data-language="rust"]');
    await expect(languageLabel).toBeVisible();
  });

  test('Test Case 2: TOML configuration blocks display with proper syntax highlighting', async ({ page }) => {
    // Navigate to the config example
    const configExample = page.locator('[data-testid="config-example"]');
    await expect(configExample).toBeVisible();

    // Check for the TOML code block
    const tomlCodeBlock = page.locator('#config-code');
    await expect(tomlCodeBlock).toBeVisible();

    // Verify the code block has the language-toml class
    await expect(tomlCodeBlock).toHaveClass(/language-toml/);

    // Check that highlight.js has processed it
    await expect(tomlCodeBlock).toHaveClass(/hljs/);

    // Check that syntax highlighting elements exist
    const highlightedElements = tomlCodeBlock.locator('.hljs-attr, .hljs-string, .hljs-number, .hljs-comment, .hljs-section');
    const count = await highlightedElements.count();
    expect(count).toBeGreaterThan(0);

    // Verify TOML-specific highlighting (attributes and string values)
    const codeContent = await tomlCodeBlock.innerHTML();
    const hasTOMLHighlighting =
      codeContent.includes('hljs-attr') || // key names like addr, work_dir
      codeContent.includes('hljs-string') || // string values
      codeContent.includes('hljs-number') || // numeric values
      codeContent.includes('hljs-comment'); // comments starting with #

    expect(hasTOMLHighlighting).toBe(true);

    // Verify the language label is correct
    const languageLabel = configExample.locator('[data-language="toml"]');
    await expect(languageLabel).toBeVisible();
  });

  test('Test Case 3: Shell commands display with appropriate formatting', async ({ page }) => {
    // Navigate to the installation code
    const installationCode = page.locator('[data-testid="installation-code"]');
    await expect(installationCode).toBeVisible();

    // Check for the bash code block
    const bashCodeBlock = page.locator('#install-code');
    await expect(bashCodeBlock).toBeVisible();

    // Verify the code block has the language-bash class
    await expect(bashCodeBlock).toHaveClass(/language-bash/);

    // Check that highlight.js has processed it
    await expect(bashCodeBlock).toHaveClass(/hljs/);

    // Check that syntax highlighting elements exist
    const highlightedElements = bashCodeBlock.locator('.hljs-comment, .hljs-built_in, .hljs-variable');
    const count = await highlightedElements.count();
    expect(count).toBeGreaterThan(0);

    // Verify shell-specific highlighting (comments, built-in commands)
    const codeContent = await bashCodeBlock.innerHTML();
    const hasBashHighlighting =
      codeContent.includes('hljs-comment') || // comments starting with #
      codeContent.includes('hljs-built_in') || // git, cd, cargo
      codeContent.includes('hljs-string'); // quoted strings

    expect(hasBashHighlighting).toBe(true);

    // Verify the language label indicates Terminal/bash
    const languageLabel = installationCode.locator('[data-language="bash"]');
    await expect(languageLabel).toBeVisible();
  });

  test('Test Case 4: Memcached commands display with distinguishable formatting', async ({ page }) => {
    // Check SET command example
    const setExample = page.locator('[data-testid="example-set"]');
    await expect(setExample).toBeVisible();

    const setCodeBlock = page.locator('#set-code');
    await expect(setCodeBlock).toBeVisible();

    // Verify the code block has the language-memcached class
    await expect(setCodeBlock).toHaveClass(/language-memcached/);

    // Check for custom memcached syntax highlighting elements
    const setContent = await setCodeBlock.innerHTML();

    // Verify memcached-specific highlighting classes exist
    expect(setContent).toContain('hljs-keyword'); // set command
    expect(setContent).toContain('hljs-string'); // key name
    expect(setContent).toContain('hljs-number'); // flags, ttl, bytes
    expect(setContent).toContain('hljs-response'); // STORED
    expect(setContent).toContain('hljs-comment'); // comments

    // Check GET command example
    const getExample = page.locator('[data-testid="example-get"]');
    await expect(getExample).toBeVisible();

    const getCodeBlock = page.locator('#get-code');
    const getContent = await getCodeBlock.innerHTML();

    expect(getContent).toContain('hljs-keyword'); // get command
    expect(getContent).toContain('hljs-response'); // VALUE, END

    // Check DELETE command example
    const deleteExample = page.locator('[data-testid="example-delete"]');
    await expect(deleteExample).toBeVisible();

    const deleteCodeBlock = page.locator('#delete-code');
    const deleteContent = await deleteCodeBlock.innerHTML();

    expect(deleteContent).toContain('hljs-keyword'); // delete command
    expect(deleteContent).toContain('hljs-response'); // DELETED

    // Verify the language labels indicate Memcached Protocol
    const memcachedLabels = page.locator('[data-language="memcached"]');
    const labelCount = await memcachedLabels.count();
    expect(labelCount).toBeGreaterThanOrEqual(3); // At least SET, GET, DELETE examples
  });

  test('Python code blocks display with proper syntax highlighting', async ({ page }) => {
    // Navigate to the client connection example
    const clientExample = page.locator('[data-testid="client-connection-example"]');
    await expect(clientExample).toBeVisible();

    // Check for the Python code block
    const pythonCodeBlock = page.locator('#client-code');
    await expect(pythonCodeBlock).toBeVisible();

    // Verify the code block has the language-python class
    await expect(pythonCodeBlock).toHaveClass(/language-python/);

    // Check that highlight.js has processed it
    await expect(pythonCodeBlock).toHaveClass(/hljs/);

    // Verify Python-specific highlighting
    const codeContent = await pythonCodeBlock.innerHTML();
    const hasPythonHighlighting =
      codeContent.includes('hljs-keyword') || // import
      codeContent.includes('hljs-comment') || // comments
      codeContent.includes('hljs-string') || // string literals
      codeContent.includes('hljs-built_in'); // print

    expect(hasPythonHighlighting).toBe(true);

    // Verify the language label is correct
    const languageLabel = clientExample.locator('[data-language="python"]');
    await expect(languageLabel).toBeVisible();
  });

  test('Code blocks have visually distinguishable styling', async ({ page }) => {
    // Get all code blocks
    const codeBlocks = page.locator('.code-block');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThan(0);

    // Check that code blocks have the dark background styling
    const firstCodeBlock = codeBlocks.first();
    await expect(firstCodeBlock).toHaveClass(/bg-mirdb-dark/);

    // Verify the code element inside has proper styling applied
    const codeElement = firstCodeBlock.locator('code');
    const classes = await codeElement.getAttribute('class');
    expect(classes).toBeTruthy();
    // Should have language class and hljs class
    expect(classes).toMatch(/language-/);
  });

  test('Highlight.js library is loaded and initialized', async ({ page }) => {
    // Check that highlight.js is available
    const hljsAvailable = await page.evaluate(() => typeof hljs !== 'undefined');
    expect(hljsAvailable).toBe(true);

    // Check that required languages are registered
    const languagesRegistered = await page.evaluate(() => {
      if (typeof hljs === 'undefined') return { rust: false, toml: false, bash: false, python: false };
      return {
        rust: hljs.getLanguage('rust') !== undefined,
        toml: hljs.getLanguage('toml') !== undefined,
        bash: hljs.getLanguage('bash') !== undefined,
        python: hljs.getLanguage('python') !== undefined
      };
    });

    expect(languagesRegistered.rust).toBe(true);
    expect(languagesRegistered.toml).toBe(true);
    expect(languagesRegistered.bash).toBe(true);
    expect(languagesRegistered.python).toBe(true);
  });
});
