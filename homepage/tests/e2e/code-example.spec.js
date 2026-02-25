/**
 * Code Example E2E Tests
 * Owner: Scenario 4 - Code Example with Syntax Highlighting
 *
 * Test cases:
 * - Code block is present
 * - Code contains server start command
 * - Code contains client connection example
 * - Syntax highlighting applied (Prism classes)
 * - Copy button works with visual feedback
 */

const { test, expect } = require('@playwright/test');

test.describe('Code Example Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Code block is present with syntax-highlighted code', async ({ page }) => {
    // Navigate to code example section
    const codeSection = page.locator('#code-example');
    await expect(codeSection).toBeVisible();

    // Verify code blocks are present in the code-example section
    const codeBlocks = codeSection.locator('.code-block');
    await expect(codeBlocks).toHaveCount(2); // Shell and Python blocks

    // Verify code elements have language classes within code-example section (Prism.js integration)
    const shellCode = codeSection.locator('code.language-bash');
    await expect(shellCode.first()).toBeVisible();

    const pythonCode = codeSection.locator('code.language-python');
    await expect(pythonCode.first()).toBeVisible();
  });

  test('TC2: Code includes command to start MirDB server', async ({ page }) => {
    const codeSection = page.locator('#code-example');
    await codeSection.scrollIntoViewIfNeeded();

    // Check for server start commands
    const shellCodeBlock = page.locator('.code-block[data-language="bash"] code');
    const shellContent = await shellCodeBlock.textContent();

    // Verify server start commands are present
    expect(shellContent).toContain('./mirdb-server');
    expect(shellContent).toContain('cargo run');
    expect(shellContent).toContain('--port 12333');
  });

  test('TC3: Code includes memcached client connection example', async ({ page }) => {
    const codeSection = page.locator('#code-example');
    await codeSection.scrollIntoViewIfNeeded();

    // Check for Python memcached client code
    const pythonCodeBlock = page.locator('.code-block[data-language="python"] code');
    const pythonContent = await pythonCodeBlock.textContent();

    // Verify memcached client connection
    expect(pythonContent).toContain('pymemcache');
    expect(pythonContent).toContain('Client');
    expect(pythonContent).toContain('localhost');
    expect(pythonContent).toContain('12333');
    expect(pythonContent).toContain('client.set');
    expect(pythonContent).toContain('client.get');
  });

  test('TC4: Syntax highlighting is applied with Prism.js classes', async ({ page }) => {
    const codeSection = page.locator('#code-example');
    await codeSection.scrollIntoViewIfNeeded();

    // Wait for Prism.js to process the code
    await page.waitForTimeout(500);

    // Check for token classes in bash code (comments)
    const bashCommentTokens = page.locator('.code-block[data-language="bash"] .token.comment');
    await expect(bashCommentTokens.first()).toBeVisible();

    // Check for token classes in Python code
    const pythonKeywordTokens = page.locator('.code-block[data-language="python"] .token.keyword');
    await expect(pythonKeywordTokens.first()).toBeVisible();

    // Verify string tokens exist (for syntax highlighting)
    const stringTokens = page.locator('.code-block[data-language="python"] .token.string');
    await expect(stringTokens.first()).toBeVisible();

    // Verify punctuation tokens for structure
    const punctuationTokens = page.locator('.code-block .token.punctuation');
    expect(await punctuationTokens.count()).toBeGreaterThan(0);
  });

  test('TC5: Copy-to-clipboard button works with visual feedback', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const codeSection = page.locator('#code-example');
    await codeSection.scrollIntoViewIfNeeded();

    // Find the first copy button
    const copyButton = page.locator('.code-block__copy').first();
    await expect(copyButton).toBeVisible();

    // Verify initial state shows "Copy" text
    const copyText = copyButton.locator('.code-block__copy-text');
    await expect(copyText).toHaveText('Copy');

    // Click the copy button
    await copyButton.click();

    // Verify visual feedback - text changes to "Copied!"
    await expect(copyText).toHaveText('Copied!');

    // Verify success class is added
    await expect(copyButton).toHaveClass(/code-block__copy--success/);

    // Get clipboard content and verify it matches the code
    const shellCode = page.locator('.code-block[data-language="bash"] code');
    const expectedCode = await shellCode.textContent();
    const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardContent).toBe(expectedCode);

    // Wait for reset and verify button returns to original state
    await page.waitForTimeout(2100);
    await expect(copyText).toHaveText('Copy');
    await expect(copyButton).not.toHaveClass(/code-block__copy--success/);
  });

  test('Code example section has proper accessibility attributes', async ({ page }) => {
    // Verify section has aria-labelledby
    const codeSection = page.locator('#code-example');
    await expect(codeSection).toHaveAttribute('aria-labelledby', 'code-example-title');

    // Verify heading exists
    const heading = page.locator('#code-example-title');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Code Example');

    // Verify copy buttons have aria-label
    const copyButtons = page.locator('.code-block__copy');
    const count = await copyButtons.count();
    for (let i = 0; i < count; i++) {
      await expect(copyButtons.nth(i)).toHaveAttribute('aria-label', 'Copy code to clipboard');
    }
  });

  test('Code blocks display language labels correctly', async ({ page }) => {
    const codeSection = page.locator('#code-example');
    await codeSection.scrollIntoViewIfNeeded();

    // Check Shell language label
    const shellLabel = page.locator('.code-block[data-language="bash"] .code-block__language');
    await expect(shellLabel).toHaveText('Shell');

    // Check Python language label
    const pythonLabel = page.locator('.code-block[data-language="python"] .code-block__language');
    await expect(pythonLabel).toHaveText('Python');
  });
});
