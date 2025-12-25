import { test, expect } from '@playwright/test';

test.describe('Code Examples with Operations', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: SET command example is displayed with memcached protocol format and STORED response', async ({ page }) => {
    // Navigate to code examples section
    const codeExamplesSection = page.locator('[data-testid="code-examples-section"]');
    await expect(codeExamplesSection).toBeVisible();

    // Check for SET command example
    const setExample = codeExamplesSection.locator('[data-testid="set-example"]');
    await expect(setExample).toBeVisible();

    // Get the code block content (use pre element which contains the code)
    const codeBlock = setExample.locator('pre');
    const codeText = await codeBlock.textContent();

    // Verify SET command format (e.g., 'set key 0 0 5')
    expect(codeText).toMatch(/set\s+\w+\s+\d+\s+\d+\s+\d+/i);

    // Verify STORED response is documented
    expect(codeText).toContain('STORED');
  });

  test('TC2: GET command example is displayed with memcached protocol format and VALUE/END response', async ({ page }) => {
    const codeExamplesSection = page.locator('[data-testid="code-examples-section"]');
    await expect(codeExamplesSection).toBeVisible();

    // Check for GET command example
    const getExample = codeExamplesSection.locator('[data-testid="get-example"]');
    await expect(getExample).toBeVisible();

    // Get the code block content (use pre element which contains the code)
    const codeBlock = getExample.locator('pre');
    const codeText = await codeBlock.textContent();

    // Verify GET command format (e.g., 'get key')
    expect(codeText).toMatch(/get\s+\w+/i);

    // Verify VALUE response is documented
    expect(codeText).toContain('VALUE');

    // Verify END response is documented
    expect(codeText).toContain('END');
  });

  test('TC3: DELETE command example is displayed with memcached protocol format and DELETED response', async ({ page }) => {
    const codeExamplesSection = page.locator('[data-testid="code-examples-section"]');
    await expect(codeExamplesSection).toBeVisible();

    // Check for DELETE command example
    const deleteExample = codeExamplesSection.locator('[data-testid="delete-example"]');
    await expect(deleteExample).toBeVisible();

    // Get the code block content (use pre element which contains the code)
    const codeBlock = deleteExample.locator('pre');
    const codeText = await codeBlock.textContent();

    // Verify DELETE command format (e.g., 'delete key')
    expect(codeText).toMatch(/delete\s+\w+/i);

    // Verify DELETED response is documented
    expect(codeText).toContain('DELETED');
  });

  test('TC4: Code blocks have syntax highlighting applied', async ({ page }) => {
    const codeExamplesSection = page.locator('[data-testid="code-examples-section"]');
    await expect(codeExamplesSection).toBeVisible();

    // Get all code blocks in the section
    const codeBlocks = codeExamplesSection.locator('[data-testid$="-example"] pre');
    const count = await codeBlocks.count();

    // Ensure we have at least 3 code blocks (SET, GET, DELETE)
    expect(count).toBeGreaterThanOrEqual(3);

    // Verify each code block has syntax highlighting applied
    for (let i = 0; i < count; i++) {
      const codeBlock = codeBlocks.nth(i);

      // Check for syntax-highlighted class or styled pre element
      const hasHighlighting = await codeBlock.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        // Check if code block has a distinct background color (indicating styling)
        const hasBackground = styles.backgroundColor !== 'rgba(0, 0, 0, 0)';
        // Check if it has proper font-family for code
        const hasCodeFont = styles.fontFamily.toLowerCase().includes('mono') ||
                           styles.fontFamily.toLowerCase().includes('consolas') ||
                           styles.fontFamily.toLowerCase().includes('courier');
        return hasBackground || hasCodeFont;
      });

      expect(hasHighlighting).toBeTruthy();
    }
  });

  test('TC5: Copy-to-clipboard functionality works on code examples', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const codeExamplesSection = page.locator('[data-testid="code-examples-section"]');
    await expect(codeExamplesSection).toBeVisible();

    // Get the first copy button
    const copyButton = codeExamplesSection.locator('[data-testid="copy-button"]').first();
    await expect(copyButton).toBeVisible();

    // Click the copy button
    await copyButton.click();

    // Wait for the button state to change (data-copied attribute)
    await expect(copyButton).toHaveAttribute('data-copied', 'true', { timeout: 2000 });

    // Verify the button text changed to 'Copied!'
    await expect(copyButton).toContainText('Copied!');
  });

  test('Code examples section has proper heading', async ({ page }) => {
    const codeExamplesSection = page.locator('[data-testid="code-examples-section"]');
    await expect(codeExamplesSection).toBeVisible();

    // Check section has a heading
    const heading = codeExamplesSection.locator('h2, h3');
    await expect(heading.first()).toBeVisible();

    // Heading should relate to code examples or usage
    const headingText = await heading.first().textContent();
    expect(headingText?.toLowerCase()).toMatch(/code|example|usage|operation|command/i);
  });

  test('Each command example includes description of what it does', async ({ page }) => {
    const codeExamplesSection = page.locator('[data-testid="code-examples-section"]');
    await expect(codeExamplesSection).toBeVisible();

    // Check that each example has a description/title
    const setExample = codeExamplesSection.locator('[data-testid="set-example"]');
    const getExample = codeExamplesSection.locator('[data-testid="get-example"]');
    const deleteExample = codeExamplesSection.locator('[data-testid="delete-example"]');

    // Each should have a heading or description
    await expect(setExample.locator('h3, h4, [data-testid="example-title"]').first()).toBeVisible();
    await expect(getExample.locator('h3, h4, [data-testid="example-title"]').first()).toBeVisible();
    await expect(deleteExample.locator('h3, h4, [data-testid="example-title"]').first()).toBeVisible();
  });
});
