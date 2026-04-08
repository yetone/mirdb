// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Code Examples E2E Tests
 * Owner: Scenario 4 - Code Examples Section
 *
 * Test cases:
 * - SET operation example is displayed
 * - GET operation example is displayed
 * - DELETE operation example is displayed
 * - Syntax highlighting is applied
 * - Copy buttons are present and functional
 */

test.describe('Code Examples Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('code examples section is visible', async ({ page }) => {
    const codeSection = page.locator('#code-examples');
    await expect(codeSection).toBeVisible();

    const title = page.locator('#code-title');
    await expect(title).toHaveText('Usage Examples');
  });

  test('SET operation example is displayed with memcached syntax', async ({ page }) => {
    const setExample = page.locator('[data-operation="set"]');
    await expect(setExample).toBeVisible();

    // Check for SET badge
    const setBadge = setExample.locator('.code-badge-set');
    await expect(setBadge).toHaveText('SET');

    // Check for SET command keyword in code
    const codeBlock = setExample.locator('.code-block code');
    await expect(codeBlock).toContainText('set');
    await expect(codeBlock).toContainText('mykey');

    // Check for STORED response
    await expect(codeBlock).toContainText('STORED');

    // Check syntax description
    const syntax = setExample.locator('.code-example-syntax');
    await expect(syntax).toContainText('set <key> <flags> <ttl> <bytes>');
  });

  test('GET operation example is displayed with memcached syntax', async ({ page }) => {
    const getExample = page.locator('[data-operation="get"]');
    await expect(getExample).toBeVisible();

    // Check for GET badge
    const getBadge = getExample.locator('.code-badge-get');
    await expect(getBadge).toHaveText('GET');

    // Check for GET command keyword in code
    const codeBlock = getExample.locator('.code-block code');
    await expect(codeBlock).toContainText('get');
    await expect(codeBlock).toContainText('mykey');

    // Check for VALUE response format
    await expect(codeBlock).toContainText('VALUE');
    await expect(codeBlock).toContainText('END');

    // Check syntax description
    const syntax = getExample.locator('.code-example-syntax');
    await expect(syntax).toContainText('get <key1>');
  });

  test('DELETE operation example is displayed with memcached syntax', async ({ page }) => {
    const deleteExample = page.locator('[data-operation="delete"]');
    await expect(deleteExample).toBeVisible();

    // Check for DELETE badge
    const deleteBadge = deleteExample.locator('.code-badge-delete');
    await expect(deleteBadge).toHaveText('DELETE');

    // Check for DELETE command keyword in code
    const codeBlock = deleteExample.locator('.code-block code');
    await expect(codeBlock).toContainText('delete');
    await expect(codeBlock).toContainText('mykey');

    // Check for DELETED response
    await expect(codeBlock).toContainText('DELETED');

    // Check syntax description
    const syntax = deleteExample.locator('.code-example-syntax');
    await expect(syntax).toContainText('delete <key>');
  });

  test('code examples have syntax highlighting applied', async ({ page }) => {
    // Check SET example has syntax highlighting classes
    const setCode = page.locator('[data-operation="set"] .code-highlighted');
    await expect(setCode).toBeVisible();

    // Check for keyword highlighting (set command)
    const setKeyword = page.locator('[data-operation="set"] .code-keyword').first();
    await expect(setKeyword).toBeVisible();
    await expect(setKeyword).toHaveText('set');

    // Check for key highlighting
    const keyHighlight = page.locator('[data-operation="set"] .code-key').first();
    await expect(keyHighlight).toBeVisible();

    // Check for number highlighting
    const numberHighlight = page.locator('[data-operation="set"] .code-number').first();
    await expect(numberHighlight).toBeVisible();

    // Check for string highlighting
    const stringHighlight = page.locator('[data-operation="set"] .code-string').first();
    await expect(stringHighlight).toBeVisible();

    // Check for comment highlighting
    const commentHighlight = page.locator('[data-operation="set"] .code-comment').first();
    await expect(commentHighlight).toBeVisible();

    // Check for response highlighting
    const responseHighlight = page.locator('[data-operation="set"] .code-response').first();
    await expect(responseHighlight).toBeVisible();

    // Verify syntax highlighting has distinct colors applied via CSS
    const keywordColor = await setKeyword.evaluate(el =>
      window.getComputedStyle(el).color
    );
    const stringColor = await stringHighlight.evaluate(el =>
      window.getComputedStyle(el).color
    );
    // Keywords and strings should have different colors
    expect(keywordColor).not.toBe(stringColor);
  });

  test('code examples have copy-to-clipboard buttons', async ({ page }) => {
    // Check all three operation examples have copy buttons
    const setExample = page.locator('[data-operation="set"]');
    const getExample = page.locator('[data-operation="get"]');
    const deleteExample = page.locator('[data-operation="delete"]');

    // SET copy button
    const setCopyBtn = setExample.locator('.code-copy-btn');
    await expect(setCopyBtn).toBeVisible();
    await expect(setCopyBtn).toHaveAttribute('data-copy');
    await expect(setCopyBtn.locator('span')).toHaveText('Copy');

    // GET copy button
    const getCopyBtn = getExample.locator('.code-copy-btn');
    await expect(getCopyBtn).toBeVisible();
    await expect(getCopyBtn).toHaveAttribute('data-copy');

    // DELETE copy button
    const deleteCopyBtn = deleteExample.locator('.code-copy-btn');
    await expect(deleteCopyBtn).toBeVisible();
    await expect(deleteCopyBtn).toHaveAttribute('data-copy');

    // Python example copy button
    const pythonCopyBtn = page.locator('.code-client-example .code-copy-btn');
    await expect(pythonCopyBtn).toBeVisible();
    await expect(pythonCopyBtn).toHaveAttribute('data-copy');
  });

  test('copy button shows visual feedback when clicked', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const setCopyBtn = page.locator('[data-operation="set"] .code-copy-btn');
    await expect(setCopyBtn).toBeVisible();

    // Click the copy button
    await setCopyBtn.click();

    // Check for visual feedback (button text changes to "Copied!")
    await expect(setCopyBtn.locator('span')).toHaveText('Copied!');

    // Check for copied class
    await expect(setCopyBtn).toHaveClass(/copied/);

    // Wait for feedback to reset
    await page.waitForTimeout(2500);
    await expect(setCopyBtn.locator('span')).toHaveText('Copy');
  });

  test('Python client example is displayed', async ({ page }) => {
    const pythonExample = page.locator('.code-client-example');
    await expect(pythonExample).toBeVisible();

    // Check title
    const title = pythonExample.locator('.code-client-title');
    await expect(title).toHaveText('Using with Python');

    // Check for Python code
    const codeBlock = pythonExample.locator('.code-block code');
    await expect(codeBlock).toContainText('pymemcache');
    await expect(codeBlock).toContainText('client.set');
    await expect(codeBlock).toContainText('client.get');
    await expect(codeBlock).toContainText('client.delete');
    await expect(codeBlock).toContainText('12333');
  });

  test('code blocks have proper language labels', async ({ page }) => {
    // Check Memcached Protocol labels
    const memcachedLabels = page.locator('.code-examples-grid .code-block-language');
    const count = await memcachedLabels.count();
    expect(count).toBe(3); // SET, GET, DELETE

    for (let i = 0; i < count; i++) {
      await expect(memcachedLabels.nth(i)).toHaveText('Memcached Protocol');
    }

    // Check Python label
    const pythonLabel = page.locator('.code-client-example .code-block-language');
    await expect(pythonLabel).toHaveText('Python');
  });

  test('navigation to code examples section works', async ({ page }) => {
    // Scroll to top first
    await page.evaluate(() => window.scrollTo(0, 0));

    // Navigate to code examples section via anchor
    await page.goto('/#code-examples');

    // Section should be visible in viewport
    const codeSection = page.locator('#code-examples');
    await expect(codeSection).toBeInViewport();
  });
});
