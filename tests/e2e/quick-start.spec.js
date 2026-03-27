/**
 * Quick Start Section E2E Tests
 * Owner: Scenario 3 - Quick Start Section
 *
 * Tests:
 * - Server start command code block
 * - Get/set operation examples
 * - Syntax highlighting
 * - Copy-to-clipboard functionality
 */
const { test, expect } = require('@playwright/test');

test.describe('Quick Start Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Check for server start command code block', async ({ page }) => {
    // Navigate to Quick Start section
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Check for server start command code block
    const codeBlocks = quickStartSection.locator('.code-block');
    await expect(codeBlocks.first()).toBeVisible();

    // Verify it contains cargo run command for starting MirDB server
    const firstCodeBlock = codeBlocks.first();
    const codeContent = await firstCodeBlock.locator('code').textContent();
    expect(codeContent).toContain('cargo run');
    expect(codeContent).toContain('Server starts on port 12333');
  });

  test('TC2: Check for get/set operation examples', async ({ page }) => {
    const quickStartSection = page.locator('#quick-start');

    // Check for SET command example
    const setCodeBlock = quickStartSection.locator('.code-block').filter({
      has: page.locator('code:has-text("set mykey")')
    });
    await expect(setCodeBlock).toBeVisible();
    const setContent = await setCodeBlock.locator('code').textContent();
    expect(setContent).toContain('set');
    expect(setContent).toContain('STORED');

    // Check for GET command example
    const getCodeBlock = quickStartSection.locator('.code-block').filter({
      has: page.locator('code:has-text("get mykey")')
    });
    await expect(getCodeBlock).toBeVisible();
    const getContent = await getCodeBlock.locator('code').textContent();
    expect(getContent).toContain('get');
    expect(getContent).toContain('VALUE');
    expect(getContent).toContain('END');
  });

  test('TC3: Verify code blocks have syntax highlighting', async ({ page }) => {
    const quickStartSection = page.locator('#quick-start');
    const codeBlocks = quickStartSection.locator('.code-block');

    // Verify code blocks use pre/code structure
    await expect(codeBlocks.first().locator('pre')).toBeVisible();
    await expect(codeBlocks.first().locator('code')).toBeVisible();

    // Verify syntax highlighting classes are present
    const highlightedElements = codeBlocks.first().locator('span[class*="keyword"], span[class*="command"], span[class*="string"], span[class*="comment"], span[class*="number"]');
    const count = await highlightedElements.count();
    expect(count).toBeGreaterThan(0);
  });

  test('TC4: Test copy-to-clipboard functionality', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const quickStartSection = page.locator('#quick-start');
    const firstCopyBtn = quickStartSection.locator('.copy-btn').first();

    // Verify copy button exists
    await expect(firstCopyBtn).toBeVisible();
    await expect(firstCopyBtn).toHaveText('Copy');

    // Click the copy button
    await firstCopyBtn.click();

    // Wait for the button text to change to "Copied!"
    await expect(firstCopyBtn).toHaveText('Copied!');
    await expect(firstCopyBtn).toHaveClass(/copied/);

    // Verify clipboard contains the code content
    const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardContent).toContain('cargo run');

    // Wait for button to reset back to "Copy"
    await expect(firstCopyBtn).toHaveText('Copy', { timeout: 3000 });
  });

  test('TC5: Check Quick Start section heading', async ({ page }) => {
    // Check for Quick Start heading
    const heading = page.locator('#quick-start h2, #quick-start-heading');
    await expect(heading.first()).toBeVisible();

    const headingText = await heading.first().textContent();
    expect(headingText.toLowerCase()).toContain('quick start');
  });

  test('Quick Start section is navigable from header', async ({ page }) => {
    // Click on Quick Start link in navigation
    const navLink = page.locator('header a[href="#quick-start"]');
    await expect(navLink.first()).toBeVisible();
    await navLink.first().click();

    // Verify the section is in view
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeInViewport();
  });

  test('Code blocks have aria labels for accessibility', async ({ page }) => {
    const quickStartSection = page.locator('#quick-start');
    const codeBlocks = quickStartSection.locator('.code-block[role="region"]');

    // At least one code block should have aria-label
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThan(0);

    // Check first code block has aria-label
    const firstBlock = codeBlocks.first();
    const ariaLabel = await firstBlock.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
  });

  test('All copy buttons have aria-labels', async ({ page }) => {
    const quickStartSection = page.locator('#quick-start');
    const copyButtons = quickStartSection.locator('.copy-btn');

    const count = await copyButtons.count();
    expect(count).toBeGreaterThan(0);

    // Check each button has an aria-label
    for (let i = 0; i < count; i++) {
      const button = copyButtons.nth(i);
      const ariaLabel = await button.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel.toLowerCase()).toContain('copy');
    }
  });
});
