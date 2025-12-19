import { test, expect } from '@playwright/test';

test.describe('Quick Start Code Examples (REQ-3, US-3)', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage
    await page.goto('file://' + process.cwd() + '/public/index.html');
  });

  test('TC1: Quick-start section is visible with code examples', async ({ page }) => {
    // Navigate to the quick-start section
    const quickstartSection = page.locator('[data-testid="quickstart-section"], .quickstart-section, #quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();

    // Verify quick-start section is visible
    await expect(quickstartSection).toBeVisible();

    // Verify the section has a heading
    const heading = quickstartSection.locator('h2');
    await expect(heading).toContainText('Quick Start');

    // Verify code examples are present
    const codeBlocks = quickstartSection.locator('.code-block, pre, code');
    await expect(codeBlocks.first()).toBeVisible();
  });

  test('TC2: Server start command example shows ./mirdb -c mirdb.toml', async ({ page }) => {
    // Navigate to the quick-start section
    const quickstartSection = page.locator('[data-testid="quickstart-section"], .quickstart-section, #quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();

    // Find code blocks containing server start command
    const codeContent = await quickstartSection.locator('code').textContent();

    // Verify the server start command is present
    expect(codeContent).toContain('./mirdb -c mirdb.toml');
  });

  test('TC3: Connection example shows telnet localhost 12333 or client library', async ({ page }) => {
    // Navigate to the quick-start section
    const quickstartSection = page.locator('[data-testid="quickstart-section"], .quickstart-section, #quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();

    // Find code blocks containing connection example
    const codeContent = await quickstartSection.locator('code').textContent();

    // Verify connection command is present (telnet or client library)
    const hasConnectionExample =
      codeContent?.includes('telnet localhost 12333') ||
      codeContent?.includes('localhost:12333') ||
      codeContent?.includes('connect');

    expect(hasConnectionExample).toBeTruthy();
  });

  test('TC4: SET operation example shows SET command with STORED response', async ({ page }) => {
    // Navigate to the quick-start section
    const quickstartSection = page.locator('[data-testid="quickstart-section"], .quickstart-section, #quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();

    // Find code blocks containing SET operation
    const codeContent = await quickstartSection.locator('code').textContent();

    // Verify SET command is present
    expect(codeContent?.toLowerCase()).toContain('set');

    // Verify STORED response is shown
    expect(codeContent).toContain('STORED');
  });

  test('TC5: GET operation example shows GET command with VALUE response', async ({ page }) => {
    // Navigate to the quick-start section
    const quickstartSection = page.locator('[data-testid="quickstart-section"], .quickstart-section, #quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();

    // Find code blocks containing GET operation
    const codeContent = await quickstartSection.locator('code').textContent();

    // Verify GET command is present
    expect(codeContent?.toLowerCase()).toContain('get');

    // Verify VALUE response is shown
    expect(codeContent).toContain('VALUE');
  });

  test('TC6: Copy-to-clipboard button exists and functions correctly', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Navigate to the quick-start section
    const quickstartSection = page.locator('[data-testid="quickstart-section"], .quickstart-section, #quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();

    // Find the copy button
    const copyButton = quickstartSection.locator('[data-testid="copy-button"], .copy-button, button[aria-label*="copy" i], button[title*="copy" i]').first();
    await expect(copyButton).toBeVisible();

    // Get expected code content before clicking
    const codeBlock = quickstartSection.locator('.code-block code, pre code').first();
    const expectedCode = await codeBlock.textContent();

    // Click the copy button
    await copyButton.click();

    // Wait for clipboard operation to complete
    await page.waitForTimeout(100);

    // Verify the clipboard contains the code
    const clipboardText = await page.evaluate(async () => {
      return await navigator.clipboard.readText();
    });

    // The clipboard should contain some of the expected code content
    expect(clipboardText.trim()).toBeTruthy();
    expect(clipboardText.length).toBeGreaterThan(10);

    // Verify confirmation feedback is shown (e.g., button text changes or tooltip appears)
    const buttonTextAfterClick = await copyButton.textContent();
    const hasConfirmation =
      buttonTextAfterClick?.toLowerCase().includes('copied') ||
      await copyButton.getAttribute('data-copied') === 'true' ||
      await page.locator('.copied-tooltip, .copy-feedback, [data-testid="copy-success"]').isVisible().catch(() => false);

    expect(hasConfirmation).toBeTruthy();
  });

  test('TC7: Code blocks have syntax highlighting styling', async ({ page }) => {
    // Navigate to the quick-start section
    const quickstartSection = page.locator('[data-testid="quickstart-section"], .quickstart-section, #quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();

    // Find the code block
    const codeBlock = quickstartSection.locator('.code-block').first();
    await expect(codeBlock).toBeVisible();

    // Find the code element for font check
    const codeElement = codeBlock.locator('code').first();

    // Verify the code block has syntax highlighting styling
    const blockStyles = await codeBlock.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        backgroundColor: style.backgroundColor
      };
    });

    const codeStyles = await codeElement.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        fontFamily: style.fontFamily
      };
    });

    // Code block should have a distinct background color (not white/transparent)
    expect(blockStyles.backgroundColor).not.toBe('rgba(0, 0, 0, 0)');
    expect(blockStyles.backgroundColor).not.toBe('transparent');

    // Code element should use a monospace font
    expect(codeStyles.fontFamily.toLowerCase()).toMatch(/mono|courier|consolas|menlo/);
  });

  test('Quick-start section is accessible via navigation link', async ({ page }) => {
    // Find the quick-start navigation link in the navbar
    const quickstartLink = page.locator('.nav-links a[href="#quickstart"]');
    await expect(quickstartLink).toBeVisible();

    // Click the quick-start link
    await quickstartLink.click();

    // Wait for scroll to complete
    await page.waitForTimeout(500);

    // Verify quick-start section is now in view
    const quickstartSection = page.locator('[data-testid="quickstart-section"], .quickstart-section, #quickstart');
    await expect(quickstartSection).toBeInViewport();
  });

  test('Quick-start section is responsive on mobile viewport', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('file://' + process.cwd() + '/public/index.html');

    // Navigate to quick-start section
    const quickstartSection = page.locator('[data-testid="quickstart-section"], .quickstart-section, #quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();

    // Verify section is visible
    await expect(quickstartSection).toBeVisible();

    // Verify code blocks are readable (visible within viewport width)
    const codeBlock = quickstartSection.locator('.code-block, pre').first();
    await expect(codeBlock).toBeVisible();

    // Verify no horizontal overflow beyond viewport
    const sectionBox = await quickstartSection.boundingBox();
    expect(sectionBox).toBeTruthy();
    expect(sectionBox!.width).toBeLessThanOrEqual(375);
  });
});
