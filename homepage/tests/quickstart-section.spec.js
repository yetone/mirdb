// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Quick Start Code Snippet Tests
 *
 * Scenario: Verify the quick-start section displays code examples with copy functionality
 */

test.describe('Quick Start Code Snippet', () => {
  test.beforeEach(async ({ page }) => {
    // Step 1: Navigate to quick-start section - Load homepage
    await page.goto('/');
  });

  /**
   * Test Case 1: Check for code snippet showing Memcached client connection
   * Expected: Code snippet demonstrates connecting with a standard Memcached client
   */
  test('should display code snippet showing Memcached client connection', async ({ page }) => {
    // Navigate to quick-start section
    const quickstartSection = page.locator('#quickstart, [data-testid="quickstart-section"], .quickstart-section');
    await quickstartSection.scrollIntoViewIfNeeded();
    await expect(quickstartSection).toBeVisible();

    // Check that code snippet is visible (use first() to handle multiple code blocks)
    const codeBlocks = quickstartSection.locator('.code-block, [data-testid="code-block"]');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Check the client code block specifically for Memcached client connection
    const clientCodeBlock = quickstartSection.locator('[data-testid="code-block-client"], .code-block:has-text("Memcached Client")');
    await expect(clientCodeBlock.first()).toBeVisible();

    // Verify code demonstrates Memcached client connection
    const codeContent = await clientCodeBlock.first().textContent();
    expect(codeContent.toLowerCase()).toMatch(/memcached|client|connect/i);
  });

  /**
   * Test Case 2: Verify default connection address in code
   * Expected: Code snippet shows connection to '0.0.0.0:12333' or equivalent
   */
  test('should display default connection address 0.0.0.0:12333', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart, [data-testid="quickstart-section"], .quickstart-section');
    await quickstartSection.scrollIntoViewIfNeeded();
    await expect(quickstartSection).toBeVisible();

    // Get all text from the quickstart section
    const sectionContent = await quickstartSection.textContent();

    // Should contain the default port 12333 or the full address
    expect(sectionContent).toMatch(/12333|0\.0\.0\.0:12333|localhost:12333/);
  });

  /**
   * Test Case 3: Click copy-to-clipboard button
   * Expected: Code is copied to clipboard with visual feedback confirmation
   */
  test('should copy code to clipboard with visual feedback', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const quickstartSection = page.locator('#quickstart, [data-testid="quickstart-section"], .quickstart-section');
    await quickstartSection.scrollIntoViewIfNeeded();
    await expect(quickstartSection).toBeVisible();

    // Find the first copy button
    const copyButton = quickstartSection.locator('[data-testid="copy-button"], .copy-button').first();
    await expect(copyButton).toBeVisible();

    // Click the copy button
    await copyButton.click();

    // Verify visual feedback (button text or state changes)
    // Check for "Copied" text or a success indicator
    await expect(async () => {
      const buttonText = await copyButton.textContent();
      const hasCopiedClass = await copyButton.evaluate(el => el.classList.contains('copied'));
      const hasCopiedFeedback = buttonText.toLowerCase().includes('copied') || hasCopiedClass;
      expect(hasCopiedFeedback).toBeTruthy();
    }).toPass({ timeout: 3000 });

    // Verify clipboard content contains code
    const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardContent.length).toBeGreaterThan(0);
    // Should contain some of the code content (like git clone or cargo)
    expect(clipboardContent).toMatch(/git|cargo|mirdb/i);
  });

  /**
   * Test Case 4: Verify syntax highlighting applied
   * Expected: Code snippet has proper syntax highlighting (keywords, strings, etc. colored)
   */
  test('should have syntax highlighting applied to code snippet', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart, [data-testid="quickstart-section"], .quickstart-section');
    await quickstartSection.scrollIntoViewIfNeeded();
    await expect(quickstartSection).toBeVisible();

    // Get the first code block
    const codeBlock = quickstartSection.locator('.code-block, [data-testid="code-block"]').first();
    await expect(codeBlock).toBeVisible();

    // Check for syntax highlighting - look for token spans with color classes
    const highlightedElements = codeBlock.locator('span[class*="token"]');
    const highlightCount = await highlightedElements.count();

    // We should have syntax highlighted elements (token spans)
    expect(highlightCount).toBeGreaterThan(0);

    // Verify different types of tokens exist (comment, keyword, string, etc.)
    const hasColoredSpans = await codeBlock.evaluate((el) => {
      const spans = el.querySelectorAll('span.token');
      // Check that at least some spans have different colors
      const colors = new Set();
      Array.from(spans).forEach(span => {
        const style = window.getComputedStyle(span);
        colors.add(style.color);
      });
      // Should have multiple different colors for syntax highlighting
      return colors.size > 1;
    });

    expect(hasColoredSpans).toBeTruthy();
  });

  /**
   * Test Case 5: Check for installation/build instructions
   * Expected: Clear steps to build and run MirDB server are visible
   */
  test('should display clear installation and build instructions', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart, [data-testid="quickstart-section"], .quickstart-section');
    await quickstartSection.scrollIntoViewIfNeeded();
    await expect(quickstartSection).toBeVisible();

    const sectionContent = await quickstartSection.textContent();

    // Should contain git clone instruction
    expect(sectionContent).toMatch(/git\s+clone/i);

    // Should contain cargo build instruction
    expect(sectionContent).toMatch(/cargo\s+build/i);

    // Should contain instructions to run the server
    expect(sectionContent).toMatch(/mirdb-server|run|\.\/target/i);
  });

  /**
   * Test Case 6: Verify default configuration values displayed
   * Expected: Default config values (port 12333, directories, sizes) are shown
   */
  test('should display default configuration values', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart, [data-testid="quickstart-section"], .quickstart-section');
    await quickstartSection.scrollIntoViewIfNeeded();
    await expect(quickstartSection).toBeVisible();

    // Look for configuration section or inline config values
    const pageContent = await quickstartSection.textContent();

    // Check for port 12333
    expect(pageContent).toMatch(/12333/);

    // Check for configuration display - look for config section or values
    const configSection = quickstartSection.locator('[data-testid="config-section"], .config-section, .config-block, .configuration');

    const hasConfigSection = await configSection.count() > 0;

    if (hasConfigSection) {
      await expect(configSection).toBeVisible();
      const configContent = await configSection.textContent();

      // Should show key config values
      expect(configContent).toMatch(/12333|port/i);
    } else {
      // Config might be inline in the code or section text
      // At minimum, port should be visible
      expect(pageContent).toMatch(/12333/);
    }
  });
});

test.describe('Quick Start Section Navigation', () => {
  test('should be accessible via navigation link', async ({ page }) => {
    await page.goto('/');

    // Find navigation link to quick start
    const navLink = page.locator('nav a[href*="quickstart"], nav a[href*="quick-start"], a:has-text("Quick Start")').first();
    await expect(navLink).toBeVisible();

    // Click navigation
    await navLink.click();

    // Verify quick start section is in view
    await page.waitForTimeout(500); // Wait for smooth scroll
    const quickstartSection = page.locator('#quickstart, [data-testid="quickstart-section"], .quickstart-section');
    await expect(quickstartSection).toBeInViewport();
  });
});
