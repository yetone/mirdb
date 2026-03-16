/**
 * Getting Started Section Tests
 * Owner: Scenario 3 - Getting Started Section with Code Examples
 *
 * Test cases:
 * - Getting Started section exists
 * - Telnet connection example present
 * - SET and GET command examples visible
 * - Code blocks properly styled
 * - Copy-to-clipboard functionality works
 */
import { test, expect } from '@playwright/test';

test.describe('Getting Started Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('TC-1: Section with heading containing "Getting Started" or "Quick Start" exists', async ({ page }) => {
    // Check for Getting Started section
    const section = page.locator('#getting-started');
    await expect(section).toBeVisible();

    // Check for heading
    const heading = section.locator('h2');
    await expect(heading).toBeVisible();
    const headingText = await heading.textContent();
    expect(
      headingText?.toLowerCase().includes('getting started') ||
      headingText?.toLowerCase().includes('quick start')
    ).toBeTruthy();
  });

  test('TC-2: Code block containing "telnet localhost" command is present', async ({ page }) => {
    const section = page.locator('#getting-started');
    await expect(section).toBeVisible();

    // Check for telnet connection example
    const codeBlocks = section.locator('pre code');
    const codeBlocksCount = await codeBlocks.count();
    expect(codeBlocksCount).toBeGreaterThan(0);

    // Find telnet localhost in any code block
    let foundTelnet = false;
    for (let i = 0; i < codeBlocksCount; i++) {
      const content = await codeBlocks.nth(i).textContent();
      if (content && content.includes('telnet localhost')) {
        foundTelnet = true;
        break;
      }
    }
    expect(foundTelnet).toBeTruthy();
  });

  test('TC-3: Code block containing "set" command with memcached protocol syntax is present', async ({ page }) => {
    const section = page.locator('#getting-started');
    await expect(section).toBeVisible();

    // Check for SET command example
    const codeBlocks = section.locator('pre code');
    const codeBlocksCount = await codeBlocks.count();

    let foundSet = false;
    for (let i = 0; i < codeBlocksCount; i++) {
      const content = await codeBlocks.nth(i).textContent();
      // Check for memcached SET command format: set <key> <flags> <exptime> <bytes>
      if (content && (content.includes('set mykey') || content.match(/set\s+\w+\s+\d+\s+\d+\s+\d+/))) {
        foundSet = true;
        break;
      }
    }
    expect(foundSet).toBeTruthy();
  });

  test('TC-4: Code block containing "get" command is present', async ({ page }) => {
    const section = page.locator('#getting-started');
    await expect(section).toBeVisible();

    // Check for GET command example
    const codeBlocks = section.locator('pre code');
    const codeBlocksCount = await codeBlocks.count();

    let foundGet = false;
    for (let i = 0; i < codeBlocksCount; i++) {
      const content = await codeBlocks.nth(i).textContent();
      if (content && content.includes('get mykey')) {
        foundGet = true;
        break;
      }
    }
    expect(foundGet).toBeTruthy();
  });

  test('TC-5: Code elements use <pre> and <code> tags with appropriate CSS classes', async ({ page }) => {
    const section = page.locator('#getting-started');
    await expect(section).toBeVisible();

    // Check for code blocks with proper structure
    const codeBlocks = section.locator('.code-block');
    const codeBlocksCount = await codeBlocks.count();
    expect(codeBlocksCount).toBeGreaterThan(0);

    // Verify each code block has pre and code elements
    for (let i = 0; i < codeBlocksCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      const pre = codeBlock.locator('pre');
      const code = codeBlock.locator('code');

      await expect(pre).toBeVisible();
      await expect(code).toBeVisible();

      // Check that code has an id for copy functionality
      const codeId = await code.getAttribute('id');
      expect(codeId).toBeTruthy();
    }

    // Verify styling is applied
    const firstCodeBlock = codeBlocks.first();
    const backgroundColor = await firstCodeBlock.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    // Should have a dark background (not transparent or white)
    expect(backgroundColor).not.toBe('rgba(0, 0, 0, 0)');
    expect(backgroundColor).not.toBe('rgb(255, 255, 255)');
  });

  test('TC-6: Code blocks have a copy button or copy-to-clipboard functionality', async ({ page }) => {
    const section = page.locator('#getting-started');
    await expect(section).toBeVisible();

    // Check for copy buttons
    const copyButtons = section.locator('.copy-button');
    const copyButtonsCount = await copyButtons.count();
    expect(copyButtonsCount).toBeGreaterThan(0);

    // Verify each copy button has required attributes
    for (let i = 0; i < copyButtonsCount; i++) {
      const button = copyButtons.nth(i);
      await expect(button).toBeVisible();

      // Check for data-copy-target attribute
      const copyTarget = await button.getAttribute('data-copy-target');
      expect(copyTarget).toBeTruthy();

      // Check for aria-label for accessibility
      const ariaLabel = await button.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel?.toLowerCase()).toContain('copy');
    }

    // Test copy functionality by clicking a button
    const firstCopyButton = copyButtons.first();
    await firstCopyButton.click();

    // Wait for the button to show feedback
    await page.waitForTimeout(100);

    // Check that the button shows copied state or changed text
    const buttonText = await firstCopyButton.textContent();
    const hasCopiedClass = await firstCopyButton.evaluate((el) =>
      el.classList.contains('copied')
    );

    // Either the button should have 'copied' class or show 'Copied!' text
    expect(hasCopiedClass || buttonText?.includes('Copied')).toBeTruthy();
  });

  test('Code blocks scroll horizontally for long content', async ({ page }) => {
    const section = page.locator('#getting-started');
    await expect(section).toBeVisible();

    const codeBlocks = section.locator('.code-block pre');
    const firstPre = codeBlocks.first();

    // Check that overflow-x is auto or scroll
    const overflowX = await firstPre.evaluate((el) => {
      return window.getComputedStyle(el).overflowX;
    });
    expect(['auto', 'scroll']).toContain(overflowX);
  });

  test('Code blocks have monospace font', async ({ page }) => {
    const section = page.locator('#getting-started');
    await expect(section).toBeVisible();

    const codeElements = section.locator('.code-block code');
    const firstCode = codeElements.first();

    const fontFamily = await firstCode.evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });

    // Check for monospace font family
    const isMonospace = fontFamily.toLowerCase().includes('mono') ||
                        fontFamily.toLowerCase().includes('consolas') ||
                        fontFamily.toLowerCase().includes('courier') ||
                        fontFamily.toLowerCase().includes('menlo');
    expect(isMonospace).toBeTruthy();
  });

  test('Section is accessible via anchor link', async ({ page }) => {
    // Navigate to home first
    await page.goto('/');

    // Click the Getting Started link
    const navLink = page.locator('a[href="#getting-started"]').first();
    await navLink.click();

    // Wait for smooth scroll
    await page.waitForTimeout(500);

    // Check that the section is in view
    const section = page.locator('#getting-started');
    await expect(section).toBeInViewport();
  });

  test('Complete session example shows connection, set, and get', async ({ page }) => {
    const section = page.locator('#getting-started');
    await expect(section).toBeVisible();

    // Look for the complete session example
    const completeCode = page.locator('#code-complete');
    await expect(completeCode).toBeVisible();

    const content = await completeCode.textContent();
    expect(content).toBeTruthy();

    // Verify it contains all three key elements
    expect(content).toContain('telnet localhost');
    expect(content).toContain('set mykey');
    expect(content).toContain('get mykey');
    expect(content).toContain('STORED');
  });
});
