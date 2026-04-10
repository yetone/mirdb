/**
 * Getting Started Section E2E Tests
 * Owner: Scenario 3 - Getting Started Section
 *
 * Test cases:
 * - Copy to clipboard functionality with visual feedback
 * - Documentation link is functional
 */

import { test, expect } from '@playwright/test';

test.describe('Getting Started Section - Installation & Usage', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Test Case 1: Installation section content', () => {
    test('should have installation section with cargo build command', async ({ page }) => {
      const gettingStarted = page.locator('#getting-started');
      await expect(gettingStarted).toBeVisible();

      // Check for installation block
      const installBlock = page.locator('#install-block');
      await expect(installBlock).toBeVisible();

      // Verify cargo build command is present
      const codeContent = installBlock.locator('.code-content');
      await expect(codeContent).toContainText('cargo build');
    });
  });

  test.describe('Test Case 2: SET command example', () => {
    test('should show SET command syntax with proper example', async ({ page }) => {
      const setBlock = page.locator('#set-command-block');
      await expect(setBlock).toBeVisible();

      // Check for SET command syntax
      const codeContent = setBlock.locator('.code-content');
      await expect(codeContent).toContainText('set key 0 0 5');
      await expect(codeContent).toContainText('value');

      // Check copy button has correct data
      const copyBtn = setBlock.locator('.copy-btn');
      const copyData = await copyBtn.getAttribute('data-copy');
      expect(copyData).toContain('set key 0 0 5');
      expect(copyData).toContain('value');
    });
  });

  test.describe('Test Case 3: GET command example', () => {
    test('should show GET command syntax with proper example', async ({ page }) => {
      const getBlock = page.locator('#get-command-block');
      await expect(getBlock).toBeVisible();

      // Check for GET command syntax
      const codeContent = getBlock.locator('.code-content');
      await expect(codeContent).toContainText('get key');

      // Check copy button has correct data
      const copyBtn = getBlock.locator('.copy-btn');
      const copyData = await copyBtn.getAttribute('data-copy');
      expect(copyData).toContain('get key');
    });
  });

  test.describe('Test Case 4: INFO command example', () => {
    test('should show INFO command for server statistics', async ({ page }) => {
      const infoBlock = page.locator('#info-command-block');
      await expect(infoBlock).toBeVisible();

      // Check for INFO command
      const codeContent = infoBlock.locator('.code-content');
      await expect(codeContent).toContainText('info');
      await expect(codeContent).toContainText('server statistics');

      // Check copy button has correct data
      const copyBtn = infoBlock.locator('.copy-btn');
      const copyData = await copyBtn.getAttribute('data-copy');
      expect(copyData).toContain('info');
    });
  });

  test.describe('Test Case 5: Copy to clipboard functionality', () => {
    test('should copy code to clipboard and show visual feedback', async ({ page, context }) => {
      // Grant clipboard permissions
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);

      // Navigate to getting started section
      await page.locator('#getting-started').scrollIntoViewIfNeeded();

      // Find a copy button
      const copyBtn = page.locator('#install-block .copy-btn');
      await expect(copyBtn).toBeVisible();

      // Get the expected copy content
      const expectedContent = await copyBtn.getAttribute('data-copy');

      // Click the copy button
      await copyBtn.click();

      // Check for visual feedback - text should change to "Copied!"
      const copyIcon = copyBtn.locator('.copy-icon');
      await expect(copyIcon).toHaveText('Copied!');

      // Read clipboard content
      const clipboardContent = await page.evaluate(async () => {
        return await navigator.clipboard.readText();
      });

      // Verify clipboard contains the expected content
      expect(clipboardContent).toBe(expectedContent);

      // Wait and verify text returns to "Copy"
      await page.waitForTimeout(2500);
      await expect(copyIcon).toHaveText('Copy');
    });

    test('copy button should have accessible label', async ({ page }) => {
      const copyBtns = page.locator('.copy-btn');
      const count = await copyBtns.count();

      for (let i = 0; i < count; i++) {
        const btn = copyBtns.nth(i);
        const ariaLabel = await btn.getAttribute('aria-label');
        expect(ariaLabel).toBeTruthy();
        expect(ariaLabel.toLowerCase()).toContain('copy');
      }
    });
  });

  test.describe('Test Case 6: Full documentation link', () => {
    test('should have a functional link to full documentation/README', async ({ page }) => {
      // Find the documentation link
      const docsLink = page.locator('#full-docs-link');
      await expect(docsLink).toBeVisible();

      // Verify link text
      await expect(docsLink).toContainText('Documentation');

      // Verify href points to GitHub README
      const href = await docsLink.getAttribute('href');
      expect(href).toContain('github.com/nicksherron/mirdb');
      expect(href).toContain('readme');

      // Verify link opens in new tab
      const target = await docsLink.getAttribute('target');
      expect(target).toBe('_blank');

      // Verify rel attribute for security
      const rel = await docsLink.getAttribute('rel');
      expect(rel).toContain('noopener');
    });

    test('documentation link should be accessible and focusable', async ({ page }) => {
      const docsLink = page.locator('#full-docs-link');

      // Navigate to the link using keyboard
      await page.locator('#getting-started').scrollIntoViewIfNeeded();

      // Check that the link is focusable
      await docsLink.focus();
      await expect(docsLink).toBeFocused();
    });
  });
});

test.describe('Getting Started Section - Visual Layout', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('code blocks should be visually distinct with proper styling', async ({ page }) => {
    const codeBlocks = page.locator('.code-block');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThanOrEqual(4); // Install, SET, GET, INFO

    for (let i = 0; i < count; i++) {
      const block = codeBlocks.nth(i);
      await expect(block).toBeVisible();

      // Check that code header exists
      const header = block.locator('.code-header');
      await expect(header).toBeVisible();

      // Check that code content exists
      const content = block.locator('.code-content');
      await expect(content).toBeVisible();
    }
  });

  test('section should have proper heading structure', async ({ page }) => {
    const section = page.locator('#getting-started');

    // Main section title
    const sectionTitle = section.locator('.section-title');
    await expect(sectionTitle).toBeVisible();
    await expect(sectionTitle).toContainText('Getting Started');
  });
});
