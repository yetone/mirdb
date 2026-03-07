/**
 * Copy Button E2E Tests
 * Owner: Scenarios 4, 15 - Quick Start and Copy Functionality
 *
 * Tests:
 * - Copy button exists and is clickable
 * - Copy button provides visual feedback on click
 * - Copy button copies code to clipboard
 * - Copy button state resets after timeout
 */

const { test, expect } = require('@playwright/test');
const path = require('path');
const http = require('http');
const fs = require('fs');

// Create a simple static file server for testing
function createServer(rootDir) {
  return http.createServer((req, res) => {
    let filePath = path.join(rootDir, req.url === '/' ? 'index.html' : req.url);

    // Basic security check
    if (!filePath.startsWith(rootDir)) {
      res.writeHead(403);
      res.end('Forbidden');
      return;
    }

    const extname = path.extname(filePath);
    const contentTypes = {
      '.html': 'text/html',
      '.css': 'text/css',
      '.js': 'application/javascript',
      '.svg': 'image/svg+xml',
      '.gif': 'image/gif',
      '.png': 'image/png',
    };
    const contentType = contentTypes[extname] || 'application/octet-stream';

    fs.readFile(filePath, (err, content) => {
      if (err) {
        if (err.code === 'ENOENT') {
          res.writeHead(404);
          res.end('Not Found');
        } else {
          res.writeHead(500);
          res.end('Internal Server Error');
        }
        return;
      }
      res.writeHead(200, { 'Content-Type': contentType });
      res.end(content);
    });
  });
}

const rootDir = path.join(__dirname, '../../');
let server;
let serverUrl;

test.beforeAll(async () => {
  server = createServer(rootDir);
  await new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      const address = server.address();
      serverUrl = `http://127.0.0.1:${address.port}`;
      resolve();
    });
  });
});

test.afterAll(async () => {
  if (server) {
    await new Promise((resolve) => server.close(resolve));
  }
});

test.describe('Copy Button Functionality (Scenario 4)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(serverUrl);
    // Wait for the page to load and JS to initialize
    await page.waitForLoadState('domcontentloaded');
    // Wait for JS to execute
    await page.waitForFunction(() => document.readyState === 'complete');
  });

  // Test Case 4: Click copy button and check clipboard
  test.describe('Test Case 4: Click copy button and check clipboard', () => {
    test('should have copy buttons in quick-start section', async ({ page }) => {
      const quickstartSection = page.locator('#quickstart');
      await expect(quickstartSection).toBeVisible();

      const copyButtons = quickstartSection.locator('.code-block__copy-btn');
      const count = await copyButtons.count();
      expect(count).toBeGreaterThan(0);
    });

    test('copy button should show "Copy" text initially', async ({ page }) => {
      const copyButton = page.locator('#quickstart .code-block__copy-btn').first();
      await expect(copyButton).toBeVisible();

      const copyText = copyButton.locator('.code-block__copy-text');
      await expect(copyText).toHaveText('Copy');
    });

    test('copy button should change to "Copied!" after click', async ({ page }) => {
      // Grant clipboard permissions
      await page.context().grantPermissions(['clipboard-write', 'clipboard-read']);

      const copyButton = page.locator('#quickstart .code-block__copy-btn').first();
      await expect(copyButton).toBeVisible();

      // Click the copy button
      await copyButton.click();

      // Check that text changed to "Copied!"
      const copyText = copyButton.locator('.code-block__copy-text');
      await expect(copyText).toHaveText('Copied!');
    });

    test('copy button should add copied class after click', async ({ page }) => {
      await page.context().grantPermissions(['clipboard-write', 'clipboard-read']);

      const copyButton = page.locator('#quickstart .code-block__copy-btn').first();
      await copyButton.click();

      // Check that button has the copied class
      await expect(copyButton).toHaveClass(/code-block__copy-btn--copied/);
    });

    test('copy button should show check icon after click', async ({ page }) => {
      await page.context().grantPermissions(['clipboard-write', 'clipboard-read']);

      const copyButton = page.locator('#quickstart .code-block__copy-btn').first();
      await copyButton.click();

      // Check icon should be visible
      const checkIcon = copyButton.locator('.code-block__check-icon');
      await expect(checkIcon).toBeVisible();

      // Copy icon should be hidden
      const copyIcon = copyButton.locator('.code-block__copy-icon');
      await expect(copyIcon).toBeHidden();
    });

    test('copy button should copy code content to clipboard', async ({ page, context }) => {
      // Grant clipboard permissions
      await context.grantPermissions(['clipboard-write', 'clipboard-read']);

      const quickstartSection = page.locator('#quickstart');
      const installationBlock = quickstartSection.locator('[data-code-block="installation"]');
      const copyButton = installationBlock.locator('.code-block__copy-btn');

      // Get the code content before clicking
      const codeElement = installationBlock.locator('code');
      const expectedCode = await codeElement.textContent();

      // Click the copy button
      await copyButton.click();

      // Wait for async clipboard operation
      await page.waitForTimeout(100);

      // Read clipboard content
      const clipboardContent = await page.evaluate(async () => {
        return await navigator.clipboard.readText();
      });

      // Verify clipboard contains the code
      expect(clipboardContent).toBe(expectedCode);
    });

    test('copy button should reset after 2 seconds', async ({ page }) => {
      await page.context().grantPermissions(['clipboard-write', 'clipboard-read']);

      const copyButton = page.locator('#quickstart .code-block__copy-btn').first();
      await copyButton.click();

      // Verify it shows "Copied!"
      const copyText = copyButton.locator('.code-block__copy-text');
      await expect(copyText).toHaveText('Copied!');

      // Wait for the reset (2 seconds + buffer)
      await page.waitForTimeout(2100);

      // Should be back to "Copy"
      await expect(copyText).toHaveText('Copy');

      // Should not have copied class
      await expect(copyButton).not.toHaveClass(/code-block__copy-btn--copied/);
    });

    test('copy buttons should be keyboard accessible', async ({ page }) => {
      await page.context().grantPermissions(['clipboard-write', 'clipboard-read']);

      const copyButton = page.locator('#quickstart .code-block__copy-btn').first();

      // Focus on the button
      await copyButton.focus();
      await expect(copyButton).toBeFocused();

      // Press Enter to activate
      await page.keyboard.press('Enter');

      // Should show "Copied!"
      const copyText = copyButton.locator('.code-block__copy-text');
      await expect(copyText).toHaveText('Copied!');
    });

    test('all copy buttons in quickstart should work independently', async ({ page }) => {
      await page.context().grantPermissions(['clipboard-write', 'clipboard-read']);

      const copyButtons = page.locator('#quickstart .code-block__copy-btn');
      const count = await copyButtons.count();

      // Click each button and verify it works
      for (let i = 0; i < count; i++) {
        const button = copyButtons.nth(i);
        await button.click();

        const copyText = button.locator('.code-block__copy-text');
        await expect(copyText).toHaveText('Copied!');

        // Wait for reset before testing next button
        await page.waitForTimeout(2100);
      }
    });
  });

  test.describe('Copy Button Accessibility', () => {
    test('copy buttons should have aria-label', async ({ page }) => {
      const copyButtons = page.locator('#quickstart .code-block__copy-btn');
      const count = await copyButtons.count();

      for (let i = 0; i < count; i++) {
        const ariaLabel = await copyButtons.nth(i).getAttribute('aria-label');
        expect(ariaLabel).toBeTruthy();
        expect(ariaLabel.toLowerCase()).toContain('copy');
      }
    });

    test('copy button icons should have aria-hidden', async ({ page }) => {
      const copyIcon = page.locator('#quickstart .code-block__copy-icon').first();
      const checkIcon = page.locator('#quickstart .code-block__check-icon').first();

      await expect(copyIcon).toHaveAttribute('aria-hidden', 'true');
      await expect(checkIcon).toHaveAttribute('aria-hidden', 'true');
    });
  });
});
