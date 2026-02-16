/**
 * Code Examples Section E2E Tests
 * Owner: Scenario 5 - Code Examples Section Implementation
 *
 * Tests for:
 * - Code examples section visibility and structure
 * - Language tab switching (Python, Go, Node.js)
 * - Tab state persistence via localStorage
 * - Keyboard navigation (arrow keys, Enter, Space)
 * - ARIA attributes for accessibility
 * - Copy-to-clipboard functionality
 * - Mobile responsiveness
 */

const { test, expect } = require('@playwright/test');

test.describe('Code Examples Section', () => {
  test.beforeEach(async ({ page }) => {
    // Clear localStorage before each test
    await page.goto('/');
    await page.evaluate(() => localStorage.clear());
    await page.reload();
  });

  test('TC1: Code examples section is visible with language tabs', async ({ page }) => {
    // Navigate to code-examples section
    await page.goto('/#code-examples');

    // Check section is visible
    const section = page.locator('#code-examples');
    await expect(section).toBeVisible();

    // Check heading is present
    const heading = page.locator('#code-examples-heading');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Code Examples');

    // Check tabs are visible
    const tablist = page.locator('.code-examples__tabs[role="tablist"]');
    await expect(tablist).toBeVisible();

    // Check all three language tabs exist
    const pythonTab = page.locator('#tab-python');
    const goTab = page.locator('#tab-go');
    const nodejsTab = page.locator('#tab-nodejs');

    await expect(pythonTab).toBeVisible();
    await expect(goTab).toBeVisible();
    await expect(nodejsTab).toBeVisible();
  });

  test('TC2: Python tab is active by default showing Python code example', async ({ page }) => {
    await page.goto('/');

    // Check Python tab is active
    const pythonTab = page.locator('#tab-python');
    await expect(pythonTab).toHaveAttribute('aria-selected', 'true');
    await expect(pythonTab).toHaveClass(/code-examples__tab--active/);

    // Check Python panel is visible
    const pythonPanel = page.locator('#panel-python');
    await expect(pythonPanel).toBeVisible();
    await expect(pythonPanel).toHaveClass(/code-examples__panel--active/);

    // Check Python code content
    const pythonCode = page.locator('#code-python');
    await expect(pythonCode).toBeVisible();
    await expect(pythonCode).toContainText('pymemcache');
  });

  test('TC3: Go tab becomes active and displays Go code example', async ({ page }) => {
    await page.goto('/');

    // Click on Go tab
    const goTab = page.locator('#tab-go');
    await goTab.click();

    // Check Go tab is now active
    await expect(goTab).toHaveAttribute('aria-selected', 'true');
    await expect(goTab).toHaveClass(/code-examples__tab--active/);

    // Check Python tab is no longer active
    const pythonTab = page.locator('#tab-python');
    await expect(pythonTab).toHaveAttribute('aria-selected', 'false');
    await expect(pythonTab).not.toHaveClass(/code-examples__tab--active/);

    // Check Go panel is visible
    const goPanel = page.locator('#panel-go');
    await expect(goPanel).toBeVisible();

    // Check Go code content contains gomemcache
    const goCode = page.locator('#code-go');
    await expect(goCode).toContainText('gomemcache');
  });

  test('TC4: Node.js tab becomes active and displays Node.js code example', async ({ page }) => {
    await page.goto('/');

    // Click on Node.js tab
    const nodejsTab = page.locator('#tab-nodejs');
    await nodejsTab.click();

    // Check Node.js tab is now active
    await expect(nodejsTab).toHaveAttribute('aria-selected', 'true');
    await expect(nodejsTab).toHaveClass(/code-examples__tab--active/);

    // Check Node.js panel is visible
    const nodejsPanel = page.locator('#panel-nodejs');
    await expect(nodejsPanel).toBeVisible();

    // Check Node.js code content
    const nodejsCode = page.locator('#code-nodejs');
    await expect(nodejsCode).toContainText('memjs');
  });

  test('TC5: Python example content shows pymemcache import and connection to localhost:12333', async ({ page }) => {
    await page.goto('/');

    const pythonCode = page.locator('#code-python');
    await expect(pythonCode).toBeVisible();

    // Check for pymemcache import
    await expect(pythonCode).toContainText('from pymemcache.client.base import Client');

    // Check for connection to localhost:12333
    await expect(pythonCode).toContainText("'localhost'");
    await expect(pythonCode).toContainText('12333');

    // Check for basic operations
    await expect(pythonCode).toContainText('client.set');
    await expect(pythonCode).toContainText('client.get');
    await expect(pythonCode).toContainText('client.delete');
  });

  test('TC6: Go example content shows memcached client import and connection to localhost:12333', async ({ page }) => {
    await page.goto('/');

    // Switch to Go tab
    await page.click('#tab-go');

    const goCode = page.locator('#code-go');
    await expect(goCode).toBeVisible();

    // Check for gomemcache import
    await expect(goCode).toContainText('github.com/bradfitz/gomemcache/memcache');

    // Check for connection to localhost:12333
    await expect(goCode).toContainText('localhost:12333');

    // Check for basic operations
    await expect(goCode).toContainText('mc.Set');
    await expect(goCode).toContainText('mc.Get');
    await expect(goCode).toContainText('mc.Delete');
  });

  test('TC7: Node.js example content shows memcached package usage and connection to localhost:12333', async ({ page }) => {
    await page.goto('/');

    // Switch to Node.js tab
    await page.click('#tab-nodejs');

    const nodejsCode = page.locator('#code-nodejs');
    await expect(nodejsCode).toBeVisible();

    // Check for memjs require
    await expect(nodejsCode).toContainText("require('memjs')");

    // Check for connection to localhost:12333
    await expect(nodejsCode).toContainText('localhost:12333');

    // Check for basic operations
    await expect(nodejsCode).toContainText('client.set');
    await expect(nodejsCode).toContainText('client.get');
    await expect(nodejsCode).toContainText('client.delete');
  });

  test('TC8: Tab keyboard navigation - arrow keys move between tabs, Enter/Space activates', async ({ page }) => {
    await page.goto('/');

    const pythonTab = page.locator('#tab-python');
    const goTab = page.locator('#tab-go');
    const nodejsTab = page.locator('#tab-nodejs');

    // Focus on Python tab
    await pythonTab.focus();

    // Press ArrowRight to move to Go tab
    await page.keyboard.press('ArrowRight');
    await expect(goTab).toBeFocused();

    // Press ArrowRight to move to Node.js tab
    await page.keyboard.press('ArrowRight');
    await expect(nodejsTab).toBeFocused();

    // Press ArrowRight to wrap around to Python tab
    await page.keyboard.press('ArrowRight');
    await expect(pythonTab).toBeFocused();

    // Press ArrowLeft to move to Node.js tab
    await page.keyboard.press('ArrowLeft');
    await expect(nodejsTab).toBeFocused();

    // Press Enter to activate Node.js tab
    await page.keyboard.press('Enter');
    await expect(nodejsTab).toHaveAttribute('aria-selected', 'true');

    // Check Node.js panel is now visible
    const nodejsPanel = page.locator('#panel-nodejs');
    await expect(nodejsPanel).toBeVisible();

    // Move to Go tab and activate with Space
    await page.keyboard.press('ArrowLeft');
    await expect(goTab).toBeFocused();
    await page.keyboard.press(' ');
    await expect(goTab).toHaveAttribute('aria-selected', 'true');

    // Check Go panel is now visible
    const goPanel = page.locator('#panel-go');
    await expect(goPanel).toBeVisible();
  });

  test('TC9: Tab state persistence - Go tab remains selected after page refresh', async ({ page }) => {
    await page.goto('/');

    // Click on Go tab
    await page.click('#tab-go');

    // Verify Go is selected
    const goTab = page.locator('#tab-go');
    await expect(goTab).toHaveAttribute('aria-selected', 'true');

    // Reload the page
    await page.reload();

    // Wait for page to load
    await page.waitForLoadState('domcontentloaded');

    // Check Go tab is still selected after reload
    await expect(page.locator('#tab-go')).toHaveAttribute('aria-selected', 'true');

    // Check Go panel is visible
    const goPanel = page.locator('#panel-go');
    await expect(goPanel).toBeVisible();
  });

  test('TC10: ARIA attributes on tabs - role=tablist, role=tab, role=tabpanel', async ({ page }) => {
    await page.goto('/');

    // Check tablist has correct role
    const tablist = page.locator('.code-examples__tabs');
    await expect(tablist).toHaveAttribute('role', 'tablist');
    await expect(tablist).toHaveAttribute('aria-label', 'Programming language examples');

    // Check each tab has correct role and attributes
    const tabs = page.locator('.code-examples__tab');
    const tabCount = await tabs.count();

    for (let i = 0; i < tabCount; i++) {
      const tab = tabs.nth(i);
      await expect(tab).toHaveAttribute('role', 'tab');

      // Check aria-controls points to valid panel
      const controlsId = await tab.getAttribute('aria-controls');
      const panel = page.locator(`#${controlsId}`);
      await expect(panel).toHaveCount(1);
    }

    // Check panels have correct role
    const pythonPanel = page.locator('#panel-python');
    const goPanel = page.locator('#panel-go');
    const nodejsPanel = page.locator('#panel-nodejs');

    await expect(pythonPanel).toHaveAttribute('role', 'tabpanel');
    await expect(pythonPanel).toHaveAttribute('aria-labelledby', 'tab-python');

    await expect(goPanel).toHaveAttribute('role', 'tabpanel');
    await expect(goPanel).toHaveAttribute('aria-labelledby', 'tab-go');

    await expect(nodejsPanel).toHaveAttribute('role', 'tabpanel');
    await expect(nodejsPanel).toHaveAttribute('aria-labelledby', 'tab-nodejs');
  });

  test('TC11: Copy button on code examples has working copy-to-clipboard functionality', async ({ page, context }) => {
    await page.goto('/');

    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Find the copy button for Python code
    const copyButton = page.locator('#panel-python .code-examples__copy-btn');
    await expect(copyButton).toBeVisible();
    await expect(copyButton).toHaveAttribute('aria-label', 'Copy to clipboard');

    // Click the copy button
    await copyButton.click();

    // Check button shows copied state
    await expect(copyButton).toHaveClass(/code-examples__copy-btn--copied/);
    await expect(copyButton).toHaveAttribute('aria-label', 'Copied!');

    // Verify clipboard content (the Python code should be copied)
    const clipboardText = await page.evaluate(async () => {
      return await navigator.clipboard.readText();
    });

    expect(clipboardText).toContain('pymemcache');
    expect(clipboardText).toContain('localhost');
    expect(clipboardText).toContain('12333');

    // Wait for copied state to reset
    await page.waitForTimeout(2500);
    await expect(copyButton).not.toHaveClass(/code-examples__copy-btn--copied/);
    await expect(copyButton).toHaveAttribute('aria-label', 'Copy to clipboard');
  });

  test('TC12: Code tabs are scrollable or wrap appropriately on narrow viewports', async ({ page }) => {
    // Set narrow mobile viewport
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto('/');

    // Check tabs section is visible
    const tabsWrapper = page.locator('.code-examples__tabs-wrapper');
    await expect(tabsWrapper).toBeVisible();

    // Check all tabs are still accessible
    const pythonTab = page.locator('#tab-python');
    const goTab = page.locator('#tab-go');
    const nodejsTab = page.locator('#tab-nodejs');

    await expect(pythonTab).toBeVisible();
    await expect(goTab).toBeVisible();
    await expect(nodejsTab).toBeVisible();

    // Check that tabs wrapper allows horizontal scrolling
    const hasOverflow = await tabsWrapper.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return styles.overflowX === 'auto' || styles.overflowX === 'scroll';
    });
    expect(hasOverflow).toBe(true);

    // Check that code panels fit within viewport
    const codePanel = page.locator('#panel-python .code-examples__code');
    await expect(codePanel).toBeVisible();

    const panelBox = await codePanel.boundingBox();
    expect(panelBox.x).toBeGreaterThanOrEqual(0);
    expect(panelBox.x + panelBox.width).toBeLessThanOrEqual(320 + 20); // Allow some padding

    // Verify tab clicking works on mobile
    await goTab.click();
    await expect(goTab).toHaveAttribute('aria-selected', 'true');
    await expect(page.locator('#panel-go')).toBeVisible();
  });
});
