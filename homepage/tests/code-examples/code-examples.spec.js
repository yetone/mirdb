/**
 * Code Examples Section Tests
 * Owner: Scenario 3 - Code Examples with Copy Functionality
 *
 * Test coverage:
 * - Tabbed interface displays with SET, GET, DELETE tabs
 * - Tab switching shows correct code examples
 * - Copy functionality works and provides visual feedback
 * - Progressive enhancement (content readable without JavaScript)
 * - Syntax highlighting applied to code blocks
 */

const { test, expect } = require('@playwright/test');
const { goToHomepage, SELECTORS } = require('../setup');

test.describe('Code Examples Section', () => {
  test.beforeEach(async ({ page }) => {
    await goToHomepage(page);
  });

  test('Test 1: Tabbed interface displays with SET, GET, DELETE tabs visible', async ({ page }) => {
    // Navigate to code examples section
    const codeExamplesSection = page.locator('#code-examples');
    await expect(codeExamplesSection).toBeVisible();

    // Check that tablist exists
    const tablist = page.locator('[role="tablist"]');
    await expect(tablist).toBeVisible();

    // Check that SET, GET, DELETE tabs are present
    const setTab = page.locator('[role="tab"]', { hasText: 'SET' });
    const getTab = page.locator('[role="tab"]', { hasText: 'GET' });
    const deleteTab = page.locator('[role="tab"]', { hasText: 'DELETE' });

    await expect(setTab).toBeVisible();
    await expect(getTab).toBeVisible();
    await expect(deleteTab).toBeVisible();
  });

  test('Test 2: Click SET tab displays SET command example with correct content', async ({ page }) => {
    // Click SET tab
    const setTab = page.locator('[role="tab"]', { hasText: 'SET' });
    await setTab.click();

    // Check SET tab is selected
    await expect(setTab).toHaveAttribute('aria-selected', 'true');

    // Find the associated panel
    const setPanel = page.locator('#panel-set');
    await expect(setPanel).toBeVisible();

    // Verify content contains SET command example
    const codeContent = await setPanel.textContent();
    expect(codeContent).toContain('SET');
    expect(codeContent).toContain('mykey');
    expect(codeContent).toContain('hello');
    expect(codeContent).toContain('STORED');
  });

  test('Test 3: Click GET tab displays GET command example with expected output', async ({ page }) => {
    // Click GET tab
    const getTab = page.locator('[role="tab"]', { hasText: 'GET' });
    await getTab.click();

    // Check GET tab is selected
    await expect(getTab).toHaveAttribute('aria-selected', 'true');

    // Find the associated panel
    const getPanel = page.locator('#panel-get');
    await expect(getPanel).toBeVisible();

    // Verify content contains GET command example
    const codeContent = await getPanel.textContent();
    expect(codeContent).toContain('GET');
    expect(codeContent).toContain('mykey');
    expect(codeContent).toContain('VALUE');
  });

  test('Test 4: Click copy button copies code and shows visual feedback', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Click SET tab to ensure it's active
    const setTab = page.locator('[role="tab"]', { hasText: 'SET' });
    await setTab.click();

    // Find copy button in the SET panel
    const copyButton = page.locator('#panel-set .copy-btn');
    await expect(copyButton).toBeVisible();

    // Store original button text
    const originalText = await copyButton.textContent();

    // Click copy button
    await copyButton.click();

    // Verify visual feedback - button text changes to "Copied!"
    await expect(copyButton).toHaveText('Copied!');

    // Verify clipboard contains expected content
    const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardContent).toContain('SET');
    expect(clipboardContent).toContain('mykey');

    // Wait for button to revert to original text
    await expect(copyButton).toHaveText(originalText, { timeout: 3000 });
  });

  test('Test 5: Code examples are visible and readable without JavaScript (progressive enhancement)', async ({ browser }) => {
    // Create a new context with JavaScript disabled
    const context = await browser.newContext({ javaScriptEnabled: false });
    const page = await context.newPage();

    // Load page without JavaScript
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Code examples section should still be visible
    const codeExamplesSection = page.locator('#code-examples');
    await expect(codeExamplesSection).toBeVisible();

    // At least one code block should be visible (using noscript fallback or visible panels)
    const codeBlocks = page.locator('#code-examples pre');
    const visibleCodeBlocks = await codeBlocks.count();
    expect(visibleCodeBlocks).toBeGreaterThan(0);

    // Check that SET command content is visible somewhere (in code-panel--active or noscript)
    const sectionContent = await codeExamplesSection.textContent();
    expect(sectionContent).toContain('SET');
    expect(sectionContent).toContain('GET');
    expect(sectionContent).toContain('DELETE');

    await context.close();
  });

  test('Test 6: Syntax highlighting is applied with visually differentiated colors', async ({ page }) => {
    // Click SET tab
    const setTab = page.locator('[role="tab"]', { hasText: 'SET' });
    await setTab.click();

    // Find code block
    const codeBlock = page.locator('#panel-set .code-block');
    await expect(codeBlock).toBeVisible();

    // Check for syntax highlighting spans with different colors
    const highlightedSpans = codeBlock.locator('span[class*="syntax-"]');
    const spanCount = await highlightedSpans.count();

    // Verify there are highlighted elements
    expect(spanCount).toBeGreaterThan(0);

    // Verify different syntax classes exist for differentiation
    const codeHTML = await codeBlock.innerHTML();
    const hasCommandHighlight = codeHTML.includes('syntax-command') || codeHTML.includes('syntax-keyword');
    const hasStringHighlight = codeHTML.includes('syntax-string') || codeHTML.includes('syntax-value');
    const hasOutputHighlight = codeHTML.includes('syntax-output') || codeHTML.includes('syntax-response');

    // At least some syntax classes should be present
    expect(hasCommandHighlight || hasStringHighlight || hasOutputHighlight).toBeTruthy();
  });

  test('Tab keyboard navigation works correctly', async ({ page }) => {
    // Focus on first tab
    const setTab = page.locator('[role="tab"]', { hasText: 'SET' });
    await setTab.focus();

    // Press ArrowRight to move to next tab
    await page.keyboard.press('ArrowRight');
    const getTab = page.locator('[role="tab"]', { hasText: 'GET' });
    await expect(getTab).toBeFocused();

    // Press ArrowRight again to move to DELETE tab
    await page.keyboard.press('ArrowRight');
    const deleteTab = page.locator('[role="tab"]', { hasText: 'DELETE' });
    await expect(deleteTab).toBeFocused();

    // Verify the panel changes when tab receives focus
    const deletePanel = page.locator('#panel-delete');
    await expect(deletePanel).toBeVisible();
  });

  test('Tabs have proper ARIA attributes', async ({ page }) => {
    // Check tablist role
    const tablist = page.locator('[role="tablist"]');
    await expect(tablist).toHaveAttribute('aria-label');

    // Check individual tabs
    const tabs = page.locator('[role="tab"]');
    const tabCount = await tabs.count();
    expect(tabCount).toBeGreaterThanOrEqual(3);

    for (let i = 0; i < tabCount; i++) {
      const tab = tabs.nth(i);
      // Each tab should have aria-controls pointing to a panel
      await expect(tab).toHaveAttribute('aria-controls');
      // Each tab should have aria-selected attribute
      await expect(tab).toHaveAttribute('aria-selected');
    }

    // Check that panels have proper role
    const panels = page.locator('[role="tabpanel"]');
    const panelCount = await panels.count();
    expect(panelCount).toBeGreaterThanOrEqual(3);

    for (let i = 0; i < panelCount; i++) {
      const panel = panels.nth(i);
      // Each panel should have aria-labelledby
      await expect(panel).toHaveAttribute('aria-labelledby');
    }
  });

  test('Copy button is accessible', async ({ page }) => {
    const setTab = page.locator('[role="tab"]', { hasText: 'SET' });
    await setTab.click();

    const copyButton = page.locator('#panel-set .copy-btn');
    await expect(copyButton).toBeVisible();

    // Copy button should have accessible label
    const ariaLabel = await copyButton.getAttribute('aria-label');
    const buttonText = await copyButton.textContent();

    // Either aria-label or visible text should indicate purpose
    const hasAccessibleName = (ariaLabel && ariaLabel.toLowerCase().includes('copy')) ||
                              (buttonText && buttonText.toLowerCase().includes('copy'));
    expect(hasAccessibleName).toBeTruthy();
  });

  test('Click DELETE tab displays DELETE command example', async ({ page }) => {
    // Click DELETE tab
    const deleteTab = page.locator('[role="tab"]', { hasText: 'DELETE' });
    await deleteTab.click();

    // Check DELETE tab is selected
    await expect(deleteTab).toHaveAttribute('aria-selected', 'true');

    // Find the associated panel
    const deletePanel = page.locator('#panel-delete');
    await expect(deletePanel).toBeVisible();

    // Verify content contains DELETE command example
    const codeContent = await deletePanel.textContent();
    expect(codeContent).toContain('DELETE');
  });
});
