// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = path.resolve(__dirname, '../index.html');

/**
 * E2E Tests for Quick Start Section with Code Examples
 * Scenario: Verify the quick-start section displays installation instructions
 * and code examples with syntax highlighting
 */

test.describe('Quick Start Section with Code Examples', () => {

  test.beforeEach(async ({ page }) => {
    await page.goto(`file://${indexPath}`);
  });

  /**
   * Test Case 1: Check quick-start section for server startup command
   * Expected: Command 'mirdb -c config.toml' or similar is displayed
   */
  test('TC1: displays server startup command mirdb -c config.toml', async ({ page }) => {
    // Navigate to quick-start section
    const quickStartSection = page.locator('[data-testid="quickstart-section"]');
    await expect(quickStartSection).toBeVisible();

    // Verify the server startup command is present
    const serverStartCode = page.locator('#server-start-code');
    await expect(serverStartCode).toBeVisible();

    const codeContent = await serverStartCode.textContent();
    expect(codeContent).toContain('mirdb -c config.toml');
  });

  /**
   * Test Case 2: Verify default port is mentioned
   * Expected: Port 12333 is referenced in connection examples
   */
  test('TC2: references port 12333 in connection examples', async ({ page }) => {
    const quickStartSection = page.locator('[data-testid="quickstart-section"]');
    await expect(quickStartSection).toBeVisible();

    // Check that port 12333 is mentioned in the connection section
    const quickStartText = await quickStartSection.textContent();
    expect(quickStartText).toContain('12333');

    // Verify the telnet command includes the port
    const connectCode = page.locator('#connect-code');
    await expect(connectCode).toBeVisible();

    const connectContent = await connectCode.textContent();
    expect(connectContent).toContain('localhost 12333');
  });

  /**
   * Test Case 3: Check for SET command example
   * Expected: SET command example with format 'set <key> <flags> <ttl> <bytes>' is present
   */
  test('TC3: displays SET command example with proper format', async ({ page }) => {
    const quickStartSection = page.locator('[data-testid="quickstart-section"]');
    await expect(quickStartSection).toBeVisible();

    // Check that SET command format is documented
    const setExample = page.locator('#set-code');
    await expect(setExample).toBeVisible();

    const setContent = await setExample.textContent();

    // Verify SET command format explanation is present
    expect(setContent).toContain('set');
    expect(setContent).toMatch(/<key>.*<flags>.*<ttl>.*<bytes>/);

    // Verify example usage
    expect(setContent).toContain('set mykey 0 0 5');
    expect(setContent).toContain('hello');
    expect(setContent).toContain('STORED');
  });

  /**
   * Test Case 4: Check for GET command example
   * Expected: GET command example showing retrieval of stored value is present
   */
  test('TC4: displays GET command example showing value retrieval', async ({ page }) => {
    const quickStartSection = page.locator('[data-testid="quickstart-section"]');
    await expect(quickStartSection).toBeVisible();

    // Check that GET command example is present
    const getExample = page.locator('#get-code');
    await expect(getExample).toBeVisible();

    const getContent = await getExample.textContent();

    // Verify GET command is shown
    expect(getContent).toContain('get mykey');

    // Verify response format is shown
    expect(getContent).toContain('VALUE mykey 0 5');
    expect(getContent).toContain('hello');
    expect(getContent).toContain('END');
  });

  /**
   * Test Case 5: Verify code blocks have syntax highlighting
   * Expected: Code blocks use syntax highlighting CSS classes (not plain monospace)
   */
  test('TC5: code blocks have syntax highlighting CSS classes', async ({ page }) => {
    const quickStartSection = page.locator('[data-testid="quickstart-section"]');
    await expect(quickStartSection).toBeVisible();

    // Wait for page to fully load including external scripts
    await page.waitForLoadState('networkidle');

    // Check that code blocks have language-specific classes
    const codeBlocks = page.locator('.code-block code[class*="language-"]');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThan(0);

    // Verify at least one code block has syntax highlighting class applied
    const firstCodeBlock = codeBlocks.first();
    const classAttribute = await firstCodeBlock.getAttribute('class');
    expect(classAttribute).toContain('language-bash');

    // Check that the code-block container has background styling (not plain white)
    const codeBlockContainer = page.locator('.code-block').first();
    const backgroundColor = await codeBlockContainer.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    // Verify it has a dark background (our code-bg color is #1f2937)
    expect(backgroundColor).not.toBe('rgba(0, 0, 0, 0)');
    expect(backgroundColor).not.toBe('rgb(255, 255, 255)');
  });

  /**
   * Test Case 6: Test copy button on code blocks
   * Expected: Copy button exists and copies code to clipboard when clicked
   */
  test('TC6: copy button exists and copies code to clipboard', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const quickStartSection = page.locator('[data-testid="quickstart-section"]');
    await expect(quickStartSection).toBeVisible();

    // Check that copy buttons exist on code blocks
    const copyButtons = page.locator('.code-block .copy-btn');
    const copyButtonCount = await copyButtons.count();
    expect(copyButtonCount).toBeGreaterThan(0);

    // Test the first copy button
    const firstCopyButton = copyButtons.first();
    await expect(firstCopyButton).toBeVisible();
    await expect(firstCopyButton).toHaveText(/Copy/);

    // Get the target code content
    const targetId = await firstCopyButton.getAttribute('data-copy-target');
    expect(targetId).toBeTruthy();

    const targetCode = page.locator(`#${targetId}`);
    const expectedContent = await targetCode.textContent();

    // Click the copy button
    await firstCopyButton.click();

    // Verify button shows "Copied!" feedback
    await expect(firstCopyButton).toHaveText(/Copied!/);

    // Read clipboard and verify content was copied
    const clipboardContent = await page.evaluate(async () => {
      return await navigator.clipboard.readText();
    });
    expect(clipboardContent).toBe(expectedContent);

    // Wait for button to reset
    await expect(firstCopyButton).toHaveText(/Copy/, { timeout: 3000 });
  });

  /**
   * Additional test: Navigation to quick-start section works
   */
  test('navigation link scrolls to quick-start section', async ({ page }) => {
    // Click on the Quick Start navigation link
    const navLink = page.locator('nav a[href="#quickstart"]');
    await expect(navLink).toBeVisible();
    await navLink.click();

    // Verify quick-start section is in viewport
    const quickStartSection = page.locator('[data-testid="quickstart-section"]');
    await expect(quickStartSection).toBeInViewport();
  });

  /**
   * Additional test: Full example session is displayed
   */
  test('full example session code block is present', async ({ page }) => {
    const fullExample = page.locator('#complete-code');
    await expect(fullExample).toBeVisible();

    const content = await fullExample.textContent();

    // Verify it contains a complete workflow
    expect(content).toContain('mirdb -c config.toml');
    expect(content).toContain('telnet localhost 12333');
    expect(content).toContain('set mykey');
    expect(content).toContain('get mykey');
    expect(content).toContain('delete mykey');
    expect(content).toContain('DELETED');
  });

});
