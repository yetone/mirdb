/**
 * Code Example Section E2E Tests
 * Owner: Scenario 3 - Code Example Section
 *
 * Tests for:
 * - Code block presence
 * - Memcached commands in example
 * - Prism.js syntax highlighting
 * - Copy-to-clipboard functionality
 * - Copy button visibility and feedback
 */

const { test, expect } = require('@playwright/test');

test.describe('Code Example Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Pre/code element exists with example code content', async ({ page }) => {
    // Verify code example section exists
    const codeSection = page.locator('#code-example');
    await expect(codeSection).toBeVisible();

    // Verify pre/code element exists within the section
    const codeBlock = codeSection.locator('pre code');
    await expect(codeBlock).toBeVisible();

    // Verify code content is not empty
    const codeContent = await codeBlock.textContent();
    expect(codeContent).toBeTruthy();
    expect(codeContent.length).toBeGreaterThan(10);
  });

  test('TC2: Code example shows GET/SET operations or connection examples', async ({ page }) => {
    // Locate the code block in the code example section
    const codeBlock = page.locator('#code-example pre code');
    await expect(codeBlock).toBeVisible();

    // Get the code content
    const codeContent = await codeBlock.textContent();

    // Verify Memcached-style commands are present
    // The example should show either GET/SET operations or connection examples
    const hasSetCommand = codeContent.toLowerCase().includes('set');
    const hasGetCommand = codeContent.toLowerCase().includes('get');
    const hasConnectionExample = codeContent.toLowerCase().includes('telnet') ||
                                  codeContent.toLowerCase().includes('connect') ||
                                  codeContent.toLowerCase().includes('localhost');

    // At least one type of Memcached-related content should be present
    expect(hasSetCommand || hasGetCommand || hasConnectionExample).toBeTruthy();

    // Verify SET and GET operations specifically
    expect(hasSetCommand).toBeTruthy();
    expect(hasGetCommand).toBeTruthy();
  });

  test('TC3: Code elements have Prism.js or highlight.js classes applied', async ({ page }) => {
    // Wait for Prism.js to initialize (it adds classes after page load)
    await page.waitForTimeout(500);

    // Check for code block with language class (Prism.js pattern)
    const codeBlock = page.locator('#code-example pre code');
    await expect(codeBlock).toBeVisible();

    // Prism.js adds language-xxx class to code elements
    const classAttribute = await codeBlock.getAttribute('class');
    expect(classAttribute).toBeTruthy();

    // Check for Prism.js language class (e.g., language-bash, language-javascript)
    const hasPrismClass = classAttribute.includes('language-');

    expect(hasPrismClass).toBeTruthy();

    // Additionally verify Prism has processed the code by checking for token spans
    // Prism.js wraps syntax elements in <span class="token ..."> elements
    const tokenSpans = page.locator('#code-example pre code span.token');
    const tokenCount = await tokenSpans.count();

    // If Prism.js is working, there should be token spans
    expect(tokenCount).toBeGreaterThan(0);
  });

  test('TC4: Copy-to-clipboard button copies code and shows success feedback', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Locate the copy button
    const copyButton = page.locator('#code-example .copy-btn');
    await expect(copyButton).toBeVisible();

    // Get the code content before clicking
    const codeBlock = page.locator('#code-example pre code');
    const expectedCode = await codeBlock.textContent();

    // Click the copy button
    await copyButton.click();

    // Verify the button shows success feedback (text changes to "Copied!")
    await expect(copyButton).toHaveText('Copied!');

    // Verify the button has the 'copied' class for styling
    await expect(copyButton).toHaveClass(/copied/);

    // Read from clipboard and verify content was copied
    const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardContent).toBe(expectedCode);

    // Wait for feedback to reset (the button text should return to "Copy" after 2 seconds)
    await page.waitForTimeout(2500);
    await expect(copyButton).toHaveText('Copy');
    await expect(copyButton).not.toHaveClass(/copied/);
  });

  test('TC5: Copy button is visible near or within the code block', async ({ page }) => {
    // Verify the code container exists
    const codeContainer = page.locator('#code-example .code-container');
    await expect(codeContainer).toBeVisible();

    // Verify copy button exists within the code container
    const copyButton = codeContainer.locator('.copy-btn');
    await expect(copyButton).toBeVisible();

    // Verify button has appropriate text
    await expect(copyButton).toHaveText('Copy');

    // Verify button is positioned within the code container (using bounding boxes)
    const containerBox = await codeContainer.boundingBox();
    const buttonBox = await copyButton.boundingBox();

    expect(containerBox).toBeTruthy();
    expect(buttonBox).toBeTruthy();

    // Button should be within the container bounds
    expect(buttonBox.x).toBeGreaterThanOrEqual(containerBox.x);
    expect(buttonBox.y).toBeGreaterThanOrEqual(containerBox.y);
    expect(buttonBox.x + buttonBox.width).toBeLessThanOrEqual(containerBox.x + containerBox.width);
    expect(buttonBox.y + buttonBox.height).toBeLessThanOrEqual(containerBox.y + containerBox.height);
  });

  test('Copy button is accessible via keyboard', async ({ page }) => {
    // Navigate to the copy button using Tab key
    const copyButton = page.locator('#code-example .copy-btn');
    await copyButton.focus();

    // Verify button is focusable
    await expect(copyButton).toBeFocused();

    // Verify button has aria-label for accessibility
    const ariaLabel = await copyButton.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel.toLowerCase()).toContain('copy');
  });

  test('Code example section has proper heading structure', async ({ page }) => {
    // Verify the section has a heading
    const sectionHeading = page.locator('#code-example h2');
    await expect(sectionHeading).toBeVisible();

    // Verify heading has appropriate content
    const headingText = await sectionHeading.textContent();
    expect(headingText).toBeTruthy();

    // Verify section has aria-labelledby for accessibility
    const codeSection = page.locator('#code-example');
    await expect(codeSection).toHaveAttribute('aria-labelledby', 'code-example-title');
  });
});
