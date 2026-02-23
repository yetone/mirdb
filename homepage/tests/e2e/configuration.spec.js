/**
 * Configuration Section E2E Tests
 * Owner: Scenario 5 - Configuration Section
 *
 * End-to-end tests for configuration section:
 * - Section is present
 * - Port configuration documented
 * - Data directory documented
 * - WAL settings documented
 * - Code examples have syntax highlighting
 */

import { test, expect } from '@playwright/test';

test.describe('Configuration Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Configuration section exists and is visible', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Verify section has proper heading
    const configTitle = page.locator('#configuration-title');
    await expect(configTitle).toBeVisible();
    await expect(configTitle).toHaveText('Configuration');

    // Verify the heading is an h2 element
    const tagName = await configTitle.evaluate((el) => el.tagName.toLowerCase());
    expect(tagName).toBe('h2');

    // Verify section has aria-labelledby for accessibility
    await expect(configSection).toHaveAttribute('aria-labelledby', 'configuration-title');
  });

  test('TC2: Port configuration example is documented', async ({ page }) => {
    // Find port configuration option
    const portOption = page.locator('.configuration__option').filter({
      has: page.locator('.configuration__option-title', { hasText: 'Port Configuration' })
    });
    await expect(portOption).toBeVisible();

    // Verify description mentions port and default
    const portDescription = portOption.locator('.configuration__option-description');
    await expect(portDescription).toContainText('port');

    // Verify code example contains --port flag
    const portCode = portOption.locator('.code-block code');
    await expect(portCode).toContainText('--port');
    await expect(portCode).toContainText('mirdb-server');
  });

  test('TC3: Data directory configuration is documented', async ({ page }) => {
    // Find data directory option
    const dataOption = page.locator('.configuration__option').filter({
      has: page.locator('.configuration__option-title', { hasText: 'Data Directory' })
    });
    await expect(dataOption).toBeVisible();

    // Verify description mentions data files
    const dataDescription = dataOption.locator('.configuration__option-description');
    await expect(dataDescription).toContainText('data');

    // Verify code example contains --data-dir flag
    const dataCode = dataOption.locator('.code-block code');
    await expect(dataCode).toContainText('--data-dir');
    await expect(dataCode).toContainText('mirdb-server');
  });

  test('TC4: WAL settings documentation is included', async ({ page }) => {
    // Find WAL settings option
    const walOption = page.locator('.configuration__option').filter({
      has: page.locator('.configuration__option-title', { hasText: 'WAL' })
    });
    await expect(walOption).toBeVisible();

    // Verify description mentions Write-Ahead Log or durability
    const walDescription = walOption.locator('.configuration__option-description');
    const descriptionText = await walDescription.textContent();
    expect(descriptionText.toLowerCase()).toMatch(/write-ahead|wal|durability/i);

    // Verify code example exists
    const walCode = walOption.locator('.code-block code');
    await expect(walCode).toBeVisible();
    await expect(walCode).toContainText('mirdb-server');
  });

  test('TC5: Configuration code examples have syntax highlighting and copy buttons', async ({ page }) => {
    const configSection = page.locator('#configuration');
    const codeBlocks = configSection.locator('.code-block');

    // Verify there are code blocks in the configuration section
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThanOrEqual(3);

    // Check each code block has required elements
    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);

      // Verify language label is present
      const languageLabel = codeBlock.locator('.code-block__language');
      await expect(languageLabel).toBeVisible();

      // Verify copy button is present
      const copyButton = codeBlock.locator('.code-block__copy');
      await expect(copyButton).toBeVisible();

      // Verify copy button has proper accessibility attributes
      await expect(copyButton).toHaveAttribute('aria-label', 'Copy to clipboard');
      await expect(copyButton).toHaveAttribute('type', 'button');

      // Verify code content is in pre/code tags
      const preElement = codeBlock.locator('pre');
      await expect(preElement).toBeVisible();

      const codeElement = codeBlock.locator('code');
      await expect(codeElement).toBeVisible();

      // Verify the code has a language class (for syntax highlighting)
      const codeClass = await codeElement.getAttribute('class');
      expect(codeClass).toMatch(/language-/);
    }
  });

  test('Copy button functionality works for configuration examples', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Find first code block in configuration section
    const configSection = page.locator('#configuration');
    const firstCodeBlock = configSection.locator('.code-block').first();
    const copyButton = firstCodeBlock.locator('.code-block__copy');

    // Click copy button
    await copyButton.click();

    // Verify button shows copied state
    await expect(copyButton).toHaveClass(/code-block__copy--copied/);

    // Read clipboard content
    const clipboardText = await page.evaluate(() => navigator.clipboard.readText());

    // Verify something was copied
    expect(clipboardText.length).toBeGreaterThan(0);
    expect(clipboardText).toContain('mirdb-server');
  });

  test('Configuration section follows Roadmap section', async ({ page }) => {
    // Get the positions of both sections
    const roadmapSection = page.locator('#roadmap');
    const configSection = page.locator('#configuration');

    await expect(roadmapSection).toBeVisible();
    await expect(configSection).toBeVisible();

    const roadmapBox = await roadmapSection.boundingBox();
    const configBox = await configSection.boundingBox();

    // Configuration section should be below Roadmap section
    expect(configBox.y).toBeGreaterThan(roadmapBox.y);
  });

  test('Configuration section has proper semantic structure', async ({ page }) => {
    const configSection = page.locator('#configuration');

    // Verify it's a section element
    const tagName = await configSection.evaluate((el) => el.tagName.toLowerCase());
    expect(tagName).toBe('section');

    // Verify h3 headings are used for configuration options
    const optionTitles = configSection.locator('.configuration__option-title');
    const count = await optionTitles.count();

    for (let i = 0; i < count; i++) {
      const title = optionTitles.nth(i);
      const titleTag = await title.evaluate((el) => el.tagName.toLowerCase());
      expect(titleTag).toBe('h3');
    }
  });

  test('Configuration options have descriptive text', async ({ page }) => {
    const configSection = page.locator('#configuration');
    const options = configSection.locator('.configuration__option');
    const optionCount = await options.count();

    // Each option should have a title and description
    for (let i = 0; i < optionCount; i++) {
      const option = options.nth(i);

      const title = option.locator('.configuration__option-title');
      await expect(title).toBeVisible();

      const description = option.locator('.configuration__option-description');
      await expect(description).toBeVisible();

      // Description should have meaningful content
      const descText = await description.textContent();
      expect(descText.length).toBeGreaterThan(10);
    }
  });
});
