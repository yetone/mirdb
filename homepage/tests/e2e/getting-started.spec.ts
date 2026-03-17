/**
 * Getting Started Section Tests
 * Owner: Scenario 5 - Getting Started Section
 *
 * Tests:
 * - E2E: Installation section contains cargo build or cargo install commands
 * - E2E: Installation section includes cargo run command for starting server
 * - E2E: Quick example shows 'set' command with key-value syntax
 * - E2E: Quick example shows 'get' command for retrieving values
 * - E2E: Quick example shows 'delete' command for removing keys
 * - E2E: Code blocks use pre/code elements with syntax highlighting class
 * - E2E: Code blocks have clickable copy button
 * - E2E: Copy button copies code content to clipboard
 */

import { test, expect, Page } from '@playwright/test';

test.describe('Getting Started Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Section contains cargo build or cargo install commands
  test('should have installation instructions with cargo build or install commands', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Check for installation subsection
    const installationSubsection = gettingStartedSection.locator('[data-testid="installation-subsection"], #installation');
    await expect(installationSubsection).toBeVisible();

    // Check for cargo build command in code block
    const installationCodeBlock = installationSubsection.locator('.code-block code');
    await expect(installationCodeBlock).toBeVisible();

    const codeText = await installationCodeBlock.textContent();
    // Should contain cargo build or cargo install
    const hasCargoCommand = codeText?.includes('cargo build') || codeText?.includes('cargo install');
    expect(hasCargoCommand).toBe(true);
  });

  // Test Case 2: Section includes cargo run command for starting server
  test('should include cargo run command for starting the server', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Check for installation subsection
    const installationSubsection = gettingStartedSection.locator('[data-testid="installation-subsection"], #installation');
    await expect(installationSubsection).toBeVisible();

    const installationCodeBlock = installationSubsection.locator('.code-block code');
    const codeText = await installationCodeBlock.textContent();

    // Should contain cargo run command
    expect(codeText).toContain('cargo run');
  });

  // Test Case 3: Example shows 'set' command with key-value syntax
  test('should show memcached set command example with key-value syntax', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Check for example subsection
    const exampleSubsection = gettingStartedSection.locator('[data-testid="example-subsection"], #example');
    await expect(exampleSubsection).toBeVisible();

    const exampleCodeBlock = exampleSubsection.locator('.code-block code');
    const codeText = await exampleCodeBlock.textContent();

    // Should contain memcached set command
    expect(codeText?.toLowerCase()).toContain('set');
    // Should show key-value pattern (set followed by a key name)
    expect(codeText).toMatch(/set\s+\w+/i);
  });

  // Test Case 4: Example shows 'get' command for retrieving values
  test('should show memcached get command example for retrieving values', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Check for example subsection
    const exampleSubsection = gettingStartedSection.locator('[data-testid="example-subsection"], #example');
    await expect(exampleSubsection).toBeVisible();

    const exampleCodeBlock = exampleSubsection.locator('.code-block code');
    const codeText = await exampleCodeBlock.textContent();

    // Should contain memcached get command
    expect(codeText?.toLowerCase()).toContain('get');
    // Should show get followed by a key name
    expect(codeText).toMatch(/get\s+\w+/i);
  });

  // Test Case 5: Example shows 'delete' command for removing keys
  test('should show memcached delete command example for removing keys', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Check for example subsection
    const exampleSubsection = gettingStartedSection.locator('[data-testid="example-subsection"], #example');
    await expect(exampleSubsection).toBeVisible();

    const exampleCodeBlock = exampleSubsection.locator('.code-block code');
    const codeText = await exampleCodeBlock.textContent();

    // Should contain memcached delete command
    expect(codeText?.toLowerCase()).toContain('delete');
    // Should show delete followed by a key name
    expect(codeText).toMatch(/delete\s+\w+/i);
  });

  // Test Case 6: Code blocks use pre/code elements with syntax highlighting class
  test('should have code blocks with syntax highlighting classes', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Get all code blocks
    const codeBlocks = gettingStartedSection.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Check each code block has proper structure
    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);

      // Should have pre element
      const preElement = codeBlock.locator('pre');
      await expect(preElement).toBeVisible();

      // Should have code element
      const codeElement = codeBlock.locator('code');
      await expect(codeElement).toBeVisible();

      // Code element should have syntax-highlight class
      const classes = await codeElement.getAttribute('class');
      expect(classes).toContain('syntax-highlight');
    }
  });

  // Test Case 7: Code blocks have clickable copy button
  test('should have copy-to-clipboard buttons on code blocks', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Get all code blocks
    const codeBlocks = gettingStartedSection.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Each code block should have a copy button
    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);

      // Should have copy button
      const copyButton = codeBlock.locator('.copy-btn, [data-copy-btn]');
      await expect(copyButton).toBeVisible();

      // Button should be clickable (have cursor: pointer style or be a button element)
      const tagName = await copyButton.evaluate(el => el.tagName.toLowerCase());
      expect(tagName).toBe('button');
    }
  });

  // Test Case 8: Copy button copies code content to clipboard
  test('should copy code content to clipboard when copy button is clicked', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Get the first code block
    const codeBlock = gettingStartedSection.locator('.code-block').first();
    await expect(codeBlock).toBeVisible();

    // Get the code content
    const codeElement = codeBlock.locator('code');
    const expectedContent = await codeElement.textContent();
    expect(expectedContent).toBeTruthy();

    // Click the copy button
    const copyButton = codeBlock.locator('.copy-btn, [data-copy-btn]');
    await expect(copyButton).toBeVisible();
    await copyButton.click();

    // Wait for copy action to complete
    await page.waitForTimeout(100);

    // Verify clipboard content
    const clipboardContent = await page.evaluate(async () => {
      return await navigator.clipboard.readText();
    });

    expect(clipboardContent).toBe(expectedContent);
  });

  // Additional test: Copy button shows feedback on click
  test('should show visual feedback when copy button is clicked', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Get the first code block's copy button
    const codeBlock = gettingStartedSection.locator('.code-block').first();
    const copyButton = codeBlock.locator('.copy-btn, [data-copy-btn]');
    await expect(copyButton).toBeVisible();

    // Get original button text
    const originalText = await copyButton.textContent();

    // Click the copy button
    await copyButton.click();

    // Button text should change to indicate success
    const newText = await copyButton.textContent();
    expect(newText).not.toBe(originalText);
    expect(newText?.toLowerCase()).toMatch(/copied|success/i);
  });

  // Additional test: Getting started section is properly positioned
  test('should be positioned after demo section', async ({ page }) => {
    const demoSection = page.locator('#demo');
    const gettingStartedSection = page.locator('#getting-started');

    await expect(demoSection).toBeVisible();
    await expect(gettingStartedSection).toBeVisible();

    // Get bounding boxes to verify order
    const demoBBox = await demoSection.boundingBox();
    const gettingStartedBBox = await gettingStartedSection.boundingBox();

    expect(demoBBox).toBeTruthy();
    expect(gettingStartedBBox).toBeTruthy();

    // Getting started section should be below demo section
    expect(gettingStartedBBox!.y).toBeGreaterThan(demoBBox!.y);
  });

  // Additional test: Section has proper accessibility attributes
  test('should have proper accessibility attributes', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Check for aria-labelledby or aria-label
    const ariaLabelledBy = await gettingStartedSection.getAttribute('aria-labelledby');
    const ariaLabel = await gettingStartedSection.getAttribute('aria-label');

    expect(ariaLabelledBy || ariaLabel).toBeTruthy();

    // Check heading exists and is properly associated
    if (ariaLabelledBy) {
      const heading = page.locator(`#${ariaLabelledBy}`);
      await expect(heading).toBeVisible();
    }
  });

  // Additional test: Both installation and example subsections exist
  test('should have both installation and quick example subsections', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Check for installation subsection
    const installationSubsection = gettingStartedSection.locator('[data-testid="installation-subsection"], #installation');
    await expect(installationSubsection).toBeVisible();

    // Should have installation heading
    const installationHeading = installationSubsection.locator('h3');
    await expect(installationHeading).toBeVisible();
    const installationText = await installationHeading.textContent();
    expect(installationText?.toLowerCase()).toContain('install');

    // Check for example subsection
    const exampleSubsection = gettingStartedSection.locator('[data-testid="example-subsection"], #example');
    await expect(exampleSubsection).toBeVisible();

    // Should have example heading
    const exampleHeading = exampleSubsection.locator('h3');
    await expect(exampleHeading).toBeVisible();
    const exampleText = await exampleHeading.textContent();
    expect(exampleText?.toLowerCase()).toContain('example');
  });
});
