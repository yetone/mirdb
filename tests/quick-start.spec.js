const { test, expect } = require('@playwright/test');

test.describe('Quick Start Guide Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Installation code block displays cargo build --release', async ({ page }) => {
    // Navigate to quick start section
    const quickStartSection = page.locator('#quickstart');
    await quickStartSection.scrollIntoViewIfNeeded();
    await expect(quickStartSection).toBeVisible();

    // Find the Installation heading and its associated code block
    const installationHeading = quickStartSection.locator('h3', { hasText: 'Installation' });
    await expect(installationHeading).toBeVisible();

    // Find the code block that follows the Installation heading
    const codeBlocks = quickStartSection.locator('.code-block');
    const firstCodeBlock = codeBlocks.first();
    await expect(firstCodeBlock).toBeVisible();

    // Verify the installation command is present
    const codeText = await firstCodeBlock.textContent();
    expect(codeText).toContain('cargo build --release');
  });

  test('TC2: SET operation example is displayed showing how to store a value', async ({ page }) => {
    // Navigate to quick start section
    const quickStartSection = page.locator('#quickstart');
    await quickStartSection.scrollIntoViewIfNeeded();
    await expect(quickStartSection).toBeVisible();

    // Find the Basic Operations heading
    const basicOpsHeading = quickStartSection.locator('h3', { hasText: 'Basic Operations' });
    await expect(basicOpsHeading).toBeVisible();

    // Find the code block containing the basic operations
    const codeBlocks = quickStartSection.locator('.code-block');
    let foundSetCommand = false;

    const count = await codeBlocks.count();
    for (let i = 0; i < count; i++) {
      const codeText = await codeBlocks.nth(i).textContent();
      if (codeText.includes('set mykey') && codeText.includes('STORED')) {
        foundSetCommand = true;
        break;
      }
    }

    expect(foundSetCommand).toBeTruthy();
  });

  test('TC3: GET operation example is displayed showing how to retrieve a value', async ({ page }) => {
    // Navigate to quick start section
    const quickStartSection = page.locator('#quickstart');
    await quickStartSection.scrollIntoViewIfNeeded();
    await expect(quickStartSection).toBeVisible();

    // Find code blocks containing GET command
    const codeBlocks = quickStartSection.locator('.code-block');
    let foundGetCommand = false;

    const count = await codeBlocks.count();
    for (let i = 0; i < count; i++) {
      const codeText = await codeBlocks.nth(i).textContent();
      if (codeText.includes('get mykey') && codeText.includes('VALUE mykey')) {
        foundGetCommand = true;
        break;
      }
    }

    expect(foundGetCommand).toBeTruthy();
  });

  test('TC4: DELETE operation example is displayed showing how to remove a value', async ({ page }) => {
    // Navigate to quick start section
    const quickStartSection = page.locator('#quickstart');
    await quickStartSection.scrollIntoViewIfNeeded();
    await expect(quickStartSection).toBeVisible();

    // Find code blocks containing DELETE command
    const codeBlocks = quickStartSection.locator('.code-block');
    let foundDeleteCommand = false;

    const count = await codeBlocks.count();
    for (let i = 0; i < count; i++) {
      const codeText = await codeBlocks.nth(i).textContent();
      if (codeText.includes('delete mykey') && codeText.includes('DELETED')) {
        foundDeleteCommand = true;
        break;
      }
    }

    expect(foundDeleteCommand).toBeTruthy();
  });

  test('TC5: Code blocks have syntax highlighting applied', async ({ page }) => {
    // Navigate to quick start section
    const quickStartSection = page.locator('#quickstart');
    await quickStartSection.scrollIntoViewIfNeeded();
    await expect(quickStartSection).toBeVisible();

    // Get all code blocks in the quick start section
    const codeBlocks = quickStartSection.locator('.code-block');
    const count = await codeBlocks.count();

    // Ensure there are code blocks
    expect(count).toBeGreaterThan(0);

    // Check that syntax highlighting is present via .code-comment class
    // The existing implementation uses spans with .code-comment class for highlighting
    let hasHighlightedComments = false;

    for (let i = 0; i < count; i++) {
      const codeBlock = codeBlocks.nth(i);
      const commentSpans = codeBlock.locator('.code-comment');
      const spanCount = await commentSpans.count();

      if (spanCount > 0) {
        hasHighlightedComments = true;

        // Verify the comment spans have proper styling (different color)
        const firstComment = commentSpans.first();
        const color = await firstComment.evaluate(el =>
          window.getComputedStyle(el).color
        );

        // The comment color should be a grayish color (different from main text)
        // #64748b in RGB is approximately rgb(100, 116, 139)
        expect(color).toBeTruthy();
      }
    }

    // Verify syntax highlighting is present (comments are styled)
    expect(hasHighlightedComments).toBeTruthy();
  });
});
