/**
 * Quick Start Section E2E Tests
 * Owner: Scenario 3 - Quick Start Section
 *
 * Test cases:
 * - Installation code block presence
 * - Copy button visibility and functionality
 * - Usage GIF renders with alt text
 * - Syntax highlighting contrast
 */

const { test, expect } = require('@playwright/test');
const { waitForPageLoad } = require('./test-utils');

test.describe('Quick Start Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);
  });

  test('should have a quick start section with proper ID', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();
  });

  test('should display installation code block with command', async ({ page }) => {
    // Navigate to quick start section
    const quickstartSection = page.locator('#quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();

    // Check for installation code block
    const installCodeBlock = page.locator('.quickstart-install .code-block');
    await expect(installCodeBlock).toBeVisible();

    // Verify installation command content
    const installCode = page.locator('#install-code');
    await expect(installCode).toContainText('cargo install mirdb-server');
  });

  test('should have copy button visible on installation code block', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();

    // Check for copy button on installation code block
    const copyButton = page.locator('.quickstart-install .copy-button');
    await expect(copyButton).toBeVisible();

    // Verify button has proper aria-label
    await expect(copyButton).toHaveAttribute('aria-label', 'Copy installation command to clipboard');

    // Verify button text
    const copyText = copyButton.locator('.copy-text');
    await expect(copyText).toHaveText('Copy');
  });

  test('should copy installation command to clipboard on button click', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const quickstartSection = page.locator('#quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();

    const copyButton = page.locator('.quickstart-install .copy-button');

    // Click the copy button
    await copyButton.click();

    // Check for visual feedback - button should show "Copied!"
    const copyText = copyButton.locator('.copy-text');
    await expect(copyText).toHaveText('Copied!');

    // Check button has copied class
    await expect(copyButton).toHaveClass(/copied/);

    // Verify clipboard contents
    const clipboardText = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardText).toContain('cargo install mirdb-server');
  });

  test('should display usage example code block', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();

    // Check for usage code block
    const usageCodeBlock = page.locator('.quickstart-usage .code-block');
    await expect(usageCodeBlock).toBeVisible();

    // Verify usage example contains set/get commands
    const usageCode = page.locator('#usage-code');
    await expect(usageCode).toContainText('mirdb-server');
    await expect(usageCode).toContainText('set mykey');
    await expect(usageCode).toContainText('get mykey');
    await expect(usageCode).toContainText('STORED');
  });

  test('should have copy button on usage example code block', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();

    // Check for copy button on usage code block
    const copyButton = page.locator('.quickstart-usage .copy-button');
    await expect(copyButton).toBeVisible();
    await expect(copyButton).toHaveAttribute('aria-label', 'Copy usage example to clipboard');
  });

  test('should display usage.gif with proper alt text', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();

    // Check for usage GIF
    const usageGif = page.locator('.usage-gif');
    await expect(usageGif).toBeVisible();

    // Verify src attribute
    await expect(usageGif).toHaveAttribute('src', 'assets/usage.gif');

    // Verify alt text is present and descriptive
    const altText = await usageGif.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText.length).toBeGreaterThan(10);
    expect(altText.toLowerCase()).toContain('usage');
  });

  test('should have lazy loading on usage.gif', async ({ page }) => {
    const usageGif = page.locator('.usage-gif');
    await expect(usageGif).toHaveAttribute('loading', 'lazy');
  });

  test('should have proper section heading hierarchy', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');

    // Main section title should be h2
    const sectionTitle = quickstartSection.locator('h2.section-title');
    await expect(sectionTitle).toBeVisible();
    await expect(sectionTitle).toHaveText('Quick Start');

    // Subtitles should be h3
    const subtitles = quickstartSection.locator('h3.quickstart-subtitle');
    const count = await subtitles.count();
    expect(count).toBeGreaterThanOrEqual(2);
  });

  test('should navigate to quickstart section when clicking Get Started button', async ({ page }) => {
    // Click the CTA button in the hero section
    const ctaButton = page.locator('.cta-button');
    await ctaButton.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(500);

    // Verify the quickstart section is in view
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();
  });

  test('code blocks should have horizontal scroll on overflow', async ({ page }) => {
    const codeBlock = page.locator('.code-block').first();

    // Check that overflow-x is set to auto or scroll
    const overflowX = await codeBlock.evaluate((el) => {
      return window.getComputedStyle(el).overflowX;
    });

    expect(['auto', 'scroll']).toContain(overflowX);
  });

  test('copy button should have visible focus state', async ({ page }) => {
    const copyButton = page.locator('.quickstart-install .copy-button');

    // Focus the button
    await copyButton.focus();

    // Check that focus is visible (outline should be present)
    const outline = await copyButton.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return styles.outline || styles.outlineWidth;
    });

    // Button should have some outline when focused
    expect(outline).not.toBe('none');
  });
});

test.describe('Quick Start Section - Syntax Highlighting', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);
  });

  test('should have syntax highlighting elements', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();

    // Check for comment highlighting
    const comments = page.locator('.code-comment');
    expect(await comments.count()).toBeGreaterThan(0);

    // Check for keyword highlighting
    const keywords = page.locator('.code-keyword');
    expect(await keywords.count()).toBeGreaterThan(0);
  });

  // Manual test: Syntax highlighting contrast ratio (WCAG AA)
  // This test documents the contrast requirements but actual measurement
  // requires manual verification or specialized tooling
  test('syntax highlighting colors should be defined', async ({ page }) => {
    const codeBlock = page.locator('.code-block').first();

    // Get CSS custom properties for syntax colors
    const colors = await page.evaluate(() => {
      const root = document.documentElement;
      const styles = getComputedStyle(root);
      return {
        codeText: styles.getPropertyValue('--color-code-text').trim(),
        codeBg: styles.getPropertyValue('--color-code-bg').trim(),
        codeComment: styles.getPropertyValue('--color-code-comment').trim(),
        codeKeyword: styles.getPropertyValue('--color-code-keyword').trim(),
      };
    });

    // Verify colors are defined
    expect(colors.codeText).toBeTruthy();
    expect(colors.codeBg).toBeTruthy();
    expect(colors.codeComment).toBeTruthy();
    expect(colors.codeKeyword).toBeTruthy();
  });
});
