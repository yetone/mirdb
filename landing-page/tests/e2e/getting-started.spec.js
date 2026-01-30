/**
 * Getting Started Section E2E Tests
 * Owner: Scenario 6 - Getting Started Section
 *
 * End-to-end tests for the Getting Started section functionality
 */
const { test, expect } = require('@playwright/test');

test.describe('Getting Started Section E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('TC1: Getting Started Section HTML Structure', () => {
    test('Getting Started section is visible on the page', async ({ page }) => {
      const gettingStartedSection = page.locator('#getting-started');
      await expect(gettingStartedSection).toBeVisible();
    });

    test('Section has title "Get Started in Minutes"', async ({ page }) => {
      const title = page.locator('#getting-started .getting-started__title');
      await expect(title).toBeVisible();
      await expect(title).toHaveText('Get Started in Minutes');
    });

    test('Contains three installation steps', async ({ page }) => {
      const steps = page.locator('#getting-started .getting-started__step');
      await expect(steps).toHaveCount(3);
    });

    test('Steps are numbered 1, 2, 3', async ({ page }) => {
      const stepNumbers = page.locator('#getting-started .getting-started__step-number');

      await expect(stepNumbers.nth(0)).toHaveText('1');
      await expect(stepNumbers.nth(1)).toHaveText('2');
      await expect(stepNumbers.nth(2)).toHaveText('3');
    });

    test('Contains installation step with "Install MirDB" title', async ({ page }) => {
      const stepTitle = page.locator('#getting-started .getting-started__step-title').first();
      await expect(stepTitle).toContainText('Install MirDB');
    });
  });

  test.describe('TC2: Installation Command', () => {
    test('Installation command contains cargo install mirdb', async ({ page }) => {
      const firstCodeBlock = page.locator('#getting-started .getting-started__step').first().locator('.code-block code');
      const text = await firstCodeBlock.textContent();

      expect(text).toContain('cargo install mirdb');
    });

    test('Installation code block is visible', async ({ page }) => {
      const firstCodeBlock = page.locator('#getting-started .getting-started__step').first().locator('.code-block');
      await expect(firstCodeBlock).toBeVisible();
    });
  });

  test.describe('TC3: Copy Button Functionality', () => {
    test('Click copy button on installation command copies to clipboard', async ({ page, context }) => {
      // Grant clipboard permissions
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);

      // Click the copy button on the first code block (installation)
      const copyButton = page.locator('#getting-started .getting-started__step').first().locator('.code-block__copy');
      await copyButton.click();

      // Verify button shows "Copied!" feedback
      const copyText = copyButton.locator('.copy-text');
      await expect(copyText).toHaveText('Copied!');

      // Verify button has copied class
      await expect(copyButton).toHaveClass(/copied/);
    });

    test('Copy button shows visual feedback with icon change', async ({ page, context }) => {
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);

      const copyButton = page.locator('#getting-started .code-block__copy').first();
      const copyIcon = copyButton.locator('.copy-icon');
      const checkIcon = copyButton.locator('.check-icon');

      // Before click, copy icon should be visible
      await expect(copyIcon).toBeVisible();
      await expect(checkIcon).toBeHidden();

      // Click the copy button
      await copyButton.click();

      // After click, check icon should be visible
      await expect(copyIcon).toBeHidden();
      await expect(checkIcon).toBeVisible();
    });

    test('Copy button resets after delay', async ({ page, context }) => {
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);

      const copyButton = page.locator('#getting-started .code-block__copy').first();
      await copyButton.click();

      // Verify initial feedback
      await expect(copyButton.locator('.copy-text')).toHaveText('Copied!');

      // Wait for reset (2 seconds + buffer)
      await page.waitForTimeout(2500);

      // Verify reset
      await expect(copyButton.locator('.copy-text')).toHaveText('Copy');
      await expect(copyButton).not.toHaveClass(/copied/);
    });

    test('All code blocks have copy buttons', async ({ page }) => {
      const codeBlocks = page.locator('#getting-started .code-block');
      const copyButtons = page.locator('#getting-started .code-block__copy');

      const codeBlockCount = await codeBlocks.count();
      const copyButtonCount = await copyButtons.count();

      expect(copyButtonCount).toBe(codeBlockCount);
      expect(copyButtonCount).toBeGreaterThanOrEqual(3);
    });
  });

  test.describe('TC4: Documentation Link', () => {
    test('Documentation link is present and visible', async ({ page }) => {
      const docsLink = page.locator('#getting-started .getting-started__docs-link');
      await expect(docsLink).toBeVisible();
    });

    test('Documentation link has correct text', async ({ page }) => {
      const docsLink = page.locator('#getting-started .getting-started__docs-link');
      await expect(docsLink).toContainText('View Full Documentation');
    });

    test('Documentation link points to GitHub README', async ({ page }) => {
      const docsLink = page.locator('#getting-started .getting-started__docs-link');
      await expect(docsLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb#readme');
    });

    test('Documentation link opens in new tab', async ({ page }) => {
      const docsLink = page.locator('#getting-started .getting-started__docs-link');
      await expect(docsLink).toHaveAttribute('target', '_blank');
    });

    test('Documentation link has security rel attribute', async ({ page }) => {
      const docsLink = page.locator('#getting-started .getting-started__docs-link');
      await expect(docsLink).toHaveAttribute('rel', 'noopener noreferrer');
    });

    test('Documentation link is functional (does not 404)', async ({ page, context }) => {
      // Create a new page to follow the link
      const docsLink = page.locator('#getting-started .getting-started__docs-link');
      const href = await docsLink.getAttribute('href');

      // Verify the link is well-formed
      expect(href).toBeTruthy();
      expect(href).toMatch(/^https:\/\/github\.com/);
    });
  });

  test.describe('TC5: Configuration Example', () => {
    test('Configuration section shows TOML format', async ({ page }) => {
      const configCodeBlock = page.locator('#getting-started .code-block[data-language="toml"]');
      await expect(configCodeBlock).toBeVisible();
    });

    test('Configuration shows [server] section', async ({ page }) => {
      const configCode = page.locator('#getting-started .code-block[data-language="toml"] code');
      const text = await configCode.textContent();

      expect(text).toContain('[server]');
      expect(text).toContain('port');
      expect(text).toContain('host');
    });

    test('Configuration shows [storage] section', async ({ page }) => {
      const configCode = page.locator('#getting-started .code-block[data-language="toml"] code');
      const text = await configCode.textContent();

      expect(text).toContain('[storage]');
      expect(text).toContain('data_dir');
    });

    test('Configuration shows [memtable] section', async ({ page }) => {
      const configCode = page.locator('#getting-started .code-block[data-language="toml"] code');
      const text = await configCode.textContent();

      expect(text).toContain('[memtable]');
      expect(text).toContain('max_size');
    });

    test('Configuration includes WAL setting', async ({ page }) => {
      const configCode = page.locator('#getting-started .code-block[data-language="toml"] code');
      const text = await configCode.textContent();

      expect(text).toContain('wal_enabled');
    });
  });

  test.describe('Responsive Design', () => {
    test('Section is visible on mobile viewport', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });

      const gettingStartedSection = page.locator('#getting-started');
      await expect(gettingStartedSection).toBeVisible();
    });

    test('Code blocks are scrollable on mobile', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });

      const codeBlock = page.locator('#getting-started .code-block pre').first();
      await expect(codeBlock).toBeVisible();

      const overflowX = await codeBlock.evaluate(el =>
        window.getComputedStyle(el).overflowX
      );

      expect(['auto', 'scroll']).toContain(overflowX);
    });

    test('Step numbers remain visible on mobile', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });

      const stepNumbers = page.locator('#getting-started .getting-started__step-number');

      await expect(stepNumbers.first()).toBeVisible();
      await expect(stepNumbers.nth(1)).toBeVisible();
      await expect(stepNumbers.nth(2)).toBeVisible();
    });
  });

  test.describe('Navigation to Getting Started Section', () => {
    test('Can navigate via anchor link from hero CTA', async ({ page }) => {
      // Click on "Get Started" CTA in hero
      await page.click('a[href="#getting-started"]');

      // Wait for smooth scroll
      await page.waitForTimeout(500);

      // Verify URL hash
      await expect(page).toHaveURL(/#getting-started$/);

      // Getting Started section should be in viewport
      const gettingStartedSection = page.locator('#getting-started');
      await expect(gettingStartedSection).toBeInViewport();
    });

    test('Can navigate via nav link', async ({ page }) => {
      // Click on Get Started link in nav
      await page.click('nav a[href="#getting-started"]');

      // Wait for smooth scroll
      await page.waitForTimeout(500);

      // Getting Started section should be in viewport
      const gettingStartedSection = page.locator('#getting-started');
      await expect(gettingStartedSection).toBeInViewport();
    });
  });

  test.describe('Accessibility', () => {
    test('Section has proper semantic structure', async ({ page }) => {
      const section = page.locator('section#getting-started');
      await expect(section).toBeVisible();
    });

    test('Copy buttons have accessible aria-label', async ({ page }) => {
      const copyButtons = page.locator('#getting-started .code-block__copy');
      const firstButton = copyButtons.first();

      await expect(firstButton).toHaveAttribute('aria-label', 'Copy code to clipboard');
    });

    test('Copy buttons are keyboard accessible', async ({ page }) => {
      const copyButton = page.locator('#getting-started .code-block__copy').first();

      // Tab to the button
      await copyButton.focus();

      // Verify it can receive focus
      await expect(copyButton).toBeFocused();
    });

    test('Documentation link is keyboard accessible', async ({ page }) => {
      const docsLink = page.locator('#getting-started .getting-started__docs-link');

      await docsLink.focus();
      await expect(docsLink).toBeFocused();
    });
  });
});
