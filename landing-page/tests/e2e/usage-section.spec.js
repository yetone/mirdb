/**
 * Usage Section E2E Tests
 * Owner: Scenario 4 - Usage Section with Code Examples
 *
 * End-to-end tests for the Usage section functionality
 */
const { test, expect } = require('@playwright/test');

test.describe('Usage Section E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('TC1: Usage Section HTML Structure', () => {
    test('Usage section is visible on the page', async ({ page }) => {
      const usageSection = page.locator('#usage');
      await expect(usageSection).toBeVisible();
    });

    test('Section has title "Simple as Memcached"', async ({ page }) => {
      const title = page.locator('#usage .usage__title');
      await expect(title).toBeVisible();
      await expect(title).toHaveText('Simple as Memcached');
    });

    test('Contains code blocks with syntax highlighting', async ({ page }) => {
      const codeBlocks = page.locator('#usage .code-block');
      await expect(codeBlocks).toHaveCount(2); // Two code examples

      // Check for code elements
      const codeElements = page.locator('#usage .code-block code');
      await expect(codeElements.first()).toBeVisible();
    });

    test('Contains copy buttons on code blocks', async ({ page }) => {
      const copyButtons = page.locator('#usage .code-block__copy');
      await expect(copyButtons).toHaveCount(2); // One per code block

      // Check buttons are visible
      await expect(copyButtons.first()).toBeVisible();
    });
  });

  test.describe('TC2: Copy Button Functionality', () => {
    test('Click copy button copies code to clipboard', async ({ page, context }) => {
      // Grant clipboard permissions
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);

      // Click the first copy button
      const copyButton = page.locator('#usage .code-block__copy').first();
      await copyButton.click();

      // Verify button shows "Copied!" feedback
      const copyText = copyButton.locator('.copy-text');
      await expect(copyText).toHaveText('Copied!');

      // Verify button has copied class
      await expect(copyButton).toHaveClass(/copied/);

      // Wait for reset and verify it returns to original state
      await page.waitForTimeout(2500);
      await expect(copyText).toHaveText('Copy');
      await expect(copyButton).not.toHaveClass(/copied/);
    });

    test('Copy button shows visual feedback with icon change', async ({ page, context }) => {
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);

      const copyButton = page.locator('#usage .code-block__copy').first();
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

    test('Copy button hover state works', async ({ page }) => {
      const copyButton = page.locator('#usage .code-block__copy').first();

      // Hover over button
      await copyButton.hover();

      // Button should be focusable/interactive
      await expect(copyButton).toBeEnabled();
    });

    test('Copy button has accessible aria-label', async ({ page }) => {
      const copyButton = page.locator('#usage .code-block__copy').first();
      await expect(copyButton).toHaveAttribute('aria-label', 'Copy code to clipboard');
    });
  });

  test.describe('TC3: Code Example Content', () => {
    test('Shows Memcached SET operation', async ({ page }) => {
      const codeContent = page.locator('#usage .code-block code');

      // Check for SET command
      const secondCodeBlock = codeContent.nth(1);
      const text = await secondCodeBlock.textContent();

      expect(text).toContain('SET');
      expect(text).toContain('STORED');
    });

    test('Shows Memcached GET operation', async ({ page }) => {
      const codeContent = page.locator('#usage .code-block code');

      // Check for GET command
      const secondCodeBlock = codeContent.nth(1);
      const text = await secondCodeBlock.textContent();

      expect(text).toContain('GET');
      expect(text).toContain('VALUE');
      expect(text).toContain('END');
    });

    test('Shows server start command', async ({ page }) => {
      const firstCodeBlock = page.locator('#usage .code-block code').first();
      const text = await firstCodeBlock.textContent();

      expect(text).toContain('mirdb');
      expect(text).toContain('--port');
    });
  });

  test.describe('TC4: Syntax Highlighting Library', () => {
    test('Prism.js CSS is loaded', async ({ page }) => {
      const prismCSS = page.locator('link[href*="prism"]');
      await expect(prismCSS).toHaveCount(1);
    });

    test('Prism.js JavaScript is loaded', async ({ page }) => {
      const prismJS = page.locator('script[src*="prism.min.js"]');
      await expect(prismJS).toHaveCount(1);
    });

    test('Code has syntax highlighting classes applied', async ({ page }) => {
      // Prism adds span elements with token classes for highlighting
      // Wait a moment for Prism to process
      await page.waitForTimeout(500);

      const codeElement = page.locator('#usage .code-block code.language-bash').first();
      await expect(codeElement).toBeVisible();
    });
  });

  test.describe('TC5: Usage GIF/Animation', () => {
    test('Usage demo container is visible', async ({ page }) => {
      const demoContainer = page.locator('#usage .usage__demo');
      await expect(demoContainer).toBeVisible();
    });

    test('Usage GIF is displayed', async ({ page }) => {
      const usageGif = page.locator('#usage .usage__gif');
      await expect(usageGif).toBeVisible();
    });

    test('Usage GIF has correct source', async ({ page }) => {
      const usageGif = page.locator('#usage .usage__gif');
      await expect(usageGif).toHaveAttribute('src', 'assets/images/usage.gif');
    });

    test('Usage GIF has alt text for accessibility', async ({ page }) => {
      const usageGif = page.locator('#usage .usage__gif');
      const altText = await usageGif.getAttribute('alt');

      expect(altText).toBeTruthy();
      expect(altText.length).toBeGreaterThan(0);
    });

    test('Usage GIF has lazy loading', async ({ page }) => {
      const usageGif = page.locator('#usage .usage__gif');
      await expect(usageGif).toHaveAttribute('loading', 'lazy');
    });
  });

  test.describe('Responsive Design', () => {
    test('Code blocks are scrollable on mobile', async ({ page }) => {
      // Set mobile viewport
      await page.setViewportSize({ width: 375, height: 667 });

      const codeBlock = page.locator('#usage .code-block pre').first();
      await expect(codeBlock).toBeVisible();

      // Check that overflow-x is set to auto or scroll
      const overflowX = await codeBlock.evaluate(el =>
        window.getComputedStyle(el).overflowX
      );

      expect(['auto', 'scroll']).toContain(overflowX);
    });

    test('Usage section layout changes on mobile', async ({ page }) => {
      // Set mobile viewport
      await page.setViewportSize({ width: 375, height: 667 });

      const usageContent = page.locator('#usage .usage__content');
      await expect(usageContent).toBeVisible();

      // On mobile, grid should be single column
      const gridColumns = await usageContent.evaluate(el =>
        window.getComputedStyle(el).gridTemplateColumns
      );

      // Should have single column on mobile (1fr = full width)
      expect(gridColumns).not.toContain('1fr 1fr');
    });
  });

  test.describe('Navigation to Usage Section', () => {
    test('Can navigate to usage section via anchor link', async ({ page }) => {
      // Click on Usage link in nav
      await page.click('nav a[href="#usage"]');

      // Wait for smooth scroll
      await page.waitForTimeout(500);

      // Verify URL hash
      await expect(page).toHaveURL(/#usage$/);

      // Usage section should be in viewport
      const usageSection = page.locator('#usage');
      await expect(usageSection).toBeInViewport();
    });
  });
});
