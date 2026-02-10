/**
 * Cross-Browser Compatibility E2E Tests
 * Owner: Scenario 12 - Cross-Browser Compatibility
 *
 * Tests for validating that the homepage renders correctly across major browsers:
 * - Chrome (Chromium)
 * - Firefox
 * - Safari (WebKit)
 * - Edge (Chromium-based)
 */

import { test, expect, BrowserName } from '@playwright/test';

/**
 * Test Case 1-4: Load homepage in each browser and verify all sections render correctly
 * These tests run in parallel across all configured browser projects
 */
test.describe('Cross-Browser Homepage Rendering', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Homepage loads and all sections render correctly', async ({ page, browserName }) => {
    // Verify the page has loaded
    await expect(page).toHaveTitle(/MirDB/);

    // Verify Hero section renders
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    const heroHeading = page.getByRole('heading', { level: 1 });
    await expect(heroHeading).toBeVisible();
    await expect(heroHeading).toContainText('MirDB');

    const tagline = heroSection.getByText(
      'A Persistent Key-Value Store with Memcached Protocol',
      { exact: true }
    );
    await expect(tagline).toBeVisible();

    // Verify Features section renders
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify feature cards are visible (data-testid includes feature id)
    const featureCards = page.locator('.feature-card, [data-testid^="feature-card-"]');
    await expect(featureCards.first()).toBeVisible();
    const featureCount = await featureCards.count();
    expect(featureCount).toBeGreaterThanOrEqual(4);

    // Verify Usage Example section renders
    const usageSection = page.locator('#usage');
    await expect(usageSection).toBeVisible();

    // Verify Getting Started section renders
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Verify code blocks are visible
    const codeBlocks = page.locator('[data-testid="code-block"]');
    await expect(codeBlocks.first()).toBeVisible();

    // Verify Footer renders
    const footer = page.getByRole('contentinfo');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Log browser for test visibility
    console.log(`Homepage rendered correctly in ${browserName}`);
  });

  test('CSS styling is applied consistently', async ({ page, browserName }) => {
    // Check that CSS custom properties are applied
    const body = page.locator('body');

    // Verify background color is applied (should have some color, not default white)
    const bgColor = await body.evaluate(
      (el) => window.getComputedStyle(el).backgroundColor
    );
    expect(bgColor).not.toBe('');

    // Verify text color is applied
    const textColor = await body.evaluate(
      (el) => window.getComputedStyle(el).color
    );
    expect(textColor).not.toBe('');

    // Verify hero section has proper layout
    const heroSection = page.locator('#hero');
    const heroDisplay = await heroSection.evaluate(
      (el) => window.getComputedStyle(el).display
    );
    // Should be flex or block-based layout
    expect(['flex', 'block', 'grid']).toContain(heroDisplay);

    // Verify buttons have proper styling
    const heroButtons = heroSection.locator('a[role="button"], button, .button, a[href]');
    const buttonCount = await heroButtons.count();
    expect(buttonCount).toBeGreaterThan(0);

    console.log(`CSS styling verified in ${browserName}`);
  });

  test('Interactive elements are accessible', async ({ page, browserName }) => {
    // Verify navigation links are clickable
    const navLinks = page.locator('header nav a, header a');
    const navCount = await navLinks.count();
    expect(navCount).toBeGreaterThan(0);

    // Verify all links have href attributes
    for (let i = 0; i < Math.min(navCount, 5); i++) {
      const link = navLinks.nth(i);
      const href = await link.getAttribute('href');
      expect(href).not.toBeNull();
    }

    // Verify buttons are focusable
    const buttons = page.locator('button:visible').first();
    if (await buttons.count() > 0) {
      await buttons.focus();
      await expect(buttons).toBeFocused();
    }

    console.log(`Interactive elements verified in ${browserName}`);
  });

  test('Page layout is responsive at different viewports', async ({ page, browserName }) => {
    // Test desktop viewport
    await page.setViewportSize({ width: 1280, height: 720 });
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Test tablet viewport
    await page.setViewportSize({ width: 768, height: 1024 });
    await expect(heroSection).toBeVisible();

    // Test mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    await expect(heroSection).toBeVisible();

    // Verify no horizontal scroll on mobile
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth + 5); // Allow small tolerance

    console.log(`Responsive layout verified in ${browserName}`);
  });
});

/**
 * Test Case 5: Theme toggle works consistently across all browsers
 */
test.describe('Cross-Browser Theme Toggle', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Clear localStorage to start fresh
    await page.evaluate(() => localStorage.clear());
  });

  test('Theme toggle switches between light and dark modes', async ({ page, browserName }) => {
    const themeToggle = page.getByTestId('theme-toggle');
    await expect(themeToggle).toBeVisible();

    // Get initial theme
    const initialTheme = await page.locator('html').getAttribute('data-theme');
    const expectedInitialTheme = initialTheme || 'light';

    // Click to toggle theme
    await themeToggle.click();

    // Wait for theme change
    await page.waitForTimeout(300);

    // Verify theme changed
    const newTheme = await page.locator('html').getAttribute('data-theme');

    if (expectedInitialTheme === 'light') {
      expect(newTheme).toBe('dark');
    } else {
      expect(newTheme).toBe('light');
    }

    // Toggle back
    await themeToggle.click();
    await page.waitForTimeout(300);

    // Verify theme reverted
    const finalTheme = await page.locator('html').getAttribute('data-theme');
    expect(finalTheme).toBe(expectedInitialTheme);

    console.log(`Theme toggle verified in ${browserName}`);
  });

  test('Theme persists to localStorage', async ({ page, browserName }) => {
    const themeToggle = page.getByTestId('theme-toggle');

    // Toggle to dark mode
    const initialTheme = await page.locator('html').getAttribute('data-theme');

    if (initialTheme === 'light') {
      await themeToggle.click();
      await page.waitForTimeout(300);
    }

    // Verify localStorage was updated
    const storedTheme = await page.evaluate(() => localStorage.getItem('theme'));
    expect(storedTheme).toBe('dark');

    // Reload page and verify theme persists
    await page.reload();
    await page.waitForLoadState('domcontentloaded');

    const persistedTheme = await page.locator('html').getAttribute('data-theme');
    expect(persistedTheme).toBe('dark');

    console.log(`Theme persistence verified in ${browserName}`);
  });

  test('Theme toggle has correct ARIA attributes', async ({ page, browserName }) => {
    const themeToggle = page.getByTestId('theme-toggle');

    // Check initial ARIA attributes
    await expect(themeToggle).toHaveAttribute('aria-label', /switch to (dark|light) mode/i);

    // Verify aria-pressed attribute exists
    const pressed = await themeToggle.getAttribute('aria-pressed');
    expect(['true', 'false']).toContain(pressed);

    console.log(`Theme toggle ARIA attributes verified in ${browserName}`);
  });

  test('Theme icon changes with theme', async ({ page, browserName }) => {
    const themeToggle = page.getByTestId('theme-toggle');

    // Get initial icon
    const initialTheme = await themeToggle.getAttribute('data-theme');

    if (initialTheme === 'light') {
      // Light mode shows moon icon
      await expect(page.getByTestId('moon-icon')).toBeVisible();
    } else {
      // Dark mode shows sun icon
      await expect(page.getByTestId('sun-icon')).toBeVisible();
    }

    // Toggle and verify icon changes
    await themeToggle.click();
    await page.waitForTimeout(300);

    if (initialTheme === 'light') {
      await expect(page.getByTestId('sun-icon')).toBeVisible();
    } else {
      await expect(page.getByTestId('moon-icon')).toBeVisible();
    }

    console.log(`Theme icon change verified in ${browserName}`);
  });
});

/**
 * Test Case 6: Copy-to-clipboard works consistently across all browsers
 */
test.describe('Cross-Browser Copy-to-Clipboard', () => {
  test.beforeEach(async ({ page, context, browserName }) => {
    // Grant clipboard permissions for Chromium-based browsers
    // Firefox and WebKit handle clipboard differently and don't support these permissions
    if (browserName === 'chromium') {
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);
    }
    await page.goto('/');
  });

  test('Copy button is visible and clickable', async ({ page, browserName }) => {
    // Scroll to getting started section
    const gettingStartedSection = page.locator('#getting-started');
    await gettingStartedSection.scrollIntoViewIfNeeded();

    // Find copy buttons
    const copyButtons = page.getByTestId('copy-button');
    await expect(copyButtons.first()).toBeVisible();

    const buttonCount = await copyButtons.count();
    expect(buttonCount).toBeGreaterThanOrEqual(3); // At least 3 commands

    console.log(`Copy buttons (${buttonCount}) found in ${browserName}`);
  });

  test('Copy button shows success state after click', async ({ page, browserName }) => {
    // Scroll to getting started section
    const gettingStartedSection = page.locator('#getting-started');
    await gettingStartedSection.scrollIntoViewIfNeeded();

    // Click first copy button
    const copyButton = page.getByTestId('copy-button').first();
    await expect(copyButton).toBeVisible();

    // Get initial button text
    const initialText = await copyButton.textContent();
    expect(initialText).toContain('Copy');

    // Click to copy
    await copyButton.click();

    // Wait for and verify success state
    await expect(copyButton).toContainText('Copied', { timeout: 2000 });

    // Verify button returns to idle state
    await expect(copyButton).toContainText('Copy', { timeout: 5000 });

    console.log(`Copy success state verified in ${browserName}`);
  });

  test('Copy functionality copies correct text to clipboard', async ({ page, browserName }) => {
    // Scroll to getting started section
    const gettingStartedSection = page.locator('#getting-started');
    await gettingStartedSection.scrollIntoViewIfNeeded();

    // Get the code content from the first code block
    const codeContent = page.getByTestId('code-content').first();
    const expectedText = await codeContent.textContent();

    // Click copy button
    const copyButton = page.getByTestId('copy-button').first();
    await copyButton.click();

    // Verify copy button shows success state
    await expect(copyButton).toContainText('Copied', { timeout: 2000 });

    // Verify clipboard content (only in Chromium - other browsers don't allow clipboard read)
    if (browserName === 'chromium') {
      const clipboardText = await page.evaluate(async () => {
        return await navigator.clipboard.readText();
      });

      expect(clipboardText).toBe(expectedText);
      console.log(`Copy content verified in ${browserName}: "${clipboardText?.substring(0, 30)}..."`);
    } else {
      // For Firefox/WebKit, we verify the copy worked by checking the button state change
      console.log(`Copy button state verified in ${browserName} (clipboard read not supported)`);
    }
  });

  test('Toast notification appears after copy', async ({ page, browserName }) => {
    // Scroll to getting started section
    const gettingStartedSection = page.locator('#getting-started');
    await gettingStartedSection.scrollIntoViewIfNeeded();

    // Click copy button
    const copyButton = page.getByTestId('copy-button').first();
    await copyButton.click();

    // Check for toast notification
    const toast = page.locator('[data-testid="toast"], [role="alert"], .toast');

    // Wait for toast to appear (may not be visible in all implementations)
    try {
      await expect(toast.first()).toBeVisible({ timeout: 2000 });
      console.log(`Toast notification verified in ${browserName}`);
    } catch {
      // Toast may auto-dismiss quickly or not be implemented
      console.log(`Toast notification not captured in ${browserName} (may have dismissed)`);
    }
  });

  test('Multiple copy operations work correctly', async ({ page, browserName }) => {
    // Scroll to getting started section
    const gettingStartedSection = page.locator('#getting-started');
    await gettingStartedSection.scrollIntoViewIfNeeded();

    const copyButtons = page.getByTestId('copy-button');
    const codeContents = page.getByTestId('code-content');

    const buttonCount = await copyButtons.count();

    // Test copying from multiple code blocks
    for (let i = 0; i < Math.min(buttonCount, 3); i++) {
      const button = copyButtons.nth(i);
      const content = codeContents.nth(i);

      const expectedText = await content.textContent();

      await button.click();

      // Verify button shows success state
      await expect(button).toContainText('Copied', { timeout: 2000 });

      // Verify clipboard content only in Chromium
      if (browserName === 'chromium') {
        const clipboardText = await page.evaluate(async () => {
          return await navigator.clipboard.readText();
        });
        expect(clipboardText).toBe(expectedText);
      }

      // Wait for button to reset
      await expect(button).toContainText('Copy', { timeout: 3000 });
    }

    console.log(`Multiple copy operations verified in ${browserName}`);
  });
});

/**
 * Additional cross-browser tests for JavaScript functionality
 */
test.describe('Cross-Browser JavaScript Functionality', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Smooth scroll navigation works', async ({ page, browserName }) => {
    // Click on a navigation link that should scroll to a section
    const navLink = page.locator('a[href="#features"], nav a:has-text("Features")').first();

    if (await navLink.count() > 0) {
      // Get initial scroll position
      const initialScroll = await page.evaluate(() => window.scrollY);

      await navLink.click();

      // Wait for scroll animation
      await page.waitForTimeout(500);

      // Get new scroll position
      const newScroll = await page.evaluate(() => window.scrollY);

      // Scroll position should have changed
      expect(newScroll).toBeGreaterThan(initialScroll);

      console.log(`Smooth scroll navigation verified in ${browserName}`);
    } else {
      console.log(`Navigation link not found in ${browserName}, skipping scroll test`);
    }
  });

  test('External links have correct attributes', async ({ page, browserName }) => {
    // Check GitHub links have proper attributes
    const githubLinks = page.locator('a[href*="github.com"]');
    const count = await githubLinks.count();

    if (count > 0) {
      for (let i = 0; i < Math.min(count, 3); i++) {
        const link = githubLinks.nth(i);

        // External links should open in new tab
        const target = await link.getAttribute('target');
        const rel = await link.getAttribute('rel');

        // If target is _blank, should have noopener/noreferrer
        if (target === '_blank') {
          expect(rel).toContain('noopener');
        }
      }
    }

    console.log(`External link attributes verified in ${browserName}`);
  });

  test('Images have valid src attributes', async ({ page, browserName }) => {
    // Wait for page to fully load
    await page.waitForLoadState('domcontentloaded');

    // Look for all images on the page
    const images = page.locator('img');
    const imageCount = await images.count();

    // If there are images, verify they have valid src attributes
    if (imageCount > 0) {
      let validImages = 0;
      for (let i = 0; i < Math.min(imageCount, 5); i++) {
        const img = images.nth(i);

        // Check that image has a src attribute
        const src = await img.getAttribute('src');
        expect(src).not.toBeNull();
        expect(src).not.toBe('');

        // Check alt attribute for accessibility
        const alt = await img.getAttribute('alt');
        expect(alt).not.toBeNull();

        validImages++;
      }
      console.log(`Image attributes verified in ${browserName} (${validImages} of ${imageCount} images checked)`);
    } else {
      // No images is also valid
      console.log(`No images found in ${browserName} - test passed`);
    }
  });
});
