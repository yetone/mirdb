// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E tests for Memcached Client Compatibility Info
 * Owner: Scenario 12
 *
 * Test cases:
 * - Compatibility information displayed
 * - Protocol documentation reference
 * - Supported commands mentioned
 */

test.describe('Memcached Client Compatibility Information', () => {
  test.beforeEach(async ({ page, baseURL }) => {
    await page.goto(baseURL);
    await page.waitForLoadState('domcontentloaded');
  });

  test('page mentions compatibility with existing memcached clients', async ({ page }) => {
    // Test case 1: Search for compatibility information
    // The page should mention that MirDB is compatible with existing memcached clients

    // Check in the features section for compatibility mention
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Look for text mentioning compatibility with memcached clients
    const pageContent = await page.textContent('body');
    const hasCompatibilityMention =
      pageContent.toLowerCase().includes('compatible') &&
      pageContent.toLowerCase().includes('memcached') &&
      pageContent.toLowerCase().includes('client');

    expect(hasCompatibilityMention).toBeTruthy();

    // Alternative: Check for specific elements that mention compatibility
    const compatibilityText = page.getByText(/compatible.*memcached.*client|memcached.*client.*compatible|existing.*memcached.*client/i);
    await expect(compatibilityText.first()).toBeVisible();
  });

  test('link or reference to memcached protocol documentation exists', async ({ page }) => {
    // Test case 2: Check for protocol documentation link
    // The page should have a reference to memcached protocol documentation

    // Look for a link to protocol documentation or a reference to the protocol
    const pageContent = await page.textContent('body');

    // Check for protocol mention anywhere on the page
    const hasProtocolMention =
      pageContent.toLowerCase().includes('memcached protocol') ||
      pageContent.toLowerCase().includes('memcached text protocol');

    expect(hasProtocolMention).toBeTruthy();

    // Also verify that there's mention of protocol in a visible element
    const protocolText = page.getByText(/memcached protocol/i);
    await expect(protocolText.first()).toBeVisible();
  });

  test('basic commands (set, get) are referenced as supported', async ({ page }) => {
    // Test case 3: Verify supported commands mentioned
    // The page should reference basic set/get commands as supported

    // Look for the Getting Started section which contains command examples
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Check for set command reference
    const setCommandText = page.getByText(/\bset\b/i);
    await expect(setCommandText.first()).toBeVisible();

    // Check for get command reference
    const getCommandText = page.getByText(/\bget\b/i);
    await expect(getCommandText.first()).toBeVisible();

    // Verify the commands are shown in a code block context
    const codeBlocks = gettingStartedSection.locator('.code-block, pre, code');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Check that the code blocks contain set and get commands
    const codeContent = await codeBlocks.allTextContents();
    const combinedCode = codeContent.join(' ').toLowerCase();

    expect(combinedCode).toContain('set');
    expect(combinedCode).toContain('get');
  });

  test('memcached protocol feature card is visible and mentions client compatibility', async ({ page }) => {
    // Additional test: Verify the Memcached Protocol feature card exists
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Look for a feature card or element mentioning Memcached Protocol
    const memcachedFeature = featuresSection.getByText(/memcached protocol/i).first();
    await expect(memcachedFeature).toBeVisible();

    // Check that there's a description mentioning clients
    const featureDescription = featuresSection.getByText(/client/i).first();
    await expect(featureDescription).toBeVisible();
  });

  test('compatibility information is accessible via keyboard navigation', async ({ page }) => {
    // Additional accessibility test: Ensure compatibility info can be reached via keyboard

    // Start from the top of the page
    await page.keyboard.press('Tab');

    // Navigate through the page until we reach the features section
    let foundFeatures = false;
    for (let i = 0; i < 30; i++) {
      const activeElement = await page.evaluate(() => {
        const el = document.activeElement;
        return el ? {
          tagName: el.tagName,
          id: el.id,
          className: el.className,
          href: el.getAttribute('href'),
          textContent: el.textContent?.substring(0, 100)
        } : null;
      });

      // Check if we've navigated to features section link or are in features section
      if (activeElement?.href?.includes('features') ||
          activeElement?.textContent?.toLowerCase().includes('memcached')) {
        foundFeatures = true;
        break;
      }

      await page.keyboard.press('Tab');
    }

    // We should be able to navigate to content related to features
    expect(foundFeatures).toBeTruthy();
  });

  test('compatibility content is visible in both light and dark modes', async ({ page }) => {
    // Test that compatibility information is visible in both themes

    // Check in light mode (default)
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const memcachedText = featuresSection.getByText(/memcached protocol/i).first();
    await expect(memcachedText).toBeVisible();

    // Switch to dark mode if theme toggle exists
    const themeToggle = page.locator('[aria-label*="theme"], [data-testid="theme-toggle"], button:has-text("Dark"), button:has-text("Light")').first();
    const hasThemeToggle = await themeToggle.count() > 0;

    if (hasThemeToggle) {
      await themeToggle.click();

      // Wait for theme to apply
      await page.waitForTimeout(100);

      // Verify content is still visible in dark mode
      await expect(memcachedText).toBeVisible();
    }
  });
});
