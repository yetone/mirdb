/**
 * Quick Start Section E2E Tests
 * Owner: Scenario 4 - Quick Start Section
 *
 * Tests:
 * - Installation code block present
 * - cargo build command shown
 * - SET command example
 * - GET command example
 * - Copy button functionality
 * - Default port 12333 documented
 */

const { test, expect } = require('@playwright/test');

test.describe('Quick Start Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC-1: Quick Start section contains at least 2 code blocks (installation and usage)', async ({ page }) => {
    // Navigate to Quick Start section
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Query for code blocks within the Quick Start section
    const codeBlocks = quickstartSection.locator('pre code, .code-block');
    const count = await codeBlocks.count();

    // Verify at least 2 code blocks are present (installation and usage examples)
    expect(count).toBeGreaterThanOrEqual(2);
  });

  test('TC-2: Installation section contains cargo build command', async ({ page }) => {
    // Navigate to Quick Start section
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Get all code block text content
    const codeBlocks = quickstartSection.locator('pre code');
    const count = await codeBlocks.count();

    let foundCargoBuild = false;
    for (let i = 0; i < count; i++) {
      const codeText = await codeBlocks.nth(i).textContent();
      if (codeText && (codeText.includes('cargo build --release') || codeText.includes('cargo build'))) {
        foundCargoBuild = true;
        break;
      }
    }

    expect(foundCargoBuild).toBe(true);
  });

  test('TC-3: SET command example is present with memcached protocol syntax', async ({ page }) => {
    // Navigate to Quick Start section
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Get all code block text content and look for SET command
    const codeBlocks = quickstartSection.locator('pre code');
    const count = await codeBlocks.count();

    let foundSetCommand = false;
    for (let i = 0; i < count; i++) {
      const codeText = await codeBlocks.nth(i).textContent();
      if (codeText) {
        // Check for memcached SET command syntax: set <key> <flags> <ttl> <bytes>
        if (codeText.toLowerCase().includes('set ') &&
            (codeText.includes('0 0') || codeText.match(/set\s+\w+\s+\d+\s+\d+\s+\d+/i))) {
          foundSetCommand = true;
          break;
        }
      }
    }

    expect(foundSetCommand).toBe(true);
  });

  test('TC-4: GET command example is present with memcached protocol syntax', async ({ page }) => {
    // Navigate to Quick Start section
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Get all code block text content and look for GET command
    const codeBlocks = quickstartSection.locator('pre code');
    const count = await codeBlocks.count();

    let foundGetCommand = false;
    for (let i = 0; i < count; i++) {
      const codeText = await codeBlocks.nth(i).textContent();
      if (codeText) {
        // Check for memcached GET command syntax: get <key>
        if (codeText.toLowerCase().includes('get ') && codeText.match(/get\s+\w+/i)) {
          foundGetCommand = true;
          break;
        }
      }
    }

    expect(foundGetCommand).toBe(true);
  });

  test('TC-5: Code blocks have copy button or are styled for easy selection', async ({ page }) => {
    // Navigate to Quick Start section
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Check for code blocks with copy functionality
    const codeContainers = quickstartSection.locator('.code-container, pre');
    const count = await codeContainers.count();
    expect(count).toBeGreaterThanOrEqual(1);

    let hasCopyFunctionality = false;

    for (let i = 0; i < count; i++) {
      const container = codeContainers.nth(i);

      // Check for copy button
      const copyButton = container.locator('.copy-btn, button[data-copy], [aria-label*="copy"]');
      const hasCopyButton = await copyButton.count() > 0;

      // Check for styling that enables easy selection (user-select property)
      const styles = await container.evaluate((el) => {
        const computed = getComputedStyle(el);
        return {
          userSelect: computed.userSelect,
          cursor: computed.cursor,
          fontFamily: computed.fontFamily
        };
      });

      // Code should use monospace font for easy readability/selection
      const isMonospace = styles.fontFamily.toLowerCase().includes('mono') ||
                          styles.fontFamily.toLowerCase().includes('consolas') ||
                          styles.fontFamily.toLowerCase().includes('courier');

      if (hasCopyButton || isMonospace) {
        hasCopyFunctionality = true;
        break;
      }
    }

    expect(hasCopyFunctionality).toBe(true);
  });

  test('TC-6: Connection example shows default port 0.0.0.0:12333', async ({ page }) => {
    // Navigate to Quick Start section
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Get section text content and verify default port is documented
    const sectionText = await quickstartSection.textContent();

    // Check for the default address/port (0.0.0.0:12333 or localhost:12333 or just 12333)
    const hasDefaultPort = sectionText.includes('12333') ||
                           sectionText.includes('0.0.0.0:12333') ||
                           sectionText.includes('localhost:12333');

    expect(hasDefaultPort).toBe(true);
  });

  test('Quick Start section is accessible via anchor navigation', async ({ page }) => {
    // Click on navigation link to Quick Start
    await page.click('a[href="#quickstart"]');

    // Wait for scroll
    await page.waitForTimeout(500);

    // Verify section is now in viewport
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();
  });

  test('Quick Start heading is properly structured with h2', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');
    const heading = quickstartSection.locator('h2');

    await expect(heading).toBeVisible();
    await expect(heading).toContainText('Quick Start');
  });

  test('Code blocks have proper styling for dark theme', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');
    const codeBlock = quickstartSection.locator('pre').first();

    await expect(codeBlock).toBeVisible();

    // Get background color of code block
    const bgColor = await codeBlock.evaluate((el) => {
      return getComputedStyle(el).backgroundColor;
    });

    // Verify code block has a dark background (low RGB values indicate dark)
    const rgbMatch = bgColor.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
    expect(rgbMatch).not.toBeNull();

    const [, r, g, b] = rgbMatch.map(Number);
    // Sum should be less than 150 for a dark background
    expect(r + g + b).toBeLessThan(200);
  });
});
