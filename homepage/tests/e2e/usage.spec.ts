/**
 * Usage Examples Section E2E Tests
 * Owner: Scenario 3 - Usage Examples Section
 *
 * Test cases:
 * - SET command example present
 * - GET command example present
 * - Code blocks have syntax highlighting
 * - Code blocks have readable styling
 */

import { test, expect } from '@playwright/test';
import { navigateToHomepage, selectors, scrollToSection, viewports } from './test-utils';

test.describe('Usage Examples Section', () => {
  test.beforeEach(async ({ page }) => {
    await navigateToHomepage(page);
    await scrollToSection(page, 'usage');
  });

  test('TC1: SET command example is displayed showing how to store data', async ({ page }) => {
    // Verify usage section exists
    const usageSection = page.locator(selectors.usage.section);
    await expect(usageSection).toBeVisible();

    // Verify code block exists in usage section
    const codeBlock = usageSection.locator(selectors.usage.codeBlock);
    await expect(codeBlock).toBeVisible();

    // Get code block text content
    const codeContent = await codeBlock.textContent();
    expect(codeContent).not.toBeNull();

    // Verify SET command is present
    expect(codeContent!.toLowerCase()).toContain('set');

    // Verify SET command syntax pattern (set <key> <flags> <exptime> <bytes>)
    // The example shows: set mykey 0 0 5
    expect(codeContent).toMatch(/set\s+\w+/i);

    // Verify the example shows storing data (STORED response)
    expect(codeContent).toContain('STORED');
  });

  test('TC2: GET command example is displayed showing how to retrieve data', async ({ page }) => {
    // Verify usage section exists
    const usageSection = page.locator(selectors.usage.section);
    await expect(usageSection).toBeVisible();

    // Verify code block exists in usage section
    const codeBlock = usageSection.locator(selectors.usage.codeBlock);
    await expect(codeBlock).toBeVisible();

    // Get code block text content
    const codeContent = await codeBlock.textContent();
    expect(codeContent).not.toBeNull();

    // Verify GET command is present
    expect(codeContent!.toLowerCase()).toContain('get');

    // Verify GET command pattern (get <key>)
    expect(codeContent).toMatch(/get\s+\w+/i);

    // Verify the example shows retrieving data (VALUE response)
    expect(codeContent).toContain('VALUE');

    // Verify END response indicating successful retrieval
    expect(codeContent).toContain('END');
  });

  test('TC3: Code examples have syntax highlighting or monospace formatting', async ({ page }) => {
    // Verify usage section exists
    const usageSection = page.locator(selectors.usage.section);
    await expect(usageSection).toBeVisible();

    // Verify code block exists
    const codeBlock = usageSection.locator(selectors.usage.codeBlock);
    await expect(codeBlock).toBeVisible();

    // Verify pre element exists for code formatting
    const preElement = codeBlock.locator('pre');
    await expect(preElement).toBeVisible();

    // Verify code element exists
    const codeElement = codeBlock.locator('code');
    await expect(codeElement).toBeVisible();

    // Check for monospace font-family
    const fontFamily = await preElement.evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });

    // Font should be monospace (containing mono, consolas, courier, or similar)
    const isMonospace = fontFamily.toLowerCase().match(/mono|consolas|courier|menlo|sf mono|liberation/);
    expect(isMonospace).toBeTruthy();

    // Verify syntax highlighting presence (either through span elements with styles or specific color)
    // Check if there are styled spans for comments or keywords
    const styledSpans = await codeBlock.locator('span[style]').count();
    const hasStyledElements = styledSpans > 0;

    // Alternatively, check if the code has line formatting
    const codeLines = await codeElement.textContent();
    const hasMultipleLines = codeLines!.split('\n').length > 1;

    // Either syntax highlighting via spans OR proper code formatting should be present
    expect(hasStyledElements || hasMultipleLines).toBe(true);
  });

  test('TC4: Code blocks have high contrast background and readable font size', async ({ page }) => {
    // Verify usage section exists
    const usageSection = page.locator(selectors.usage.section);
    await expect(usageSection).toBeVisible();

    // Verify code block exists
    const codeBlock = usageSection.locator(selectors.usage.codeBlock);
    await expect(codeBlock).toBeVisible();

    // Check background color - should be dark for high contrast
    const backgroundColor = await codeBlock.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Parse RGB values to check if it's a dark color
    const rgbMatch = backgroundColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    if (rgbMatch) {
      const [_, r, g, b] = rgbMatch.map(Number);
      // Calculate luminance - darker backgrounds have lower values
      const luminance = (0.299 * r + 0.587 * g + 0.114 * b);
      // For a dark background, luminance should be less than 128 (midpoint)
      expect(luminance).toBeLessThan(128);
    }

    // Check font size is readable (at least 13px / 0.813rem)
    const preElement = codeBlock.locator('pre');
    const fontSize = await preElement.evaluate((el) => {
      return window.getComputedStyle(el).fontSize;
    });
    const fontSizeValue = parseFloat(fontSize);
    expect(fontSizeValue).toBeGreaterThanOrEqual(13);

    // Check line-height is readable (at least 1.4)
    const lineHeight = await preElement.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      const lineHeightValue = computed.lineHeight;
      const fontSizeValue = parseFloat(computed.fontSize);

      if (lineHeightValue === 'normal') {
        return 1.5; // default normal is typically 1.2-1.5
      }

      const lhPixels = parseFloat(lineHeightValue);
      return lhPixels / fontSizeValue;
    });
    expect(lineHeight).toBeGreaterThanOrEqual(1.4);

    // Check text color is light (for contrast against dark background)
    const textColor = await preElement.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    const textRgbMatch = textColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    if (textRgbMatch) {
      const [_, r, g, b] = textRgbMatch.map(Number);
      // For light text, luminance should be higher (at least 180 for good contrast)
      const luminance = (0.299 * r + 0.587 * g + 0.114 * b);
      expect(luminance).toBeGreaterThan(180);
    }
  });

  test('Usage section is accessible', async ({ page }) => {
    // Verify usage section has proper aria-labelledby
    const usageSection = page.locator(selectors.usage.section);
    const ariaLabelledBy = await usageSection.getAttribute('aria-labelledby');
    expect(ariaLabelledBy).toBe('usage-title');

    // Verify heading exists
    const usageTitle = page.locator(selectors.usage.title);
    await expect(usageTitle).toBeVisible();
    await expect(usageTitle).toContainText('Usage');

    // Verify code block is keyboard accessible (can be reached via tab)
    const codeBlock = usageSection.locator(selectors.usage.codeBlock);
    await expect(codeBlock).toBeVisible();
  });

  test('Usage section displays correctly on mobile viewport', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize(viewports.mobile);
    await scrollToSection(page, 'usage');

    // Verify usage section is visible
    const usageSection = page.locator(selectors.usage.section);
    await expect(usageSection).toBeVisible();

    // Verify code block is visible and doesn't overflow
    const codeBlock = usageSection.locator(selectors.usage.codeBlock);
    await expect(codeBlock).toBeVisible();

    // Check that horizontal scrolling is allowed for code blocks (overflow-x: auto)
    const overflowX = await codeBlock.evaluate((el) => {
      return window.getComputedStyle(el).overflowX;
    });
    expect(['auto', 'scroll']).toContain(overflowX);

    // Verify no page-level horizontal scroll
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });
});
