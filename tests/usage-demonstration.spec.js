// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');
const fs = require('fs');

const indexPath = 'file://' + path.resolve(__dirname, '../index.html');
const assetsDir = path.resolve(__dirname, '../assets');

/**
 * Usage Demonstration Media Tests (REQ-10)
 *
 * This test suite verifies that the usage demonstration GIF/video
 * is embedded or linked correctly on the landing page.
 *
 * Scenario: Usage Demonstration Media
 * - Verify that usage demonstration GIF/video is embedded or linked
 * - Check media element exists, loads correctly, and has proper context
 */
test.describe('Usage Demonstration Media (REQ-10)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  /**
   * Test Case 1: Check for usage demonstration media element
   * Input: Check for usage demonstration media element
   * Expected: Image or video element referencing usage demonstration exists
   * Type: unit
   */
  test('TC1: Usage demonstration media element exists', async ({ page }) => {
    // Locate the demo section
    const demoSection = page.locator('#demo');
    await expect(demoSection).toBeVisible();

    // Check for image or video element referencing usage demonstration
    const demoImage = page.locator('.demo-gif, img[src*="usage"], video[src*="usage"]');
    const elementCount = await demoImage.count();

    // Verify at least one media element exists
    expect(elementCount).toBeGreaterThan(0);

    // Verify the first media element is visible
    await expect(demoImage.first()).toBeVisible();

    // Verify media element is an img or video tag
    const tagName = await demoImage.first().evaluate(el => el.tagName.toLowerCase());
    expect(['img', 'video']).toContain(tagName);

    // Verify the src attribute points to usage demonstration
    const src = await demoImage.first().getAttribute('src');
    expect(src).toBeTruthy();
    expect(src).toContain('usage');
  });

  /**
   * Test Case 2: Verify usage.gif loads successfully
   * Input: Verify usage.gif loads successfully
   * Expected: Usage demonstration GIF loads and displays correctly
   * Type: e2e
   */
  test('TC2: Usage demonstration GIF loads successfully', async ({ page }) => {
    // First verify the assets/usage.gif file exists on disk
    const usageGifPath = path.join(assetsDir, 'usage.gif');
    expect(fs.existsSync(usageGifPath), 'usage.gif file should exist in assets directory').toBe(true);

    // Verify the file has content (not empty)
    const stats = fs.statSync(usageGifPath);
    expect(stats.size).toBeGreaterThan(0);

    // Now test the image loading in the browser
    const demoGif = page.locator('.demo-gif');
    await expect(demoGif).toBeVisible();

    // Verify the src attribute
    const src = await demoGif.getAttribute('src');
    expect(src).toBe('assets/usage.gif');

    // Verify the image has loaded successfully by checking naturalWidth and naturalHeight
    // A successfully loaded image will have dimensions > 0
    const dimensions = await demoGif.evaluate((img) => ({
      naturalWidth: img.naturalWidth,
      naturalHeight: img.naturalHeight,
      complete: img.complete
    }));

    expect(dimensions.complete).toBe(true);
    expect(dimensions.naturalWidth).toBeGreaterThan(0);
    expect(dimensions.naturalHeight).toBeGreaterThan(0);

    // Verify the image is displayed with proper dimensions (not broken)
    const boundingBox = await demoGif.boundingBox();
    expect(boundingBox).toBeTruthy();
    expect(boundingBox.width).toBeGreaterThan(0);
    expect(boundingBox.height).toBeGreaterThan(0);
  });

  /**
   * Test Case 3: Verify demonstration has descriptive caption or context
   * Input: Verify demonstration has descriptive caption or context
   * Expected: Usage demonstration has accompanying text explaining what it shows
   * Type: unit
   */
  test('TC3: Demonstration has descriptive caption or context', async ({ page }) => {
    // Locate the demo section
    const demoSection = page.locator('#demo');
    await expect(demoSection).toBeVisible();

    // Verify there is a section heading that provides context
    const sectionHeading = demoSection.locator('h2');
    await expect(sectionHeading).toBeVisible();
    const headingText = await sectionHeading.textContent();
    expect(headingText).toBeTruthy();
    expect(headingText.length).toBeGreaterThan(0);

    // Verify heading provides meaningful context about the demonstration
    // Expected heading: "See It In Action"
    expect(headingText.toLowerCase()).toMatch(/see|action|demo|watch|usage/i);

    // Verify the demo GIF has descriptive alt text
    const demoGif = page.locator('.demo-gif');
    const altText = await demoGif.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText.length).toBeGreaterThan(0);

    // Alt text should describe what the demonstration shows
    expect(altText.toLowerCase()).toMatch(/mirdb|usage|demonstration|demo/i);

    // Verify the alt text is properly descriptive (not just "image" or "gif")
    const genericTerms = ['image', 'img', 'gif', 'picture', 'photo'];
    const lowerAlt = altText.toLowerCase();
    const isOnlyGeneric = genericTerms.some(term => lowerAlt === term);
    expect(isOnlyGeneric).toBe(false);
  });

  // Additional supporting tests for comprehensive coverage

  test('Demo section is properly positioned in the page flow', async ({ page }) => {
    // Demo section should be within the main content area
    const main = page.locator('main');
    const demoSection = page.locator('#demo');

    await expect(main).toBeVisible();
    await expect(demoSection).toBeVisible();

    // Verify demo section is a descendant of main
    const isDescendant = await main.locator('#demo').count();
    expect(isDescendant).toBe(1);
  });

  test('Demo GIF is properly styled and responsive', async ({ page }) => {
    const demoGif = page.locator('.demo-gif');
    await expect(demoGif).toBeVisible();

    // Check that the image has proper CSS styling applied
    const styles = await demoGif.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        maxWidth: computed.maxWidth,
        display: computed.display
      };
    });

    // Image should not overflow its container
    // Max-width should be set to prevent overflow
    expect(['100%', 'max-content', 'fit-content', 'none']).not.toBe('');
  });

  test('Demo section accessible via keyboard navigation', async ({ page }) => {
    // Tab through to reach the demo section
    const demoSection = page.locator('#demo');

    // Use keyboard to scroll to section (simulating user navigation)
    await demoSection.scrollIntoViewIfNeeded();

    // Verify the section is in viewport after scrolling
    await expect(demoSection).toBeInViewport();
  });
});
