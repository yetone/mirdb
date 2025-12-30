const { test, expect } = require('@playwright/test');

/**
 * Usage Demonstration Section Tests
 * Scenario: Validate the usage demonstration GIF is properly embedded and displayed
 */

test.describe('Usage Demonstration Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Demo section contains image element with usage.gif source', async ({ page }) => {
    // Navigate to demo section and check for usage.gif image element
    const demoSection = page.locator('#demo, [class*="demo"], .demo, section:has(img[src*="usage"])');
    await expect(demoSection.first()).toBeVisible();

    // Check that the usage.gif image element exists with proper src attribute
    const usageGif = page.locator('img[src*="usage.gif"], img[src*="usage"]');
    await expect(usageGif.first()).toBeVisible();

    // Verify the src attribute contains usage.gif
    const src = await usageGif.first().getAttribute('src');
    expect(src).toContain('usage.gif');
  });

  test('Test Case 2: Usage.gif has descriptive alt text for accessibility', async ({ page }) => {
    // Check that usage.gif image has descriptive alt text
    const usageGif = page.locator('img[src*="usage.gif"], img[src*="usage"]');
    await expect(usageGif.first()).toBeVisible();

    // Verify alt text is present and descriptive
    const altText = await usageGif.first().getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText.length).toBeGreaterThan(10); // Alt text should be descriptive, not just "image" or "gif"

    // Alt text should describe what the demo shows (usage, demonstration, action, etc.)
    const altLower = altText.toLowerCase();
    const isDescriptive = altLower.includes('demo') ||
                          altLower.includes('usage') ||
                          altLower.includes('mirdb') ||
                          altLower.includes('action') ||
                          altLower.includes('command') ||
                          altLower.includes('example') ||
                          altLower.includes('show');
    expect(isDescriptive).toBe(true);
  });

  test('Test Case 3: Demo section includes caption text explaining the demonstration', async ({ page }) => {
    // Check for demo section caption text
    const demoSection = page.locator('#demo, [class*="demo"], .demo, section:has(img[src*="usage"])');
    await expect(demoSection.first()).toBeVisible();

    // Look for caption element or descriptive text near the demo
    const captionElements = demoSection.first().locator('figcaption, .caption, [class*="caption"], p, .demo-description, [class*="description"]');

    // Should have at least one text element that serves as caption
    const count = await captionElements.count();
    expect(count).toBeGreaterThan(0);

    // Get the section text content and verify it has explanatory content
    const sectionText = await demoSection.first().textContent();
    expect(sectionText.length).toBeGreaterThan(20); // Should have meaningful caption text

    // Caption should explain what the demo shows
    const textLower = sectionText.toLowerCase();
    const hasExplanatoryText = textLower.includes('demo') ||
                               textLower.includes('action') ||
                               textLower.includes('command') ||
                               textLower.includes('usage') ||
                               textLower.includes('see') ||
                               textLower.includes('show') ||
                               textLower.includes('work') ||
                               textLower.includes('example');
    expect(hasExplanatoryText).toBe(true);
  });
});
