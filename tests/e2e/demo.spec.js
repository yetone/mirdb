/**
 * Demo Section E2E Tests
 * Owner: Scenario 3 - Demonstration Section with Usage GIF
 *
 * Test cases:
 * - Demo section exists
 * - Usage GIF (usage.gif) is displayed
 * - GIF has alt text for accessibility
 * - Caption/description text is present
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

const pageUrl = `file://${path.resolve(process.cwd(), 'index.html')}`;

test.describe('Demo Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(pageUrl);
  });

  test('TC1: Demo section element exists with correct id', async ({ page }) => {
    // Test case 1: Check for demo section element
    // Expected: Section with id='demo' or 'demonstration' exists
    const demoSection = page.locator('section#demo, section#demonstration');
    await expect(demoSection).toBeVisible();

    // Verify it's actually a section element
    const tagName = await demoSection.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('section');
  });

  test('TC2: Usage GIF image is displayed with correct src', async ({ page }) => {
    // Test case 2: Check for img element with usage.gif src
    // Expected: Image element exists with src containing 'usage.gif'
    const usageGif = page.locator('img[src*="usage.gif"]');
    await expect(usageGif).toBeVisible();

    // Verify the image is within the demo section
    const demoSection = page.locator('section#demo, section#demonstration');
    const gifInDemo = demoSection.locator('img[src*="usage.gif"]');
    await expect(gifInDemo).toBeVisible();
  });

  test('TC3: Usage GIF has descriptive alt text for accessibility', async ({ page }) => {
    // Test case 3: Check for alt text on usage GIF
    // Expected: Usage GIF image has descriptive alt attribute for accessibility
    const usageGif = page.locator('img[src*="usage.gif"]');
    const altText = await usageGif.getAttribute('alt');

    // Alt text should exist and be descriptive (not empty or generic)
    expect(altText).toBeTruthy();
    expect(altText.length).toBeGreaterThan(10); // Should be descriptive, not just "gif" or "image"

    // Should mention usage, demo, or MirDB
    const hasDescriptiveContent =
      altText.toLowerCase().includes('usage') ||
      altText.toLowerCase().includes('demo') ||
      altText.toLowerCase().includes('mirdb') ||
      altText.toLowerCase().includes('command');
    expect(hasDescriptiveContent).toBe(true);
  });

  test('TC4: Demo caption text exists explaining the demonstration', async ({ page }) => {
    // Test case 4: Check for demo caption text
    // Expected: Descriptive text element exists near the usage GIF explaining the demonstration
    const demoSection = page.locator('section#demo, section#demonstration');

    // Look for caption text near the GIF
    const caption = demoSection.locator('.demo-caption, figcaption, p');
    await expect(caption.first()).toBeVisible();

    // Get the text content
    const captionText = await caption.first().textContent();

    // Caption should be descriptive (minimum length check)
    expect(captionText.length).toBeGreaterThan(20);

    // Caption should describe the demo functionality
    const hasDescriptiveCaption =
      captionText.toLowerCase().includes('demo') ||
      captionText.toLowerCase().includes('show') ||
      captionText.toLowerCase().includes('command') ||
      captionText.toLowerCase().includes('use') ||
      captionText.toLowerCase().includes('memcached');
    expect(hasDescriptiveCaption).toBe(true);
  });

  test('Demo section has proper heading', async ({ page }) => {
    // Verify the demo section has a heading
    const demoSection = page.locator('section#demo, section#demonstration');
    const heading = demoSection.locator('h2, h3');
    await expect(heading).toBeVisible();

    const headingText = await heading.textContent();
    expect(headingText.length).toBeGreaterThan(0);
  });

  test('Usage GIF is loaded and has valid dimensions', async ({ page }) => {
    // Verify the GIF is actually loaded with proper dimensions
    const usageGif = page.locator('img[src*="usage.gif"]');
    await expect(usageGif).toBeVisible();

    // Check that the image has loaded (natural width > 0)
    const naturalWidth = await usageGif.evaluate(img => img.naturalWidth);
    expect(naturalWidth).toBeGreaterThan(0);

    // Check displayed dimensions are reasonable
    const boundingBox = await usageGif.boundingBox();
    expect(boundingBox.width).toBeGreaterThan(100);
    expect(boundingBox.height).toBeGreaterThan(50);
  });
});
