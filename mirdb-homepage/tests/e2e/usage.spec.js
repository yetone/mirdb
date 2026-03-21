/**
 * Usage Example Section E2E Tests
 * Owner: Scenario 4 - Usage Example Section
 *
 * Tests:
 * - Usage section presence
 * - Code snippet or GIF display
 * - Image accessibility (alt text)
 */

const { test, expect } = require('@playwright/test');

test.describe('Usage Example Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC1: Usage section exists with id="usage" containing example content', async ({ page }) => {
    // Verify usage section exists with correct id
    const usageSection = page.locator('#usage');
    await expect(usageSection).toBeVisible();

    // Verify section has the usage class
    await expect(usageSection).toHaveClass(/usage/);

    // Verify section contains example content (heading and either code or image)
    const usageHeading = page.locator('#usage h2');
    await expect(usageHeading).toBeVisible();
    await expect(usageHeading).toContainText(/usage/i);

    // Verify there's actual content in the section (not empty)
    const contentCount = await page.locator('#usage *').count();
    expect(contentCount).toBeGreaterThan(1);
  });

  test('TC2: Section contains code snippet or GIF showing basic MirDB usage pattern', async ({ page }) => {
    const usageSection = page.locator('#usage');
    await expect(usageSection).toBeVisible();

    // Check for either a code snippet OR a GIF/image demonstrating usage
    const codeSnippet = page.locator('#usage pre code, #usage .usage-code, #usage code');
    const usageImage = page.locator('#usage img');

    // At least one of these should be present
    const hasCode = await codeSnippet.count() > 0;
    const hasImage = await usageImage.count() > 0;

    expect(hasCode || hasImage).toBeTruthy();

    // If there's a code snippet, verify it contains MirDB-related content
    if (hasCode) {
      const codeText = await codeSnippet.first().textContent();
      // Should contain some key-value or memcached-related commands
      expect(codeText.toLowerCase()).toMatch(/set|get|value|key|mirdb|memcache|telnet|localhost/i);
    }

    // If there's an image, verify it exists and is loaded
    if (hasImage) {
      const img = usageImage.first();
      await expect(img).toBeVisible();

      // Verify image has a src attribute
      const src = await img.getAttribute('src');
      expect(src).toBeTruthy();
    }
  });

  test('TC3: Usage demonstration image/GIF has descriptive alt text for accessibility', async ({ page }) => {
    const usageSection = page.locator('#usage');
    await expect(usageSection).toBeVisible();

    // Find all images in the usage section
    const usageImages = page.locator('#usage img');
    const imageCount = await usageImages.count();

    // If there are images, verify they all have alt text
    if (imageCount > 0) {
      for (let i = 0; i < imageCount; i++) {
        const img = usageImages.nth(i);
        const altText = await img.getAttribute('alt');

        // Alt text should exist and be descriptive (not empty)
        expect(altText).toBeTruthy();
        expect(altText.length).toBeGreaterThan(5);

        // Alt text should be descriptive of MirDB usage
        expect(altText.toLowerCase()).toMatch(/usage|demo|example|mirdb|commands|terminal/i);
      }
    } else {
      // If no images, verify there's at least code content with proper accessibility
      const codeBlock = page.locator('#usage pre, #usage code');
      const hasCode = await codeBlock.count() > 0;

      // Either images with alt text or code blocks should exist
      expect(hasCode).toBeTruthy();
    }
  });

  test('Usage section has proper heading hierarchy', async ({ page }) => {
    const usageSection = page.locator('#usage');
    await expect(usageSection).toBeVisible();

    // Verify h2 heading exists for the section
    const h2Heading = page.locator('#usage h2');
    await expect(h2Heading).toBeVisible();

    // Verify the section is properly structured
    await expect(usageSection).toHaveAttribute('id', 'usage');
  });

  test('Usage section is navigable via anchor link', async ({ page }) => {
    // Navigate directly to the usage section via anchor
    await page.goto('/#usage');
    await page.waitForLoadState('domcontentloaded');

    // Wait for smooth scroll to complete
    await page.waitForTimeout(500);

    // Verify the usage section is visible
    const usageSection = page.locator('#usage');
    await expect(usageSection).toBeVisible();

    // Check that the section is in the viewport (visible on screen)
    const isInViewport = await page.evaluate(() => {
      const el = document.querySelector('#usage');
      if (!el) return false;
      const rect = el.getBoundingClientRect();
      // Section should be at least partially visible in viewport
      return rect.top < window.innerHeight && rect.bottom > 0;
    });

    expect(isInViewport).toBeTruthy();
  });

  test('Usage section content is readable on mobile viewport', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });

    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Navigate to usage section
    const usageSection = page.locator('#usage');
    await usageSection.scrollIntoViewIfNeeded();

    // Verify content is visible
    const usageHeading = page.locator('#usage h2');
    await expect(usageHeading).toBeVisible();

    // Verify content doesn't overflow horizontally
    const hasHorizontalOverflow = await page.evaluate(() => {
      const el = document.querySelector('#usage');
      if (!el) return false;
      return el.scrollWidth > el.clientWidth + 20; // Allow 20px tolerance
    });

    expect(hasHorizontalOverflow).toBeFalsy();
  });
});
