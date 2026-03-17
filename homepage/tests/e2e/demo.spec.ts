/**
 * Demo Section Tests
 * Owner: Scenario 4 - Demo Section with Usage GIF
 *
 * Tests:
 * - E2E: Demo asset (gif or video) is present in demo section
 * - E2E: Section contains label text like 'See MirDB in Action'
 * - E2E: Demo has caption explaining what it demonstrates
 * - Unit: Asset has loading='lazy' attribute for performance
 * - Unit: Image has descriptive alt text for accessibility
 */

import { test, expect } from '@playwright/test';

test.describe('Demo Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Demo asset (gif or video) is present in demo section
  test('should display demo asset (gif or video) in demo section', async ({ page }) => {
    const demoSection = page.locator('#demo');
    await expect(demoSection).toBeVisible();

    // Check for either image (gif) or video element
    const demoImage = demoSection.locator('img[src*="usage"]');
    const demoVideo = demoSection.locator('video source[src*="usage"]');

    // At least one of these should exist
    const imageCount = await demoImage.count();
    const videoCount = await demoVideo.count();

    expect(imageCount + videoCount).toBeGreaterThan(0);
  });

  // Test Case 2: Section contains label text like 'See MirDB in Action'
  test('should have section heading with appropriate label', async ({ page }) => {
    const demoSection = page.locator('#demo');
    await expect(demoSection).toBeVisible();

    // Look for heading with text about seeing MirDB in action
    const heading = demoSection.locator('h2, h3');
    await expect(heading).toBeVisible();

    const headingText = await heading.textContent();
    // Check for action-oriented label containing key terms
    expect(headingText?.toLowerCase()).toMatch(/see|action|demo|watch|how it works/);
  });

  // Test Case 3: Demo has caption explaining what it demonstrates
  test('should have descriptive caption for demo', async ({ page }) => {
    const demoSection = page.locator('#demo');
    await expect(demoSection).toBeVisible();

    // Look for caption using figcaption, p with specific class, or data-testid
    const caption = demoSection.locator('figcaption, .demo-caption, [data-testid="demo-caption"]');
    await expect(caption).toBeVisible();

    const captionText = await caption.textContent();
    // Caption should have meaningful content (more than just whitespace)
    expect(captionText?.trim().length).toBeGreaterThan(10);
  });

  // Test Case 4: Asset has loading='lazy' attribute for performance (unit test)
  test('should have lazy loading attribute on demo asset', async ({ page }) => {
    const demoSection = page.locator('#demo');
    await expect(demoSection).toBeVisible();

    // Check for lazy loading on image
    const demoImage = demoSection.locator('img[src*="usage"]');
    const imageCount = await demoImage.count();

    if (imageCount > 0) {
      const loadingAttr = await demoImage.getAttribute('loading');
      expect(loadingAttr).toBe('lazy');
    } else {
      // If using video, check for lazy loading attribute or preload="none"
      const demoVideo = demoSection.locator('video');
      const videoCount = await demoVideo.count();

      if (videoCount > 0) {
        // For video, check preload attribute (none or metadata is lazy-friendly)
        const preloadAttr = await demoVideo.getAttribute('preload');
        expect(['none', 'metadata']).toContain(preloadAttr);
      }
    }
  });

  // Test Case 5: Image has descriptive alt text for accessibility (unit test)
  test('should have descriptive alt text on demo asset', async ({ page }) => {
    const demoSection = page.locator('#demo');
    await expect(demoSection).toBeVisible();

    // Check for alt text on image
    const demoImage = demoSection.locator('img[src*="usage"]');
    const imageCount = await demoImage.count();

    if (imageCount > 0) {
      const altText = await demoImage.getAttribute('alt');
      // Alt text should exist and be descriptive (not empty or just "image")
      expect(altText).toBeTruthy();
      expect(altText?.trim().length).toBeGreaterThan(5);
      expect(altText?.toLowerCase()).not.toBe('image');
      expect(altText?.toLowerCase()).not.toBe('gif');
    } else {
      // If using video, check for aria-label or title
      const demoVideo = demoSection.locator('video');
      const ariaLabel = await demoVideo.getAttribute('aria-label');
      const title = await demoVideo.getAttribute('title');

      // Either aria-label or title should be present
      expect(ariaLabel || title).toBeTruthy();
    }
  });

  // Additional test: Demo section has proper ARIA labeling
  test('should have proper accessibility attributes on demo section', async ({ page }) => {
    const demoSection = page.locator('#demo');
    await expect(demoSection).toBeVisible();

    // Check for aria-labelledby or aria-label
    const ariaLabelledBy = await demoSection.getAttribute('aria-labelledby');
    const ariaLabel = await demoSection.getAttribute('aria-label');

    expect(ariaLabelledBy || ariaLabel).toBeTruthy();
  });

  // Additional test: Demo section is properly positioned in page flow
  test('should be positioned after features section', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const demoSection = page.locator('#demo');

    await expect(featuresSection).toBeVisible();
    await expect(demoSection).toBeVisible();

    // Get bounding boxes to verify order
    const featuresBBox = await featuresSection.boundingBox();
    const demoBBox = await demoSection.boundingBox();

    expect(featuresBBox).toBeTruthy();
    expect(demoBBox).toBeTruthy();

    // Demo section should be below features section
    expect(demoBBox!.y).toBeGreaterThan(featuresBBox!.y);
  });
});
