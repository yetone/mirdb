/**
 * Accessibility E2E Tests - Images and Alt Text
 * Owner: Scenario 13 (Accessibility - Images and Alt Text)
 *
 * Test groups:
 * - All images have alt attribute
 * - Logo has descriptive alt text
 * - Status badges have alt text describing their purpose
 * - Icon-only buttons have aria-label or screen reader text
 */

import { test, expect } from '@playwright/test';
import { waitForLoad } from './utils';

test.describe('Accessibility - Images and Alt Text', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForLoad(page);
  });

  test('TC1: All images have alt attribute (can be empty for decorative)', async ({ page }) => {
    // Query all img elements on the page
    const images = await page.locator('img').all();

    expect(images.length).toBeGreaterThan(0);

    // Check each image has an alt attribute
    for (const img of images) {
      const hasAlt = await img.evaluate((el) => el.hasAttribute('alt'));
      const src = await img.getAttribute('src');
      expect(hasAlt, `Image ${src} should have an alt attribute`).toBe(true);
    }
  });

  test('TC2: Logo has descriptive alt text', async ({ page }) => {
    // Find the logo image
    const logo = page.locator('#hero-logo, .hero-logo, img[src*="logo"]').first();

    await expect(logo).toBeVisible();

    // Get the alt text
    const altText = await logo.getAttribute('alt');

    // Alt text should be present and descriptive (not empty, not generic)
    expect(altText).not.toBeNull();
    expect(altText).not.toBe('');
    expect(altText!.toLowerCase()).not.toBe('image');
    expect(altText!.toLowerCase()).not.toBe('icon');
    expect(altText!.toLowerCase()).not.toBe('logo');

    // Should contain meaningful description (e.g., 'MirDB' or product name)
    expect(altText!.toLowerCase()).toContain('mirdb');
  });

  test('TC3: Status badges have alt text describing their purpose', async ({ page }) => {
    // Find the badges section
    const badgesSection = page.locator('#badges, .badges');
    await expect(badgesSection).toBeVisible();

    // Find all badge images within the badges section
    const badgeImages = await badgesSection.locator('img').all();

    expect(badgeImages.length).toBeGreaterThan(0);

    for (const badge of badgeImages) {
      const altText = await badge.getAttribute('alt');

      // Each badge should have alt text
      expect(altText, 'Badge should have alt text').not.toBeNull();
      expect(altText!.trim(), 'Badge alt text should not be empty').not.toBe('');

      // Alt text should be descriptive, not generic
      expect(altText!.toLowerCase()).not.toBe('image');
      expect(altText!.toLowerCase()).not.toBe('icon');
      expect(altText!.toLowerCase()).not.toBe('badge');

      // Alt text should describe the badge's purpose (e.g., build status, license, stars)
      const isDescriptive =
        altText!.toLowerCase().includes('build') ||
        altText!.toLowerCase().includes('status') ||
        altText!.toLowerCase().includes('license') ||
        altText!.toLowerCase().includes('mit') ||
        altText!.toLowerCase().includes('stars') ||
        altText!.toLowerCase().includes('github') ||
        altText!.toLowerCase().includes('version') ||
        altText!.toLowerCase().includes('coverage');

      expect(isDescriptive, `Badge alt text "${altText}" should describe its purpose`).toBe(true);
    }
  });

  test('TC4: Icon-only buttons have aria-label or screen reader text', async ({ page }) => {
    // Find all buttons and button-like elements
    const buttons = await page.locator('button, [role="button"], a.btn, .btn').all();

    for (const button of buttons) {
      // Get button text content
      const textContent = await button.textContent();
      const trimmedText = textContent?.trim() || '';

      // If the button has visible text, it's accessible
      if (trimmedText.length > 0 && !/^[\s\u200B-\u200D\uFEFF]+$/.test(trimmedText)) {
        continue;
      }

      // For icon-only buttons (no text content), check for accessibility
      const ariaLabel = await button.getAttribute('aria-label');
      const ariaLabelledby = await button.getAttribute('aria-labelledby');
      const title = await button.getAttribute('title');

      // Check for visually hidden text inside the button
      const srText = await button.locator('.sr-only, .visually-hidden, [class*="screen-reader"]').textContent().catch(() => '');

      const hasAccessibleName =
        (ariaLabel && ariaLabel.trim().length > 0) ||
        (ariaLabelledby && ariaLabelledby.trim().length > 0) ||
        (title && title.trim().length > 0) ||
        (srText && srText.trim().length > 0);

      expect(hasAccessibleName, `Icon-only button should have aria-label, aria-labelledby, title, or screen reader text`).toBe(true);
    }

    // Additionally, check badge links have aria-labels
    const badgeLinks = await page.locator('#badges a, .badges a').all();

    for (const link of badgeLinks) {
      const ariaLabel = await link.getAttribute('aria-label');
      const linkText = await link.textContent();
      const trimmedLinkText = linkText?.trim() || '';

      // Badge links that only contain images should have aria-labels
      if (trimmedLinkText.length === 0) {
        expect(ariaLabel, 'Badge link should have aria-label').not.toBeNull();
        expect(ariaLabel!.trim(), 'Badge link aria-label should not be empty').not.toBe('');
      }
    }
  });

  test('All images have non-generic alt text', async ({ page }) => {
    // Additional test to verify all images have meaningful alt text
    const images = await page.locator('img').all();

    const genericAltValues = ['image', 'icon', 'picture', 'photo', 'img', 'graphic'];

    for (const img of images) {
      const altText = await img.getAttribute('alt');
      const src = await img.getAttribute('src');

      // Skip decorative images (empty alt is valid for decorative images)
      if (altText === '') {
        continue;
      }

      // Alt text should not be null for non-decorative images
      expect(altText, `Image ${src} should have alt attribute`).not.toBeNull();

      // Alt text should not be generic
      const isGeneric = genericAltValues.some(generic =>
        altText!.toLowerCase() === generic
      );

      expect(isGeneric, `Image ${src} has generic alt text "${altText}". Use descriptive text.`).toBe(false);
    }
  });

  test('Usage GIF has descriptive alt text', async ({ page }) => {
    // Find the usage GIF
    const usageGif = page.locator('#usage-gif, .usage-gif, img[src*="usage"]').first();

    const isVisible = await usageGif.isVisible().catch(() => false);

    if (isVisible) {
      const altText = await usageGif.getAttribute('alt');

      // Alt text should be present and descriptive
      expect(altText).not.toBeNull();
      expect(altText!.trim()).not.toBe('');
      expect(altText!.toLowerCase()).not.toBe('image');
      expect(altText!.toLowerCase()).not.toBe('gif');
      expect(altText!.toLowerCase()).not.toBe('usage');

      // Should describe what the GIF shows
      const isDescriptive =
        altText!.toLowerCase().includes('usage') ||
        altText!.toLowerCase().includes('demo') ||
        altText!.toLowerCase().includes('example') ||
        altText!.toLowerCase().includes('command') ||
        altText!.toLowerCase().includes('memcached') ||
        altText!.toLowerCase().includes('mirdb');

      expect(isDescriptive, `Usage GIF alt text "${altText}" should describe the content`).toBe(true);
    }
  });
});
