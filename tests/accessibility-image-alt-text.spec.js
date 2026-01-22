// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Accessibility - Image Alt Text', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Logo has descriptive alt text', async ({ page }) => {
    // Check the hero logo image
    const heroLogo = page.locator('.hero-logo');
    await expect(heroLogo).toBeVisible();

    // Verify the logo has the optimized placeholder source for performance
    await expect(heroLogo).toHaveAttribute('src', 'assets/logo-placeholder.svg');

    // Verify the logo has descriptive alt text
    const altText = await heroLogo.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText).toContain('MirDB');
    expect(altText).toContain('Logo');

    // Also check the navigation logo
    const navLogo = page.locator('.nav-logo');
    await expect(navLogo).toBeVisible();

    const navAltText = await navLogo.getAttribute('alt');
    expect(navAltText).toBeTruthy();
    expect(navAltText).toContain('MirDB');
    expect(navAltText).toContain('Logo');
  });

  test('TC2: Usage demo GIF (usage.gif) has descriptive alt text', async ({ page }) => {
    // Scroll to the demo section to ensure it's visible
    await page.locator('#demo').scrollIntoViewIfNeeded();

    // Find the usage demo image
    const demoGif = page.locator('.demo-gif');
    await expect(demoGif).toBeVisible();

    // Verify the image source
    await expect(demoGif).toHaveAttribute('src', 'assets/usage.gif');

    // Verify the alt text describes the demonstration
    const altText = await demoGif.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText.toLowerCase()).toContain('mirdb');
    expect(altText.toLowerCase()).toContain('demonstration');
    // Should mention the operations being demonstrated
    expect(altText.toLowerCase()).toMatch(/set|get|delete|operation/);
  });

  test('TC3: CI badge has alt text indicating build status purpose', async ({ page }) => {
    // Check the hero section CI badge
    const heroBadge = page.locator('.hero-badge img');
    await expect(heroBadge).toBeVisible();

    // Verify the alt text indicates build status purpose
    const heroAltText = await heroBadge.getAttribute('alt');
    expect(heroAltText).toBeTruthy();
    expect(heroAltText.toLowerCase()).toContain('circleci');
    expect(heroAltText.toLowerCase()).toContain('build');
    expect(heroAltText.toLowerCase()).toContain('status');

    // Scroll to footer and check the footer CI badge
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));

    const footerBadge = page.locator('.footer-links img[src*="circleci"]');
    await expect(footerBadge).toBeVisible();

    const footerAltText = await footerBadge.getAttribute('alt');
    expect(footerAltText).toBeTruthy();
    expect(footerAltText.toLowerCase()).toContain('circleci');
    expect(footerAltText.toLowerCase()).toContain('build');
    expect(footerAltText.toLowerCase()).toContain('status');
  });

  test('All images on the page have non-empty alt attributes', async ({ page }) => {
    // Get all img elements on the page
    const images = page.locator('img');
    const imageCount = await images.count();

    // Verify there are images on the page
    expect(imageCount).toBeGreaterThan(0);

    // Check each image has a non-empty alt attribute
    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i);
      const src = await img.getAttribute('src');
      const alt = await img.getAttribute('alt');

      // Every image must have an alt attribute
      expect(alt, `Image with src "${src}" is missing alt attribute`).not.toBeNull();
      // Alt text should be descriptive (non-empty for informative images)
      expect(alt.trim().length, `Image with src "${src}" has empty alt text`).toBeGreaterThan(0);
    }
  });

  test('Alt text is meaningful and descriptive, not just generic placeholders', async ({ page }) => {
    // Get all images
    const images = page.locator('img');
    const imageCount = await images.count();

    // List of generic/placeholder alt text patterns to avoid
    const genericPatterns = [
      /^image$/i,
      /^photo$/i,
      /^picture$/i,
      /^icon$/i,
      /^img$/i,
      /^\d+$/,
      /^untitled$/i,
      /^placeholder$/i,
    ];

    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i);
      const alt = await img.getAttribute('alt');
      const src = await img.getAttribute('src');

      // Check alt text is not a generic placeholder
      for (const pattern of genericPatterns) {
        expect(
          pattern.test(alt),
          `Image with src "${src}" has generic alt text: "${alt}"`
        ).toBeFalsy();
      }
    }
  });

  test('Logo images have consistent alt text describing MirDB branding', async ({ page }) => {
    // Find all logo images (both in nav and hero)
    const logoImages = page.locator('img[src*="logo"]');
    const logoCount = await logoImages.count();

    expect(logoCount).toBeGreaterThan(0);

    for (let i = 0; i < logoCount; i++) {
      const logo = logoImages.nth(i);
      const alt = await logo.getAttribute('alt');

      // Logo alt text should mention MirDB
      expect(alt.toLowerCase()).toContain('mirdb');
      // And should indicate it's a logo
      expect(alt.toLowerCase()).toContain('logo');
    }
  });
});
