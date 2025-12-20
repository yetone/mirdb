// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

// Test Case 1: Hero section contains h1 with 'MirDB' text, tagline element, and at least 2 CTA buttons
test.describe('Hero Section Content Display', () => {
  test.beforeEach(async ({ page }) => {
    const filePath = path.resolve(__dirname, '../index.html');
    await page.goto(`file://${filePath}`);
  });

  test('TC1: Hero section contains h1 with MirDB text, tagline, and CTA buttons', async ({ page }) => {
    // Verify hero section exists
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify h1 contains MirDB
    const h1 = heroSection.locator('h1');
    await expect(h1).toBeVisible();
    await expect(h1).toContainText('MirDB');

    // Verify tagline element exists
    const tagline = heroSection.locator('[data-testid="hero-tagline"]');
    await expect(tagline).toBeVisible();
    const taglineText = await tagline.textContent();
    expect(taglineText.length).toBeGreaterThan(0);

    // Verify at least 2 CTA buttons
    const ctaButtons = heroSection.locator('[data-testid^="cta-"]');
    await expect(ctaButtons).toHaveCount(2);
  });

  // Test Case 2: Hero section is fully visible without scrolling on standard viewport (1920x1080)
  test('TC2: Hero section renders above the fold on 1920x1080 viewport', async ({ page }) => {
    // Set viewport to standard 1920x1080
    await page.setViewportSize({ width: 1920, height: 1080 });

    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Get hero section bounding box
    const boundingBox = await heroSection.boundingBox();
    expect(boundingBox).not.toBeNull();

    // Verify hero section is within viewport (above the fold)
    // The bottom of the hero section should be within the viewport height
    expect(boundingBox.y).toBeGreaterThanOrEqual(0);
    expect(boundingBox.y + boundingBox.height).toBeLessThanOrEqual(1080);
  });

  // Test Case 3: Primary CTA button has text 'Get Started' or 'Download' and links to appropriate resource
  test('TC3: Primary CTA button has correct text and links to appropriate resource', async ({ page }) => {
    const heroSection = page.locator('[data-testid="hero-section"]');
    const primaryCta = heroSection.locator('[data-testid="cta-primary"]');

    await expect(primaryCta).toBeVisible();

    // Verify button text is 'Get Started' or 'Download'
    const buttonText = await primaryCta.textContent();
    const hasValidText = buttonText.includes('Get Started') || buttonText.includes('Download');
    expect(hasValidText).toBeTruthy();

    // Verify button has an href attribute (links to a resource)
    const href = await primaryCta.getAttribute('href');
    expect(href).not.toBeNull();
    expect(href.length).toBeGreaterThan(0);
  });

  // Test Case 4: Secondary CTA button links to documentation resource
  test('TC4: Secondary CTA button links to documentation resource', async ({ page }) => {
    const heroSection = page.locator('[data-testid="hero-section"]');
    const secondaryCta = heroSection.locator('[data-testid="cta-secondary"]');

    await expect(secondaryCta).toBeVisible();

    // Verify button has documentation-related text
    const buttonText = await secondaryCta.textContent();
    const hasDocText = buttonText.toLowerCase().includes('doc') ||
                       buttonText.toLowerCase().includes('learn') ||
                       buttonText.toLowerCase().includes('view');
    expect(hasDocText).toBeTruthy();

    // Verify button links to documentation
    const href = await secondaryCta.getAttribute('href');
    expect(href).not.toBeNull();
    expect(href.length).toBeGreaterThan(0);
  });
});
