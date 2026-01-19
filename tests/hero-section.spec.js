// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '../index.html');

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  // Test Case 1: Hero section is present with product name 'MirDB' visible
  test('TC1: Hero section is present with product name MirDB visible', async ({ page }) => {
    // Verify hero section exists
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify product name 'MirDB' is visible
    const productName = page.locator('.hero-title');
    await expect(productName).toBeVisible();
    await expect(productName).toHaveText('MirDB');
  });

  // Test Case 2: Logo image element exists and loads successfully
  test('TC2: Logo image element exists with logo.gif source', async ({ page }) => {
    // Check for logo element with src containing 'logo.gif'
    const logo = page.locator('#logo');
    await expect(logo).toBeVisible();

    // Verify src attribute contains 'logo.gif'
    const logoSrc = await logo.getAttribute('src');
    expect(logoSrc).toContain('logo.gif');

    // Verify the image has natural dimensions (loaded successfully)
    const naturalWidth = await logo.evaluate((img) => img.naturalWidth);
    expect(naturalWidth).toBeGreaterThan(0);
  });

  // Test Case 3: Primary CTA button exists and is clickable
  test('TC3: Primary CTA button with View on GitHub is present and clickable', async ({ page }) => {
    // Look for button with text 'Get Started' or 'View on GitHub'
    const primaryCta = page.locator('#cta-primary');
    await expect(primaryCta).toBeVisible();

    // Check button text matches expected values
    const buttonText = await primaryCta.textContent();
    const validTexts = ['Get Started', 'View on GitHub'];
    expect(validTexts.some(text => buttonText.includes(text))).toBeTruthy();

    // Verify button is enabled/clickable
    await expect(primaryCta).toBeEnabled();

    // Verify it has an href attribute (making it clickable)
    const href = await primaryCta.getAttribute('href');
    expect(href).toBeTruthy();
  });

  // Test Case 4: Secondary CTA exists with View Documentation link
  test('TC4: Secondary CTA link with View Documentation is present', async ({ page }) => {
    // Look for link with text 'View Documentation' or similar
    const secondaryCta = page.locator('#cta-secondary');
    await expect(secondaryCta).toBeVisible();

    // Check button text
    const linkText = await secondaryCta.textContent();
    expect(linkText.toLowerCase()).toContain('documentation');

    // Verify it has an href attribute
    const href = await secondaryCta.getAttribute('href');
    expect(href).toBeTruthy();
  });

  // Test Case 5: Value proposition text mentioning persistence and memcached is visible above the fold
  test('TC5: Subheadline mentioning persistence and memcached compatibility is visible above the fold', async ({ page }) => {
    // Set viewport to standard desktop size (1280x720)
    await page.setViewportSize({ width: 1280, height: 720 });

    // Find the subheadline element
    const subtitle = page.locator('.hero-subtitle');
    await expect(subtitle).toBeVisible();

    // Get the subtitle text
    const subtitleText = await subtitle.textContent();
    const lowerText = subtitleText.toLowerCase();

    // Verify it mentions persistence
    expect(lowerText).toContain('persist');

    // Verify it mentions memcached
    expect(lowerText).toContain('memcached');

    // Verify the element is above the fold (within viewport)
    const box = await subtitle.boundingBox();
    expect(box).toBeTruthy();
    expect(box.y + box.height).toBeLessThan(720); // Should be fully visible in 720px height
  });

  // Additional test: Hero section is visible above the fold
  test('Hero section is visible above the fold on desktop viewport', async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 720 });

    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify hero content is within viewport
    const heroContent = page.locator('.hero-content');
    const box = await heroContent.boundingBox();
    expect(box).toBeTruthy();
    expect(box.y).toBeGreaterThanOrEqual(0);
  });
});
