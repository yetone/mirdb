// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Product name MirDB is displayed prominently with h1 styling', async ({ page }) => {
    // Check that the product name "MirDB" is displayed prominently
    const heroSection = page.locator('.hero, section.hero, [data-testid="hero"]').first();
    await expect(heroSection).toBeVisible();

    // Check for h1 with MirDB text
    const productName = page.locator('h1');
    await expect(productName).toBeVisible();
    await expect(productName).toContainText('MirDB');
  });

  test('TC2: Logo image is present and renders correctly', async ({ page }) => {
    // Check for logo image in hero section
    const logo = page.locator('.hero img, section.hero img, [data-testid="hero"] img, .logo, img.logo, img[alt*="logo" i], img[alt*="MirDB" i]').first();
    await expect(logo).toBeVisible();

    // Verify the image source contains logo reference
    const src = await logo.getAttribute('src');
    expect(src).toBeTruthy();
    expect(src.toLowerCase()).toMatch(/logo/i);
  });

  test('TC3: Tagline with Persistent Key-Value Store and Memcached Protocol is visible', async ({ page }) => {
    // Check for the value proposition tagline in the hero section
    const heroSection = page.locator('.hero, section.hero, [data-testid="hero"]').first();
    const tagline = heroSection.locator('text=/Persistent Key-Value Store/i');
    await expect(tagline).toBeVisible();

    // Also verify Memcached Protocol is mentioned in the hero tagline
    const memcachedText = heroSection.locator('text=/Memcached Protocol/i');
    await expect(memcachedText).toBeVisible();
  });

  test('TC4: Primary CTA button is visible and clickable', async ({ page }) => {
    // Check for primary CTA button (Get Started, View on GitHub, etc.)
    const primaryCta = page.locator('.hero a.btn-primary, .hero .cta-primary, .hero a[href*="github"], a:has-text("Get Started"), a:has-text("View on GitHub"), .hero button:has-text("Get Started")').first();
    await expect(primaryCta).toBeVisible();

    // Verify it's clickable (not disabled)
    await expect(primaryCta).toBeEnabled();

    // Verify it has appropriate text
    const text = await primaryCta.textContent();
    expect(text.toLowerCase()).toMatch(/get started|github|documentation/i);
  });

  test('TC5: Secondary CTA button is visible', async ({ page }) => {
    // Check for secondary CTA button (Learn More, etc.)
    const secondaryCta = page.locator('.hero a.btn-secondary, .hero .cta-secondary, a:has-text("Learn More"), a:has-text("Documentation"), .hero button:has-text("Learn More")').first();
    await expect(secondaryCta).toBeVisible();

    // Verify it has appropriate text
    const text = await secondaryCta.textContent();
    expect(text.toLowerCase()).toMatch(/learn more|documentation|features/i);
  });
});
