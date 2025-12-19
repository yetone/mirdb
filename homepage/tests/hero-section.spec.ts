import { test, expect } from '@playwright/test';

test.describe('Hero Section Display and Value Proposition', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage
    await page.goto('file://' + process.cwd() + '/public/index.html');
  });

  test('TC1: Hero section displays MirDB product name prominently', async ({ page }) => {
    // Verify the hero section exists
    const heroSection = page.locator('[data-testid="hero-section"], .hero-section, #hero, section.hero');
    await expect(heroSection).toBeVisible();

    // Verify MirDB product name is displayed prominently
    const productName = page.locator('h1');
    await expect(productName).toContainText('MirDB');
    await expect(productName).toBeVisible();
  });

  test('TC2: Tagline communicates persistent Memcached-compatible key-value store value proposition', async ({ page }) => {
    // Check for tagline content that communicates the value proposition
    const tagline = page.getByTestId('tagline');
    await expect(tagline).toBeVisible();

    // Verify the tagline contains key value proposition elements
    const taglineText = await tagline.textContent();
    expect(taglineText?.toLowerCase()).toMatch(/persistent|memcached|key-value|key value/);
  });

  test('TC3: Primary CTA button is visible, clickable, and navigates to appropriate destination', async ({ page }) => {
    // Find the primary CTA button
    const ctaButton = page.locator('[data-testid="cta-button"], .cta-button, .hero-cta, a.btn-primary, button.btn-primary').first();
    await expect(ctaButton).toBeVisible();

    // Verify the button is clickable (has valid href or is interactive)
    const isLink = await ctaButton.evaluate(el => el.tagName.toLowerCase() === 'a');
    if (isLink) {
      const href = await ctaButton.getAttribute('href');
      expect(href).toBeTruthy();
      expect(href).not.toBe('#');
    }

    // Verify the button has actionable text
    const buttonText = await ctaButton.textContent();
    expect(buttonText).toBeTruthy();
    expect(buttonText!.length).toBeGreaterThan(0);
  });

  test('TC4: Hero section is fully visible above the fold on standard viewport sizes', async ({ page }) => {
    // Set a standard desktop viewport
    await page.setViewportSize({ width: 1280, height: 720 });
    await page.goto('file://' + process.cwd() + '/public/index.html');

    // Verify hero section is visible without scrolling
    const heroSection = page.locator('[data-testid="hero-section"], .hero-section, #hero, section.hero');
    await expect(heroSection).toBeVisible();

    // Check that the hero section is within the initial viewport
    const boundingBox = await heroSection.boundingBox();
    expect(boundingBox).toBeTruthy();
    expect(boundingBox!.y).toBeGreaterThanOrEqual(0);
    expect(boundingBox!.y + boundingBox!.height).toBeLessThanOrEqual(720);

    // Test on tablet viewport
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('file://' + process.cwd() + '/public/index.html');
    await expect(heroSection).toBeVisible();

    // Test on mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('file://' + process.cwd() + '/public/index.html');
    await expect(heroSection).toBeVisible();
  });
});
