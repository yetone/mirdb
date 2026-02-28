/**
 * Hero Section E2E Tests
 * Owner: Scenario 1 - Hero Section Display
 *
 * Tests:
 * - Page loads and hero visible
 * - Product name visible within 1s
 * - CTA button clickable and navigates correctly
 */

import { test, expect } from '@playwright/test';

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Product name "MirDB" is rendered in an h1 element with bold styling', async ({ page }) => {
    // Wait for the h1 to be visible within 1 second as per REQ-1
    const h1 = page.locator('h1');
    await expect(h1).toBeVisible({ timeout: 1000 });

    // Verify the product name
    await expect(h1).toHaveText('MirDB');

    // Verify bold styling (font-weight should be >= 600)
    const fontWeight = await h1.evaluate((el) => {
      return window.getComputedStyle(el).fontWeight;
    });
    expect(parseInt(fontWeight)).toBeGreaterThanOrEqual(600);
  });

  test('TC2: Hero section contains product name, tagline paragraph, and at least one CTA button', async ({ page }) => {
    // Verify hero section exists
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Verify product name
    const productName = heroSection.locator('h1');
    await expect(productName).toBeVisible();
    await expect(productName).toHaveText('MirDB');

    // Verify tagline paragraph
    const tagline = heroSection.locator('.hero__tagline');
    await expect(tagline).toBeVisible();
    const taglineText = await tagline.textContent();
    expect(taglineText).toContain('key-value store');

    // Verify at least one CTA button
    const ctaButtons = heroSection.locator('.btn');
    const ctaCount = await ctaButtons.count();
    expect(ctaCount).toBeGreaterThanOrEqual(1);
  });

  test('TC3: Primary CTA button displays clear action text', async ({ page }) => {
    const primaryCta = page.locator('.btn--primary');
    await expect(primaryCta).toBeVisible();

    // Verify clear action text
    const ctaText = await primaryCta.textContent();
    const validCtaTexts = ['Get Started', 'Learn More', 'Try Now', 'Start Now', 'Get Started Free'];
    const hasValidText = validCtaTexts.some(text => ctaText.includes(text));
    expect(hasValidText).toBe(true);
  });

  test('TC4: Button click navigates to appropriate destination or triggers expected action', async ({ page }) => {
    const primaryCta = page.locator('.btn--primary');
    await expect(primaryCta).toBeVisible();

    // Get the href attribute
    const href = await primaryCta.getAttribute('href');
    expect(href).toBeTruthy();

    // Click the button
    await primaryCta.click();

    // If it's an anchor link, verify the URL hash changed
    if (href.startsWith('#')) {
      const currentUrl = page.url();
      expect(currentUrl).toContain(href);
    }
  });

  test('Hero section is visible immediately on page load', async ({ page }) => {
    const heroSection = page.locator('.hero');

    // Should be visible immediately (within 100ms is effectively immediate)
    await expect(heroSection).toBeVisible({ timeout: 100 });
  });

  test('CTA buttons have proper focus states for accessibility', async ({ page }) => {
    const primaryCta = page.locator('.btn--primary');

    // Focus the CTA button
    await primaryCta.focus();

    // Verify the button has focus
    await expect(primaryCta).toBeFocused();
  });

  test('Hero section has proper ARIA attributes', async ({ page }) => {
    const heroSection = page.locator('.hero');

    // Verify aria-labelledby attribute
    const ariaLabelledBy = await heroSection.getAttribute('aria-labelledby');
    expect(ariaLabelledBy).toBe('hero-title');

    // Verify the referenced element exists
    const referencedElement = page.locator('#hero-title');
    await expect(referencedElement).toBeVisible();
  });

  test('Hero section is responsive - visible on mobile viewport', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });

    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    const h1 = page.locator('h1');
    await expect(h1).toBeVisible();

    const ctaButton = page.locator('.btn--primary');
    await expect(ctaButton).toBeVisible();
  });

  test('Hero section is responsive - visible on tablet viewport', async ({ page }) => {
    // Set tablet viewport
    await page.setViewportSize({ width: 768, height: 1024 });

    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    const h1 = page.locator('h1');
    await expect(h1).toBeVisible();
  });

  test('Hero section is responsive - visible on desktop viewport', async ({ page }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1280, height: 800 });

    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    const h1 = page.locator('h1');
    await expect(h1).toBeVisible();
  });
});
