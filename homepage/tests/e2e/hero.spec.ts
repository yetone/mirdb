/**
 * Hero Section E2E Tests
 * Owner: Scenario 1 - Hero Section Display
 *
 * Tests for:
 * - Hero section visibility
 * - Product name in h1
 * - Tagline content verification
 * - CTA button functionality
 */

import { test, expect } from '@playwright/test';
import { PRODUCT_NAME, HERO_KEYWORDS, CTA_TEXT, SELECTORS } from '../fixtures/test-data';

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('hero section is visible at the top of the page', async ({ page }) => {
    const heroSection = page.locator(SELECTORS.HERO.SECTION);
    await expect(heroSection).toBeVisible();

    // Verify hero is near the top of the page
    const boundingBox = await heroSection.boundingBox();
    expect(boundingBox).toBeTruthy();
    expect(boundingBox!.y).toBeLessThan(200);
  });

  test('hero section contains h1 element with MirDB text', async ({ page }) => {
    const heroTitle = page.locator(`${SELECTORS.HERO.SECTION} h1`);
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toContainText(PRODUCT_NAME);
  });

  test('headline contains keywords: persistent, Memcached, key-value', async ({ page }) => {
    const heroHeadline = page.locator(SELECTORS.HERO.HEADLINE);
    await expect(heroHeadline).toBeVisible();

    const headlineText = await heroHeadline.textContent();
    expect(headlineText).toBeTruthy();

    for (const keyword of HERO_KEYWORDS) {
      expect(headlineText!.toLowerCase()).toContain(keyword.toLowerCase());
    }
  });

  test('CTA button with Get Started text exists and is clickable', async ({ page }) => {
    const ctaButton = page.locator(SELECTORS.HERO.CTA);
    await expect(ctaButton).toBeVisible();
    await expect(ctaButton).toContainText(CTA_TEXT);

    // Verify it's clickable (has proper role and is enabled)
    await expect(ctaButton).toBeEnabled();

    // Verify minimum touch target size (44x44 for accessibility)
    const boundingBox = await ctaButton.boundingBox();
    expect(boundingBox).toBeTruthy();
    expect(boundingBox!.width).toBeGreaterThanOrEqual(44);
    expect(boundingBox!.height).toBeGreaterThanOrEqual(44);
  });

  test('CTA button has correct href to quickstart section', async ({ page }) => {
    const ctaButton = page.locator(SELECTORS.HERO.CTA);
    const href = await ctaButton.getAttribute('href');
    expect(href).toBe('#quickstart');
  });

  test('hero section has proper accessibility attributes', async ({ page }) => {
    const heroSection = page.locator(SELECTORS.HERO.SECTION);

    // Check aria-labelledby
    const ariaLabelledBy = await heroSection.getAttribute('aria-labelledby');
    expect(ariaLabelledBy).toBe('hero-title');

    // Verify the referenced title exists
    const titleElement = page.locator('#hero-title');
    await expect(titleElement).toBeVisible();
  });

  test('hero description provides context about MirDB', async ({ page }) => {
    const description = page.locator(SELECTORS.HERO.DESCRIPTION);
    await expect(description).toBeVisible();

    const descText = await description.textContent();
    expect(descText).toBeTruthy();
    // Should mention key features
    expect(descText!.toLowerCase()).toContain('rust');
  });
});
