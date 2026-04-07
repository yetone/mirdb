/**
 * Homepage E2E Tests
 * Owner: Scenario 1 - Homepage Hero Section Content
 *
 * Test coverage:
 * - Hero section content verification (MirDB name, tagline, value proposition)
 * - CTA button functionality (Get Started links to Quick Start)
 * - Hero section visibility above the fold
 */
import { test, expect } from '@playwright/test';

test.describe('Homepage Hero Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Hero section contains MirDB text, tagline, and CTA button', async ({ page }) => {
    // Verify hero section exists
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify MirDB project name is displayed
    const heroTitle = page.locator('#hero h1');
    await expect(heroTitle).toContainText('MirDB');

    // Verify tagline describes persistent key-value store with Memcached protocol
    const tagline = page.locator('#hero .tagline');
    await expect(tagline).toBeVisible();
    const taglineText = await tagline.textContent();
    expect(taglineText).toMatch(/persistent.*key-value.*store/i);
    expect(taglineText).toMatch(/memcached.*protocol/i);

    // Verify value proposition text exists
    const valueProp = page.locator('#hero .value-prop');
    await expect(valueProp).toBeVisible();

    // Verify Get Started CTA button exists
    const ctaButton = page.locator('#get-started-btn');
    await expect(ctaButton).toBeVisible();
    await expect(ctaButton).toContainText('Get Started');
  });

  test('TC2: Hero section is visible above the fold on desktop viewport', async ({ page }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1280, height: 720 });
    await page.goto('/');

    // Verify hero section is in viewport without scrolling
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();
    await expect(heroSection).toBeInViewport();

    // Verify the CTA button is also visible above the fold
    const ctaButton = page.locator('#get-started-btn');
    await expect(ctaButton).toBeInViewport();
  });

  test('TC3: Get Started button navigates to Quick Start section', async ({ page }) => {
    // Click the Get Started button
    const ctaButton = page.locator('#get-started-btn');
    await expect(ctaButton).toHaveAttribute('href', '#quick-start');

    // Click and verify navigation
    await ctaButton.click();

    // Verify URL hash changes to quick-start
    await expect(page).toHaveURL(/#quick-start/);

    // Verify quick-start section is now visible
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();
  });
});
