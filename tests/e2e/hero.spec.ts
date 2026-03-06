/**
 * Hero Section E2E Tests
 * Owner: Scenario 2 - Hero Section Display
 *
 * Test coverage:
 * - MirDB heading (H1)
 * - Tagline text content
 * - Get Started CTA button
 * - CTA button functionality
 */
import { test, expect } from '@playwright/test';

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: H1 element exists containing MirDB text', async ({ page }) => {
    // Check for main heading with 'MirDB' text
    const heading = page.locator('h1');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('MirDB');
  });

  test('TC2: Tagline contains key terms: Persistent, Key-Value, Memcached', async ({ page }) => {
    // Check for tagline mentioning key terms
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Check for tagline text containing all required keywords
    const tagline = page.locator('.hero-tagline');
    await expect(tagline).toBeVisible();

    const taglineText = await tagline.textContent();
    expect(taglineText).toContain('Persistent');
    expect(taglineText).toContain('Key-Value');
    expect(taglineText).toContain('Memcached');
  });

  test('TC3: Get Started button exists and is visible', async ({ page }) => {
    // Check for Get Started button/anchor element
    const ctaButton = page.locator('[data-testid="get-started-btn"]');
    await expect(ctaButton).toBeVisible();
    await expect(ctaButton).toContainText('Get Started');

    // Verify it's clickable (has href attribute or is a button)
    const href = await ctaButton.getAttribute('href');
    expect(href).toBeTruthy();
  });

  test('TC4: Click Get Started navigates to quick-start section', async ({ page }) => {
    // Click Get Started button and verify navigation
    const ctaButton = page.locator('[data-testid="get-started-btn"]');
    await expect(ctaButton).toBeVisible();

    // Click the button
    await ctaButton.click();

    // Wait for navigation - the URL should contain #quick-start
    await page.waitForURL(/.*#quick-start/);

    // Verify we're at the quick-start section
    const currentUrl = page.url();
    expect(currentUrl).toContain('#quick-start');

    // Verify the quick-start section exists in the DOM
    // Note: Section may not be visible yet as content is added by Scenario 4
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toHaveCount(1);
  });

  test('Hero section is positioned correctly below header', async ({ page }) => {
    // Verify hero section exists and is visible
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify header exists above hero
    const header = page.locator('header');
    await expect(header).toBeVisible();

    // Check that hero is after header in DOM
    const headerBox = await header.boundingBox();
    const heroBox = await heroSection.boundingBox();

    expect(heroBox).toBeTruthy();
    expect(headerBox).toBeTruthy();

    if (headerBox && heroBox) {
      // Hero should be below header
      expect(heroBox.y).toBeGreaterThanOrEqual(headerBox.y + headerBox.height - 5);
    }
  });

  test('Hero title has proper H1 semantics for accessibility', async ({ page }) => {
    // Verify there's exactly one H1 on the page
    const h1Elements = page.locator('h1');
    const count = await h1Elements.count();
    expect(count).toBe(1);

    // Verify it's the MirDB title
    await expect(h1Elements.first()).toContainText('MirDB');
  });

  test('CTA button has proper styling and is prominent', async ({ page }) => {
    const ctaButton = page.locator('[data-testid="get-started-btn"]');
    await expect(ctaButton).toBeVisible();

    // Check the button has background color (is styled)
    const bgColor = await ctaButton.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Should have a non-transparent background
    expect(bgColor).not.toBe('rgba(0, 0, 0, 0)');
    expect(bgColor).not.toBe('transparent');
  });
});
