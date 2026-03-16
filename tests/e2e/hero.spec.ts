/**
 * Hero Section and Branding Tests
 * Owner: Scenario 1 - Hero Section and Branding Display
 *
 * Test cases:
 * - Logo image exists with correct src and alt text
 * - Product name 'MirDB' in heading
 * - Value proposition tagline visible
 * - CTA buttons (Get Started, View on GitHub) present
 */

import { test, expect } from '@playwright/test';

test.describe('Hero Section and Branding Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC1: Logo image exists with correct src and alt text', async ({ page }) => {
    // Verify logo image element exists
    const logo = page.locator('.hero-logo');
    await expect(logo).toBeVisible();

    // Verify logo src points to assets/logo.gif (relative path or with images folder)
    const src = await logo.getAttribute('src');
    expect(src).toContain('logo.gif');

    // Verify alt text is present and meaningful
    const altText = await logo.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText!.toLowerCase()).toContain('mirdb');
  });

  test('TC2: Product name MirDB appears in h1 heading element', async ({ page }) => {
    // Find the h1 element with the product name
    const heading = page.locator('h1.hero-title');
    await expect(heading).toBeVisible();

    // Verify text contains 'MirDB'
    await expect(heading).toContainText('MirDB');
  });

  test('TC3: Value proposition tagline is visible', async ({ page }) => {
    // Find the tagline element
    const tagline = page.locator('.hero-tagline');
    await expect(tagline).toBeVisible();

    // Verify it contains 'Persistent Key-Value Store'
    const taglineText = await tagline.textContent();
    expect(taglineText).toContain('Persistent Key-Value Store');

    // Verify it contains 'Memcached protocol'
    expect(taglineText).toContain('Memcached protocol');
  });

  test('TC4: CTA buttons Get Started and View on GitHub are present', async ({ page }) => {
    // Find the CTA container
    const ctaContainer = page.locator('.hero-cta');
    await expect(ctaContainer).toBeVisible();

    // Verify 'Get Started' button exists
    const getStartedBtn = page.locator('.hero-cta .btn:has-text("Get Started")');
    await expect(getStartedBtn).toBeVisible();
    const getStartedHref = await getStartedBtn.getAttribute('href');
    expect(getStartedHref).toBe('#getting-started');

    // Verify 'View on GitHub' button exists
    const githubBtn = page.locator('.hero-cta .btn:has-text("View on GitHub")');
    await expect(githubBtn).toBeVisible();
    const githubHref = await githubBtn.getAttribute('href');
    expect(githubHref).toBe('https://github.com/yetone/mirdb');
  });

  test('Hero section is fully visible and accessible', async ({ page }) => {
    // Verify hero section exists
    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    // Verify all main hero elements are in the viewport
    const heroBox = await hero.boundingBox();
    expect(heroBox).toBeTruthy();
    expect(heroBox!.height).toBeGreaterThan(200);
  });
});
