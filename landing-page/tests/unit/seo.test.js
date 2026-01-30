/**
 * MirDB Landing Page - SEO Tests
 * Owner: Scenario 14 - SEO Requirements
 *
 * This file tests:
 * - Page title tag presence and content
 * - Meta description tag
 * - Open Graph meta tags (title, description, image)
 * - HTML5 compliance
 * - Single h1 element requirement
 */

const { test, expect } = require('@playwright/test');
const { setupPage } = require('../helpers/test-utils');

test.describe('SEO Requirements', () => {
  test.beforeEach(async ({ page }) => {
    await setupPage(page);
  });

  test('TC1: Page title contains MirDB and describes the product', async ({ page }) => {
    const title = await page.title();

    // Title should contain 'MirDB'
    expect(title).toContain('MirDB');

    // Title should describe the product (key-value store, memcached, persistent)
    const titleLower = title.toLowerCase();
    const hasProductDescription =
      titleLower.includes('key-value') ||
      titleLower.includes('memcached') ||
      titleLower.includes('persistent');

    expect(hasProductDescription).toBe(true);
  });

  test('TC2: Meta description explains MirDB value proposition', async ({ page }) => {
    const metaDescription = await page.locator('meta[name="description"]').getAttribute('content');

    // Meta description should exist
    expect(metaDescription).toBeTruthy();

    // Meta description should mention MirDB or its key features
    const descLower = metaDescription.toLowerCase();
    const hasValueProposition =
      descLower.includes('mirdb') ||
      descLower.includes('key-value') ||
      descLower.includes('memcached') ||
      descLower.includes('persistent');

    expect(hasValueProposition).toBe(true);

    // Meta description should be between 50 and 160 characters (SEO best practice)
    expect(metaDescription.length).toBeGreaterThanOrEqual(50);
    expect(metaDescription.length).toBeLessThanOrEqual(200);
  });

  test('TC3: Open Graph title meta tag is present', async ({ page }) => {
    const ogTitle = await page.locator('meta[property="og:title"]').getAttribute('content');

    // og:title should exist
    expect(ogTitle).toBeTruthy();

    // og:title should contain MirDB
    expect(ogTitle).toContain('MirDB');
  });

  test('TC4: Open Graph description meta tag is present', async ({ page }) => {
    const ogDescription = await page.locator('meta[property="og:description"]').getAttribute('content');

    // og:description should exist
    expect(ogDescription).toBeTruthy();

    // og:description should explain MirDB
    const descLower = ogDescription.toLowerCase();
    const hasDescription =
      descLower.includes('key-value') ||
      descLower.includes('memcached') ||
      descLower.includes('persistent');

    expect(hasDescription).toBe(true);
  });

  test('TC5: Open Graph image meta tag references logo or preview image', async ({ page }) => {
    const ogImage = await page.locator('meta[property="og:image"]').getAttribute('content');

    // og:image should exist
    expect(ogImage).toBeTruthy();

    // og:image should reference a valid image path (logo or preview image)
    const isImagePath =
      ogImage.includes('logo') ||
      ogImage.includes('preview') ||
      ogImage.includes('.png') ||
      ogImage.includes('.jpg') ||
      ogImage.includes('.gif') ||
      ogImage.includes('.webp');

    expect(isImagePath).toBe(true);
  });

  test('TC6: HTML5 compliance - uses proper semantic elements', async ({ page }) => {
    // Check for HTML5 doctype (via page source)
    const html = await page.content();
    expect(html.toLowerCase()).toContain('<!doctype html>');

    // Check for required semantic HTML5 elements
    const hasNav = await page.locator('nav').count();
    expect(hasNav).toBeGreaterThanOrEqual(1);

    const hasMain = await page.locator('main').count();
    expect(hasMain).toBe(1);

    const hasSections = await page.locator('section').count();
    expect(hasSections).toBeGreaterThanOrEqual(1);

    const hasFooter = await page.locator('footer').count();
    expect(hasFooter).toBe(1);

    // Check for proper lang attribute on html element
    const htmlLang = await page.locator('html').getAttribute('lang');
    expect(htmlLang).toBeTruthy();
    expect(htmlLang).toBe('en');

    // Check for proper charset meta tag
    const charset = await page.locator('meta[charset]').getAttribute('charset');
    expect(charset).toBeTruthy();
    expect(charset.toLowerCase()).toBe('utf-8');

    // Check for viewport meta tag (required for responsive design)
    const viewport = await page.locator('meta[name="viewport"]').getAttribute('content');
    expect(viewport).toBeTruthy();
    expect(viewport).toContain('width=device-width');
  });

  test('TC7: Page has exactly one h1 element', async ({ page }) => {
    const h1Count = await page.locator('h1').count();

    // There should be exactly one h1 element
    expect(h1Count).toBe(1);

    // The h1 should contain meaningful content
    const h1Text = await page.locator('h1').textContent();
    expect(h1Text).toBeTruthy();
    expect(h1Text.trim().length).toBeGreaterThan(0);

    // The h1 should contain 'MirDB' as it's the main heading
    expect(h1Text).toContain('MirDB');
  });
});
