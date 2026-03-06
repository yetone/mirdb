/**
 * SEO E2E Tests
 * Owner: Scenario 13 - SEO Best Practices
 *
 * Test coverage:
 * - Title element
 * - Meta description
 * - Single H1
 * - Viewport meta tag
 * - Canonical URL
 * - Open Graph tags
 */

import { test, expect } from '@playwright/test';

test.describe('SEO Best Practices', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('page has title element containing MirDB', async ({ page }) => {
    // Test Case 1: Check page has title element
    const title = await page.title();
    expect(title).toContain('MirDB');
    expect(title.length).toBeGreaterThan(0);
    expect(title.length).toBeLessThanOrEqual(60); // SEO best practice: titles under 60 chars
  });

  test('meta description exists with appropriate content', async ({ page }) => {
    // Test Case 2: Check meta description exists
    const metaDescription = page.locator('meta[name="description"]');
    await expect(metaDescription).toHaveCount(1);

    const content = await metaDescription.getAttribute('content');
    expect(content).toBeTruthy();
    expect(content).toContain('MirDB');
    expect(content!.length).toBeGreaterThan(50); // Should be descriptive
    expect(content!.length).toBeLessThanOrEqual(160); // SEO best practice: under 160 chars
  });

  test('page has exactly one H1 element', async ({ page }) => {
    // Test Case 3: Verify single H1 on page
    const h1Elements = page.locator('h1');
    await expect(h1Elements).toHaveCount(1);

    // Verify H1 contains meaningful content
    const h1Text = await h1Elements.textContent();
    expect(h1Text?.trim()).toBeTruthy();
  });

  test('viewport meta tag exists for mobile responsiveness', async ({ page }) => {
    // Test Case 4: Check viewport meta tag
    const viewportMeta = page.locator('meta[name="viewport"]');
    await expect(viewportMeta).toHaveCount(1);

    const content = await viewportMeta.getAttribute('content');
    expect(content).toContain('width=device-width');
    expect(content).toContain('initial-scale=1');
  });

  test('canonical URL link tag exists', async ({ page }) => {
    // Test Case 5: Verify canonical URL is set
    const canonicalLink = page.locator('link[rel="canonical"]');
    await expect(canonicalLink).toHaveCount(1);

    const href = await canonicalLink.getAttribute('href');
    expect(href).toBeTruthy();
    // Canonical should be a valid URL
    expect(href).toMatch(/^https?:\/\/.+/);
  });

  test('Open Graph tags exist for social sharing', async ({ page }) => {
    // Test Case 6: Check Open Graph tags for social sharing

    // OG Title
    const ogTitle = page.locator('meta[property="og:title"]');
    await expect(ogTitle).toHaveCount(1);
    const ogTitleContent = await ogTitle.getAttribute('content');
    expect(ogTitleContent).toBeTruthy();
    expect(ogTitleContent).toContain('MirDB');

    // OG Description
    const ogDescription = page.locator('meta[property="og:description"]');
    await expect(ogDescription).toHaveCount(1);
    const ogDescContent = await ogDescription.getAttribute('content');
    expect(ogDescContent).toBeTruthy();

    // OG Image (recommended for social sharing)
    const ogImage = page.locator('meta[property="og:image"]');
    await expect(ogImage).toHaveCount(1);
    const ogImageContent = await ogImage.getAttribute('content');
    expect(ogImageContent).toBeTruthy();

    // OG Type (optional but good practice)
    const ogType = page.locator('meta[property="og:type"]');
    await expect(ogType).toHaveCount(1);

    // OG URL (optional but good practice)
    const ogUrl = page.locator('meta[property="og:url"]');
    await expect(ogUrl).toHaveCount(1);
  });

  test('charset meta tag is present', async ({ page }) => {
    // Additional SEO check: charset should be defined
    const charsetMeta = page.locator('meta[charset]');
    await expect(charsetMeta).toHaveCount(1);

    const charset = await charsetMeta.getAttribute('charset');
    expect(charset?.toLowerCase()).toBe('utf-8');
  });

  test('html lang attribute is set', async ({ page }) => {
    // Additional SEO check: language should be defined
    const htmlLang = await page.locator('html').getAttribute('lang');
    expect(htmlLang).toBeTruthy();
    expect(htmlLang).toBe('en');
  });
});
