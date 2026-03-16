/**
 * SEO and Meta Tags Tests
 * Owner: Scenario 13 - SEO and Meta Tags
 *
 * Test cases:
 * - Lighthouse SEO score >= 90
 * - Title tag with 'MirDB'
 * - Meta description exists
 * - Viewport meta tag
 * - Open Graph tags present
 * - Canonical URL
 * - Page is indexable
 */

import { test, expect, chromium, Browser, Page } from '@playwright/test';
import { playAudit } from 'playwright-lighthouse';

const LIGHTHOUSE_PORT = 9222;

test.describe('SEO and Meta Tags', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Lighthouse SEO audit score is 90 or higher', async () => {
    // Launch browser with remote debugging for Lighthouse
    const browser: Browser = await chromium.launch({
      args: [`--remote-debugging-port=${LIGHTHOUSE_PORT}`],
    });
    const page: Page = await browser.newPage();

    try {
      await page.goto('http://localhost:8080/');

      // Run Lighthouse SEO audit
      const result = await playAudit({
        page,
        thresholds: {
          seo: 90,
        },
        port: LIGHTHOUSE_PORT,
        reports: {
          formats: {
            html: false,
            json: false,
          },
        },
      });

      // Verify SEO score meets threshold
      const seoScore = result.lhr.categories.seo.score * 100;
      expect(seoScore).toBeGreaterThanOrEqual(90);
    } finally {
      await browser.close();
    }
  });

  test('TC2: Page title contains MirDB and is under 60 characters', async ({ page }) => {
    // Get the page title
    const title = await page.title();

    // Verify title contains 'MirDB'
    expect(title).toContain('MirDB');

    // Verify title is under 60 characters for optimal SEO
    expect(title.length).toBeLessThan(60);
  });

  test('TC3: Meta description exists, is descriptive, and under 160 characters', async ({ page }) => {
    // Get meta description
    const metaDescription = await page.locator('meta[name="description"]').getAttribute('content');

    // Verify meta description exists
    expect(metaDescription).toBeTruthy();

    // Verify meta description has content (is descriptive)
    expect(metaDescription!.length).toBeGreaterThan(50);

    // Verify meta description is under 160 characters for optimal SEO
    expect(metaDescription!.length).toBeLessThanOrEqual(160);

    // Verify it mentions key product terms
    expect(metaDescription!.toLowerCase()).toContain('mirdb');
  });

  test('TC4: Viewport meta tag with proper settings exists', async ({ page }) => {
    // Get viewport meta tag
    const viewport = await page.locator('meta[name="viewport"]').getAttribute('content');

    // Verify viewport exists
    expect(viewport).toBeTruthy();

    // Verify viewport contains width=device-width
    expect(viewport).toContain('width=device-width');

    // Verify viewport contains initial-scale=1
    expect(viewport).toMatch(/initial-scale\s*=\s*1/);
  });

  test('TC5: Open Graph tags are present (og:title, og:description, og:image)', async ({ page }) => {
    // Check og:title
    const ogTitle = await page.locator('meta[property="og:title"]').getAttribute('content');
    expect(ogTitle).toBeTruthy();
    expect(ogTitle).toContain('MirDB');

    // Check og:description
    const ogDescription = await page.locator('meta[property="og:description"]').getAttribute('content');
    expect(ogDescription).toBeTruthy();
    expect(ogDescription!.length).toBeGreaterThan(0);

    // Check og:image
    const ogImage = await page.locator('meta[property="og:image"]').getAttribute('content');
    expect(ogImage).toBeTruthy();
    expect(ogImage).toMatch(/\.(png|jpg|jpeg|gif|webp)$/i);
  });

  test('TC6: Canonical link tag is present', async ({ page }) => {
    // Get canonical link
    const canonical = await page.locator('link[rel="canonical"]').getAttribute('href');

    // Verify canonical exists
    expect(canonical).toBeTruthy();

    // Verify canonical is a valid URL
    expect(canonical).toMatch(/^https?:\/\//);
  });

  test('TC7: Page is indexable (no noindex directive)', async ({ page }) => {
    // Get robots meta tag
    const robots = await page.locator('meta[name="robots"]').getAttribute('content');

    // If robots meta tag exists, verify it doesn't contain noindex
    if (robots) {
      expect(robots.toLowerCase()).not.toContain('noindex');
    }

    // Alternatively, check for X-Robots-Tag header (optional)
    // If no robots meta tag, page is indexable by default - that's fine

    // Verify page doesn't have a noindex directive anywhere
    const noindexMeta = await page.locator('meta[name="robots"][content*="noindex"]').count();
    expect(noindexMeta).toBe(0);
  });

  // Additional SEO best practices tests
  test('Additional: og:type meta tag is present', async ({ page }) => {
    const ogType = await page.locator('meta[property="og:type"]').getAttribute('content');
    expect(ogType).toBeTruthy();
  });

  test('Additional: og:url meta tag is present', async ({ page }) => {
    const ogUrl = await page.locator('meta[property="og:url"]').getAttribute('content');
    expect(ogUrl).toBeTruthy();
    expect(ogUrl).toMatch(/^https?:\/\//);
  });

  test('Additional: charset meta tag is present', async ({ page }) => {
    const charset = await page.locator('meta[charset]').getAttribute('charset');
    expect(charset?.toLowerCase()).toBe('utf-8');
  });

  test('Additional: html lang attribute is set', async ({ page }) => {
    const lang = await page.locator('html').getAttribute('lang');
    expect(lang).toBeTruthy();
    expect(lang).toBe('en');
  });
});
