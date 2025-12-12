import { test, expect } from '@playwright/test';
import * as fs from 'fs';
import * as path from 'path';

// These tests verify SEO meta tags are present in the built HTML output
// They work by reading the static build output directly when browser tests cannot run

test.describe('SEO Basics', () => {
  let htmlContent: string;

  test.beforeAll(async () => {
    // Read the built HTML file directly
    const distPath = path.join(__dirname, '..', 'dist', 'index.html');
    if (fs.existsSync(distPath)) {
      htmlContent = fs.readFileSync(distPath, 'utf-8');
    }
  });

  test('page title contains MirDB and relevant keywords', async ({ page }) => {
    // Test Case 1: Check page title tag
    let title: string;

    try {
      // Try browser-based test first
      await page.goto('/');
      title = await page.title();
    } catch {
      // Fall back to static HTML analysis if browser unavailable
      const titleMatch = htmlContent.match(/<title>([^<]+)<\/title>/i);
      title = titleMatch ? titleMatch[1] : '';
    }

    // Title should contain 'MirDB'
    expect(title.toLowerCase()).toContain('mirdb');

    // Title should contain relevant keywords about the product
    const titleLower = title.toLowerCase();
    const hasRelevantKeywords =
      titleLower.includes('key-value') ||
      titleLower.includes('memcached') ||
      titleLower.includes('persistent') ||
      titleLower.includes('store');

    expect(hasRelevantKeywords).toBe(true);
  });

  test('meta description is present and describes MirDB', async ({ page }) => {
    // Test Case 2: Check meta description
    let metaDescription: string | null;

    try {
      // Try browser-based test first
      await page.goto('/');
      metaDescription = await page.locator('meta[name="description"]').getAttribute('content');
    } catch {
      // Fall back to static HTML analysis if browser unavailable
      const metaMatch = htmlContent.match(/<meta\s+name="description"\s+content="([^"]+)"/i);
      metaDescription = metaMatch ? metaMatch[1] : null;
    }

    // Meta description should exist
    expect(metaDescription).toBeTruthy();

    // Meta description should mention MirDB or describe the product
    const descLower = metaDescription!.toLowerCase();
    const describesMirDB =
      descLower.includes('mirdb') ||
      descLower.includes('key-value') ||
      descLower.includes('memcached') ||
      descLower.includes('persistent');

    expect(describesMirDB).toBe(true);

    // Meta description should be a reasonable length (50-160 characters is optimal for SEO)
    expect(metaDescription!.length).toBeGreaterThan(30);
    expect(metaDescription!.length).toBeLessThan(200);
  });

  test('Open Graph meta tags are present', async ({ page }) => {
    // Test Case 3: Check Open Graph meta tags
    let ogTitle: string | null;
    let ogDescription: string | null;
    let ogType: string | null;

    try {
      // Try browser-based test first
      await page.goto('/');
      ogTitle = await page.locator('meta[property="og:title"]').getAttribute('content');
      ogDescription = await page.locator('meta[property="og:description"]').getAttribute('content');
      ogType = await page.locator('meta[property="og:type"]').getAttribute('content');
    } catch {
      // Fall back to static HTML analysis if browser unavailable
      const ogTitleMatch = htmlContent.match(/<meta\s+property="og:title"\s+content="([^"]+)"/i);
      const ogDescMatch = htmlContent.match(/<meta\s+property="og:description"\s+content="([^"]+)"/i);
      const ogTypeMatch = htmlContent.match(/<meta\s+property="og:type"\s+content="([^"]+)"/i);

      ogTitle = ogTitleMatch ? ogTitleMatch[1] : null;
      ogDescription = ogDescMatch ? ogDescMatch[1] : null;
      ogType = ogTypeMatch ? ogTypeMatch[1] : null;
    }

    // og:title should be present
    expect(ogTitle).toBeTruthy();
    expect(ogTitle!.length).toBeGreaterThan(0);

    // og:description should be present
    expect(ogDescription).toBeTruthy();
    expect(ogDescription!.length).toBeGreaterThan(0);

    // og:type should be present
    expect(ogType).toBeTruthy();
    expect(ogType!.length).toBeGreaterThan(0);
  });
});
