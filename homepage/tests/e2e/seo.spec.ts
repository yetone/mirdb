/**
 * SEO E2E Tests
 * Owners: Scenarios 16, 17 (SEO), Scenario 24 (Static Hosting)
 *
 * Test groups:
 * - Meta tags presence
 * - Title tag content
 * - Open Graph tags
 * - Semantic HTML structure
 * - Heading hierarchy for SEO
 * - Relative asset paths
 * - Static hosting compatibility
 */

import { test, expect } from '@playwright/test';
import { waitForLoad } from './utils';

test.describe('SEO - Meta Tags (Scenario 16)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForLoad(page);
  });

  test('Test Case 1: Title tag exists and contains MirDB', async ({ page }) => {
    const title = await page.title();
    expect(title).toBeTruthy();
    expect(title.toLowerCase()).toContain('mirdb');
  });

  test('Test Case 2: Meta description exists and is 50-160 characters', async ({ page }) => {
    const description = await page.locator('meta[name="description"]').getAttribute('content');
    expect(description).toBeTruthy();
    expect(description!.length).toBeGreaterThanOrEqual(50);
    expect(description!.length).toBeLessThanOrEqual(160);
  });

  test('Test Case 3: Viewport meta tag present with width=device-width', async ({ page }) => {
    const viewport = await page.locator('meta[name="viewport"]').getAttribute('content');
    expect(viewport).toBeTruthy();
    expect(viewport).toContain('width=device-width');
  });

  test('Test Case 4: Open Graph tags present (og:title and og:description)', async ({ page }) => {
    const ogTitle = await page.locator('meta[property="og:title"]').getAttribute('content');
    const ogDescription = await page.locator('meta[property="og:description"]').getAttribute('content');

    expect(ogTitle).toBeTruthy();
    expect(ogDescription).toBeTruthy();
  });

  test('Test Case 5: Favicon link tag present in document head', async ({ page }) => {
    const favicon = page.locator('link[rel="icon"], link[rel="shortcut icon"]');
    await expect(favicon).toHaveCount(1);
    const href = await favicon.getAttribute('href');
    expect(href).toBeTruthy();
  });
});
