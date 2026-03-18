/**
 * SEO and Performance E2E Tests.
 * Owner: Scenario 8 - Performance and SEO
 *
 * Tests:
 * - Page load time
 * - Title tag
 * - Meta description
 * - Open Graph tags
 * - Semantic HTML
 * - Heading hierarchy
 */

import { test, expect } from '@playwright/test';

test.describe('SEO and Performance', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Page loads within 2000ms (DOMContentLoaded)', async ({ page }) => {
    // Navigate to a fresh page and measure performance
    const startTime = Date.now();

    // Create a new page context and navigate
    await page.goto('/', { waitUntil: 'domcontentloaded' });

    const loadTime = Date.now() - startTime;

    // Verify DOMContentLoaded fires within 2000ms
    expect(loadTime).toBeLessThan(2000);
  });

  test('TC2: Title contains MirDB and describes the product', async ({ page }) => {
    // Query for title element content
    const title = await page.title();

    // Verify title contains 'MirDB'
    expect(title).toContain('MirDB');

    // Verify title describes the product (should mention key-value store or similar)
    expect(title.toLowerCase()).toMatch(/key-value|store|database|persistent/i);
  });

  test('TC3: Meta description exists with meaningful content about MirDB', async ({ page }) => {
    // Query for meta description tag
    const metaDescription = await page.locator('meta[name="description"]').getAttribute('content');

    // Verify meta description exists
    expect(metaDescription).toBeTruthy();

    // Verify it has meaningful content about MirDB
    expect(metaDescription?.toLowerCase()).toMatch(/mirdb|key-value|memcached|persistent/i);

    // Verify description has reasonable length (50-160 chars recommended for SEO)
    expect(metaDescription?.length).toBeGreaterThan(50);
    expect(metaDescription?.length).toBeLessThan(200);
  });

  test('TC4: Open Graph meta tags exist (og:title, og:description, og:image)', async ({ page }) => {
    // Query for og:title
    const ogTitle = await page.locator('meta[property="og:title"]').getAttribute('content');
    expect(ogTitle).toBeTruthy();
    expect(ogTitle).toContain('MirDB');

    // Query for og:description
    const ogDescription = await page.locator('meta[property="og:description"]').getAttribute('content');
    expect(ogDescription).toBeTruthy();
    expect(ogDescription?.length).toBeGreaterThan(20);

    // Query for og:image
    const ogImage = await page.locator('meta[property="og:image"]').getAttribute('content');
    expect(ogImage).toBeTruthy();
    expect(ogImage).toMatch(/\.(svg|png|jpg|jpeg|webp)$/i);
  });

  test('TC5: Page uses semantic HTML elements (header, main, section)', async ({ page }) => {
    // Check for header element
    const header = page.locator('header');
    await expect(header.first()).toBeVisible();

    // Check for main element
    const main = page.locator('main');
    await expect(main).toBeVisible();

    // Check for section elements (should have multiple sections)
    const sections = page.locator('section');
    const sectionCount = await sections.count();
    expect(sectionCount).toBeGreaterThanOrEqual(3);

    // Verify sections have meaningful IDs or aria-labels for accessibility
    for (let i = 0; i < Math.min(sectionCount, 3); i++) {
      const section = sections.nth(i);
      const hasId = await section.getAttribute('id');
      const hasAriaLabel = await section.getAttribute('aria-label');
      const hasAriaLabelledby = await section.getAttribute('aria-labelledby');

      // Section should have either id, aria-label, or aria-labelledby
      expect(hasId || hasAriaLabel || hasAriaLabelledby).toBeTruthy();
    }
  });

  test('TC6: Single h1 and proper heading hierarchy (h1 > h2 > h3)', async ({ page }) => {
    // Verify exactly one h1
    const h1Elements = page.locator('h1');
    const h1Count = await h1Elements.count();
    expect(h1Count).toBe(1);

    // Get h1 text content
    const h1Text = await h1Elements.first().textContent();
    expect(h1Text).toContain('MirDB');

    // Check heading hierarchy - collect all headings
    const allHeadings = await page.evaluate(() => {
      const headings: { level: number; text: string }[] = [];
      document.querySelectorAll('h1, h2, h3, h4, h5, h6').forEach((h) => {
        const level = parseInt(h.tagName.substring(1));
        headings.push({ level, text: h.textContent?.trim() || '' });
      });
      return headings;
    });

    // Verify we have h2 elements following h1
    const h2Elements = allHeadings.filter(h => h.level === 2);
    expect(h2Elements.length).toBeGreaterThanOrEqual(2);

    // Verify no heading levels are skipped (e.g., h1 directly to h3 without h2)
    // Check that the heading sequence doesn't skip more than one level
    for (let i = 1; i < allHeadings.length; i++) {
      const prevLevel = allHeadings[i - 1].level;
      const currLevel = allHeadings[i].level;

      // When going deeper (higher number), shouldn't skip levels
      if (currLevel > prevLevel) {
        expect(currLevel - prevLevel).toBeLessThanOrEqual(1);
      }
    }
  });

  test('Semantic structure includes all major page regions', async ({ page }) => {
    // Verify the page has proper landmark structure for accessibility/SEO

    // Header region (hero section uses header tag)
    const headerRegion = page.locator('header');
    expect(await headerRegion.count()).toBeGreaterThanOrEqual(1);

    // Main content region
    const mainRegion = page.locator('main');
    expect(await mainRegion.count()).toBe(1);

    // Multiple content sections within main
    const sectionsInMain = page.locator('main section');
    expect(await sectionsInMain.count()).toBeGreaterThanOrEqual(2);
  });

  test('Page has correct document language attribute', async ({ page }) => {
    // Verify html lang attribute is set for SEO and accessibility
    const htmlLang = await page.locator('html').getAttribute('lang');
    expect(htmlLang).toBe('en');
  });

  test('Meta viewport is properly configured for mobile', async ({ page }) => {
    // Verify viewport meta tag for responsive design
    const viewport = await page.locator('meta[name="viewport"]').getAttribute('content');
    expect(viewport).toBeTruthy();
    expect(viewport).toContain('width=device-width');
    expect(viewport).toContain('initial-scale=1');
  });

  test('Favicon is configured', async ({ page }) => {
    // Verify favicon link is present
    const favicon = page.locator('link[rel="icon"]');
    const faviconHref = await favicon.getAttribute('href');
    expect(faviconHref).toBeTruthy();
  });
});
