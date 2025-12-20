// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

test.describe('SEO and Meta Tags (Scenario 10)', () => {
  test.beforeEach(async ({ page }) => {
    const filePath = path.resolve(__dirname, '../index.html');
    await page.goto(`file://${filePath}`);
  });

  test.describe('Test Case 1: Page Title Element', () => {
    test('should have a title tag containing "MirDB" keyword', async ({ page }) => {
      const title = await page.title();
      expect(title).toBeTruthy();
      expect(title.toLowerCase()).toContain('mirdb');
    });

    test('should have a descriptive title with key value proposition', async ({ page }) => {
      const title = await page.title();
      // Title should be meaningful and not just "MirDB"
      expect(title.length).toBeGreaterThan(10);
      // Should contain relevant keywords
      expect(title.toLowerCase()).toMatch(/mirdb/);
    });
  });

  test.describe('Test Case 2: Meta Description Tag', () => {
    test('should have a meta description tag', async ({ page }) => {
      const metaDescription = await page.locator('meta[name="description"]');
      await expect(metaDescription).toHaveCount(1);
    });

    test('should have meta description with 50-160 characters', async ({ page }) => {
      const metaDescription = await page.locator('meta[name="description"]');
      const content = await metaDescription.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content.length).toBeGreaterThanOrEqual(50);
      expect(content.length).toBeLessThanOrEqual(160);
    });

    test('should have meta description with compelling content about MirDB', async ({ page }) => {
      const metaDescription = await page.locator('meta[name="description"]');
      const content = await metaDescription.getAttribute('content');
      // Should mention MirDB or key features
      expect(content.toLowerCase()).toMatch(/mirdb|key-value|memcached|rust|persistent/);
    });
  });

  test.describe('Test Case 3: Semantic HTML Elements', () => {
    test('should use header semantic element', async ({ page }) => {
      const header = await page.locator('header');
      await expect(header).toHaveCount(1);
      await expect(header).toBeVisible();
    });

    test('should use main semantic element', async ({ page }) => {
      const main = await page.locator('main');
      await expect(main).toHaveCount(1);
    });

    test('should use footer semantic element', async ({ page }) => {
      const footer = await page.locator('footer');
      await expect(footer).toHaveCount(1);
      await expect(footer).toBeVisible();
    });

    test('should have proper heading hierarchy', async ({ page }) => {
      // Should have exactly one h1
      const h1Elements = await page.locator('h1');
      await expect(h1Elements).toHaveCount(1);

      // H1 should contain MirDB
      const h1Text = await h1Elements.first().textContent();
      expect(h1Text).toContain('MirDB');
    });

    test('should use section elements for content organization', async ({ page }) => {
      const sections = await page.locator('section');
      const sectionCount = await sections.count();
      expect(sectionCount).toBeGreaterThanOrEqual(3); // At least hero, features, getting started
    });
  });

  test.describe('Test Case 4: Open Graph Meta Tags', () => {
    test('should have og:title meta tag', async ({ page }) => {
      const ogTitle = await page.locator('meta[property="og:title"]');
      await expect(ogTitle).toHaveCount(1);
      const content = await ogTitle.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content.toLowerCase()).toContain('mirdb');
    });

    test('should have og:description meta tag', async ({ page }) => {
      const ogDescription = await page.locator('meta[property="og:description"]');
      await expect(ogDescription).toHaveCount(1);
      const content = await ogDescription.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content.length).toBeGreaterThan(20);
    });

    test('should have og:type meta tag', async ({ page }) => {
      const ogType = await page.locator('meta[property="og:type"]');
      await expect(ogType).toHaveCount(1);
      const content = await ogType.getAttribute('content');
      expect(content).toBe('website');
    });

    test('should have og:url meta tag', async ({ page }) => {
      const ogUrl = await page.locator('meta[property="og:url"]');
      await expect(ogUrl).toHaveCount(1);
      const content = await ogUrl.getAttribute('content');
      expect(content).toBeTruthy();
    });
  });

  test.describe('Test Case 5: Canonical URL Tag', () => {
    test('should have canonical link element', async ({ page }) => {
      const canonical = await page.locator('link[rel="canonical"]');
      await expect(canonical).toHaveCount(1);
    });

    test('should have valid href in canonical link', async ({ page }) => {
      const canonical = await page.locator('link[rel="canonical"]');
      const href = await canonical.getAttribute('href');
      expect(href).toBeTruthy();
      // Should be a valid URL format
      expect(href).toMatch(/^https?:\/\//);
    });
  });

  test.describe('Additional SEO Best Practices', () => {
    test('should have lang attribute on html element', async ({ page }) => {
      const htmlLang = await page.locator('html').getAttribute('lang');
      expect(htmlLang).toBe('en');
    });

    test('should have charset meta tag', async ({ page }) => {
      const charset = await page.locator('meta[charset]');
      await expect(charset).toHaveCount(1);
      const charsetValue = await charset.getAttribute('charset');
      expect(charsetValue.toLowerCase()).toBe('utf-8');
    });

    test('should have viewport meta tag for mobile responsiveness', async ({ page }) => {
      const viewport = await page.locator('meta[name="viewport"]');
      await expect(viewport).toHaveCount(1);
      const content = await viewport.getAttribute('content');
      expect(content).toContain('width=device-width');
    });
  });
});
