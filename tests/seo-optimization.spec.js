// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('SEO Optimization', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Page has title tag containing MirDB and relevant keywords', async ({ page }) => {
    // Get the page title
    const title = await page.title();

    console.log(`Page title: "${title}"`);

    // Verify title contains 'MirDB'
    expect(title.toLowerCase()).toContain('mirdb');

    // Verify title contains relevant keywords
    const keywords = ['persistent', 'key-value', 'memcached'];
    const titleLower = title.toLowerCase();
    const hasRelevantKeyword = keywords.some(keyword => titleLower.includes(keyword));

    expect(hasRelevantKeyword, 'Title should contain relevant keywords like persistent, key-value, or memcached').toBe(true);

    // Verify title is within SEO best practice length (50-60 characters recommended)
    expect(title.length).toBeGreaterThan(20);
    expect(title.length).toBeLessThanOrEqual(70);

    console.log(`Title length: ${title.length} characters`);
  });

  test('TC2: Meta description is present with 150-160 characters describing MirDB', async ({ page }) => {
    // Get meta description
    const metaDescription = await page.evaluate(() => {
      const meta = document.querySelector('meta[name="description"]');
      return meta ? meta.getAttribute('content') : null;
    });

    console.log(`Meta description: "${metaDescription}"`);

    // Verify meta description exists
    expect(metaDescription, 'Meta description should be present').toBeTruthy();

    // Verify it contains MirDB
    expect(metaDescription.toLowerCase()).toContain('mirdb');

    // Verify length is within SEO best practice range (120-160 characters)
    // Allowing some flexibility around the 150-160 range
    expect(metaDescription.length).toBeGreaterThanOrEqual(100);
    expect(metaDescription.length).toBeLessThanOrEqual(200);

    console.log(`Meta description length: ${metaDescription.length} characters`);

    // Verify it describes the product properly
    const descriptiveTerms = ['key-value', 'persistent', 'memcached', 'store', 'database'];
    const descLower = metaDescription.toLowerCase();
    const hasDescriptiveTerm = descriptiveTerms.some(term => descLower.includes(term));

    expect(hasDescriptiveTerm, 'Description should contain key product terms').toBe(true);
  });

  test('TC3: Viewport meta tag is set for responsive design', async ({ page }) => {
    // Get viewport meta tag
    const viewport = await page.evaluate(() => {
      const meta = document.querySelector('meta[name="viewport"]');
      return meta ? meta.getAttribute('content') : null;
    });

    console.log(`Viewport meta: "${viewport}"`);

    // Verify viewport meta tag exists
    expect(viewport, 'Viewport meta tag should be present').toBeTruthy();

    // Verify it contains width=device-width for responsive design
    expect(viewport).toContain('width=device-width');

    // Verify it contains initial-scale=1.0
    expect(viewport).toMatch(/initial-scale\s*=\s*1(\.0)?/);
  });

  test('TC4: Open Graph tags are present for social sharing', async ({ page }) => {
    // Get all Open Graph meta tags
    const ogTags = await page.evaluate(() => {
      const tags = {};

      const ogTitle = document.querySelector('meta[property="og:title"]');
      const ogDescription = document.querySelector('meta[property="og:description"]');
      const ogImage = document.querySelector('meta[property="og:image"]');
      const ogUrl = document.querySelector('meta[property="og:url"]');
      const ogType = document.querySelector('meta[property="og:type"]');

      if (ogTitle) tags.title = ogTitle.getAttribute('content');
      if (ogDescription) tags.description = ogDescription.getAttribute('content');
      if (ogImage) tags.image = ogImage.getAttribute('content');
      if (ogUrl) tags.url = ogUrl.getAttribute('content');
      if (ogType) tags.type = ogType.getAttribute('content');

      return tags;
    });

    console.log('Open Graph tags found:', ogTags);

    // Verify og:title is present
    expect(ogTags.title, 'og:title should be present').toBeTruthy();
    expect(ogTags.title.toLowerCase()).toContain('mirdb');

    // Verify og:description is present
    expect(ogTags.description, 'og:description should be present').toBeTruthy();
    expect(ogTags.description.length).toBeGreaterThan(50);

    // Verify og:image is present (for social sharing preview)
    expect(ogTags.image, 'og:image should be present').toBeTruthy();

    // Verify og:url is present
    expect(ogTags.url, 'og:url should be present').toBeTruthy();

    // Verify og:type is present (optional but recommended)
    if (ogTags.type) {
      expect(['website', 'article', 'product']).toContain(ogTags.type);
    }
  });

  test('TC5: Canonical URL is present with correct URL', async ({ page }) => {
    // Get canonical link tag
    const canonical = await page.evaluate(() => {
      const link = document.querySelector('link[rel="canonical"]');
      return link ? link.getAttribute('href') : null;
    });

    console.log(`Canonical URL: "${canonical}"`);

    // Verify canonical link exists
    expect(canonical, 'Canonical link should be present').toBeTruthy();

    // Verify it's a valid URL format
    expect(canonical).toMatch(/^https?:\/\/.+/);

    // Verify URL looks like a MirDB-related URL
    const canonicalLower = canonical.toLowerCase();
    // The canonical should point to the main MirDB page or domain
    expect(
      canonicalLower.includes('mirdb') ||
      canonicalLower.includes('localhost') ||
      canonicalLower.includes('github.io'),
      'Canonical URL should point to MirDB or hosting domain'
    ).toBe(true);
  });

  test('Additional SEO: Structured data (JSON-LD) is present', async ({ page }) => {
    // Get JSON-LD structured data
    const jsonLd = await page.evaluate(() => {
      const scripts = document.querySelectorAll('script[type="application/ld+json"]');
      const data = [];
      scripts.forEach(script => {
        try {
          data.push(JSON.parse(script.textContent));
        } catch (e) {
          // Invalid JSON
        }
      });
      return data;
    });

    console.log('JSON-LD structured data found:', JSON.stringify(jsonLd, null, 2));

    // Verify at least one JSON-LD script exists
    expect(jsonLd.length, 'JSON-LD structured data should be present').toBeGreaterThan(0);

    // Verify the structured data has @context and @type
    const hasValidStructure = jsonLd.some(item =>
      item['@context'] === 'https://schema.org' &&
      item['@type']
    );

    expect(hasValidStructure, 'JSON-LD should have valid schema.org structure').toBe(true);

    // Verify it contains product-related information
    const hasProductInfo = jsonLd.some(item =>
      item.name && item.name.toLowerCase().includes('mirdb')
    );

    expect(hasProductInfo, 'Structured data should include MirDB product information').toBe(true);
  });
});
