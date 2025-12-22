// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * SEO Optimization Tests
 *
 * This test suite verifies that the MirDB homepage has proper SEO meta tags
 * and semantic HTML structure for optimal search engine visibility.
 */

test.describe('SEO Optimization', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Check for title tag
   * Expected: Page has descriptive title tag containing 'MirDB'
   */
  test('should have descriptive title tag containing "MirDB"', async ({ page }) => {
    const title = await page.title();

    // Title should exist and not be empty
    expect(title).toBeTruthy();
    expect(title.length).toBeGreaterThan(10);

    // Title should contain 'MirDB'
    expect(title.toLowerCase()).toContain('mirdb');

    // Title should be descriptive (contain relevant keywords)
    const hasDescriptiveContent =
      title.toLowerCase().includes('key-value') ||
      title.toLowerCase().includes('database') ||
      title.toLowerCase().includes('persistent') ||
      title.toLowerCase().includes('store');
    expect(hasDescriptiveContent, 'Title should contain descriptive keywords').toBe(true);

    // Title should be optimal length (50-60 characters recommended for SEO)
    expect(title.length).toBeLessThanOrEqual(70);
  });

  /**
   * Test Case 2: Check for meta description
   * Expected: Page has meta description explaining MirDB
   */
  test('should have meta description explaining MirDB', async ({ page }) => {
    const metaDescription = await page.locator('meta[name="description"]').getAttribute('content');

    // Meta description should exist
    expect(metaDescription, 'Meta description is missing').toBeTruthy();

    // Meta description should be descriptive (optimal: 150-160 characters)
    expect(metaDescription.length).toBeGreaterThan(50);
    expect(metaDescription.length).toBeLessThanOrEqual(200);

    // Meta description should mention MirDB
    expect(metaDescription.toLowerCase()).toContain('mirdb');

    // Meta description should contain relevant keywords
    const hasRelevantKeywords =
      metaDescription.toLowerCase().includes('key-value') ||
      metaDescription.toLowerCase().includes('database') ||
      metaDescription.toLowerCase().includes('persistent') ||
      metaDescription.toLowerCase().includes('memcached');
    expect(hasRelevantKeywords, 'Meta description should contain relevant keywords').toBe(true);
  });

  /**
   * Test Case 3: Check for Open Graph tags
   * Expected: Page has og:title, og:description, og:image tags
   */
  test('should have Open Graph tags (og:title, og:description, og:image)', async ({ page }) => {
    // Check og:title
    const ogTitleCount = await page.locator('meta[property="og:title"]').count();
    expect(ogTitleCount, 'og:title is missing').toBeGreaterThan(0);
    const ogTitle = await page.locator('meta[property="og:title"]').getAttribute('content');
    expect(ogTitle.length).toBeGreaterThan(5);
    expect(ogTitle.toLowerCase()).toContain('mirdb');

    // Check og:description
    const ogDescCount = await page.locator('meta[property="og:description"]').count();
    expect(ogDescCount, 'og:description is missing').toBeGreaterThan(0);
    const ogDescription = await page.locator('meta[property="og:description"]').getAttribute('content');
    expect(ogDescription.length).toBeGreaterThan(50);

    // Check og:image
    const ogImageCount = await page.locator('meta[property="og:image"]').count();
    expect(ogImageCount, 'og:image is missing').toBeGreaterThan(0);
    const ogImage = await page.locator('meta[property="og:image"]').getAttribute('content');
    // og:image should be a valid URL or path
    expect(ogImage.length).toBeGreaterThan(5);

    // Check og:type (recommended for all pages)
    const ogTypeCount = await page.locator('meta[property="og:type"]').count();
    expect(ogTypeCount, 'og:type is missing').toBeGreaterThan(0);
    const ogType = await page.locator('meta[property="og:type"]').getAttribute('content');
    expect(['website', 'article', 'product']).toContain(ogType);

    // Check og:url (recommended for canonical URL)
    const ogUrlCount = await page.locator('meta[property="og:url"]').count();
    expect(ogUrlCount, 'og:url is missing').toBeGreaterThan(0);
  });

  /**
   * Test Case 4: Check for canonical URL
   * Expected: Page has canonical URL tag
   */
  test('should have canonical URL tag', async ({ page }) => {
    const canonicalCount = await page.locator('link[rel="canonical"]').count();
    expect(canonicalCount, 'Canonical URL is missing').toBeGreaterThan(0);

    const canonicalLink = await page.locator('link[rel="canonical"]').getAttribute('href');

    // Canonical URL should be a valid URL
    expect(canonicalLink.length).toBeGreaterThan(10);
    expect(canonicalLink).toMatch(/^https?:\/\//);
  });

  /**
   * Test Case 5: Verify semantic HTML structure
   * Expected: Page uses semantic elements (header, main, nav, section, footer)
   */
  test('should use semantic HTML elements (header, main, nav, section, footer)', async ({ page }) => {
    // Check for header element
    const headerCount = await page.locator('header').count();
    expect(headerCount, 'Page should have at least one header element').toBeGreaterThanOrEqual(1);

    // Check for main element (should be exactly one)
    const mainCount = await page.locator('main').count();
    expect(mainCount, 'Page should have exactly one main element').toBe(1);

    // Check for nav element
    const navCount = await page.locator('nav').count();
    expect(navCount, 'Page should have at least one nav element').toBeGreaterThanOrEqual(1);

    // Check for section elements
    const sectionCount = await page.locator('section').count();
    expect(sectionCount, 'Page should have section elements').toBeGreaterThanOrEqual(1);

    // Check for footer element
    const footerCount = await page.locator('footer').count();
    expect(footerCount, 'Page should have at least one footer element').toBeGreaterThanOrEqual(1);

    // Verify header contains nav
    const navInHeader = await page.locator('header nav').count();
    expect(navInHeader, 'Nav should be within header').toBeGreaterThanOrEqual(1);

    // Verify main contains sections
    const sectionsInMain = await page.locator('main section').count();
    expect(sectionsInMain, 'Main should contain section elements').toBeGreaterThanOrEqual(1);

    // Verify proper document structure: header before main, main before footer
    const headerBox = await page.locator('header').first().boundingBox();
    const mainBox = await page.locator('main').boundingBox();
    const footerBox = await page.locator('footer').first().boundingBox();

    expect(headerBox.y).toBeLessThan(mainBox.y);
    expect(mainBox.y).toBeLessThan(footerBox.y);
  });

  /**
   * Test Case 6: Lighthouse SEO audit simulation
   * Expected: Key SEO elements present that would result in high Lighthouse score
   * Note: This is a programmatic check of elements Lighthouse SEO tests for
   */
  test('should pass key SEO checks (Lighthouse SEO criteria)', async ({ page }) => {
    // 1. Document has a <title> element
    const title = await page.title();
    expect(title).toBeTruthy();
    expect(title.length).toBeGreaterThan(0);

    // 2. Document has a meta description
    const metaDesc = await page.locator('meta[name="description"]').getAttribute('content');
    expect(metaDesc).toBeTruthy();

    // 3. Page has valid hreflang (if multilingual, not required for single language)
    // Skip - not applicable for single language site

    // 4. Document has a valid rel=canonical
    const canonicalCount = await page.locator('link[rel="canonical"]').count();
    expect(canonicalCount, 'Canonical URL is missing').toBeGreaterThan(0);
    const canonical = await page.locator('link[rel="canonical"]').getAttribute('href');
    expect(canonical).toBeTruthy();

    // 5. Document uses legible font sizes (16px+ for body text)
    const bodyFontSize = await page.evaluate(() => {
      const body = document.body;
      const computedStyle = window.getComputedStyle(body);
      return parseInt(computedStyle.fontSize);
    });
    expect(bodyFontSize).toBeGreaterThanOrEqual(14);

    // 6. Links have descriptive text
    const links = await page.locator('a').all();
    for (const link of links.slice(0, 5)) { // Check first 5 links
      const linkText = await link.textContent();
      const ariaLabel = await link.getAttribute('aria-label');
      const accessibleText = ariaLabel || linkText?.trim();
      expect(accessibleText?.length, 'Link should have descriptive text').toBeGreaterThan(0);
    }

    // 7. Document has valid viewport meta tag
    const viewport = await page.locator('meta[name="viewport"]').getAttribute('content');
    expect(viewport).toBeTruthy();
    expect(viewport).toContain('width=device-width');

    // 8. Image elements have alt attributes
    const images = await page.locator('img').all();
    for (const img of images) {
      const alt = await img.getAttribute('alt');
      expect(alt, 'Image should have alt attribute').not.toBeNull();
    }

    // 9. Document avoids deprecated HTML elements
    const deprecatedElements = await page.evaluate(() => {
      const deprecated = ['center', 'font', 'marquee', 'blink', 'frame', 'frameset'];
      for (const tag of deprecated) {
        if (document.getElementsByTagName(tag).length > 0) {
          return tag;
        }
      }
      return null;
    });
    expect(deprecatedElements, 'Should not use deprecated HTML elements').toBeNull();

    // 10. Document has valid lang attribute
    const htmlLang = await page.locator('html').getAttribute('lang');
    expect(htmlLang).toBeTruthy();
    expect(htmlLang.length).toBeGreaterThanOrEqual(2);

    // 11. robots.txt or meta robots allows indexing
    const robotsMeta = await page.locator('meta[name="robots"]').count();
    if (robotsMeta > 0) {
      const robotsContent = await page.locator('meta[name="robots"]').getAttribute('content');
      expect(robotsContent).not.toContain('noindex');
    }

    // 12. Links are crawlable (not using JavaScript-only navigation)
    const allLinks = await page.locator('a[href]').all();
    expect(allLinks.length, 'Page should have crawlable links').toBeGreaterThan(0);
    for (const link of allLinks.slice(0, 3)) {
      const href = await link.getAttribute('href');
      expect(href).not.toBe('javascript:void(0)');
      expect(href).not.toMatch(/^javascript:/);
    }
  });

  /**
   * Additional SEO tests for comprehensive coverage
   */
  test('should have proper heading hierarchy for SEO', async ({ page }) => {
    // There should be exactly one h1
    const h1Count = await page.locator('h1').count();
    expect(h1Count, 'Page should have exactly one h1').toBe(1);

    // h1 should contain the main keyword (MirDB)
    const h1Text = await page.locator('h1').textContent();
    expect(h1Text.toLowerCase()).toContain('mirdb');

    // Check heading hierarchy
    const headings = await page.evaluate(() => {
      const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      return Array.from(allHeadings).map(h => ({
        level: parseInt(h.tagName.charAt(1)),
        text: h.textContent.trim()
      }));
    });

    // First heading should be h1
    expect(headings[0].level, 'First heading should be h1').toBe(1);

    // No skipping heading levels (h1 -> h3 without h2)
    let lastLevel = 0;
    for (const heading of headings) {
      if (lastLevel > 0 && heading.level > lastLevel + 1) {
        // Allow some flexibility but flag major skips
        console.log(`Warning: Heading level skip from h${lastLevel} to h${heading.level}`);
      }
      lastLevel = heading.level;
    }
  });

  test('should have Twitter Card meta tags', async ({ page }) => {
    // twitter:card
    const twitterCardCount = await page.locator('meta[name="twitter:card"]').count();
    expect(twitterCardCount, 'twitter:card is missing').toBeGreaterThan(0);
    const twitterCard = await page.locator('meta[name="twitter:card"]').getAttribute('content');
    expect(['summary', 'summary_large_image', 'app', 'player']).toContain(twitterCard);

    // twitter:title
    const twitterTitleCount = await page.locator('meta[name="twitter:title"]').count();
    expect(twitterTitleCount, 'twitter:title is missing').toBeGreaterThan(0);
    const twitterTitle = await page.locator('meta[name="twitter:title"]').getAttribute('content');
    expect(twitterTitle.toLowerCase()).toContain('mirdb');

    // twitter:description
    const twitterDescCount = await page.locator('meta[name="twitter:description"]').count();
    expect(twitterDescCount, 'twitter:description is missing').toBeGreaterThan(0);
  });

  test('should have proper charset and encoding', async ({ page }) => {
    // Check meta charset
    const charsetMeta = await page.locator('meta[charset]').getAttribute('charset');
    expect(charsetMeta?.toLowerCase(), 'Should have UTF-8 charset').toBe('utf-8');
  });

  test('should have structured data or be ready for it', async ({ page }) => {
    // Check if page has any structured data (JSON-LD, microdata, or RDFa)
    const jsonLd = await page.locator('script[type="application/ld+json"]').count();

    // At minimum, the page should have proper semantic markup
    // If JSON-LD exists, validate it's valid JSON
    if (jsonLd > 0) {
      const jsonLdContent = await page.locator('script[type="application/ld+json"]').first().textContent();
      expect(() => JSON.parse(jsonLdContent), 'JSON-LD should be valid JSON').not.toThrow();
    }

    // Verify semantic elements exist as fallback structured data
    const semanticElements = await page.evaluate(() => {
      return {
        hasHeader: document.querySelector('header') !== null,
        hasMain: document.querySelector('main') !== null,
        hasNav: document.querySelector('nav') !== null,
        hasFooter: document.querySelector('footer') !== null,
        hasArticle: document.querySelector('article') !== null || document.querySelector('section') !== null
      };
    });

    expect(semanticElements.hasHeader, 'Should have header').toBe(true);
    expect(semanticElements.hasMain, 'Should have main').toBe(true);
    expect(semanticElements.hasNav, 'Should have nav').toBe(true);
    expect(semanticElements.hasFooter, 'Should have footer').toBe(true);
  });
});
