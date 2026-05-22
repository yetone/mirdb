/**
 * SEO and Meta Tags E2E Tests
 * Owner: Scenario 10 - SEO and Meta Tags
 *
 * Tests:
 * - Title tag presence and content
 * - Meta description tag
 * - Viewport meta tag
 * - Open Graph (og:) meta tags
 * - Twitter Card meta tags
 * - Canonical URL link tag
 * - Favicon and touch icon links
 * - Structured data (JSON-LD)
 */

const { test, expect } = require('@playwright/test');

test.describe('SEO and Meta Tags', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should have a descriptive title tag', async ({ page }) => {
    const title = await page.title();
    expect(title).toBeTruthy();
    expect(title.length).toBeGreaterThan(0);
    // Title should include product name
    expect(title.toLowerCase()).toContain('mirdb');
    // Title should describe the service
    expect(
      title.toLowerCase().includes('url') ||
      title.toLowerCase().includes('shorten') ||
      title.toLowerCase().includes('link')
    ).toBe(true);
  });

  test('should have a meta description tag with product summary', async ({ page }) => {
    const metaDescription = page.locator('meta[name="description"]');
    await expect(metaDescription).toHaveCount(1);

    const content = await metaDescription.getAttribute('content');
    expect(content).toBeTruthy();
    expect(content.length).toBeGreaterThan(20);
    // Should describe the product
    expect(
      content.toLowerCase().includes('url') ||
      content.toLowerCase().includes('shorten') ||
      content.toLowerCase().includes('link')
    ).toBe(true);
  });

  test('should have a viewport meta tag for responsive behavior', async ({ page }) => {
    const viewportMeta = page.locator('meta[name="viewport"]');
    await expect(viewportMeta).toHaveCount(1);

    const content = await viewportMeta.getAttribute('content');
    expect(content).toBeTruthy();
    // Should contain width=device-width
    expect(content).toContain('width=device-width');
    // Should contain initial-scale
    expect(content).toContain('initial-scale');
  });

  test('should have all required Open Graph meta tags', async ({ page }) => {
    // og:title
    const ogTitle = page.locator('meta[property="og:title"]');
    await expect(ogTitle).toHaveCount(1);
    const ogTitleContent = await ogTitle.getAttribute('content');
    expect(ogTitleContent).toBeTruthy();
    expect(ogTitleContent.toLowerCase()).toContain('mirdb');

    // og:description
    const ogDesc = page.locator('meta[property="og:description"]');
    await expect(ogDesc).toHaveCount(1);
    const ogDescContent = await ogDesc.getAttribute('content');
    expect(ogDescContent).toBeTruthy();
    expect(ogDescContent.length).toBeGreaterThan(20);

    // og:type
    const ogType = page.locator('meta[property="og:type"]');
    await expect(ogType).toHaveCount(1);
    const ogTypeContent = await ogType.getAttribute('content');
    expect(ogTypeContent).toBe('website');

    // og:url
    const ogUrl = page.locator('meta[property="og:url"]');
    await expect(ogUrl).toHaveCount(1);
    const ogUrlContent = await ogUrl.getAttribute('content');
    expect(ogUrlContent).toBeTruthy();
    expect(ogUrlContent.startsWith('http')).toBe(true);

    // og:image
    const ogImage = page.locator('meta[property="og:image"]');
    await expect(ogImage).toHaveCount(1);
    const ogImageContent = await ogImage.getAttribute('content');
    expect(ogImageContent).toBeTruthy();
    expect(ogImageContent.startsWith('http')).toBe(true);
  });

  test('should have Twitter Card meta tags', async ({ page }) => {
    // twitter:card
    const twitterCard = page.locator('meta[name="twitter:card"]');
    await expect(twitterCard).toHaveCount(1);
    const twitterCardContent = await twitterCard.getAttribute('content');
    expect(twitterCardContent).toBeTruthy();

    // twitter:title
    const twitterTitle = page.locator('meta[name="twitter:title"]');
    await expect(twitterTitle).toHaveCount(1);
    const twitterTitleContent = await twitterTitle.getAttribute('content');
    expect(twitterTitleContent).toBeTruthy();
    expect(twitterTitleContent.toLowerCase()).toContain('mirdb');

    // twitter:description
    const twitterDesc = page.locator('meta[name="twitter:description"]');
    await expect(twitterDesc).toHaveCount(1);
    const twitterDescContent = await twitterDesc.getAttribute('content');
    expect(twitterDescContent).toBeTruthy();
    expect(twitterDescContent.length).toBeGreaterThan(20);
  });

  test('should have a canonical link tag pointing to the homepage', async ({ page }) => {
    const canonicalLink = page.locator('link[rel="canonical"]');
    await expect(canonicalLink).toHaveCount(1);

    const href = await canonicalLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href.startsWith('http')).toBe(true);
    // Should point to root URL
    expect(href.endsWith('/')).toBe(true);
  });

  test('should have favicon link tags for various device types', async ({ page }) => {
    // Standard favicon (SVG)
    const faviconSvg = page.locator('link[rel="icon"][type="image/svg+xml"]');
    await expect(faviconSvg).toHaveCount(1);
    const faviconSvgHref = await faviconSvg.getAttribute('href');
    expect(faviconSvgHref).toBeTruthy();

    // PNG favicon (32x32)
    const faviconPng = page.locator('link[rel="icon"][type="image/png"]');
    await expect(faviconPng).toHaveCount(1);
    const faviconPngHref = await faviconPng.getAttribute('href');
    expect(faviconPngHref).toBeTruthy();
    const faviconPngSizes = await faviconPng.getAttribute('sizes');
    expect(faviconPngSizes).toBe('32x32');

    // Apple touch icon
    const appleTouchIcon = page.locator('link[rel="apple-touch-icon"]');
    await expect(appleTouchIcon).toHaveCount(1);
    const appleTouchHref = await appleTouchIcon.getAttribute('href');
    expect(appleTouchHref).toBeTruthy();
    const appleTouchSizes = await appleTouchIcon.getAttribute('sizes');
    expect(appleTouchSizes).toBe('180x180');
  });

  test('should have structured data (JSON-LD) with WebSite schema', async ({ page }) => {
    const jsonLdScript = page.locator('script[type="application/ld+json"]');
    await expect(jsonLdScript).toHaveCount(1);

    const jsonLdContent = await jsonLdScript.textContent();
    expect(jsonLdContent).toBeTruthy();

    let structuredData;
    try {
      structuredData = JSON.parse(jsonLdContent);
    } catch (e) {
      throw new Error('JSON-LD structured data is not valid JSON');
    }

    // Should have @context pointing to schema.org
    expect(structuredData['@context']).toBe('https://schema.org');

    // Should have @type of WebSite
    expect(structuredData['@type']).toBe('WebSite');

    // Should have name
    expect(structuredData.name).toBeTruthy();
    expect(structuredData.name.toLowerCase()).toContain('mirdb');

    // Should have description
    expect(structuredData.description).toBeTruthy();

    // Should have url
    expect(structuredData.url).toBeTruthy();
    expect(structuredData.url.startsWith('http')).toBe(true);
  });
});
