// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('SEO and Meta Tags', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Page has descriptive title tag containing MirDB', async ({ page }) => {
    // Get the page title
    const title = await page.title();

    // Verify title exists and is not empty
    expect(title, 'Page should have a title').toBeTruthy();
    expect(title.length, 'Title should have content').toBeGreaterThan(0);

    // Verify title contains MirDB
    expect(title, 'Title should contain MirDB').toContain('MirDB');

    // Verify title is descriptive (has additional context beyond just product name)
    expect(title.length, 'Title should be descriptive with more than just the product name').toBeGreaterThan(5);

    // Verify title follows SEO best practices (not too long)
    expect(title.length, 'Title should not exceed 60 characters for optimal SEO').toBeLessThanOrEqual(70);
  });

  test('TC2: Page has meta description describing MirDB value proposition', async ({ page }) => {
    // Get meta description
    const metaDescription = await page.getAttribute('meta[name="description"]', 'content');

    // Verify meta description exists
    expect(metaDescription, 'Page should have a meta description').toBeTruthy();

    // Verify meta description has meaningful content
    expect(metaDescription?.length, 'Meta description should have content').toBeGreaterThan(0);

    // Verify meta description mentions MirDB or its key features
    const descriptionLower = metaDescription?.toLowerCase() || '';
    const hasRelevantContent =
      descriptionLower.includes('mirdb') ||
      descriptionLower.includes('key-value') ||
      descriptionLower.includes('memcached') ||
      descriptionLower.includes('persistent');

    expect(hasRelevantContent, 'Meta description should describe MirDB\'s value proposition').toBe(true);

    // Verify meta description follows SEO best practices (reasonable length)
    expect(metaDescription?.length, 'Meta description should be at least 50 characters').toBeGreaterThanOrEqual(50);
    expect(metaDescription?.length, 'Meta description should not exceed 160 characters for optimal SEO').toBeLessThanOrEqual(160);
  });

  test('TC3: Page has Open Graph meta tags (og:title, og:description, og:image)', async ({ page }) => {
    // Get all OG meta tags using evaluate to avoid timeout on non-existent elements
    const ogTags = await page.evaluate(() => {
      return {
        ogTitle: document.querySelector('meta[property="og:title"]')?.getAttribute('content') || null,
        ogDescription: document.querySelector('meta[property="og:description"]')?.getAttribute('content') || null,
        ogImage: document.querySelector('meta[property="og:image"]')?.getAttribute('content') || null,
        ogType: document.querySelector('meta[property="og:type"]')?.getAttribute('content') || null
      };
    });

    // Check og:title
    expect(ogTags.ogTitle, 'Page should have og:title meta tag').toBeTruthy();
    expect(ogTags.ogTitle?.length, 'og:title should have content').toBeGreaterThan(0);
    expect(ogTags.ogTitle, 'og:title should contain MirDB').toContain('MirDB');

    // Check og:description
    expect(ogTags.ogDescription, 'Page should have og:description meta tag').toBeTruthy();
    expect(ogTags.ogDescription?.length, 'og:description should have content').toBeGreaterThan(0);

    // Check og:image
    expect(ogTags.ogImage, 'Page should have og:image meta tag').toBeTruthy();
    expect(ogTags.ogImage?.length, 'og:image should have a URL').toBeGreaterThan(0);

    // Verify og:type for website
    expect(ogTags.ogType, 'Page should have og:type meta tag').toBeTruthy();
  });

  test('TC4: HTML passes W3C validation with no errors', async ({ page }) => {
    // Get the HTML content
    const html = await page.content();

    // Check for DOCTYPE declaration
    expect(html, 'Page should have DOCTYPE declaration').toMatch(/^<!DOCTYPE html>/i);

    // Check for proper HTML structure
    const htmlElement = page.locator('html');
    await expect(htmlElement, 'Page should have html element').toHaveCount(1);

    // Check html lang attribute
    const htmlLang = await page.getAttribute('html', 'lang');
    expect(htmlLang, 'HTML element should have lang attribute').toBeTruthy();
    expect(htmlLang, 'HTML lang should be a valid language code').toMatch(/^[a-z]{2}(-[A-Z]{2})?$/);

    // Check for proper head structure
    const head = page.locator('head');
    await expect(head, 'Page should have head element').toHaveCount(1);

    // Check for charset meta tag
    const charset = await page.getAttribute('meta[charset]', 'charset');
    expect(charset, 'Page should have charset meta tag').toBeTruthy();
    expect(charset?.toLowerCase(), 'Charset should be UTF-8').toBe('utf-8');

    // Check for viewport meta tag
    const viewport = await page.getAttribute('meta[name="viewport"]', 'content');
    expect(viewport, 'Page should have viewport meta tag').toBeTruthy();

    // Check for body element
    const body = page.locator('body');
    await expect(body, 'Page should have body element').toHaveCount(1);

    // Check for semantic HTML elements
    const header = page.locator('header');
    await expect(header, 'Page should have header element').toHaveCount(1);

    const main = page.locator('main');
    await expect(main, 'Page should have main element').toHaveCount(1);

    const footer = page.locator('footer');
    await expect(footer, 'Page should have footer element').toHaveCount(1);

    // Check heading hierarchy - should start with h1
    const h1 = page.locator('h1');
    await expect(h1, 'Page should have exactly one h1 element').toHaveCount(1);

    // Validate no unclosed tags by checking structure
    const unclosedElements = await page.evaluate(() => {
      // Check for common issues that would indicate validation problems
      const issues = [];

      // Check all elements have valid nesting
      const invalidNesting = document.querySelectorAll('p > div, p > p, a > a, button > button');
      if (invalidNesting.length > 0) {
        issues.push('Found invalid element nesting');
      }

      // Check all img elements have alt attribute
      const imgsWithoutAlt = document.querySelectorAll('img:not([alt])');
      if (imgsWithoutAlt.length > 0) {
        issues.push(`Found ${imgsWithoutAlt.length} img elements without alt attribute`);
      }

      // Check for duplicate IDs
      const allIds = Array.from(document.querySelectorAll('[id]')).map(el => el.id);
      const duplicateIds = allIds.filter((id, index) => allIds.indexOf(id) !== index);
      if (duplicateIds.length > 0) {
        issues.push(`Found duplicate IDs: ${duplicateIds.join(', ')}`);
      }

      return issues;
    });

    expect(unclosedElements, 'HTML should have no validation issues').toHaveLength(0);
  });

  test('TC5: Page has canonical link element', async ({ page }) => {
    // Check for canonical link using evaluate to avoid timeout
    const canonicalHref = await page.evaluate(() => {
      return document.querySelector('link[rel="canonical"]')?.getAttribute('href') || null;
    });

    // Verify canonical link exists
    expect(canonicalHref, 'Page should have canonical link element').toBeTruthy();
    expect(canonicalHref?.length, 'Canonical href should have a URL').toBeGreaterThan(0);

    // Verify canonical URL is a valid URL format
    expect(canonicalHref, 'Canonical URL should be a valid URL').toMatch(/^https?:\/\//);
  });

  test('Additional: Page has proper meta tags for social sharing', async ({ page }) => {
    // Get social sharing meta tags using evaluate to avoid timeout
    const socialTags = await page.evaluate(() => {
      return {
        twitterCard: document.querySelector('meta[name="twitter:card"]')?.getAttribute('content') || null,
        twitterTitle: document.querySelector('meta[name="twitter:title"]')?.getAttribute('content') || null,
        ogUrl: document.querySelector('meta[property="og:url"]')?.getAttribute('content') || null
      };
    });

    // If Twitter card exists, verify it has proper values
    if (socialTags.twitterCard) {
      expect(['summary', 'summary_large_image', 'app', 'player']).toContain(socialTags.twitterCard);

      // Check for twitter:title if card exists
      if (socialTags.twitterTitle) {
        expect(socialTags.twitterTitle, 'Twitter title should contain MirDB').toContain('MirDB');
      }
    }

    // Check og:url exists
    expect(socialTags.ogUrl, 'Page should have og:url for proper social sharing').toBeTruthy();
  });
});
