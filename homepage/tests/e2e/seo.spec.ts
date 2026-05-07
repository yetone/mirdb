import { test, expect, type Page } from '@playwright/test';

async function getMetaContent(page: Page, selector: string): Promise<string | null> {
  const locator = page.locator(`head ${selector}`).first();
  if ((await locator.count()) === 0) {
    return null;
  }
  return locator.getAttribute('content');
}

async function getLinkHref(page: Page, rel: string): Promise<string | null> {
  const locator = page.locator(`head link[rel="${rel}"]`).first();
  if ((await locator.count()) === 0) {
    return null;
  }
  return locator.getAttribute('href');
}

test.describe('SEO and Meta Tags', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Test Case 1: Title tag', () => {
    test('document title contains MirDB and descriptive text', async ({ page }) => {
      const title = await page.title();
      expect(title).toContain('MirDB');
      // descriptive text beyond just the brand name
      expect(title.length).toBeGreaterThan('MirDB'.length + 5);
      // The title should reference the product type or protocol
      expect(title).toMatch(/key-value|memcached|persistent/i);
    });

    test('title tag is present in the document head', async ({ page }) => {
      const titleLocator = page.locator('head > title');
      await expect(titleLocator).toHaveCount(1);
      const titleText = await titleLocator.textContent();
      expect(titleText).toBeTruthy();
      expect(titleText!.trim().length).toBeGreaterThan(0);
    });
  });

  test.describe('Test Case 2: Meta description', () => {
    test('meta description tag is present', async ({ page }) => {
      const description = await getMetaContent(page, 'meta[name="description"]');
      expect(description).not.toBeNull();
      expect(description!.trim().length).toBeGreaterThan(0);
    });

    test('meta description is under 160 characters', async ({ page }) => {
      const description = await getMetaContent(page, 'meta[name="description"]');
      expect(description).not.toBeNull();
      expect(description!.length).toBeLessThanOrEqual(160);
      // Meaningful description (not too short)
      expect(description!.length).toBeGreaterThanOrEqual(50);
    });

    test('meta description contains compelling product summary', async ({ page }) => {
      const description = await getMetaContent(page, 'meta[name="description"]');
      expect(description).not.toBeNull();
      expect(description!).toMatch(/MirDB/i);
      expect(description!).toMatch(/key-value|memcached|persistent|store|database/i);
    });

    test('viewport meta tag is present for responsive rendering', async ({ page }) => {
      const viewport = await getMetaContent(page, 'meta[name="viewport"]');
      expect(viewport).not.toBeNull();
      expect(viewport!).toContain('width=device-width');
    });
  });

  test.describe('Test Case 3: Open Graph tags', () => {
    test('og:title is present', async ({ page }) => {
      const ogTitle = await getMetaContent(page, 'meta[property="og:title"]');
      expect(ogTitle).not.toBeNull();
      expect(ogTitle!).toContain('MirDB');
    });

    test('og:description is present', async ({ page }) => {
      const ogDescription = await getMetaContent(page, 'meta[property="og:description"]');
      expect(ogDescription).not.toBeNull();
      expect(ogDescription!.length).toBeGreaterThan(0);
    });

    test('og:type is set to website', async ({ page }) => {
      const ogType = await getMetaContent(page, 'meta[property="og:type"]');
      expect(ogType).toBe('website');
    });

    test('og:image is present and references the logo', async ({ page }) => {
      const ogImage = await getMetaContent(page, 'meta[property="og:image"]');
      expect(ogImage).not.toBeNull();
      expect(ogImage!.length).toBeGreaterThan(0);
      // Image URL should be a fully qualified URL
      expect(ogImage!).toMatch(/^https?:\/\//);
      // Should reference logo (or a similar branded image asset)
      expect(ogImage!).toMatch(/logo|mirdb|og/i);
    });

    test('og:url is present and is a valid URL', async ({ page }) => {
      const ogUrl = await getMetaContent(page, 'meta[property="og:url"]');
      expect(ogUrl).not.toBeNull();
      expect(ogUrl!).toMatch(/^https?:\/\//);
    });
  });

  test.describe('Test Case 4: Twitter Card tags', () => {
    test('twitter:card is summary_large_image', async ({ page }) => {
      const twitterCard = await getMetaContent(page, 'meta[name="twitter:card"]');
      expect(twitterCard).toBe('summary_large_image');
    });

    test('twitter:title is present', async ({ page }) => {
      const twitterTitle = await getMetaContent(page, 'meta[name="twitter:title"]');
      expect(twitterTitle).not.toBeNull();
      expect(twitterTitle!).toContain('MirDB');
    });

    test('twitter:description is present', async ({ page }) => {
      const twitterDescription = await getMetaContent(page, 'meta[name="twitter:description"]');
      expect(twitterDescription).not.toBeNull();
      expect(twitterDescription!.length).toBeGreaterThan(0);
    });

    test('twitter:image is present', async ({ page }) => {
      const twitterImage = await getMetaContent(page, 'meta[name="twitter:image"]');
      expect(twitterImage).not.toBeNull();
      expect(twitterImage!).toMatch(/^https?:\/\//);
    });
  });

  test.describe('Test Case 5: Structured data (JSON-LD)', () => {
    test('JSON-LD script tag with SoftwareApplication schema is present', async ({ page }) => {
      const jsonLdLocator = page.locator('script[type="application/ld+json"]').first();
      await expect(jsonLdLocator).toHaveCount(1);

      const rawJson = await jsonLdLocator.textContent();
      expect(rawJson).toBeTruthy();

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      let parsed: any;
      expect(() => {
        parsed = JSON.parse(rawJson!);
      }).not.toThrow();

      expect(parsed['@context']).toBe('https://schema.org');
      expect(parsed['@type']).toBe('SoftwareApplication');
    });

    test('JSON-LD contains name, description, and url', async ({ page }) => {
      const jsonLdLocator = page.locator('script[type="application/ld+json"]').first();
      const rawJson = await jsonLdLocator.textContent();
      expect(rawJson).toBeTruthy();
      const parsed = JSON.parse(rawJson!);

      expect(parsed.name).toBeTruthy();
      expect(parsed.name).toContain('MirDB');
      expect(parsed.description).toBeTruthy();
      expect(parsed.description.length).toBeGreaterThan(0);
      expect(parsed.url).toBeTruthy();
      expect(parsed.url).toMatch(/^https?:\/\//);
    });

    test('JSON-LD contains repository information', async ({ page }) => {
      const jsonLdLocator = page.locator('script[type="application/ld+json"]').first();
      const rawJson = await jsonLdLocator.textContent();
      expect(rawJson).toBeTruthy();
      const parsed = JSON.parse(rawJson!);

      // Repository info via codeRepository (Schema.org SoftwareSourceCode/SoftwareApplication)
      expect(parsed.codeRepository).toBeTruthy();
      expect(parsed.codeRepository).toMatch(/^https?:\/\/github\.com\//);
      expect(parsed.codeRepository).toContain('mirdb');
    });
  });

  test.describe('Canonical URL', () => {
    test('canonical link tag is present and points to a valid URL', async ({ page }) => {
      const canonical = await getLinkHref(page, 'canonical');
      expect(canonical).not.toBeNull();
      expect(canonical!).toMatch(/^https?:\/\//);
    });
  });
});
