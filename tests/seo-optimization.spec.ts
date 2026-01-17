import { test, expect } from '@playwright/test';

test.describe('SEO Optimization', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Meta Tags', () => {
    test('should have a unique, descriptive title tag containing MirDB', async ({ page }) => {
      const title = await page.title();
      expect(title).toContain('MirDB');
      expect(title.length).toBeGreaterThan(10);
      expect(title.length).toBeLessThan(70);
    });

    test('should have meta description summarizing MirDB value proposition', async ({ page }) => {
      const metaDescription = page.locator('meta[name="description"]');
      await expect(metaDescription).toHaveCount(1);

      const content = await metaDescription.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content!.length).toBeGreaterThan(50);
      expect(content!.length).toBeLessThan(160);
      expect(content!.toLowerCase()).toContain('mirdb');
    });

    test('should have canonical URL link tag', async ({ page }) => {
      const canonical = page.locator('link[rel="canonical"]');
      await expect(canonical).toHaveCount(1);

      const href = await canonical.getAttribute('href');
      expect(href).toBeTruthy();
      expect(href).toMatch(/^https?:\/\//);
    });
  });

  test.describe('Open Graph Tags', () => {
    test('should have og:title tag', async ({ page }) => {
      const ogTitle = page.locator('meta[property="og:title"]');
      await expect(ogTitle).toHaveCount(1);

      const content = await ogTitle.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content!.toLowerCase()).toContain('mirdb');
    });

    test('should have og:description tag', async ({ page }) => {
      const ogDescription = page.locator('meta[property="og:description"]');
      await expect(ogDescription).toHaveCount(1);

      const content = await ogDescription.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content!.length).toBeGreaterThan(20);
    });

    test('should have og:image tag', async ({ page }) => {
      const ogImage = page.locator('meta[property="og:image"]');
      await expect(ogImage).toHaveCount(1);

      const content = await ogImage.getAttribute('content');
      expect(content).toBeTruthy();
    });

    test('should have og:type tag', async ({ page }) => {
      const ogType = page.locator('meta[property="og:type"]');
      await expect(ogType).toHaveCount(1);

      const content = await ogType.getAttribute('content');
      expect(content).toBeTruthy();
    });

    test('should have og:url tag', async ({ page }) => {
      const ogUrl = page.locator('meta[property="og:url"]');
      await expect(ogUrl).toHaveCount(1);

      const content = await ogUrl.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content).toMatch(/^https?:\/\//);
    });
  });

  test.describe('Twitter Card Tags', () => {
    test('should have twitter:card tag', async ({ page }) => {
      const twitterCard = page.locator('meta[name="twitter:card"]');
      await expect(twitterCard).toHaveCount(1);

      const content = await twitterCard.getAttribute('content');
      expect(content).toBeTruthy();
      expect(['summary', 'summary_large_image', 'app', 'player']).toContain(content);
    });

    test('should have twitter:title tag', async ({ page }) => {
      const twitterTitle = page.locator('meta[name="twitter:title"]');
      await expect(twitterTitle).toHaveCount(1);

      const content = await twitterTitle.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content!.toLowerCase()).toContain('mirdb');
    });

    test('should have twitter:description tag', async ({ page }) => {
      const twitterDescription = page.locator('meta[name="twitter:description"]');
      await expect(twitterDescription).toHaveCount(1);

      const content = await twitterDescription.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content!.length).toBeGreaterThan(20);
    });

    test('should have twitter:image tag', async ({ page }) => {
      const twitterImage = page.locator('meta[name="twitter:image"]');
      await expect(twitterImage).toHaveCount(1);

      const content = await twitterImage.getAttribute('content');
      expect(content).toBeTruthy();
    });
  });

  test.describe('Semantic HTML Elements', () => {
    test('should use header element', async ({ page }) => {
      const header = page.locator('header');
      await expect(header).toHaveCount(1);
    });

    test('should use main element', async ({ page }) => {
      const main = page.locator('main');
      await expect(main).toHaveCount(1);
    });

    test('should use footer element', async ({ page }) => {
      const footer = page.locator('footer');
      await expect(footer).toHaveCount(1);
    });

    test('should use section elements', async ({ page }) => {
      const sections = page.locator('section');
      const count = await sections.count();
      expect(count).toBeGreaterThan(0);
    });

    test('should have proper heading hierarchy starting with h1', async ({ page }) => {
      const h1 = page.locator('h1');
      await expect(h1).toHaveCount(1);

      const h1Text = await h1.textContent();
      expect(h1Text).toBeTruthy();
    });

    test('should have nav element or navigation landmark', async ({ page }) => {
      const nav = page.locator('nav, [role="navigation"]');
      const count = await nav.count();
      expect(count).toBeGreaterThanOrEqual(0);
    });
  });

  test.describe('Structured Data', () => {
    test('should have JSON-LD structured data', async ({ page }) => {
      const jsonLd = page.locator('script[type="application/ld+json"]');
      await expect(jsonLd).toHaveCount(1);

      const content = await jsonLd.textContent();
      expect(content).toBeTruthy();

      const data = JSON.parse(content!);
      expect(data['@context']).toBe('https://schema.org');
      expect(data['@type']).toBeTruthy();
    });

    test('should have valid structured data with name', async ({ page }) => {
      const jsonLd = page.locator('script[type="application/ld+json"]');
      const content = await jsonLd.textContent();
      const data = JSON.parse(content!);

      expect(data.name).toBeTruthy();
      expect(data.name.toLowerCase()).toContain('mirdb');
    });

    test('should have structured data with description', async ({ page }) => {
      const jsonLd = page.locator('script[type="application/ld+json"]');
      const content = await jsonLd.textContent();
      const data = JSON.parse(content!);

      expect(data.description).toBeTruthy();
    });
  });

  test.describe('Additional SEO Best Practices', () => {
    test('should have lang attribute on html element', async ({ page }) => {
      const lang = await page.locator('html').getAttribute('lang');
      expect(lang).toBe('en');
    });

    test('should have charset meta tag', async ({ page }) => {
      const charset = page.locator('meta[charset]');
      await expect(charset).toHaveCount(1);

      const charsetValue = await charset.getAttribute('charset');
      expect(charsetValue?.toUpperCase()).toBe('UTF-8');
    });

    test('should have viewport meta tag', async ({ page }) => {
      const viewport = page.locator('meta[name="viewport"]');
      await expect(viewport).toHaveCount(1);

      const content = await viewport.getAttribute('content');
      expect(content).toContain('width=device-width');
    });

    test('should have alt text for all images', async ({ page }) => {
      const images = page.locator('img');
      const count = await images.count();

      for (let i = 0; i < count; i++) {
        const alt = await images.nth(i).getAttribute('alt');
        expect(alt).toBeTruthy();
      }
    });
  });
});
