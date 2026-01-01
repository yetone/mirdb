const { test, expect } = require('@playwright/test');

test.describe('SEO Optimization', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Meta Tags', () => {
    test('page has descriptive title tag containing MirDB', async ({ page }) => {
      const title = await page.title();
      expect(title).toContain('MirDB');
      expect(title.length).toBeGreaterThan(10);
      expect(title.length).toBeLessThan(70); // SEO best practice for title length
    });

    test('meta description is present and describes MirDB', async ({ page }) => {
      const metaDescription = page.locator('meta[name="description"]');
      await expect(metaDescription).toHaveCount(1);

      const content = await metaDescription.getAttribute('content');
      expect(content).toContain('MirDB');
      expect(content.length).toBeGreaterThan(50);
      expect(content.length).toBeLessThan(160); // SEO best practice for description length
    });

    test('canonical URL meta tag is present', async ({ page }) => {
      const canonical = page.locator('link[rel="canonical"]');
      await expect(canonical).toHaveCount(1);

      const href = await canonical.getAttribute('href');
      expect(href).toBeTruthy();
      expect(href).toMatch(/^https?:\/\//); // Should be a valid URL
    });

    test('robots meta tag is present and allows indexing', async ({ page }) => {
      const robots = page.locator('meta[name="robots"]');
      await expect(robots).toHaveCount(1);

      const content = await robots.getAttribute('content');
      expect(content).toContain('index');
      expect(content).toContain('follow');
    });
  });

  test.describe('Semantic HTML Structure', () => {
    test('page uses semantic header element', async ({ page }) => {
      const header = page.locator('header[data-testid="hero-header"]');
      await expect(header).toHaveCount(1);
      await expect(header).toBeVisible();
    });

    test('page uses semantic main element', async ({ page }) => {
      const main = page.locator('main[data-testid="main-content"]');
      await expect(main).toHaveCount(1);
      await expect(main).toBeVisible();
    });

    test('page uses semantic section elements', async ({ page }) => {
      const sections = page.locator('main section');
      const count = await sections.count();
      expect(count).toBeGreaterThanOrEqual(3); // At least features, commands, quickstart sections
    });

    test('page uses semantic article elements for feature cards', async ({ page }) => {
      const articles = page.locator('article.feature-card');
      const count = await articles.count();
      expect(count).toBeGreaterThanOrEqual(3); // Feature cards are wrapped in article elements
    });

    test('page uses semantic footer element', async ({ page }) => {
      const footer = page.locator('footer[data-testid="footer"]');
      await expect(footer).toHaveCount(1);
      await expect(footer).toBeVisible();
    });

    test('page uses semantic nav element', async ({ page }) => {
      const nav = page.locator('nav[data-testid="navigation-header"]');
      await expect(nav).toHaveCount(1);
      await expect(nav).toBeVisible();
    });

    test('heading hierarchy is correct (h1 -> h2 -> h3)', async ({ page }) => {
      // Should have exactly one h1
      const h1Count = await page.locator('h1').count();
      expect(h1Count).toBe(1);

      // Should have multiple h2s for sections
      const h2Count = await page.locator('h2').count();
      expect(h2Count).toBeGreaterThanOrEqual(3);

      // h1 should contain MirDB
      const h1Text = await page.locator('h1').textContent();
      expect(h1Text).toContain('MirDB');
    });
  });

  test.describe('Open Graph Tags', () => {
    test('og:title meta tag is present', async ({ page }) => {
      const ogTitle = page.locator('meta[property="og:title"]');
      await expect(ogTitle).toHaveCount(1);

      const content = await ogTitle.getAttribute('content');
      expect(content).toContain('MirDB');
    });

    test('og:description meta tag is present', async ({ page }) => {
      const ogDescription = page.locator('meta[property="og:description"]');
      await expect(ogDescription).toHaveCount(1);

      const content = await ogDescription.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content.length).toBeGreaterThan(50);
    });

    test('og:image meta tag is present with valid image URL', async ({ page }) => {
      const ogImage = page.locator('meta[property="og:image"]');
      await expect(ogImage).toHaveCount(1);

      const content = await ogImage.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content).toMatch(/^https?:\/\/.*\.(png|jpg|jpeg|gif|webp)$/i);
    });

    test('og:type meta tag is present', async ({ page }) => {
      const ogType = page.locator('meta[property="og:type"]');
      await expect(ogType).toHaveCount(1);

      const content = await ogType.getAttribute('content');
      expect(content).toBe('website');
    });

    test('og:url meta tag is present', async ({ page }) => {
      const ogUrl = page.locator('meta[property="og:url"]');
      await expect(ogUrl).toHaveCount(1);

      const content = await ogUrl.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content).toMatch(/^https?:\/\//);
    });

    test('og:site_name meta tag is present', async ({ page }) => {
      const ogSiteName = page.locator('meta[property="og:site_name"]');
      await expect(ogSiteName).toHaveCount(1);

      const content = await ogSiteName.getAttribute('content');
      expect(content).toBe('MirDB');
    });
  });

  test.describe('Twitter Card Tags', () => {
    test('twitter:card meta tag is present', async ({ page }) => {
      const twitterCard = page.locator('meta[name="twitter:card"]');
      await expect(twitterCard).toHaveCount(1);

      const content = await twitterCard.getAttribute('content');
      expect(['summary', 'summary_large_image']).toContain(content);
    });

    test('twitter:title meta tag is present', async ({ page }) => {
      const twitterTitle = page.locator('meta[name="twitter:title"]');
      await expect(twitterTitle).toHaveCount(1);

      const content = await twitterTitle.getAttribute('content');
      expect(content).toContain('MirDB');
    });

    test('twitter:description meta tag is present', async ({ page }) => {
      const twitterDescription = page.locator('meta[name="twitter:description"]');
      await expect(twitterDescription).toHaveCount(1);

      const content = await twitterDescription.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content.length).toBeGreaterThan(50);
    });

    test('twitter:image meta tag is present', async ({ page }) => {
      const twitterImage = page.locator('meta[name="twitter:image"]');
      await expect(twitterImage).toHaveCount(1);

      const content = await twitterImage.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content).toMatch(/^https?:\/\/.*\.(png|jpg|jpeg|gif|webp)$/i);
    });
  });

  test.describe('Accessibility & SEO Best Practices', () => {
    test('html element has lang attribute', async ({ page }) => {
      const html = page.locator('html');
      const lang = await html.getAttribute('lang');
      expect(lang).toBe('en');
    });

    test('images have alt attributes', async ({ page }) => {
      const images = page.locator('img');
      const count = await images.count();

      for (let i = 0; i < count; i++) {
        const alt = await images.nth(i).getAttribute('alt');
        expect(alt).toBeTruthy();
      }
    });

    test('SVG diagrams have proper accessibility attributes', async ({ page }) => {
      const svg = page.locator('svg[role="img"]');
      const count = await svg.count();

      if (count > 0) {
        for (let i = 0; i < count; i++) {
          const ariaLabel = await svg.nth(i).getAttribute('aria-label');
          const title = await svg.nth(i).locator('title').count();
          const desc = await svg.nth(i).locator('desc').count();

          // SVG should have either aria-label or title element
          expect(ariaLabel || title > 0 || desc > 0).toBeTruthy();
        }
      }
    });

    test('links have descriptive text', async ({ page }) => {
      const links = page.locator('a');
      const count = await links.count();

      for (let i = 0; i < count; i++) {
        const text = await links.nth(i).textContent();
        const ariaLabel = await links.nth(i).getAttribute('aria-label');

        // Each link should have either visible text or aria-label
        expect(text?.trim() || ariaLabel).toBeTruthy();
      }
    });

    test('viewport meta tag is present for mobile responsiveness', async ({ page }) => {
      const viewport = page.locator('meta[name="viewport"]');
      await expect(viewport).toHaveCount(1);

      const content = await viewport.getAttribute('content');
      expect(content).toContain('width=device-width');
    });
  });
});
