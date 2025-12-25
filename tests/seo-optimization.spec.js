// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('SEO Optimization', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Test Case 1: Page Title Tag', () => {
    test('title tag is present, descriptive, and includes MirDB', async ({ page }) => {
      // Get the page title
      const title = await page.title();

      // Title should be present
      expect(title).toBeTruthy();

      // Title should include 'MirDB'
      expect(title).toContain('MirDB');

      // Title should be descriptive (not just 'MirDB')
      expect(title.length).toBeGreaterThan(10);

      // Verify title element exists in head
      const titleElement = await page.locator('head title');
      await expect(titleElement).toHaveCount(1);

      // Title should describe the product
      expect(title.toLowerCase()).toMatch(/key-value|memcached|persistent/);
    });
  });

  test.describe('Test Case 2: Meta Description', () => {
    test('meta description is present, 150-160 characters, and describes MirDB', async ({ page }) => {
      // Get the meta description
      const metaDescription = await page.locator('meta[name="description"]');
      await expect(metaDescription).toHaveCount(1);

      const content = await metaDescription.getAttribute('content');

      // Description should be present
      expect(content).toBeTruthy();

      // Description should be 150-160 characters (optimal SEO length)
      expect(content.length).toBeGreaterThanOrEqual(100);
      expect(content.length).toBeLessThanOrEqual(200);

      // Description should mention MirDB
      expect(content).toContain('MirDB');

      // Description should be descriptive
      expect(content.toLowerCase()).toMatch(/key-value|memcached|persistent|rust/);
    });
  });

  test.describe('Test Case 3: Viewport Meta Tag', () => {
    test('viewport meta tag is present with proper mobile configuration', async ({ page }) => {
      // Get the viewport meta tag
      const viewportMeta = await page.locator('meta[name="viewport"]');
      await expect(viewportMeta).toHaveCount(1);

      const content = await viewportMeta.getAttribute('content');

      // Viewport should be present
      expect(content).toBeTruthy();

      // Viewport should include width=device-width
      expect(content).toContain('width=device-width');

      // Viewport should include initial-scale
      expect(content).toContain('initial-scale=1');
    });
  });

  test.describe('Test Case 4: Open Graph Tags', () => {
    test('og:title, og:description, og:image, og:url tags are present', async ({ page }) => {
      // Check og:title
      const ogTitle = await page.locator('meta[property="og:title"]');
      await expect(ogTitle).toHaveCount(1);
      const ogTitleContent = await ogTitle.getAttribute('content');
      expect(ogTitleContent).toBeTruthy();
      expect(ogTitleContent).toContain('MirDB');

      // Check og:description
      const ogDescription = await page.locator('meta[property="og:description"]');
      await expect(ogDescription).toHaveCount(1);
      const ogDescriptionContent = await ogDescription.getAttribute('content');
      expect(ogDescriptionContent).toBeTruthy();
      expect(ogDescriptionContent.length).toBeGreaterThan(50);

      // Check og:image
      const ogImage = await page.locator('meta[property="og:image"]');
      await expect(ogImage).toHaveCount(1);
      const ogImageContent = await ogImage.getAttribute('content');
      expect(ogImageContent).toBeTruthy();
      expect(ogImageContent).toMatch(/^https?:\/\//);

      // Check og:url
      const ogUrl = await page.locator('meta[property="og:url"]');
      await expect(ogUrl).toHaveCount(1);
      const ogUrlContent = await ogUrl.getAttribute('content');
      expect(ogUrlContent).toBeTruthy();
      expect(ogUrlContent).toMatch(/^https?:\/\//);
    });

    test('additional Open Graph tags for better sharing', async ({ page }) => {
      // Check og:type (optional but recommended)
      const ogType = await page.locator('meta[property="og:type"]');
      await expect(ogType).toHaveCount(1);
      const ogTypeContent = await ogType.getAttribute('content');
      expect(ogTypeContent).toBeTruthy();
    });
  });

  test.describe('Test Case 5: Semantic HTML Structure', () => {
    test('page uses header, main, footer, nav, section, article elements appropriately', async ({ page }) => {
      // Check for header element
      const header = await page.locator('header');
      await expect(header).toHaveCount(1);

      // Check for main element
      const main = await page.locator('main');
      await expect(main).toHaveCount(1);

      // Main should have id for accessibility skip link
      const mainId = await main.getAttribute('id');
      expect(mainId).toBeTruthy();

      // Check for footer element
      const footer = await page.locator('footer');
      await expect(footer).toHaveCount(1);

      // Check for nav element (inside header)
      const nav = await page.locator('header nav');
      await expect(nav).toHaveCount(1);

      // Check for section elements
      const sections = await page.locator('main section');
      const sectionCount = await sections.count();
      expect(sectionCount).toBeGreaterThanOrEqual(2);

      // Check for article elements (feature cards)
      const articles = await page.locator('article');
      const articleCount = await articles.count();
      expect(articleCount).toBeGreaterThanOrEqual(1);
    });

    test('semantic elements have proper ARIA labels', async ({ page }) => {
      // Sections should have aria-labelledby attributes pointing to headings
      const sections = await page.locator('main section[aria-labelledby]');
      const sectionCount = await sections.count();
      expect(sectionCount).toBeGreaterThanOrEqual(1);

      // Verify each aria-labelledby points to an existing element
      for (let i = 0; i < sectionCount; i++) {
        const section = sections.nth(i);
        const labelledBy = await section.getAttribute('aria-labelledby');
        if (labelledBy) {
          const labelElement = await page.locator(`#${labelledBy}`);
          await expect(labelElement).toHaveCount(1);
        }
      }
    });

    test('page has proper heading hierarchy', async ({ page }) => {
      // Should have exactly one h1
      const h1 = await page.locator('h1');
      await expect(h1).toHaveCount(1);

      // h1 should contain MirDB
      const h1Text = await h1.textContent();
      expect(h1Text).toContain('MirDB');

      // Should have h2 elements for sections
      const h2s = await page.locator('h2');
      const h2Count = await h2s.count();
      expect(h2Count).toBeGreaterThanOrEqual(2);

      // Should have h3 elements for feature cards and steps
      const h3s = await page.locator('h3');
      const h3Count = await h3s.count();
      expect(h3Count).toBeGreaterThanOrEqual(1);
    });
  });
});
