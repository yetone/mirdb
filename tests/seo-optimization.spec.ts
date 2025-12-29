import { test, expect } from '@playwright/test';

test.describe('SEO Optimization (NFR-6)', () => {
  test.describe('Test Case 1: Title Meta Tag', () => {
    test('page has title tag with descriptive content including MirDB', async ({ page }) => {
      // Input: Check for title meta tag
      // Expected: Page has <title> tag with descriptive content including 'MirDB'
      await page.goto('/');

      // Get the page title
      const title = await page.title();

      // Verify title exists and contains MirDB
      expect(title).toBeTruthy();
      expect(title).toContain('MirDB');

      // Verify title is descriptive (more than just product name)
      expect(title.length).toBeGreaterThan(10);

      // Verify title element exists in head
      const titleElement = await page.locator('head title');
      await expect(titleElement).toBeAttached();
    });
  });

  test.describe('Test Case 2: Meta Description', () => {
    test('page has meta description tag with relevant content', async ({ page }) => {
      // Input: Check for meta description
      // Expected: Page has meta description tag with relevant content
      await page.goto('/');

      // Check for meta description tag
      const metaDescription = page.locator('meta[name="description"]');
      await expect(metaDescription).toBeAttached();

      // Get the description content
      const content = await metaDescription.getAttribute('content');

      // Verify description exists and has relevant content
      expect(content).toBeTruthy();
      expect(content!.length).toBeGreaterThan(50);

      // Verify description contains relevant keywords
      const contentLower = content!.toLowerCase();
      expect(
        contentLower.includes('mirdb') ||
        contentLower.includes('key-value') ||
        contentLower.includes('memcached') ||
        contentLower.includes('persistent')
      ).toBe(true);
    });
  });

  test.describe('Test Case 3: Viewport Meta Tag', () => {
    test('page has viewport meta tag for responsive design', async ({ page }) => {
      // Input: Check for viewport meta tag
      // Expected: Page has viewport meta tag for responsive design
      await page.goto('/');

      // Check for viewport meta tag
      const viewportMeta = page.locator('meta[name="viewport"]');
      await expect(viewportMeta).toBeAttached();

      // Get the viewport content
      const content = await viewportMeta.getAttribute('content');

      // Verify viewport is configured for responsive design
      expect(content).toBeTruthy();
      expect(content).toContain('width=device-width');
    });
  });

  test.describe('Test Case 4: Semantic HTML Structure', () => {
    test('page uses semantic elements: header, main, footer, nav, section', async ({ page }) => {
      // Input: Check semantic HTML structure
      // Expected: Page uses semantic elements: header, main, footer, nav, section
      await page.goto('/');

      // Check for header element
      const header = page.locator('header');
      await expect(header).toBeAttached();

      // Check for main element
      const main = page.locator('main');
      await expect(main).toBeAttached();

      // Check for footer element
      const footer = page.locator('footer');
      await expect(footer).toBeAttached();

      // Check for nav element
      const nav = page.locator('nav');
      await expect(nav).toBeAttached();

      // Check for section elements
      const sections = page.locator('section');
      const sectionCount = await sections.count();
      expect(sectionCount).toBeGreaterThan(0);
    });
  });

  test.describe('Test Case 5: Open Graph Tags', () => {
    test('page has og:title and og:description meta tags for social sharing', async ({ page }) => {
      // Input: Check for Open Graph tags
      // Expected: Page has og:title, og:description meta tags for social sharing
      await page.goto('/');

      // Check for og:title meta tag
      const ogTitle = page.locator('meta[property="og:title"]');
      await expect(ogTitle).toBeAttached();
      const ogTitleContent = await ogTitle.getAttribute('content');
      expect(ogTitleContent).toBeTruthy();
      expect(ogTitleContent).toContain('MirDB');

      // Check for og:description meta tag
      const ogDescription = page.locator('meta[property="og:description"]');
      await expect(ogDescription).toBeAttached();
      const ogDescContent = await ogDescription.getAttribute('content');
      expect(ogDescContent).toBeTruthy();
      expect(ogDescContent!.length).toBeGreaterThan(20);
    });
  });
});
