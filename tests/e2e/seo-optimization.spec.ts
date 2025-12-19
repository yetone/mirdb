import { test, expect } from '@playwright/test';

test.describe('SEO Optimization', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Page has title tag containing MirDB', async ({ page }) => {
    // Test Case 1: Check for title tag
    // Expected: Page has title tag containing 'MirDB'
    const title = await page.title();
    expect(title.toLowerCase()).toContain('mirdb');
  });

  test('TC2: Meta description present with relevant keywords', async ({ page }) => {
    // Test Case 2: Check for meta description
    // Expected: Meta description present with relevant keywords
    const metaDescription = page.locator('meta[name="description"]');
    await expect(metaDescription).toHaveCount(1);

    const content = await metaDescription.getAttribute('content');
    expect(content).toBeTruthy();
    expect(content!.length).toBeGreaterThan(50);

    // Check for relevant keywords in the description
    const lowerContent = content!.toLowerCase();
    expect(lowerContent).toMatch(/mirdb|key-value|memcached|persistent/);
  });

  test('TC3: Viewport meta tag present for responsive design', async ({ page }) => {
    // Test Case 3: Check for viewport meta tag
    // Expected: Viewport meta tag present for responsive design
    const viewport = page.locator('meta[name="viewport"]');
    await expect(viewport).toHaveCount(1);

    const content = await viewport.getAttribute('content');
    expect(content).toBeTruthy();
    expect(content!.toLowerCase()).toContain('width=device-width');
  });

  test('TC4: Open Graph title tag present', async ({ page }) => {
    // Test Case 4: Check Open Graph title tag
    // Expected: og:title meta tag present
    const ogTitle = page.locator('meta[property="og:title"]');
    await expect(ogTitle).toHaveCount(1);

    const content = await ogTitle.getAttribute('content');
    expect(content).toBeTruthy();
    expect(content!.toLowerCase()).toContain('mirdb');
  });

  test('TC5: Open Graph description tag present', async ({ page }) => {
    // Test Case 5: Check Open Graph description tag
    // Expected: og:description meta tag present
    const ogDescription = page.locator('meta[property="og:description"]');
    await expect(ogDescription).toHaveCount(1);

    const content = await ogDescription.getAttribute('content');
    expect(content).toBeTruthy();
    expect(content!.length).toBeGreaterThan(20);
  });

  test('TC6: Canonical link tag present', async ({ page }) => {
    // Test Case 6: Check canonical URL
    // Expected: Canonical link tag present
    const canonical = page.locator('link[rel="canonical"]');
    await expect(canonical).toHaveCount(1);

    const href = await canonical.getAttribute('href');
    expect(href).toBeTruthy();
    // Canonical should be a valid URL
    expect(href).toMatch(/^https?:\/\//);
  });

  test('TC7: Page has exactly one h1 element', async ({ page }) => {
    // Test Case 7: Verify single h1 tag
    // Expected: Page has exactly one h1 element
    const h1Elements = page.locator('h1');
    const count = await h1Elements.count();
    expect(count).toBe(1);

    // Verify the h1 contains meaningful content
    const h1Text = await h1Elements.textContent();
    expect(h1Text).toBeTruthy();
    expect(h1Text!.trim().length).toBeGreaterThan(0);
  });
});
