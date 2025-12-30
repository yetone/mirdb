// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Semantic HTML Structure - Document Structure and SEO', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Page contains semantic landmarks (header, main, footer, nav)', async ({ page }) => {
    // Test Case 1: Query for semantic landmarks (header, main, footer, nav)
    // Expected: Page contains header, main, and footer landmark elements

    // Verify header element exists
    const header = page.locator('header');
    await expect(header).toBeVisible();
    const headerCount = await header.count();
    expect(headerCount).toBeGreaterThanOrEqual(1);

    // Verify main element exists
    const main = page.locator('main');
    await expect(main).toBeVisible();
    const mainCount = await main.count();
    expect(mainCount).toBe(1); // Should have exactly one main element

    // Verify footer element exists
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();
    const footerCount = await footer.count();
    expect(footerCount).toBeGreaterThanOrEqual(1);

    // Verify nav element exists (either standalone or inside footer)
    const nav = page.locator('nav');
    const navCount = await nav.count();
    expect(navCount).toBeGreaterThanOrEqual(1);
  });

  test('TC2: Page has descriptive title element containing MirDB', async ({ page }) => {
    // Test Case 2: Verify page has descriptive title element
    // Expected: Title element exists and contains 'MirDB'

    const title = await page.title();

    // Verify title is not empty
    expect(title).toBeTruthy();
    expect(title.length).toBeGreaterThan(0);

    // Verify title contains 'MirDB'
    expect(title).toContain('MirDB');

    // Verify title is descriptive (more than just the product name)
    expect(title.length).toBeGreaterThan(5);
  });

  test('TC3: Meta description tag exists and describes MirDB', async ({ page }) => {
    // Test Case 3: Verify meta description tag exists
    // Expected: Meta description is present and describes MirDB

    const metaDescription = page.locator('meta[name="description"]');
    const descriptionCount = await metaDescription.count();

    // Verify meta description exists
    expect(descriptionCount).toBe(1);

    // Get the content attribute
    const content = await metaDescription.getAttribute('content');

    // Verify content is not empty
    expect(content).toBeTruthy();
    expect(content.length).toBeGreaterThan(0);

    // Verify content mentions MirDB or key features
    const contentLower = content.toLowerCase();
    const containsRelevantContent =
      contentLower.includes('mirdb') ||
      contentLower.includes('key-value') ||
      contentLower.includes('memcached') ||
      contentLower.includes('persistent');

    expect(containsRelevantContent).toBeTruthy();
  });

  test('TC4: Open Graph meta tags are present for social sharing', async ({ page }) => {
    // Test Case 4: Verify Open Graph meta tags for social sharing
    // Expected: og:title, og:description, og:image tags are present

    // Verify og:title exists
    const ogTitle = page.locator('meta[property="og:title"]');
    const ogTitleCount = await ogTitle.count();
    expect(ogTitleCount).toBe(1);

    const ogTitleContent = await ogTitle.getAttribute('content');
    expect(ogTitleContent).toBeTruthy();
    expect(ogTitleContent.length).toBeGreaterThan(0);

    // Verify og:description exists
    const ogDescription = page.locator('meta[property="og:description"]');
    const ogDescriptionCount = await ogDescription.count();
    expect(ogDescriptionCount).toBe(1);

    const ogDescriptionContent = await ogDescription.getAttribute('content');
    expect(ogDescriptionContent).toBeTruthy();
    expect(ogDescriptionContent.length).toBeGreaterThan(0);

    // Verify og:image exists
    const ogImage = page.locator('meta[property="og:image"]');
    const ogImageCount = await ogImage.count();
    expect(ogImageCount).toBe(1);

    const ogImageContent = await ogImage.getAttribute('content');
    expect(ogImageContent).toBeTruthy();
    expect(ogImageContent.length).toBeGreaterThan(0);
  });

  test('TC5: Page uses semantic section elements for content organization', async ({ page }) => {
    // Test Case 5: Verify page uses semantic section elements
    // Expected: Content is organized using section and/or article elements

    // Count section elements
    const sections = page.locator('section');
    const sectionCount = await sections.count();

    // Count article elements
    const articles = page.locator('article');
    const articleCount = await articles.count();

    // Total semantic content elements should be at least 1
    const totalSemanticElements = sectionCount + articleCount;
    expect(totalSemanticElements).toBeGreaterThanOrEqual(1);

    // Verify sections have identifiable purposes (id or heading)
    if (sectionCount > 0) {
      const allSections = await sections.all();
      for (const section of allSections) {
        // Each section should have an id or a heading inside it
        const hasId = await section.getAttribute('id');
        const hasHeading = await section.locator('h1, h2, h3, h4, h5, h6').count();

        const hasIdentifiablePurpose = hasId || hasHeading > 0;
        expect(hasIdentifiablePurpose).toBeTruthy();
      }
    }
  });
});
