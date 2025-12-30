// @ts-check
const { test, expect } = require('@playwright/test');

const pageUrl = 'file://' + process.cwd() + '/index.html';

test.describe('Semantic HTML Structure', () => {

  /**
   * Test Case 1: Check for semantic HTML5 elements
   * Expected: Page uses header, nav, main, section, and footer elements appropriately
   */
  test('should use semantic HTML5 elements (header, nav, main, section, footer)', async ({ page }) => {
    await page.goto(pageUrl);

    // Check for header element
    const headerCount = await page.locator('header').count();
    expect(headerCount).toBeGreaterThanOrEqual(1);

    // Check for nav element
    const navCount = await page.locator('nav').count();
    expect(navCount).toBeGreaterThanOrEqual(1);

    // Check for main element
    const mainCount = await page.locator('main').count();
    expect(mainCount).toBe(1);

    // Check for section elements
    const sectionCount = await page.locator('section').count();
    expect(sectionCount).toBeGreaterThanOrEqual(1);

    // Check for footer element
    const footerCount = await page.locator('footer').count();
    expect(footerCount).toBeGreaterThanOrEqual(1);

    // Verify article elements are used appropriately (for feature cards)
    const articleCount = await page.locator('article').count();
    expect(articleCount).toBeGreaterThanOrEqual(0); // Articles are optional but semantic
  });

  /**
   * Test Case 2: Check page title tag
   * Expected: Page has descriptive title tag containing 'MirDB'
   */
  test('should have descriptive title tag containing MirDB', async ({ page }) => {
    await page.goto(pageUrl);

    const title = await page.title();

    // Title should exist and not be empty
    expect(title).toBeTruthy();
    expect(title.length).toBeGreaterThan(0);

    // Title should contain 'MirDB'
    expect(title.toLowerCase()).toContain('mirdb');

    // Title should be descriptive (not just the product name)
    expect(title.length).toBeGreaterThan(5);
  });

  /**
   * Test Case 3: Check meta description
   * Expected: Page has meta description describing MirDB
   */
  test('should have meta description describing MirDB', async ({ page }) => {
    await page.goto(pageUrl);

    // Check for meta description tag
    const metaDescription = await page.locator('meta[name="description"]');
    const metaCount = await metaDescription.count();

    expect(metaCount).toBe(1);

    // Get the content attribute
    const content = await metaDescription.getAttribute('content');

    // Description should exist and not be empty
    expect(content).toBeTruthy();
    expect(content.length).toBeGreaterThan(10);

    // Description should mention MirDB or key features
    const lowerContent = content.toLowerCase();
    const hasMirDB = lowerContent.includes('mirdb');
    const hasKeyValue = lowerContent.includes('key-value') || lowerContent.includes('key value');
    const hasPersistent = lowerContent.includes('persistent');
    const hasMemcached = lowerContent.includes('memcached');

    // Should mention at least MirDB or describe the product
    expect(hasMirDB || hasKeyValue || hasPersistent || hasMemcached).toBe(true);
  });

  /**
   * Test Case 4: Check h1 element
   * Expected: Page has exactly one h1 element containing product name
   */
  test('should have exactly one h1 element containing product name', async ({ page }) => {
    await page.goto(pageUrl);

    // Count h1 elements
    const h1Elements = await page.locator('h1').all();

    // Should have exactly one h1
    expect(h1Elements.length).toBe(1);

    // Get the h1 text content
    const h1Text = await h1Elements[0].textContent();

    // h1 should contain MirDB (the product name)
    expect(h1Text.toLowerCase()).toContain('mirdb');
  });

  /**
   * Test Case 5: Validate HTML structure
   * Expected: HTML passes basic structure validation without critical errors
   */
  test('should have valid HTML structure', async ({ page }) => {
    await page.goto(pageUrl);

    // Check for DOCTYPE
    const hasDoctype = await page.evaluate(() => {
      return document.doctype !== null;
    });
    expect(hasDoctype).toBe(true);

    // Check for html lang attribute
    const htmlLang = await page.locator('html').getAttribute('lang');
    expect(htmlLang).toBeTruthy();

    // Check for charset meta tag
    const charsetMeta = await page.locator('meta[charset]').count();
    expect(charsetMeta).toBe(1);

    // Check for viewport meta tag
    const viewportMeta = await page.locator('meta[name="viewport"]').count();
    expect(viewportMeta).toBe(1);

    // Verify proper nesting - main should not be inside header or footer
    const mainInHeader = await page.locator('header main').count();
    const mainInFooter = await page.locator('footer main').count();
    expect(mainInHeader).toBe(0);
    expect(mainInFooter).toBe(0);

    // Verify header comes before main
    const elements = await page.evaluate(() => {
      const header = document.querySelector('header');
      const main = document.querySelector('main');
      const footer = document.querySelector('footer');

      if (!header || !main || !footer) return null;

      const headerPos = header.compareDocumentPosition(main);
      const mainFooterPos = main.compareDocumentPosition(footer);

      // Node.DOCUMENT_POSITION_FOLLOWING = 4
      return {
        headerBeforeMain: (headerPos & 4) === 4,
        mainBeforeFooter: (mainFooterPos & 4) === 4
      };
    });

    expect(elements).not.toBeNull();
    expect(elements.headerBeforeMain).toBe(true);
    expect(elements.mainBeforeFooter).toBe(true);

    // Check that h2s are inside sections (proper semantic grouping)
    const h2Count = await page.locator('section h2').count();
    const totalH2 = await page.locator('h2').count();

    // Most h2 elements should be within sections
    expect(h2Count).toBeGreaterThanOrEqual(totalH2 - 1); // Allow for edge cases
  });

  /**
   * Additional test: Verify heading hierarchy
   */
  test('should have proper heading hierarchy', async ({ page }) => {
    await page.goto(pageUrl);

    // Get all headings in document order
    const headings = await page.evaluate(() => {
      const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      return Array.from(allHeadings).map(h => ({
        level: parseInt(h.tagName[1]),
        text: h.textContent?.trim().substring(0, 50)
      }));
    });

    // Should have at least one heading
    expect(headings.length).toBeGreaterThan(0);

    // First heading should be h1
    expect(headings[0].level).toBe(1);

    // Check for skipped heading levels (e.g., h1 -> h3 without h2)
    let previousLevel = 0;
    let skippedLevels = false;

    for (const heading of headings) {
      if (heading.level > previousLevel + 1) {
        skippedLevels = true;
        console.log(`Skipped level: h${previousLevel} -> h${heading.level} ("${heading.text}")`);
      }
      previousLevel = heading.level;
    }

    expect(skippedLevels).toBe(false);
  });

  /**
   * Additional test: Verify semantic landmarks are properly used
   */
  test('should use semantic landmarks correctly', async ({ page }) => {
    await page.goto(pageUrl);

    // Check that nav is used for navigation
    const nav = await page.locator('nav');
    if (await nav.count() > 0) {
      // Nav should contain links
      const linksInNav = await page.locator('nav a').count();
      expect(linksInNav).toBeGreaterThan(0);
    }

    // Check that footer contains relevant content (not main content)
    const footerLinks = await page.locator('footer a').count();
    expect(footerLinks).toBeGreaterThanOrEqual(0); // Footer can have links

    // Main should contain the primary content sections
    const sectionsInMain = await page.locator('main section').count();
    expect(sectionsInMain).toBeGreaterThan(0);
  });
});
