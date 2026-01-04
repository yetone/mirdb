/**
 * Accessibility Tests: Semantic HTML and Heading Hierarchy
 * Tests NFR-3: WCAG 2.1 AA compliance - semantic structure
 */
const { test, expect } = require('@playwright/test');

test.describe('Accessibility - Semantic HTML', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Test Case 1: Single h1 Element', () => {
    test('Page contains exactly one h1 element', async ({ page }) => {
      const h1Elements = await page.locator('h1').all();
      expect(h1Elements.length).toBe(1);
    });

    test('h1 element contains the product name (MirDB)', async ({ page }) => {
      const h1 = page.locator('h1');
      await expect(h1).toBeVisible();
      await expect(h1).toContainText('MirDB');
    });
  });

  test.describe('Test Case 2: Heading Hierarchy', () => {
    test('Headings follow logical order without skipping levels', async ({ page }) => {
      const headings = await page.locator('h1, h2, h3, h4, h5, h6').all();
      let lastLevel = 0;
      const issues = [];

      for (const heading of headings) {
        const tagName = await heading.evaluate(el => el.tagName);
        const currentLevel = parseInt(tagName.charAt(1));
        // Check that we don't skip more than one level when going deeper
        if (currentLevel > lastLevel + 1 && lastLevel !== 0) {
          issues.push(`Heading level skipped: from h${lastLevel} to ${tagName.toLowerCase()}`);
        }
        lastLevel = currentLevel;
      }

      expect(issues).toEqual([]);
    });

    test('Page starts with h1 as the first heading', async ({ page }) => {
      const firstHeading = page.locator('h1, h2, h3, h4, h5, h6').first();
      await expect(firstHeading).toBeVisible();
      const tagName = await firstHeading.evaluate(el => el.tagName);
      expect(tagName).toBe('H1');
    });

    test('h2 elements are used for section headings', async ({ page }) => {
      const h2Elements = await page.locator('h2').all();
      expect(h2Elements.length).toBeGreaterThanOrEqual(1);
    });

    test('h3 elements are nested under h2 sections', async ({ page }) => {
      const sections = await page.locator('section').all();
      let hasProperNesting = true;

      for (const section of sections) {
        const h2 = await section.locator('h2').count();
        const h3Count = await section.locator('h3').count();

        // If section has h3s, it should also have an h2
        if (h3Count > 0 && h2 === 0) {
          hasProperNesting = false;
        }
      }

      expect(hasProperNesting).toBe(true);
    });
  });

  test.describe('Test Case 3: Semantic HTML Elements', () => {
    test('Page uses header element', async ({ page }) => {
      const header = page.locator('header');
      await expect(header).toBeVisible();
    });

    test('Page uses main element', async ({ page }) => {
      const main = page.locator('main');
      await expect(main).toBeVisible();
    });

    test('Page uses nav element for navigation', async ({ page }) => {
      const nav = page.locator('nav');
      await expect(nav).toBeVisible();
    });

    test('Page uses section elements for content sections', async ({ page }) => {
      const sections = await page.locator('section').all();
      expect(sections.length).toBeGreaterThanOrEqual(1);
    });

    test('Page uses footer element', async ({ page }) => {
      const footer = page.locator('footer');
      await expect(footer).toBeVisible();
    });

    test('Nav element is placed inside header', async ({ page }) => {
      const navInHeader = page.locator('header nav');
      await expect(navInHeader).toBeVisible();
    });

    test('Sections are placed inside main element', async ({ page }) => {
      const sectionsInMain = await page.locator('main section').all();
      expect(sectionsInMain.length).toBeGreaterThanOrEqual(1);
    });
  });

  test.describe('Test Case 4: Landmark Regions', () => {
    test('Page has banner landmark (header)', async ({ page }) => {
      // Banner landmark is implicit for <header> at the top level
      const header = page.locator('body > header');
      await expect(header).toBeVisible();
    });

    test('Page has main landmark', async ({ page }) => {
      // Main landmark is implicit for <main> element
      const main = page.locator('main');
      await expect(main).toBeVisible();
    });

    test('Page has navigation landmark', async ({ page }) => {
      // Navigation landmark is implicit for <nav> element
      const nav = page.locator('nav');
      await expect(nav).toBeVisible();
    });

    test('Page has contentinfo landmark (footer)', async ({ page }) => {
      // Contentinfo landmark is implicit for <footer> at the top level
      const footer = page.locator('body > footer');
      await expect(footer).toBeVisible();
    });

    test('Only one main landmark exists', async ({ page }) => {
      const mainElements = await page.locator('main').all();
      expect(mainElements.length).toBe(1);
    });

    test('Main content is not nested inside header or footer', async ({ page }) => {
      const mainInHeader = await page.locator('header main').count();
      const mainInFooter = await page.locator('footer main').count();

      expect(mainInHeader).toBe(0);
      expect(mainInFooter).toBe(0);
    });
  });
});
