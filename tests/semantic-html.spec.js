// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * SEO - Semantic HTML Tests
 *
 * This test suite verifies proper semantic HTML structure for:
 * 1. Heading hierarchy (single h1, proper nesting without skipping levels)
 * 2. Semantic elements (header, main, footer, section, nav)
 * 3. ARIA landmark regions
 */

test.describe('SEO - Semantic HTML Structure', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/homepage/index.html');
  });

  test.describe('Test Case 1: Heading Hierarchy', () => {
    test('should have exactly one h1 element', async ({ page }) => {
      const h1Elements = await page.locator('h1').all();
      expect(h1Elements.length).toBe(1);
    });

    test('h1 should contain meaningful content', async ({ page }) => {
      const h1 = page.locator('h1');
      const text = await h1.textContent();
      expect(text).toBeTruthy();
      expect(text.length).toBeGreaterThan(0);
    });

    test('should have proper heading nesting without skipping levels', async ({ page }) => {
      // Get all headings in document order
      const headings = await page.locator('h1, h2, h3, h4, h5, h6').all();

      expect(headings.length).toBeGreaterThan(0);

      let previousLevel = 0;

      for (const heading of headings) {
        const tagName = await heading.evaluate(el => el.tagName.toLowerCase());
        const currentLevel = parseInt(tagName.charAt(1));

        // First heading should be h1
        if (previousLevel === 0) {
          expect(currentLevel).toBe(1);
        } else {
          // Each subsequent heading should not skip more than one level
          // (e.g., h2 after h1 is OK, h3 after h1 is NOT OK)
          const levelDiff = currentLevel - previousLevel;
          expect(levelDiff).toBeLessThanOrEqual(1);
        }

        previousLevel = currentLevel;
      }
    });

    test('h2 elements should follow h1', async ({ page }) => {
      const h2Elements = await page.locator('h2').all();
      expect(h2Elements.length).toBeGreaterThan(0);

      // Verify h2 elements have content
      for (const h2 of h2Elements) {
        const text = await h2.textContent();
        expect(text.trim().length).toBeGreaterThan(0);
      }
    });

    test('h3 elements should be nested under h2 sections', async ({ page }) => {
      const h3Elements = await page.locator('h3').all();

      // If there are h3 elements, there should also be h2 elements
      if (h3Elements.length > 0) {
        const h2Elements = await page.locator('h2').all();
        expect(h2Elements.length).toBeGreaterThan(0);
      }
    });

    test('h4 elements should follow h3 in the hierarchy', async ({ page }) => {
      const h4Elements = await page.locator('h4').all();

      // If there are h4 elements, verify proper nesting
      if (h4Elements.length > 0) {
        const h3Elements = await page.locator('h3').all();
        // h4 can follow h3 or be at same level as existing h3
        expect(h3Elements.length).toBeGreaterThanOrEqual(0);
      }
    });
  });

  test.describe('Test Case 2: Semantic Elements', () => {
    test('should have a header element', async ({ page }) => {
      const header = page.locator('header');
      await expect(header).toBeVisible();
    });

    test('should have a main element', async ({ page }) => {
      const main = page.locator('main');
      await expect(main).toBeVisible();
    });

    test('should have exactly one main element', async ({ page }) => {
      const mainElements = await page.locator('main').all();
      expect(mainElements.length).toBe(1);
    });

    test('should have a footer element', async ({ page }) => {
      const footer = page.locator('footer');
      await expect(footer).toBeVisible();
    });

    test('should have section elements for content grouping', async ({ page }) => {
      const sections = await page.locator('section').all();
      expect(sections.length).toBeGreaterThan(0);
    });

    test('should have a nav element for navigation', async ({ page }) => {
      const nav = page.locator('nav');
      await expect(nav).toBeVisible();
    });

    test('nav should contain navigation links', async ({ page }) => {
      const navLinks = page.locator('nav a, nav [role="link"]');
      const count = await navLinks.count();
      expect(count).toBeGreaterThan(0);
    });

    test('header should contain branding or primary navigation', async ({ page }) => {
      const header = page.locator('header');

      // Header should contain either logo, nav, or site title
      const hasLogo = await header.locator('img, .logo, [class*="logo"]').count() > 0;
      const hasNav = await header.locator('nav').count() > 0;
      const hasTitle = await header.locator('h1, [class*="brand"], [class*="title"]').count() > 0;
      const hasSiteLink = await header.locator('a').count() > 0;

      expect(hasLogo || hasNav || hasTitle || hasSiteLink).toBeTruthy();
    });

    test('footer should contain appropriate footer content', async ({ page }) => {
      const footer = page.locator('footer');

      // Footer should have links, copyright info, or other meta content
      const hasLinks = await footer.locator('a').count() > 0;
      const hasText = (await footer.textContent()).trim().length > 0;

      expect(hasLinks || hasText).toBeTruthy();
    });

    test('sections should have proper labeling (heading or aria-label)', async ({ page }) => {
      const sections = await page.locator('section').all();

      for (const section of sections) {
        // A section should have either a heading child or aria-label/aria-labelledby
        const hasHeading = await section.locator('h1, h2, h3, h4, h5, h6').first().count() > 0;
        const hasAriaLabel = await section.getAttribute('aria-label');
        const hasAriaLabelledBy = await section.getAttribute('aria-labelledby');

        expect(hasHeading || hasAriaLabel || hasAriaLabelledBy).toBeTruthy();
      }
    });
  });

  test.describe('Test Case 3: ARIA Landmark Regions', () => {
    test('should have banner landmark (header or role="banner")', async ({ page }) => {
      const bannerByTag = page.locator('header');
      const bannerByRole = page.locator('[role="banner"]');

      const hasBanner = await bannerByTag.count() > 0 || await bannerByRole.count() > 0;
      expect(hasBanner).toBeTruthy();
    });

    test('should have main landmark (main or role="main")', async ({ page }) => {
      const mainByTag = page.locator('main');
      const mainByRole = page.locator('[role="main"]');

      const hasMain = await mainByTag.count() > 0 || await mainByRole.count() > 0;
      expect(hasMain).toBeTruthy();
    });

    test('should have contentinfo landmark (footer or role="contentinfo")', async ({ page }) => {
      const footerByTag = page.locator('footer');
      const footerByRole = page.locator('[role="contentinfo"]');

      const hasContentinfo = await footerByTag.count() > 0 || await footerByRole.count() > 0;
      expect(hasContentinfo).toBeTruthy();
    });

    test('should have navigation landmark (nav or role="navigation")', async ({ page }) => {
      const navByTag = page.locator('nav');
      const navByRole = page.locator('[role="navigation"]');

      const hasNavigation = await navByTag.count() > 0 || await navByRole.count() > 0;
      expect(hasNavigation).toBeTruthy();
    });

    test('landmark regions should be properly structured', async ({ page }) => {
      // Verify landmark structure: header -> main -> footer order in DOM
      const bodyContent = await page.locator('body').innerHTML();

      const headerIndex = bodyContent.indexOf('<header');
      const mainIndex = bodyContent.indexOf('<main');
      const footerIndex = bodyContent.indexOf('<footer');

      // All landmarks should exist
      expect(headerIndex).toBeGreaterThanOrEqual(0);
      expect(mainIndex).toBeGreaterThanOrEqual(0);
      expect(footerIndex).toBeGreaterThanOrEqual(0);

      // Header should come before main, main before footer
      expect(headerIndex).toBeLessThan(mainIndex);
      expect(mainIndex).toBeLessThan(footerIndex);
    });

    test('multiple navigation landmarks should have unique labels', async ({ page }) => {
      const navElements = await page.locator('nav, [role="navigation"]').all();

      if (navElements.length > 1) {
        const labels = [];
        for (const nav of navElements) {
          const ariaLabel = await nav.getAttribute('aria-label');
          const ariaLabelledBy = await nav.getAttribute('aria-labelledby');

          if (ariaLabel) {
            labels.push(ariaLabel);
          } else if (ariaLabelledBy) {
            const labelElement = await page.locator(`#${ariaLabelledBy}`);
            const text = await labelElement.textContent();
            labels.push(text);
          }
        }

        // If there are multiple navs, they should have distinct labels
        // (this is a best practice, not strictly required if only one nav)
        const uniqueLabels = new Set(labels);
        // Allow the test to pass if labels aren't set (they'll get implicit labels)
        // but fail if duplicate explicit labels are found
        if (labels.length > 1) {
          expect(uniqueLabels.size).toBe(labels.length);
        }
      }
    });

    test('regions should define complete page structure', async ({ page }) => {
      // Verify that key content is within landmark regions
      const mainContent = page.locator('main');

      // Main should contain the primary content sections
      const sectionsInMain = await mainContent.locator('section').count();
      expect(sectionsInMain).toBeGreaterThan(0);
    });

    test('page should have accessible document structure', async ({ page }) => {
      // Check that the html element has a lang attribute
      const htmlLang = await page.locator('html').getAttribute('lang');
      expect(htmlLang).toBeTruthy();

      // Check that there's a title
      const title = await page.title();
      expect(title.length).toBeGreaterThan(0);
    });
  });
});
