import { test, expect } from '@playwright/test';

test.describe('Accessibility - Semantic HTML', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Page has exactly one h1 element containing the product name', async ({ page }) => {
    // Find all h1 elements on the page
    const h1Elements = page.locator('h1');
    const h1Count = await h1Elements.count();

    // Page should have exactly one h1 element
    expect(h1Count).toBe(1);

    // The h1 should contain the product name "MirDB"
    const h1Text = await h1Elements.first().textContent();
    expect(h1Text).toContain('MirDB');
  });

  test('TC2: Headings follow proper hierarchy without skipping levels', async ({ page }) => {
    // Get all heading elements
    const headings = await page.$$eval('h1, h2, h3, h4, h5, h6', (elements) => {
      return elements.map((el) => ({
        tag: el.tagName.toLowerCase(),
        level: parseInt(el.tagName.charAt(1)),
        text: el.textContent?.trim().substring(0, 50) || ''
      }));
    });

    // Ensure there are headings
    expect(headings.length).toBeGreaterThan(0);

    // First heading should be h1
    expect(headings[0].level).toBe(1);

    // Check that heading levels don't skip (e.g., no h1 directly followed by h3)
    for (let i = 1; i < headings.length; i++) {
      const currentLevel = headings[i].level;
      const previousLevel = headings[i - 1].level;

      // When going to a deeper level, it should only go one level deeper at most
      // (e.g., h1 -> h2 is ok, h1 -> h3 is not ok)
      // Going to same level or shallower level is always ok
      if (currentLevel > previousLevel) {
        expect(currentLevel - previousLevel).toBeLessThanOrEqual(1);
      }
    }
  });

  test('TC3: Page uses semantic HTML elements (header, nav, main, section, article, footer)', async ({ page }) => {
    // Check for header element
    const header = page.locator('header');
    const headerExists = (await header.count()) > 0 || (await page.locator('nav.navbar').count()) > 0;
    expect(headerExists).toBeTruthy();

    // Check for nav element
    const nav = page.locator('nav');
    const navCount = await nav.count();
    expect(navCount).toBeGreaterThan(0);

    // Check for main element OR main content sections
    const main = page.locator('main');
    const mainCount = await main.count();
    const sections = page.locator('body > section, body > .container > section');
    const sectionCount = await sections.count();

    // Page should have either a main element or multiple sections
    const hasMainContent = mainCount > 0 || sectionCount > 0;
    expect(hasMainContent).toBeTruthy();

    // Check for section elements
    const allSections = page.locator('section');
    const allSectionCount = await allSections.count();
    expect(allSectionCount).toBeGreaterThan(0);

    // Check for footer element
    const footer = page.locator('footer');
    const footerCount = await footer.count();
    expect(footerCount).toBeGreaterThan(0);
  });

  test('TC4: Major page regions have proper ARIA landmarks or HTML5 equivalents', async ({ page }) => {
    // Check for navigation landmark (nav element or role="navigation")
    const navLandmark = page.locator('nav, [role="navigation"]');
    const hasNavigation = (await navLandmark.count()) > 0;
    expect(hasNavigation).toBeTruthy();

    // Check for main content landmark (main element or role="main")
    const mainLandmark = page.locator('main, [role="main"]');
    const hasMain = (await mainLandmark.count()) > 0;

    // If no explicit main landmark, check for primary content sections
    const contentSections = page.locator('section');
    const hasContentSections = (await contentSections.count()) > 0;

    // Either main landmark or content sections should exist
    expect(hasMain || hasContentSections).toBeTruthy();

    // Check for banner landmark (header element or role="banner")
    const bannerLandmark = page.locator('header, [role="banner"]');
    const bannerExists = (await bannerLandmark.count()) > 0;

    // Check for navbar as alternative to header
    const navbarExists = (await page.locator('nav.navbar').count()) > 0;
    expect(bannerExists || navbarExists).toBeTruthy();

    // Check for contentinfo landmark (footer element or role="contentinfo")
    const footerLandmark = page.locator('footer, [role="contentinfo"]');
    const hasFooter = (await footerLandmark.count()) > 0;
    expect(hasFooter).toBeTruthy();

    // Check for region landmarks (section with aria-label or aria-labelledby, or named regions)
    const sections = page.locator('section[id], section[aria-label], section[aria-labelledby], [role="region"]');
    const regionCount = await sections.count();
    expect(regionCount).toBeGreaterThan(0);
  });
});
