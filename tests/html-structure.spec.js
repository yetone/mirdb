// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

/**
 * HTML Structure and Semantics Test Suite
 *
 * Validates that the homepage follows proper HTML5 semantic structure
 * including DOCTYPE, lang attribute, meta tags, and semantic elements.
 */

// Helper to get file URL (consistent with other tests)
const getFileUrl = () => {
  return 'file://' + path.resolve(__dirname, '..', 'index.html');
};

test.describe('HTML Structure and Semantics', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(getFileUrl());
    await page.waitForLoadState('domcontentloaded');
  });

  test.describe('Document Structure', () => {
    test('TC1: should have DOCTYPE declaration at start of document', async ({ page }) => {
      // Get the raw HTML content to check DOCTYPE
      const html = await page.content();
      // DOCTYPE should be at the very beginning (case-insensitive)
      expect(html.trim().toLowerCase().startsWith('<!doctype html>')).toBe(true);
    });

    test('TC2: should have html element with lang="en" attribute', async ({ page }) => {
      const htmlElement = page.locator('html');
      await expect(htmlElement).toHaveAttribute('lang', 'en');
    });

    test('TC3: should have meta charset="UTF-8" in head', async ({ page }) => {
      const metaCharset = page.locator('head meta[charset="UTF-8"]');
      await expect(metaCharset).toHaveCount(1);
    });

    test('TC4: should have viewport meta tag for responsive design', async ({ page }) => {
      const viewportMeta = page.locator('head meta[name="viewport"]');
      await expect(viewportMeta).toHaveCount(1);

      const content = await viewportMeta.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content).toContain('width=device-width');
      expect(content).toContain('initial-scale=1');
    });
  });

  test.describe('Semantic Elements', () => {
    test('TC5: should have semantic header element with logo and navigation', async ({ page }) => {
      // Check header element exists
      const header = page.locator('body > header');
      await expect(header).toHaveCount(1);

      // Header should contain navigation
      const navInHeader = header.locator('nav');
      await expect(navInHeader).toHaveCount(1);

      // Header should contain logo (either text logo link or img)
      const logoLink = header.locator('a.logo, a.nav-logo, .logo, [class*="logo"]');
      const logoCount = await logoLink.count();
      expect(logoCount).toBeGreaterThanOrEqual(1);
    });

    test('TC6: should have semantic main element wrapping primary content', async ({ page }) => {
      // Check main element exists
      const main = page.locator('body > main');
      await expect(main).toHaveCount(1);

      // Main should contain primary content sections
      const sectionsInMain = main.locator('section');
      const sectionCount = await sectionsInMain.count();
      expect(sectionCount).toBeGreaterThanOrEqual(1);
    });

    test('TC7: should have semantic nav element containing navigation links', async ({ page }) => {
      // Check nav element exists
      const nav = page.locator('nav');
      await expect(nav).toHaveCount(1);

      // Nav should contain links
      const linksInNav = nav.locator('a');
      const linkCount = await linksInNav.count();
      expect(linkCount).toBeGreaterThanOrEqual(2);

      // Nav should have links to key sections
      const navLinksHtml = await nav.innerHTML();
      expect(navLinksHtml.toLowerCase()).toMatch(/features|getting[- ]started|github/i);
    });

    test('TC8: should have semantic footer element with copyright and links', async ({ page }) => {
      // Check footer element exists
      const footer = page.locator('body > footer');
      await expect(footer).toHaveCount(1);

      // Footer should contain copyright information
      const footerText = await footer.textContent();
      expect(footerText).toBeTruthy();
      // Check for copyright symbol or year
      expect(footerText).toMatch(/©|copyright|\d{4}/i);

      // Footer should contain links (GitHub link)
      const linksInFooter = footer.locator('a');
      const linkCount = await linksInFooter.count();
      expect(linkCount).toBeGreaterThanOrEqual(1);
    });
  });

  test.describe('Heading Hierarchy', () => {
    test('TC9: should have proper heading hierarchy starting with h1', async ({ page }) => {
      // There should be exactly one h1
      const h1Elements = page.locator('h1');
      await expect(h1Elements).toHaveCount(1);

      // h1 should contain the product name
      const h1Text = await h1Elements.textContent();
      expect(h1Text?.toLowerCase()).toContain('mirdb');
    });

    test('TC10: should have h2 elements for section headings', async ({ page }) => {
      const h2Elements = page.locator('h2');
      const h2Count = await h2Elements.count();
      expect(h2Count).toBeGreaterThanOrEqual(1);
    });
  });

  test.describe('Content Sections', () => {
    test('TC11: should have a hero section as primary introduction', async ({ page }) => {
      // Check for hero section (either by class or as first section in main)
      const heroSection = page.locator('section.hero, main > section:first-of-type');
      await expect(heroSection).toHaveCount(1);
    });

    test('TC12: should have a features section with id="features"', async ({ page }) => {
      const featuresSection = page.locator('section#features, #features');
      await expect(featuresSection).toHaveCount(1);
    });

    test('TC13: should have a getting-started section', async ({ page }) => {
      const gettingStartedSection = page.locator('section#getting-started, #getting-started');
      await expect(gettingStartedSection).toHaveCount(1);
    });
  });
});
