/**
 * SEO E2E Tests
 * Owners: Scenarios 16, 17 (SEO), Scenario 24 (Static Hosting)
 *
 * Test groups:
 * - Meta tags presence
 * - Title tag content
 * - Open Graph tags
 * - Semantic HTML structure
 * - Heading hierarchy for SEO
 * - Relative asset paths
 * - Static hosting compatibility
 */

import { test, expect } from '@playwright/test';
import { waitForLoad } from './utils';

test.describe('SEO - Meta Tags (Scenario 16)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForLoad(page);
  });

  test('Test Case 1: Title tag exists and contains MirDB', async ({ page }) => {
    const title = await page.title();
    expect(title).toBeTruthy();
    expect(title.toLowerCase()).toContain('mirdb');
  });

  test('Test Case 2: Meta description exists and is 50-160 characters', async ({ page }) => {
    const description = await page.locator('meta[name="description"]').getAttribute('content');
    expect(description).toBeTruthy();
    expect(description!.length).toBeGreaterThanOrEqual(50);
    expect(description!.length).toBeLessThanOrEqual(160);
  });

  test('Test Case 3: Viewport meta tag present with width=device-width', async ({ page }) => {
    const viewport = await page.locator('meta[name="viewport"]').getAttribute('content');
    expect(viewport).toBeTruthy();
    expect(viewport).toContain('width=device-width');
  });

  test('Test Case 4: Open Graph tags present (og:title and og:description)', async ({ page }) => {
    const ogTitle = await page.locator('meta[property="og:title"]').getAttribute('content');
    const ogDescription = await page.locator('meta[property="og:description"]').getAttribute('content');

    expect(ogTitle).toBeTruthy();
    expect(ogDescription).toBeTruthy();
  });

  test('Test Case 5: Favicon link tag present in document head', async ({ page }) => {
    const favicon = page.locator('link[rel="icon"], link[rel="shortcut icon"]');
    await expect(favicon).toHaveCount(1);
    const href = await favicon.getAttribute('href');
    expect(href).toBeTruthy();
  });
});

test.describe('SEO - Semantic HTML Structure (Scenario 17)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForLoad(page);
  });

  test('page has header element containing logo/nav', async ({ page }) => {
    // Check for header element
    const header = page.locator('header');
    await expect(header).toBeVisible();

    // Header should contain the logo
    const logo = header.locator('img[alt*="Logo"], img.hero-logo');
    await expect(logo).toBeVisible();

    // Header should contain navigation elements (CTA links)
    const headerLinks = header.locator('a');
    const linkCount = await headerLinks.count();
    expect(linkCount).toBeGreaterThan(0);
  });

  test('page has exactly one main element', async ({ page }) => {
    const mainElements = page.locator('main');
    const count = await mainElements.count();

    // There should be exactly one main element
    expect(count).toBe(1);

    // The main element should be visible
    await expect(mainElements.first()).toBeVisible();

    // The main element should have role="main" for older browsers
    const role = await mainElements.first().getAttribute('role');
    expect(role).toBe('main');
  });

  test('page has footer element', async ({ page }) => {
    // Check for footer element
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Footer should contain links
    const footerLinks = footer.locator('a');
    const linkCount = await footerLinks.count();
    expect(linkCount).toBeGreaterThan(0);

    // Footer should contain copyright or legal text
    const footerText = await footer.textContent();
    expect(footerText).toBeTruthy();
    expect(footerText?.toLowerCase()).toMatch(/copyright|©|\d{4}|license/i);
  });

  test('content is organized into semantic section elements', async ({ page }) => {
    // Check for section elements
    const sections = page.locator('section');
    const sectionCount = await sections.count();

    // There should be multiple semantic section elements
    expect(sectionCount).toBeGreaterThanOrEqual(3);

    // Verify key sections exist with proper identifiers
    const expectedSections = ['badges', 'features', 'usage', 'quickstart', 'roadmap'];
    for (const sectionId of expectedSections) {
      const section = page.locator(`section#${sectionId}, section.${sectionId}, section.${sectionId}-section`);
      const exists = await section.count() > 0;
      expect(exists, `Section "${sectionId}" should exist`).toBe(true);
    }

    // Each section should have aria-labelledby or aria-label for accessibility
    for (let i = 0; i < sectionCount; i++) {
      const section = sections.nth(i);
      const ariaLabelledBy = await section.getAttribute('aria-labelledby');
      const ariaLabel = await section.getAttribute('aria-label');
      const hasAriaAttribute = ariaLabelledBy !== null || ariaLabel !== null;
      expect(hasAriaAttribute, `Section ${i + 1} should have aria-labelledby or aria-label`).toBe(true);
    }
  });

  test('proper heading hierarchy exists for SEO', async ({ page }) => {
    // There should be exactly one h1
    const h1Elements = page.locator('h1');
    const h1Count = await h1Elements.count();
    expect(h1Count).toBe(1);

    // The h1 should be meaningful (contains text)
    const h1Text = await h1Elements.first().textContent();
    expect(h1Text?.trim().length).toBeGreaterThan(0);
    expect(h1Text).toContain('MirDB');

    // There should be multiple h2 elements for sections
    const h2Elements = page.locator('h2');
    const h2Count = await h2Elements.count();
    expect(h2Count).toBeGreaterThanOrEqual(3);

    // h2 elements should have meaningful content
    for (let i = 0; i < h2Count; i++) {
      const h2Text = await h2Elements.nth(i).textContent();
      expect(h2Text?.trim().length).toBeGreaterThan(0);
    }

    // h3 elements should exist under feature/content sections
    const h3Elements = page.locator('h3');
    const h3Count = await h3Elements.count();
    expect(h3Count).toBeGreaterThanOrEqual(1);
  });

  test('article elements are used for self-contained content', async ({ page }) => {
    // Check for article elements in features section
    const articles = page.locator('article');
    const articleCount = await articles.count();

    // Features are wrapped in article elements
    expect(articleCount).toBeGreaterThanOrEqual(1);

    // Each article should have a heading
    for (let i = 0; i < articleCount; i++) {
      const article = articles.nth(i);
      const heading = article.locator('h1, h2, h3, h4, h5, h6');
      const hasHeading = await heading.count() > 0;
      expect(hasHeading, `Article ${i + 1} should have a heading`).toBe(true);
    }
  });

  test('nav elements are used for navigation', async ({ page }) => {
    // Footer should have nav element
    const footerNav = page.locator('footer nav');
    const footerNavCount = await footerNav.count();
    expect(footerNavCount).toBeGreaterThanOrEqual(1);

    // Nav should have aria-label for accessibility
    if (footerNavCount > 0) {
      const ariaLabel = await footerNav.first().getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
    }
  });
});
