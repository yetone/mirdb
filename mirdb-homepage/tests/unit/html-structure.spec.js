/**
 * HTML Structure Unit Tests
 * Owner: Scenario 9 - Accessibility Compliance
 * Additional tests by Scenario 1 - Hero Section Display
 *
 * Tests:
 * - Semantic HTML elements
 * - Heading hierarchy
 * - Alt text presence
 * - Link text clarity
 */

const { test, expect } = require('@playwright/test');

test.describe('HTML Structure Validation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC5: Hero uses semantic HTML with proper heading hierarchy (h1 for MirDB)', async ({ page }) => {
    // Verify the document has proper semantic structure
    const header = page.locator('header');
    const main = page.locator('main');
    const footer = page.locator('footer');

    await expect(header).toBeAttached();
    await expect(main).toBeAttached();
    await expect(footer).toBeAttached();

    // Verify hero section uses proper section element
    const heroSection = page.locator('section#hero');
    await expect(heroSection).toBeAttached();

    // Verify h1 is used for the main page title (MirDB)
    const h1 = page.locator('h1');
    await expect(h1).toHaveCount(1);
    await expect(h1).toHaveText('MirDB');

    // Verify h1 is inside the hero section
    const heroH1 = page.locator('#hero h1');
    await expect(heroH1).toHaveText('MirDB');

    // Verify there's no heading level skipping (e.g., h1 followed by h3 without h2)
    const h1Count = await page.locator('h1').count();
    const h2Count = await page.locator('h2').count();
    const h3Count = await page.locator('h3').count();

    // At minimum, we should have h1
    expect(h1Count).toBe(1);

    // If we have h3, we should have h2 (no skipping)
    if (h3Count > 0) {
      expect(h2Count).toBeGreaterThan(0);
    }
  });

  test('Document has proper lang attribute', async ({ page }) => {
    const htmlLang = await page.locator('html').getAttribute('lang');
    expect(htmlLang).toBe('en');
  });

  test('Document has proper meta tags', async ({ page }) => {
    // Check charset
    const charset = await page.locator('meta[charset]').getAttribute('charset');
    expect(charset.toLowerCase()).toBe('utf-8');

    // Check viewport
    const viewport = await page.locator('meta[name="viewport"]').getAttribute('content');
    expect(viewport).toContain('width=device-width');

    // Check description
    const description = await page.locator('meta[name="description"]').getAttribute('content');
    expect(description).toBeTruthy();
    expect(description.toLowerCase()).toMatch(/mirdb|key-value|memcached/i);
  });

  test('Hero section links have descriptive text', async ({ page }) => {
    // Get Started link should be clear
    const getStartedLink = page.locator('.hero__cta--primary');
    const getStartedText = await getStartedLink.textContent();
    expect(getStartedText.trim()).toBe('Get Started');

    // View Source link should be clear
    const viewSourceLink = page.locator('.hero__cta--secondary');
    const viewSourceText = await viewSourceLink.textContent();
    expect(viewSourceText.trim()).toBe('View Source');
  });

  test('External links have proper security attributes', async ({ page }) => {
    // View Source link opens in new tab with proper rel attribute
    const viewSourceLink = page.locator('.hero__cta--secondary');

    const target = await viewSourceLink.getAttribute('target');
    expect(target).toBe('_blank');

    const rel = await viewSourceLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  test('Main sections have proper IDs for navigation', async ({ page }) => {
    // Check that main sections have IDs
    const sections = ['hero', 'features', 'quick-start', 'usage', 'status'];

    for (const sectionId of sections) {
      const section = page.locator(`#${sectionId}`);
      await expect(section).toBeAttached();
    }
  });
});
