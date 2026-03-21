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

  test('TC5: Page uses semantic HTML structure (header, nav, main, section, footer)', async ({ page }) => {
    // Verify the document has proper semantic structure
    const header = page.locator('header');
    const nav = page.locator('nav');
    const main = page.locator('main');
    const footer = page.locator('footer');

    await expect(header).toBeAttached();
    await expect(nav).toBeAttached();
    await expect(main).toBeAttached();
    await expect(footer).toBeAttached();

    // Verify all content sections use semantic section element
    const heroSection = page.locator('section#hero');
    const featuresSection = page.locator('section#features');
    const quickStartSection = page.locator('section#quick-start');
    const usageSection = page.locator('section#usage');
    const statusSection = page.locator('section#status');

    await expect(heroSection).toBeAttached();
    await expect(featuresSection).toBeAttached();
    await expect(quickStartSection).toBeAttached();
    await expect(usageSection).toBeAttached();
    await expect(statusSection).toBeAttached();

    // Verify nav is inside header
    const headerNav = page.locator('header nav');
    await expect(headerNav).toBeAttached();

    // Verify sections are inside main
    const mainSections = page.locator('main section');
    const sectionCount = await mainSections.count();
    expect(sectionCount).toBeGreaterThanOrEqual(5);
  });

  test('TC6: Headings follow logical order (h1 > h2 > h3) without skipping levels', async ({ page }) => {
    // Verify h1 is used for the main page title (MirDB)
    const h1 = page.locator('h1');
    await expect(h1).toHaveCount(1);
    await expect(h1).toHaveText('MirDB');

    // Verify h1 is inside the hero section
    const heroH1 = page.locator('#hero h1');
    await expect(heroH1).toHaveText('MirDB');

    // Get all headings in document order
    const allHeadings = await page.evaluate(() => {
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      return Array.from(headings).map(h => ({
        level: parseInt(h.tagName.substring(1)),
        text: h.textContent.trim()
      }));
    });

    // At minimum, we should have h1
    expect(allHeadings.length).toBeGreaterThan(0);
    expect(allHeadings[0].level).toBe(1);

    // Verify no heading level is skipped
    for (let i = 1; i < allHeadings.length; i++) {
      const currentLevel = allHeadings[i].level;
      const previousLevel = allHeadings[i - 1].level;

      // A heading can go to same level, one level deeper, or any level shallower
      // It should NOT skip levels going deeper (e.g., h1 -> h3 is invalid)
      if (currentLevel > previousLevel) {
        expect(currentLevel - previousLevel).toBeLessThanOrEqual(1);
      }
    }

    // Verify we have h2s for main sections
    const h2Count = await page.locator('h2').count();
    expect(h2Count).toBeGreaterThanOrEqual(4); // Features, Quick Start, Usage, Status

    // Verify we have h3s for feature cards
    const h3Count = await page.locator('h3').count();
    expect(h3Count).toBeGreaterThanOrEqual(6); // 6 feature cards
  });

  test('TC7: All images have appropriate alt text describing their content', async ({ page }) => {
    // Get all images
    const images = page.locator('img');
    const imageCount = await images.count();

    // All images should have alt attribute
    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i);
      const alt = await img.getAttribute('alt');

      // Alt text must exist and be non-empty
      expect(alt).toBeTruthy();
      expect(alt.trim().length).toBeGreaterThan(0);

      // Alt text should be descriptive (not just "image" or filename)
      expect(alt.toLowerCase()).not.toMatch(/^image$|^img$|^photo$|\.jpg$|\.png$|\.gif$/i);
    }

    // Specifically check usage demo GIF has meaningful alt text
    const usageGif = page.locator('.usage__gif');
    if (await usageGif.count() > 0) {
      const usageAlt = await usageGif.getAttribute('alt');
      expect(usageAlt).toBeTruthy();
      expect(usageAlt.toLowerCase()).toMatch(/usage|demo|mirdb|terminal|command/i);
    }

    // Check CircleCI badge has appropriate alt
    const statusBadge = page.locator('.status-badge');
    if (await statusBadge.count() > 0) {
      const badgeAlt = await statusBadge.getAttribute('alt');
      expect(badgeAlt).toBeTruthy();
      expect(badgeAlt.toLowerCase()).toMatch(/circleci|build|status/i);
    }
  });

  test('TC8: Link text is descriptive (no "click here" or bare URLs)', async ({ page }) => {
    // Get all links
    const links = page.locator('a[href]');
    const linkCount = await links.count();

    // Bad link text patterns
    const badPatterns = [
      /^click here$/i,
      /^click$/i,
      /^here$/i,
      /^link$/i,
      /^read more$/i,
      /^learn more$/i,
      /^https?:\/\//i, // Bare URLs
      /^www\./i
    ];

    for (let i = 0; i < linkCount; i++) {
      const link = links.nth(i);
      const linkText = await link.textContent();
      const trimmedText = linkText.trim();

      // Link must have text (or aria-label)
      const ariaLabel = await link.getAttribute('aria-label');
      const hasAccessibleName = trimmedText.length > 0 || (ariaLabel && ariaLabel.length > 0);
      expect(hasAccessibleName).toBe(true);

      // If link has visible text, check it's not a bad pattern
      if (trimmedText.length > 0) {
        for (const pattern of badPatterns) {
          expect(trimmedText).not.toMatch(pattern);
        }
      }
    }

    // Verify specific important links have good text
    const getStartedLink = page.locator('.hero__cta--primary');
    const getStartedText = await getStartedLink.textContent();
    expect(getStartedText.trim()).toBe('Get Started');

    const viewSourceLink = page.locator('.hero__cta--secondary');
    const viewSourceText = await viewSourceLink.textContent();
    expect(viewSourceText.trim()).toBe('View Source');

    // Documentation link should be descriptive
    const docsLink = page.locator('.quickstart-docs-link a');
    if (await docsLink.count() > 0) {
      const docsText = await docsLink.textContent();
      expect(docsText.toLowerCase()).toMatch(/documentation|docs|readme/i);
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

  test('External links have proper security attributes', async ({ page }) => {
    // All target="_blank" links should have rel="noopener"
    const externalLinks = page.locator('a[target="_blank"]');
    const linkCount = await externalLinks.count();

    for (let i = 0; i < linkCount; i++) {
      const link = externalLinks.nth(i);
      const rel = await link.getAttribute('rel');
      expect(rel).toContain('noopener');
    }
  });

  test('Main sections have proper IDs for navigation', async ({ page }) => {
    // Check that main sections have IDs
    const sections = ['hero', 'features', 'quick-start', 'usage', 'status'];

    for (const sectionId of sections) {
      const section = page.locator(`#${sectionId}`);
      await expect(section).toBeAttached();
    }
  });

  test('Feature cards use article elements for proper semantics', async ({ page }) => {
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();

    expect(cardCount).toBeGreaterThanOrEqual(6);

    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      const tagName = await card.evaluate(el => el.tagName.toLowerCase());
      expect(tagName).toBe('article');
    }
  });

  test('Navigation uses proper list structure', async ({ page }) => {
    const navMenu = page.locator('.nav-menu');
    const tagName = await navMenu.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('ul');

    // Nav items should be list items
    const navItems = page.locator('.nav-menu li');
    const itemCount = await navItems.count();
    expect(itemCount).toBeGreaterThanOrEqual(4);
  });
});
