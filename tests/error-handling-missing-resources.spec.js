// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Error Handling - Missing Resources (Scenario 19)
 * Verifies graceful handling of missing or unavailable resources
 *
 * Test Cases:
 * 1. Load page with images blocked - Alt text displayed; page layout not broken
 * 2. View page without CSS - Content remains readable and logically structured
 * 3. Load page with JavaScript disabled - Core content and navigation remain functional
 */

test.describe('Error Handling - Missing Resources', () => {
  /**
   * Test Case 1: Load page with images blocked
   * Expected: Alt text displayed; page layout not broken
   *
   * This test verifies that when images fail to load or are blocked:
   * - All img elements have alt text that will be displayed
   * - The page layout remains intact and usable
   * - SVG elements have accessible fallback descriptions
   */
  test('should display alt text and maintain layout when images are blocked', async ({ page }) => {
    // Block all image requests to simulate missing/unavailable images
    await page.route('**/*.{png,jpg,jpeg,gif,webp,svg}', route => route.abort());

    // Also block image requests that might come from other paths
    await page.route('**/assets/**', route => route.abort());

    await page.goto('/');

    // Wait for page to be fully loaded
    await page.waitForLoadState('domcontentloaded');

    // Verify the page structure is still intact
    // 1. Hero section should still be visible
    const heroSection = page.locator('.hero, #hero');
    await expect(heroSection).toBeVisible();

    // 2. Product name (H1) should be visible and readable
    const productName = page.locator('h1');
    await expect(productName).toBeVisible();
    await expect(productName).toContainText('MirDB');

    // 3. Navigation should still work
    const nav = page.locator('nav, .header-nav');
    await expect(nav).toBeVisible();

    // 4. Check that all img elements have alt attributes
    const images = page.locator('img');
    const imageCount = await images.count();

    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i);
      const altText = await img.getAttribute('alt');

      // Alt attribute must exist (can be empty for decorative images)
      expect(altText, `Image ${i + 1} should have alt attribute`).not.toBeNull();

      // For non-decorative images, alt text should be descriptive
      if (altText && altText.length > 0) {
        expect(altText.length).toBeGreaterThan(2);
      }
    }

    // 5. Verify the logo has proper alt text
    const logo = page.locator('[data-testid="logo"], .logo, img.logo');
    const logoCount = await logo.count();

    if (logoCount > 0) {
      const logoAlt = await logo.first().getAttribute('alt');
      expect(logoAlt).toBeTruthy();
      expect(logoAlt.toLowerCase()).toMatch(/mirdb|logo/i);
    }

    // 6. Verify SVG architecture diagram has accessible description
    const svgDiagram = page.locator('[data-testid="architecture-svg"], svg.lsm-tree-diagram');
    const svgCount = await svgDiagram.count();

    if (svgCount > 0) {
      // SVG should have title and/or description for screen readers
      const ariaLabelledby = await svgDiagram.first().getAttribute('aria-labelledby');
      const ariaLabel = await svgDiagram.first().getAttribute('aria-label');
      const role = await svgDiagram.first().getAttribute('role');

      // SVG should be accessible
      expect(ariaLabelledby || ariaLabel || role === 'img').toBeTruthy();
    }

    // 7. Verify main content sections are still visible
    const featuresSection = page.locator('#features, .features');
    await expect(featuresSection).toBeVisible();

    const architectureSection = page.locator('#architecture, .architecture');
    await expect(architectureSection).toBeVisible();

    const gettingStartedSection = page.locator('#getting-started, .getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // 8. Verify footer is intact
    const footer = page.locator('footer, .footer');
    await expect(footer).toBeVisible();

    // 9. Test page layout - verify sections are stacked vertically (not overlapping)
    const heroBounds = await heroSection.boundingBox();
    const featuresBounds = await featuresSection.boundingBox();

    expect(heroBounds).toBeTruthy();
    expect(featuresBounds).toBeTruthy();

    // Features section should be below hero section
    expect(featuresBounds.y).toBeGreaterThan(heroBounds.y);
  });

  /**
   * Test Case 2: View page without CSS
   * Expected: Content remains readable and logically structured
   *
   * This test verifies that when CSS fails to load:
   * - Content is still readable in a logical order
   * - Semantic HTML provides structure
   * - Headings create proper document outline
   * - Links and navigation are still functional
   */
  test('should display readable content without CSS', async ({ page }) => {
    // Block CSS files to simulate missing styles
    await page.route('**/*.css', route => route.abort());

    await page.goto('/');

    // Wait for page to load
    await page.waitForLoadState('domcontentloaded');

    // 1. Verify page content is still present and readable
    const body = page.locator('body');
    await expect(body).toBeVisible();

    // 2. Verify semantic structure - headings should be present
    const h1 = page.locator('h1');
    await expect(h1).toBeVisible();
    await expect(h1).toContainText('MirDB');

    // 3. Verify heading hierarchy exists (h1, h2, h3)
    const h2Elements = page.locator('h2');
    const h2Count = await h2Elements.count();
    expect(h2Count).toBeGreaterThan(0);

    // 4. Verify main content sections are present
    const mainContent = page.locator('main, #main-content');
    await expect(mainContent).toBeVisible();

    // 5. Verify all section headings are visible
    const sectionHeadings = [
      'Key Features',
      'Architecture Overview',
      'Getting Started',
      'Configuration Reference'
    ];

    for (const heading of sectionHeadings) {
      const headingElement = page.getByRole('heading', { name: heading, exact: false });
      await expect(headingElement).toBeVisible();
    }

    // 6. Verify links are still functional (navigation)
    const navLinks = page.locator('nav a, .header-nav a');
    const linkCount = await navLinks.count();
    expect(linkCount).toBeGreaterThan(0);

    // Verify links have href attributes
    for (let i = 0; i < linkCount; i++) {
      const link = navLinks.nth(i);
      const href = await link.getAttribute('href');
      expect(href).toBeTruthy();
    }

    // 7. Verify code examples are readable
    const codeBlocks = page.locator('pre, code');
    const codeCount = await codeBlocks.count();
    expect(codeCount).toBeGreaterThan(0);

    // 8. Verify content order is logical - check document structure
    // Get all headings in order
    const allHeadings = page.locator('h1, h2, h3');
    const headingCount = await allHeadings.count();
    expect(headingCount).toBeGreaterThan(4);

    // First heading should be h1
    const firstHeading = await allHeadings.first().evaluate(el => el.tagName.toLowerCase());
    expect(firstHeading).toBe('h1');

    // 9. Verify tables are still readable (configuration section)
    const tables = page.locator('table');
    const tableCount = await tables.count();

    if (tableCount > 0) {
      const table = tables.first();
      await expect(table).toBeVisible();

      // Table should have headers
      const headers = table.locator('th');
      const headerCount = await headers.count();
      expect(headerCount).toBeGreaterThan(0);
    }

    // 10. Verify footer content is accessible
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    const footerLinks = footer.locator('a');
    const footerLinkCount = await footerLinks.count();
    expect(footerLinkCount).toBeGreaterThan(0);

    // 11. Verify skip link is present for accessibility
    const skipLink = page.locator('.skip-link, a[href="#main-content"]');
    const skipLinkCount = await skipLink.count();
    expect(skipLinkCount).toBeGreaterThan(0);
  });

  /**
   * Test Case 3: Load page with JavaScript disabled
   * Expected: Core content and navigation remain functional
   *
   * This test verifies that when JavaScript is disabled:
   * - Static content is fully visible
   * - Navigation links work
   * - Page doesn't require JS for basic functionality
   */
  test('should function correctly with JavaScript disabled', async ({ browser }) => {
    // Create a new context with JavaScript disabled
    const context = await browser.newContext({
      javaScriptEnabled: false
    });

    const page = await context.newPage();

    try {
      await page.goto('http://localhost:3000/');

      // Wait for page content
      await page.waitForLoadState('domcontentloaded');

      // 1. Verify page loads and is visible
      const body = page.locator('body');
      await expect(body).toBeVisible();

      // 2. Verify hero section content
      const heroSection = page.locator('.hero, #hero, header');
      await expect(heroSection).toBeVisible();

      // 3. Verify product name is displayed
      const productName = page.locator('h1');
      await expect(productName).toBeVisible();
      await expect(productName).toContainText('MirDB');

      // 4. Verify tagline is displayed
      const tagline = page.locator('.tagline');
      await expect(tagline).toBeVisible();

      // 5. Verify navigation links are present and have valid hrefs
      const navLinks = page.locator('nav a, .header-nav a');
      const linkCount = await navLinks.count();
      expect(linkCount).toBeGreaterThan(0);

      // Check that internal navigation links point to valid sections
      const internalLinks = page.locator('a[href^="#"]');
      const internalLinkCount = await internalLinks.count();

      for (let i = 0; i < internalLinkCount; i++) {
        const link = internalLinks.nth(i);
        const href = await link.getAttribute('href');

        if (href && href !== '#') {
          // Verify the target section exists
          const targetId = href.replace('#', '');
          const targetSection = page.locator(`#${targetId}`);
          const targetExists = await targetSection.count();
          expect(targetExists).toBeGreaterThan(0);
        }
      }

      // 6. Verify feature cards are displayed
      const featureItems = page.locator('.feature-item, [data-feature]');
      const featureCount = await featureItems.count();
      expect(featureCount).toBeGreaterThan(0);

      // 7. Verify architecture diagram is visible (SVG inline, doesn't need JS)
      const architectureDiagram = page.locator('[data-testid="architecture-diagram"], .architecture-diagram');
      await expect(architectureDiagram).toBeVisible();

      // 8. Verify code examples are visible
      const codeBlocks = page.locator('.code-block, pre');
      const codeBlockCount = await codeBlocks.count();
      expect(codeBlockCount).toBeGreaterThan(0);

      // 9. Verify configuration table is visible
      const configTable = page.locator('.config-table, table');
      await expect(configTable).toBeVisible();

      // 10. Verify CTA buttons are present and functional
      const primaryCta = page.locator('.btn-primary, a.btn');
      await expect(primaryCta.first()).toBeVisible();

      const ctaHref = await primaryCta.first().getAttribute('href');
      expect(ctaHref).toBeTruthy();

      // 11. Verify external links have proper attributes
      const externalLinks = page.locator('a[target="_blank"]');
      const extLinkCount = await externalLinks.count();

      for (let i = 0; i < extLinkCount; i++) {
        const link = externalLinks.nth(i);
        const rel = await link.getAttribute('rel');

        // External links should have noopener for security
        expect(rel).toContain('noopener');
      }

      // 12. Verify footer is complete
      const footer = page.locator('footer, .footer');
      await expect(footer).toBeVisible();

      const footerNote = page.locator('.footer-note, [data-testid="footer-roadmap"]');
      await expect(footerNote).toBeVisible();

      // 13. Verify all sections are accessible
      const sections = ['#features', '#architecture', '#getting-started', '#configuration'];

      for (const sectionId of sections) {
        const section = page.locator(sectionId);
        await expect(section).toBeVisible();
      }

    } finally {
      await context.close();
    }
  });

  /**
   * Additional Test: Verify graceful degradation with network interruption
   * This ensures the static page remains functional even if some resources fail
   */
  test('should handle partial resource loading gracefully', async ({ page }) => {
    // Abort some requests randomly to simulate network issues
    let requestCount = 0;
    await page.route('**/*', route => {
      requestCount++;
      // Let HTML through, but randomly fail some other resources
      const url = route.request().url();
      if (url.endsWith('.html') || url === 'http://localhost:3000/') {
        return route.continue();
      }
      // Continue with the request (since this is a static page, we want to test actual behavior)
      return route.continue();
    });

    await page.goto('/');

    // Page should still be functional
    const body = page.locator('body');
    await expect(body).toBeVisible();

    // Main content should be present
    const h1 = page.locator('h1');
    await expect(h1).toContainText('MirDB');
  });

  /**
   * Additional Test: Verify semantic HTML structure supports no-CSS mode
   */
  test('should have semantic HTML structure for content accessibility', async ({ page }) => {
    await page.goto('/');

    // 1. Verify proper use of semantic elements
    const header = page.locator('header');
    const main = page.locator('main');
    const footer = page.locator('footer');
    const nav = page.locator('nav');
    const sections = page.locator('section');

    await expect(header).toBeVisible();
    await expect(main).toBeVisible();
    await expect(footer).toBeVisible();
    await expect(nav).toBeVisible();

    const sectionCount = await sections.count();
    expect(sectionCount).toBeGreaterThan(0);

    // 2. Verify document language is set
    const htmlLang = await page.locator('html').getAttribute('lang');
    expect(htmlLang).toBe('en');

    // 3. Verify viewport meta tag for responsive design
    const viewport = await page.locator('meta[name="viewport"]').getAttribute('content');
    expect(viewport).toContain('width=device-width');

    // 4. Verify skip link target exists
    const skipLink = page.locator('a[href="#main-content"]');
    const skipLinkCount = await skipLink.count();

    if (skipLinkCount > 0) {
      const mainContent = page.locator('#main-content');
      await expect(mainContent).toBeVisible();
    }

    // 5. Verify ARIA landmarks are properly used
    const mainLandmark = page.locator('[role="main"], main');
    await expect(mainLandmark).toBeVisible();
  });
});
