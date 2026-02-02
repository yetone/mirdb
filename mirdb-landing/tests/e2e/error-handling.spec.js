/**
 * Error Handling - Missing Assets E2E Tests
 * Owner: Scenario 20 - Error Handling - Missing Assets
 *
 * Tests:
 * - Graceful degradation when CSS fails to load
 * - Core content functional when JavaScript fails to load
 * - Alt text displays when images fail to load
 */

import { test, expect } from '@playwright/test';

test.describe('Error Handling - Missing Assets', () => {
  test.describe('TC1: Block CSS file loading', () => {
    test('Page content remains readable with unstyled HTML', async ({ page }) => {
      // Block all CSS files from loading
      await page.route('**/*.css', (route) => {
        route.abort();
      });

      // Navigate to the page
      await page.goto('/');

      // Verify the page still loads and renders content
      await expect(page).toHaveTitle(/MirDB/);

      // Verify main content areas are present and contain text
      const mainContent = page.locator('main');
      await expect(mainContent).toBeVisible();

      // Verify navigation is present
      const header = page.locator('header');
      await expect(header).toBeVisible();

      // Check that navigation links are visible and functional
      const navLinks = page.locator('.nav-links a, nav a');
      const navLinksCount = await navLinks.count();
      expect(navLinksCount).toBeGreaterThan(0);

      // Verify feature section content is readable
      const featureSection = page.locator('#features');
      await expect(featureSection).toBeVisible();

      // Verify text content is present (unstyled but readable)
      const featuresHeading = page.locator('#features-heading, #features h2');
      await expect(featuresHeading).toContainText(/Features/i);

      // Check code examples section
      const codeSection = page.locator('#code-examples');
      await expect(codeSection).toBeVisible();

      // Verify footer is present
      const footer = page.locator('footer');
      await expect(footer).toBeVisible();

      // Verify all internal links are still functional
      const internalLinks = page.locator('a[href^="#"]');
      const internalLinksCount = await internalLinks.count();
      expect(internalLinksCount).toBeGreaterThan(0);

      // Verify the first internal link points to a valid section
      if (internalLinksCount > 0) {
        const firstLink = internalLinks.first();
        const href = await firstLink.getAttribute('href');
        if (href && href.startsWith('#') && href.length > 1) {
          const targetId = href.substring(1);
          const targetElement = page.locator(`#${targetId}`);
          const exists = await targetElement.count();
          expect(exists).toBeGreaterThan(0);
        }
      }
    });
  });

  test.describe('TC2: Block JavaScript file loading', () => {
    test('Core page content and links remain functional', async ({ page }) => {
      // Block all JavaScript files from loading
      await page.route('**/*.js', (route) => {
        // Skip blocking Playwright's own scripts
        const url = route.request().url();
        if (url.includes('playwright') || url.includes('node_modules')) {
          route.continue();
        } else {
          route.abort();
        }
      });

      // Navigate to the page
      await page.goto('/');

      // Verify the page still loads
      await expect(page).toHaveTitle(/MirDB/);

      // Verify all sections are present and visible
      const sections = ['#features', '#code-examples', '#architecture', '#status', '#specifications'];
      for (const section of sections) {
        const element = page.locator(section);
        const count = await element.count();
        if (count > 0) {
          await expect(element).toBeVisible();
        }
      }

      // Verify navigation links are clickable and functional
      const navLinks = page.locator('nav a[href^="#"]');
      const navLinksCount = await navLinks.count();
      expect(navLinksCount).toBeGreaterThan(0);

      // Test that clicking an internal link scrolls to the section (basic functionality)
      const featuresLink = page.locator('a[href="#features"]').first();
      const featuresLinkCount = await featuresLink.count();
      if (featuresLinkCount > 0) {
        await featuresLink.click();
        // Verify we can still see the features section after clicking
        await expect(page.locator('#features')).toBeVisible();
      }

      // Verify external links (GitHub) are present and have correct href
      const githubLink = page.locator('a[href*="github.com/yetone/mirdb"]').first();
      await expect(githubLink).toBeVisible();
      const href = await githubLink.getAttribute('href');
      expect(href).toContain('github.com/yetone/mirdb');

      // Verify external links have proper security attributes
      const externalLinks = page.locator('a[target="_blank"]');
      const externalLinksCount = await externalLinks.count();
      for (let i = 0; i < externalLinksCount; i++) {
        const link = externalLinks.nth(i);
        const rel = await link.getAttribute('rel');
        expect(rel).toContain('noopener');
      }

      // Verify code examples are still visible (just content, no copy functionality expected)
      const codeBlocks = page.locator('pre, code, .code-block');
      const codeBlocksCount = await codeBlocks.count();
      expect(codeBlocksCount).toBeGreaterThan(0);

      // Verify specifications are still readable
      const specs = page.locator('#specifications');
      const specsCount = await specs.count();
      if (specsCount > 0) {
        await expect(specs).toBeVisible();
        // Check that spec values are visible
        const specValues = page.locator('.spec-value, [data-testid*="spec-"]');
        const specValuesCount = await specValues.count();
        if (specValuesCount > 0) {
          const firstSpec = specValues.first();
          await expect(firstSpec).toBeVisible();
        }
      }
    });
  });

  test.describe('TC3: Block image loading', () => {
    test('Alt text displays for images that fail to load', async ({ page }) => {
      // Track image load failures
      const failedImages = [];

      // Block all image files from loading
      await page.route(/\.(gif|png|jpg|jpeg|webp|ico)$/i, (route) => {
        failedImages.push(route.request().url());
        route.abort();
      });

      // Also block image content types
      await page.route('**/*', async (route) => {
        const request = route.request();
        const resourceType = request.resourceType();
        if (resourceType === 'image') {
          failedImages.push(request.url());
          route.abort();
        } else {
          route.continue();
        }
      });

      // Navigate to the page
      await page.goto('/', { waitUntil: 'domcontentloaded' });

      // Scroll the entire page to trigger any lazy-loaded images
      await page.evaluate(() => {
        window.scrollTo(0, document.body.scrollHeight);
      });
      await page.waitForTimeout(500);
      await page.evaluate(() => {
        window.scrollTo(0, 0);
      });

      // Verify page still loads and functions
      await expect(page).toHaveTitle(/MirDB/);

      // Get all images on the page
      const images = page.locator('img');
      const imageCount = await images.count();

      // Verify that all images have alt text
      for (let i = 0; i < imageCount; i++) {
        const img = images.nth(i);
        const alt = await img.getAttribute('alt');

        // All images MUST have alt text for accessibility
        expect(alt, `Image ${i} should have alt text`).toBeTruthy();
        expect(alt.length, `Image ${i} alt text should not be empty`).toBeGreaterThan(0);
      }

      // Specifically check important images

      // Architecture diagram
      const architectureDiagram = page.locator('.architecture-diagram, img[src*="architecture"]');
      const archCount = await architectureDiagram.count();
      if (archCount > 0) {
        const archAlt = await architectureDiagram.first().getAttribute('alt');
        expect(archAlt).toBeTruthy();
        expect(archAlt.length).toBeGreaterThan(10); // Should have descriptive alt text
      }

      // Status badge (external image)
      const statusBadge = page.locator('.status-badge, img[src*="circleci"]');
      const badgeCount = await statusBadge.count();
      if (badgeCount > 0) {
        const badgeAlt = await statusBadge.first().getAttribute('alt');
        expect(badgeAlt).toBeTruthy();
      }

      // Verify page content is still accessible without images
      const mainContent = page.locator('main');
      await expect(mainContent).toBeVisible();

      // Verify text content is still present and readable
      const headings = page.locator('h1, h2, h3');
      const headingsCount = await headings.count();
      expect(headingsCount).toBeGreaterThan(0);

      // Verify navigation still works
      const navLinks = page.locator('nav a');
      const navLinksCount = await navLinks.count();
      expect(navLinksCount).toBeGreaterThan(0);
    });

    test('Page remains functional when SVG files fail to load', async ({ page }) => {
      // Block SVG files
      await page.route('**/*.svg', (route) => {
        route.abort();
      });

      // Navigate to the page
      await page.goto('/');

      // Verify page still functions
      await expect(page).toHaveTitle(/MirDB/);

      // Verify main content is still accessible
      const mainContent = page.locator('main');
      await expect(mainContent).toBeVisible();

      // Check that inline SVGs (icons) are still present and don't break page
      const inlineSvgs = page.locator('svg');
      const inlineSvgCount = await inlineSvgs.count();
      // Inline SVGs should still be present (they're not blocked by route)
      expect(inlineSvgCount).toBeGreaterThan(0);

      // Check img elements with SVG src have alt text for fallback
      const svgImages = page.locator('img[src$=".svg"]');
      const svgImgCount = await svgImages.count();
      for (let i = 0; i < svgImgCount; i++) {
        const img = svgImages.nth(i);
        const alt = await img.getAttribute('alt');
        expect(alt, `SVG image ${i} should have alt text for fallback`).toBeTruthy();
      }

      // Verify navigation and links still work
      const navLinks = page.locator('nav a');
      const navLinksCount = await navLinks.count();
      expect(navLinksCount).toBeGreaterThan(0);
    });
  });

  test.describe('Additional graceful degradation tests', () => {
    test('Page remains navigable with both CSS and JS blocked', async ({ page }) => {
      // Block both CSS and JS
      await page.route('**/*.css', (route) => route.abort());
      await page.route('**/*.js', (route) => {
        const url = route.request().url();
        if (url.includes('playwright') || url.includes('node_modules')) {
          route.continue();
        } else {
          route.abort();
        }
      });

      // Navigate to the page
      await page.goto('/');

      // Verify basic page structure
      await expect(page).toHaveTitle(/MirDB/);

      // Verify semantic HTML structure provides usable navigation
      const header = page.locator('header');
      await expect(header).toBeVisible();

      const main = page.locator('main');
      await expect(main).toBeVisible();

      const footer = page.locator('footer');
      await expect(footer).toBeVisible();

      // Verify links are functional
      const links = page.locator('a[href]');
      const linksCount = await links.count();
      expect(linksCount).toBeGreaterThan(0);

      // Verify headings provide document structure
      const h1 = page.locator('h1');
      const h2 = page.locator('h2');
      const h1Count = await h1.count();
      const h2Count = await h2.count();
      expect(h1Count + h2Count).toBeGreaterThan(0);
    });

    test('External resources failure does not break page', async ({ page }) => {
      // Block external resources (e.g., CircleCI badge)
      await page.route(/^https?:\/\/(?!localhost)/, (route) => {
        route.abort();
      });

      // Navigate to the page
      await page.goto('/');

      // Page should still load and function
      await expect(page).toHaveTitle(/MirDB/);

      // All sections should still be visible
      const sections = ['#features', '#code-examples', '#architecture', '#status', '#specifications'];
      for (const section of sections) {
        const element = page.locator(section);
        const count = await element.count();
        if (count > 0) {
          await expect(element).toBeVisible();
        }
      }

      // Navigation should work
      const navLinks = page.locator('nav a');
      const navLinksCount = await navLinks.count();
      expect(navLinksCount).toBeGreaterThan(0);
    });
  });
});
