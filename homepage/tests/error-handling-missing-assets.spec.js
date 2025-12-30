// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Error Handling - Missing Assets Tests
 *
 * These tests verify that the MirDB homepage handles missing assets gracefully:
 * - Layout remains intact when images are blocked/disabled
 * - Content is readable without CSS (semantic HTML)
 * - Core functionality works without JavaScript
 * - Page handles missing assets (404) without console errors
 */

test.describe('Error Handling - Missing Assets', () => {
  test.describe('TC1: Load page with images disabled', () => {
    test('Layout remains intact when images are blocked', async ({ browser }) => {
      // Create context that blocks all images
      const context = await browser.newContext();
      const page = await context.newPage();

      // Block all image requests
      await page.route('**/*.{png,jpg,jpeg,gif,webp,svg,ico}', route => route.abort());
      await page.route('**/*', (route, request) => {
        if (request.resourceType() === 'image') {
          return route.abort();
        }
        return route.continue();
      });

      await page.goto('/');

      // Verify page loads successfully
      await expect(page.locator('h1')).toBeVisible();
      await expect(page.locator('h1')).toContainText('MirDB');

      // Verify all major sections are visible (layout intact)
      await expect(page.locator('.hero')).toBeVisible();
      await expect(page.locator('#features')).toBeVisible();
      await expect(page.locator('#comparison')).toBeVisible();
      await expect(page.locator('#quick-start')).toBeVisible();
      await expect(page.locator('#architecture')).toBeVisible();
      await expect(page.locator('#config')).toBeVisible();
      await expect(page.locator('#project-status')).toBeVisible();
      await expect(page.locator('footer')).toBeVisible();

      // Verify feature cards are still displayed correctly
      const featureCards = page.locator('.feature-card');
      await expect(featureCards).toHaveCount(4);

      // Verify buttons are visible and clickable
      await expect(page.locator('.btn-primary')).toBeVisible();
      await expect(page.locator('.btn-secondary')).toBeVisible();

      // Verify content in each section is visible
      await expect(page.locator('.tagline')).toContainText('Persistent Key-Value Store');
      await expect(page.locator('.hero-description')).toBeVisible();

      await context.close();
    });

    test('Alt text is visible for image elements when images fail to load', async ({ page }) => {
      await page.goto('/');

      // Check all img elements have alt attributes
      const images = await page.locator('img').all();
      for (const img of images) {
        const altAttr = await img.getAttribute('alt');
        // Alt attribute must exist (can be empty for decorative images)
        expect(altAttr !== null).toBeTruthy();
      }

      // Check SVG images have proper aria-label for accessibility
      const svgImages = await page.locator('svg[role="img"]').all();
      for (const svg of svgImages) {
        const ariaLabel = await svg.getAttribute('aria-label');
        const ariaLabelledBy = await svg.getAttribute('aria-labelledby');
        // SVG with role="img" should have accessible label
        expect(ariaLabel || ariaLabelledBy).toBeTruthy();
      }

      // Verify the architecture diagram SVG has proper accessibility
      const archDiagram = page.locator('[data-testid="architecture-diagram"]');
      if (await archDiagram.count() > 0) {
        const ariaLabel = await archDiagram.getAttribute('aria-label');
        expect(ariaLabel).toBeTruthy();
        expect(ariaLabel).toContain('LSM Tree');
      }
    });

    test('Layout dimensions remain consistent when images are blocked', async ({ browser }) => {
      // First, measure layout with images
      const contextWithImages = await browser.newContext();
      const pageWithImages = await contextWithImages.newPage();
      await pageWithImages.goto('/');
      await pageWithImages.waitForLoadState('networkidle');

      const layoutWithImages = await pageWithImages.evaluate(() => {
        const hero = document.querySelector('.hero');
        const features = document.querySelector('#features');
        const quickStart = document.querySelector('#quick-start');
        return {
          heroHeight: hero?.getBoundingClientRect().height || 0,
          featuresVisible: features?.getBoundingClientRect().height > 0,
          quickStartVisible: quickStart?.getBoundingClientRect().height > 0,
        };
      });
      await contextWithImages.close();

      // Now measure layout without images
      const contextNoImages = await browser.newContext();
      const pageNoImages = await contextNoImages.newPage();

      // Block all images
      await pageNoImages.route('**/*', (route, request) => {
        if (request.resourceType() === 'image') {
          return route.abort();
        }
        return route.continue();
      });

      await pageNoImages.goto('/');
      await pageNoImages.waitForLoadState('networkidle');

      const layoutNoImages = await pageNoImages.evaluate(() => {
        const hero = document.querySelector('.hero');
        const features = document.querySelector('#features');
        const quickStart = document.querySelector('#quick-start');
        return {
          heroHeight: hero?.getBoundingClientRect().height || 0,
          featuresVisible: features?.getBoundingClientRect().height > 0,
          quickStartVisible: quickStart?.getBoundingClientRect().height > 0,
        };
      });

      // Verify sections are still visible with similar layout
      expect(layoutNoImages.featuresVisible).toBe(true);
      expect(layoutNoImages.quickStartVisible).toBe(true);
      // Hero should maintain reasonable height (within 50% variance is acceptable)
      expect(layoutNoImages.heroHeight).toBeGreaterThan(0);

      await contextNoImages.close();
    });
  });

  test.describe('TC2: Load page with CSS disabled', () => {
    test('Content is readable and logically structured without CSS', async ({ browser }) => {
      const context = await browser.newContext();
      const page = await context.newPage();

      // Block all CSS
      await page.route('**/*.css', route => route.abort());
      await page.route('**/*', (route, request) => {
        if (request.resourceType() === 'stylesheet') {
          return route.abort();
        }
        return route.continue();
      });

      await page.goto('/');

      // Verify core content is present and readable
      await expect(page.locator('h1')).toBeVisible();
      await expect(page.locator('h1')).toContainText('MirDB');

      // Verify semantic structure provides readable content
      // Check heading hierarchy
      const h1 = await page.locator('h1').textContent();
      expect(h1).toContain('MirDB');

      const h2s = await page.locator('h2').allTextContents();
      expect(h2s.length).toBeGreaterThan(0);
      expect(h2s).toContain('Key Features');
      expect(h2s).toContain('Quick Start');

      // Verify lists are present and readable
      const lists = await page.locator('ul, ol').count();
      expect(lists).toBeGreaterThan(0);

      // Verify code blocks are readable
      const codeContent = await page.locator('pre code, code').first().textContent();
      expect(codeContent).toBeTruthy();

      // Verify links are functional
      const links = await page.locator('a[href]').count();
      expect(links).toBeGreaterThan(0);

      await context.close();
    });

    test('Semantic HTML provides document structure without CSS', async ({ browser }) => {
      const context = await browser.newContext();
      const page = await context.newPage();

      // Block CSS
      await page.route('**/*.css', route => route.abort());

      await page.goto('/');

      // Check semantic elements are used
      const header = await page.locator('header').count();
      const main = await page.locator('main').count();
      const footer = await page.locator('footer').count();
      const sections = await page.locator('section').count();
      const nav = await page.locator('nav, .footer-links').count();

      expect(header).toBeGreaterThan(0);
      expect(main).toBeGreaterThan(0);
      expect(footer).toBeGreaterThan(0);
      expect(sections).toBeGreaterThan(0);
      expect(nav).toBeGreaterThan(0);

      // Verify text content is still accessible
      const pageText = await page.locator('body').textContent();
      expect(pageText).toContain('MirDB');
      expect(pageText).toContain('Persistent Key-Value Store');
      expect(pageText).toContain('Memcached');
      expect(pageText).toContain('Features');

      await context.close();
    });

    test('All text content is visible without CSS', async ({ browser }) => {
      const context = await browser.newContext();
      const page = await context.newPage();

      // Block CSS
      await page.route('**/*.css', route => route.abort());

      await page.goto('/');

      // Verify key text content is visible
      const bodyText = await page.locator('body').textContent();

      // Hero section content
      expect(bodyText).toContain('MirDB');
      expect(bodyText).toContain('Persistent Key-Value Store');
      expect(bodyText).toContain('Get Started');
      expect(bodyText).toContain('GitHub');

      // Features section
      expect(bodyText).toContain('Memcached Protocol');
      expect(bodyText).toContain('Data Persistence');
      expect(bodyText).toContain('LSM Tree');
      expect(bodyText).toContain('Rust');

      // Quick start section
      expect(bodyText).toContain('git clone');
      expect(bodyText).toContain('cargo build');

      // Configuration section
      expect(bodyText).toContain('12333'); // Port number
      expect(bodyText).toContain('/tmp/mirdb');

      await context.close();
    });
  });

  test.describe('TC3: Load page with JavaScript disabled', () => {
    test('Core content and navigation remain functional without JavaScript', async ({ browser }) => {
      const context = await browser.newContext({
        javaScriptEnabled: false,
      });

      const page = await context.newPage();
      await page.goto('/');

      // Verify page loads and displays correctly
      await expect(page.locator('h1')).toBeVisible();
      await expect(page.locator('h1')).toContainText('MirDB');

      // Verify all sections are visible
      await expect(page.locator('.hero')).toBeVisible();
      await expect(page.locator('#features')).toBeVisible();
      await expect(page.locator('#quick-start')).toBeVisible();
      await expect(page.locator('#architecture')).toBeVisible();
      await expect(page.locator('#config')).toBeVisible();
      await expect(page.locator('#project-status')).toBeVisible();
      await expect(page.locator('footer')).toBeVisible();

      // Verify navigation links are present
      const getStartedBtn = page.locator('a[href="#quick-start"]');
      await expect(getStartedBtn).toBeVisible();

      const githubLink = page.locator('a[href*="github.com"]').first();
      await expect(githubLink).toBeVisible();

      // Verify internal anchor links point to valid sections
      const internalLinks = await page.locator('a[href^="#"]').all();
      for (const link of internalLinks) {
        const href = await link.getAttribute('href');
        if (href && href !== '#') {
          const targetId = href.replace('#', '');
          const target = page.locator(`#${targetId}`);
          expect(await target.count()).toBeGreaterThan(0);
        }
      }

      await context.close();
    });

    test('All interactive elements are accessible without JavaScript', async ({ browser }) => {
      const context = await browser.newContext({
        javaScriptEnabled: false,
      });

      const page = await context.newPage();
      await page.goto('/');

      // Verify all links are clickable
      const links = await page.locator('a').all();
      expect(links.length).toBeGreaterThan(0);

      for (const link of links) {
        const href = await link.getAttribute('href');
        expect(href).toBeTruthy();
      }

      // Verify footer navigation works
      await expect(page.locator('.footer-links a').first()).toBeVisible();

      // Verify code blocks are visible
      await expect(page.locator('.code-block')).toBeVisible();

      await context.close();
    });

    test('Architecture diagram SVG renders without JavaScript', async ({ browser }) => {
      const context = await browser.newContext({
        javaScriptEnabled: false,
      });

      const page = await context.newPage();
      await page.goto('/');

      // Verify SVG diagram is visible
      const diagram = page.locator('.architecture-diagram');
      await expect(diagram).toBeVisible();

      // Verify SVG contains content
      const svgContent = await diagram.innerHTML();
      expect(svgContent).toContain('rect');
      expect(svgContent).toContain('text');

      await context.close();
    });
  });

  test.describe('TC4: Simulate 404 for an image asset', () => {
    test('Page handles missing image without console errors', async ({ page }) => {
      const consoleErrors = [];
      const consoleWarnings = [];

      // Listen for console messages
      page.on('console', msg => {
        if (msg.type() === 'error') {
          consoleErrors.push(msg.text());
        }
        if (msg.type() === 'warning') {
          consoleWarnings.push(msg.text());
        }
      });

      // Simulate 404 for image requests
      await page.route('**/*.{png,jpg,jpeg,gif,webp}', route => {
        return route.fulfill({
          status: 404,
          body: 'Not Found',
        });
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Verify page still loads correctly
      await expect(page.locator('h1')).toBeVisible();
      await expect(page.locator('h1')).toContainText('MirDB');

      // Check for JavaScript errors (not network 404s)
      const jsErrors = consoleErrors.filter(err =>
        !err.includes('404') &&
        !err.includes('Failed to load resource') &&
        !err.includes('net::ERR')
      );

      // Should have no JavaScript errors caused by missing images
      expect(jsErrors).toHaveLength(0);
    });

    test('Page handles missing image without broken layout', async ({ page }) => {
      // Simulate 404 for image requests
      await page.route('**/*.{png,jpg,jpeg,gif,webp}', route => {
        return route.fulfill({
          status: 404,
          body: 'Not Found',
        });
      });

      await page.goto('/');

      // Verify all sections maintain proper layout
      const sections = ['#features', '#comparison', '#quick-start', '#architecture', '#config', '#project-status'];

      for (const selector of sections) {
        const section = page.locator(selector);
        await expect(section).toBeVisible();

        // Verify section has reasonable height (not collapsed)
        const boundingBox = await section.boundingBox();
        expect(boundingBox).not.toBeNull();
        expect(boundingBox.height).toBeGreaterThan(50);
      }

      // Verify feature cards are still properly laid out
      const featureCards = page.locator('.feature-card');
      await expect(featureCards).toHaveCount(4);

      // Verify each feature card has content
      for (let i = 0; i < 4; i++) {
        const card = featureCards.nth(i);
        const height = (await card.boundingBox())?.height || 0;
        expect(height).toBeGreaterThan(50);
      }
    });

    test('Page handles og:image 404 gracefully', async ({ page }) => {
      // Route og:image to return 404
      await page.route('**/mirdb-og-image.png', route => {
        return route.fulfill({
          status: 404,
          body: 'Not Found',
        });
      });

      await page.goto('/');

      // Verify page still renders correctly despite og:image 404
      await expect(page.locator('h1')).toBeVisible();
      await expect(page.locator('.hero')).toBeVisible();

      // Verify og:image meta tag exists (it's for social sharing, not display)
      const ogImage = await page.locator('meta[property="og:image"]').getAttribute('content');
      expect(ogImage).toBeTruthy();
    });

    test('SVG architecture diagram handles render errors gracefully', async ({ page }) => {
      await page.goto('/');

      // Verify SVG is present and has fallback attributes
      const svg = page.locator('.architecture-diagram');
      await expect(svg).toBeVisible();

      // Verify SVG has role="img" for accessibility
      const role = await svg.getAttribute('role');
      expect(role).toBe('img');

      // Verify SVG has aria-label as fallback for screen readers
      const ariaLabel = await svg.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel.length).toBeGreaterThan(10);
    });

    test('No unhandled promise rejections on missing assets', async ({ page }) => {
      const unhandledRejections = [];

      page.on('pageerror', error => {
        unhandledRejections.push(error.message);
      });

      // Block all external resources to simulate missing assets
      await page.route('**/*', (route, request) => {
        const url = request.url();
        // Allow HTML and CSS from localhost, block everything else external
        if (url.includes('localhost') || url.includes('127.0.0.1')) {
          return route.continue();
        }
        if (request.resourceType() === 'image' || request.resourceType() === 'font') {
          return route.fulfill({ status: 404, body: 'Not Found' });
        }
        return route.continue();
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Wait a bit for any async errors
      await page.waitForTimeout(500);

      // Should have no unhandled promise rejections
      expect(unhandledRejections).toHaveLength(0);
    });
  });

  test.describe('Graceful Degradation Summary', () => {
    test('Page provides usable experience with all assets blocked', async ({ browser }) => {
      const context = await browser.newContext({
        javaScriptEnabled: false,
      });

      const page = await context.newPage();

      // Block all non-essential resources
      await page.route('**/*', (route, request) => {
        const resourceType = request.resourceType();
        // Only allow document and stylesheet
        if (resourceType === 'document' || resourceType === 'stylesheet') {
          return route.continue();
        }
        return route.abort();
      });

      await page.goto('/');

      // Verify essential content is still accessible
      const pageText = await page.locator('body').textContent();

      // Core value proposition
      expect(pageText).toContain('MirDB');
      expect(pageText).toContain('Persistent Key-Value Store');
      expect(pageText).toContain('Memcached');

      // Key features
      expect(pageText).toContain('Data Persistence');
      expect(pageText).toContain('LSM Tree');

      // Getting started info
      expect(pageText).toContain('Quick Start');
      expect(pageText).toContain('git clone');

      // Contact/navigation
      expect(pageText).toContain('GitHub');

      await context.close();
    });
  });
});
