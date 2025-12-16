import { test, expect } from '@playwright/test';

test.describe('Error Handling - Missing Assets', () => {
  test.describe('Test Case 1: Block architecture diagram image from loading', () => {
    test('TC1: Alt text or placeholder displays gracefully when image fails to load', async ({ page }) => {
      // Set up request interception to block image resources
      await page.route('**/*.{png,jpg,jpeg,gif,webp,svg}', route => {
        // Allow SVG elements in HTML (they're inline), but block external image files
        const request = route.request();
        const url = request.url();
        // Only block actual image file requests, not inline SVG or data URLs
        if (url.startsWith('http') && (
          url.endsWith('.png') ||
          url.endsWith('.jpg') ||
          url.endsWith('.jpeg') ||
          url.endsWith('.gif') ||
          url.endsWith('.webp')
        )) {
          route.abort();
        } else {
          route.continue();
        }
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Verify the architecture diagram section exists
      const architectureSection = page.locator('#architecture');
      await expect(architectureSection).toBeVisible();

      // Check that the SVG diagram is present (it's inline SVG, not an external image)
      const svgDiagram = page.locator('.architecture-diagram svg, svg.lsm-diagram');
      const svgCount = await svgDiagram.count();

      if (svgCount > 0) {
        // The diagram uses inline SVG which is resilient to image blocking
        await expect(svgDiagram.first()).toBeVisible();

        // Check for accessibility attributes (alt text equivalent for SVG)
        const hasTitle = await svgDiagram.first().locator('title').count() > 0;
        const hasDesc = await svgDiagram.first().locator('desc').count() > 0;
        const ariaLabel = await svgDiagram.first().getAttribute('aria-label');
        const ariaDescribedBy = await svgDiagram.first().getAttribute('aria-describedby');

        // SVG should have accessible text
        expect(hasTitle || hasDesc || ariaLabel || ariaDescribedBy).toBeTruthy();
      }

      // Check that the architecture explanation content is visible regardless of images
      const explanationCards = page.locator('.explanation-card');
      await expect(explanationCards.first()).toBeVisible();

      // Verify the section title is visible
      const sectionTitle = architectureSection.locator('.section-title');
      await expect(sectionTitle).toContainText('Architecture Overview');

      // Verify any img tags have alt text for graceful degradation
      const images = await page.locator('img').all();
      for (const img of images) {
        const altText = await img.getAttribute('alt');
        const role = await img.getAttribute('role');

        // All images should have alt attribute (can be empty for decorative)
        const hasAltAttribute = altText !== null;
        const isDecorative = role === 'presentation' || altText === '';

        expect(hasAltAttribute || isDecorative).toBeTruthy();
      }

      // Verify the page layout is not broken
      const heroSection = page.locator('.hero-section');
      await expect(heroSection).toBeVisible();

      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();
    });

    test('TC1b: Diagram wrapper provides fallback context', async ({ page }) => {
      // Block ALL image resources aggressively
      await page.route('**/*', route => {
        const request = route.request();
        const resourceType = request.resourceType();

        if (resourceType === 'image') {
          route.abort();
        } else {
          route.continue();
        }
      });

      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Check that the architecture diagram wrapper has ARIA attributes
      const diagramWrapper = page.locator('.architecture-diagram');

      if (await diagramWrapper.count() > 0) {
        // Check for role="img" and aria-label on the diagram wrapper
        const role = await diagramWrapper.getAttribute('role');
        const ariaLabel = await diagramWrapper.getAttribute('aria-label');

        // The wrapper should have accessibility information
        expect(role === 'img' || ariaLabel).toBeTruthy();
      }

      // Content should still be accessible
      const explanationContent = page.locator('.architecture-explanation');
      await expect(explanationContent).toBeVisible();
    });
  });

  test.describe('Test Case 2: Load page with JavaScript disabled', () => {
    test('TC2: Core content (text, links) remains accessible without JavaScript', async ({ browser }) => {
      // Create a context with JavaScript disabled
      const context = await browser.newContext({
        javaScriptEnabled: false
      });
      const page = await context.newPage();

      await page.goto('http://localhost:3000/');
      await page.waitForLoadState('domcontentloaded');

      // Hero Section - Core content should be visible
      const heroTitle = page.locator('#hero-title, .hero-section h1');
      await expect(heroTitle).toBeVisible();
      await expect(heroTitle).toContainText('MirDB');

      const tagline = page.locator('.tagline');
      await expect(tagline).toBeVisible();
      await expect(tagline).toContainText('key-value store');

      // CTA buttons should be visible and functional (they're links, not JS-dependent)
      const getStartedBtn = page.locator('a.cta-button', { hasText: 'Get Started' });
      await expect(getStartedBtn).toBeVisible();
      const getStartedHref = await getStartedBtn.getAttribute('href');
      expect(getStartedHref).toBeTruthy();

      const githubBtn = page.locator('a.cta-button', { hasText: 'View on GitHub' });
      await expect(githubBtn).toBeVisible();
      const githubHref = await githubBtn.getAttribute('href');
      expect(githubHref).toContain('github.com');

      // Features Section - Content should be readable
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();

      const featureCards = page.locator('.feature-card');
      const featureCount = await featureCards.count();
      expect(featureCount).toBeGreaterThan(0);

      // Check a specific feature is present
      const memcachedFeature = page.locator('.feature-title', { hasText: 'Memcached' });
      await expect(memcachedFeature.first()).toBeVisible();

      // Navigation links should work
      const navLinks = page.locator('.nav-link');
      const navCount = await navLinks.count();
      expect(navCount).toBeGreaterThan(0);

      // Each nav link should have a valid href
      for (let i = 0; i < navCount; i++) {
        const link = navLinks.nth(i);
        const href = await link.getAttribute('href');
        expect(href).toBeTruthy();
      }

      // Quick Start Section - Code examples should be readable
      const quickStartSection = page.locator('#quickstart');
      await expect(quickStartSection).toBeVisible();

      const codeBlocks = page.locator('.code-block');
      const codeCount = await codeBlocks.count();
      expect(codeCount).toBeGreaterThan(0);

      // Project Status Section should be visible
      const statusSection = page.locator('#project-status');
      await expect(statusSection).toBeVisible();

      // Footer links should work
      const footerLinks = page.locator('.footer-link');
      const footerLinkCount = await footerLinks.count();
      expect(footerLinkCount).toBeGreaterThan(0);

      await context.close();
    });

    test('TC2b: Mobile navigation toggle gracefully degrades without JS', async ({ browser }) => {
      // Create a mobile viewport context with JavaScript disabled
      const context = await browser.newContext({
        javaScriptEnabled: false,
        viewport: { width: 375, height: 667 }
      });
      const page = await context.newPage();

      await page.goto('http://localhost:3000/');
      await page.waitForLoadState('domcontentloaded');

      // The navigation toggle button exists (for JS-enabled browsers)
      const navToggle = page.locator('.nav-toggle');

      // Even without JS, the main content is accessible
      const mainContent = page.locator('#main-content, main');
      await expect(mainContent).toBeVisible();

      // The hero section content is accessible
      const heroContent = page.locator('.hero-content');
      await expect(heroContent).toBeVisible();

      // Core links should still work
      const ctaButtons = page.locator('.cta-button');
      const ctaCount = await ctaButtons.count();
      expect(ctaCount).toBeGreaterThan(0);

      await context.close();
    });
  });

  test.describe('Test Case 3: Test with slow network simulation', () => {
    test('TC3: Page shows content progressively, no broken layout', async ({ browser }) => {
      // Create a context with slow 3G network conditions
      const context = await browser.newContext();
      const page = await context.newPage();

      // Emulate slow 3G network
      const client = await page.context().newCDPSession(page);
      await client.send('Network.emulateNetworkConditions', {
        offline: false,
        downloadThroughput: (500 * 1024) / 8, // 500 Kbps download
        uploadThroughput: (500 * 1024) / 8,   // 500 Kbps upload
        latency: 400, // 400ms latency (slow 3G)
      });

      const loadStart = Date.now();

      // Navigate and track DOMContentLoaded
      await page.goto('http://localhost:3000/', { waitUntil: 'domcontentloaded' });
      const domContentLoadedTime = Date.now() - loadStart;

      // Check that core content is visible quickly (even if styles are loading)
      const heroTitle = page.locator('#hero-title, .hero-section h1');
      await expect(heroTitle).toBeVisible({ timeout: 10000 });

      // Verify the page structure is correct (no broken layout)
      const body = page.locator('body');
      await expect(body).toBeVisible();

      // Check that CSS is applying correctly (even if loading slowly)
      const heroSection = page.locator('.hero-section');
      const heroBox = await heroSection.boundingBox();
      expect(heroBox).toBeTruthy();
      expect(heroBox!.width).toBeGreaterThan(0);
      expect(heroBox!.height).toBeGreaterThan(0);

      // Wait for network idle to ensure everything is loaded
      await page.waitForLoadState('networkidle');
      const fullLoadTime = Date.now() - loadStart;

      console.log(`DOM Content Loaded (slow 3G): ${domContentLoadedTime}ms`);
      console.log(`Full Load (slow 3G): ${fullLoadTime}ms`);

      // Verify final layout is correct
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();
      const featuresBox = await featuresSection.boundingBox();
      expect(featuresBox).toBeTruthy();

      // Check that content flows correctly (no overlapping elements)
      const heroBottom = heroBox!.y + heroBox!.height;
      // Features section should be below hero (no overlap)
      // Allow for some CSS margin/padding adjustments
      expect(featuresBox!.y).toBeGreaterThanOrEqual(heroBottom - 100);

      // Verify text is readable
      const tagline = page.locator('.tagline');
      await expect(tagline).toBeVisible();
      const taglineText = await tagline.textContent();
      expect(taglineText?.length).toBeGreaterThan(10);

      await context.close();
    });

    test('TC3b: CSS loads and applies styling correctly on slow network', async ({ browser }) => {
      const context = await browser.newContext();
      const page = await context.newPage();

      // Emulate slow network
      const client = await page.context().newCDPSession(page);
      await client.send('Network.emulateNetworkConditions', {
        offline: false,
        downloadThroughput: (1024 * 1024) / 8, // 1 Mbps
        uploadThroughput: (512 * 1024) / 8,
        latency: 200,
      });

      await page.goto('http://localhost:3000/', { waitUntil: 'networkidle' });

      // Check that CSS variables are applied (indicating CSS is loaded)
      const primaryColor = await page.evaluate(() => {
        return getComputedStyle(document.documentElement).getPropertyValue('--primary-color').trim();
      });
      expect(primaryColor).toBeTruthy();

      // Check that layout classes are working
      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();

      const gridDisplay = await featuresGrid.evaluate(el => {
        return getComputedStyle(el).display;
      });
      expect(gridDisplay).toBe('grid');

      // Check hero gradient background
      const heroSection = page.locator('.hero-section');
      const heroBg = await heroSection.evaluate(el => {
        return getComputedStyle(el).background;
      });
      expect(heroBg).toContain('gradient');

      await context.close();
    });
  });

  test.describe('Test Case 4: Check for console errors on page load', () => {
    test('TC4: No JavaScript errors in browser console', async ({ page }) => {
      const consoleErrors: string[] = [];
      const consoleWarnings: string[] = [];

      // Listen for console messages
      page.on('console', msg => {
        if (msg.type() === 'error') {
          consoleErrors.push(`${msg.type()}: ${msg.text()}`);
        } else if (msg.type() === 'warning') {
          consoleWarnings.push(`${msg.type()}: ${msg.text()}`);
        }
      });

      // Listen for page errors (uncaught exceptions)
      const pageErrors: string[] = [];
      page.on('pageerror', error => {
        pageErrors.push(error.message);
      });

      // Navigate to the page
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Scroll through the page to trigger any lazy-loaded errors
      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
      await page.waitForTimeout(500);
      await page.evaluate(() => window.scrollTo(0, 0));
      await page.waitForTimeout(500);

      // Log any errors/warnings found
      if (consoleErrors.length > 0) {
        console.log('Console Errors:', consoleErrors);
      }
      if (consoleWarnings.length > 0) {
        console.log('Console Warnings:', consoleWarnings);
      }
      if (pageErrors.length > 0) {
        console.log('Page Errors (uncaught):', pageErrors);
      }

      // Assert no JavaScript errors
      expect(pageErrors).toHaveLength(0);
      expect(consoleErrors).toHaveLength(0);
    });

    test('TC4b: No 404 errors for resources', async ({ page }) => {
      const failedRequests: string[] = [];

      // Track failed requests
      page.on('requestfailed', request => {
        failedRequests.push(`${request.url()} - ${request.failure()?.errorText}`);
      });

      // Track 404 responses
      const notFoundResponses: string[] = [];
      page.on('response', response => {
        if (response.status() === 404) {
          notFoundResponses.push(`${response.url()} - 404 Not Found`);
        }
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Log any issues
      if (failedRequests.length > 0) {
        console.log('Failed Requests:', failedRequests);
      }
      if (notFoundResponses.length > 0) {
        console.log('404 Responses:', notFoundResponses);
      }

      // Assert no failed requests
      expect(failedRequests).toHaveLength(0);
      expect(notFoundResponses).toHaveLength(0);
    });

    test('TC4c: No network errors during interaction', async ({ page }) => {
      const errors: string[] = [];

      page.on('console', msg => {
        if (msg.type() === 'error') {
          errors.push(msg.text());
        }
      });

      page.on('pageerror', error => {
        errors.push(error.message);
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Interact with the page - click navigation links
      const navLinks = page.locator('.nav-link[href^="#"]');
      const navCount = await navLinks.count();

      for (let i = 0; i < Math.min(navCount, 5); i++) {
        await navLinks.nth(i).click();
        await page.waitForTimeout(200);
      }

      // Click the mobile nav toggle (if visible)
      const navToggle = page.locator('.nav-toggle');
      if (await navToggle.isVisible()) {
        await navToggle.click();
        await page.waitForTimeout(200);
      }

      // Click the back to top link
      const backToTop = page.locator('.back-to-top');
      if (await backToTop.isVisible()) {
        await backToTop.click();
        await page.waitForTimeout(200);
      }

      // No errors should occur during interaction
      expect(errors).toHaveLength(0);
    });
  });
});
