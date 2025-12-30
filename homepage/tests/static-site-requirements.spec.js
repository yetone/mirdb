// @ts-check
const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

/**
 * Static Site Requirements Tests
 *
 * These tests verify that the MirDB homepage works as a static site:
 * - Works without backend server (static hosting compatible)
 * - No API dependencies for core content
 * - Relative paths for assets
 * - Core content visible without JavaScript
 */

test.describe('Static Site Requirements', () => {
  test.describe('Test Case 1: Static File Server Compatibility', () => {
    test('Page loads and functions correctly when served from simple HTTP server', async ({ page }) => {
      // Navigate to the homepage
      const response = await page.goto('/');

      // Verify the page loads successfully
      expect(response).not.toBeNull();
      expect(response.status()).toBe(200);

      // Verify essential content is present
      await expect(page.locator('h1')).toContainText('MirDB');
      await expect(page.locator('.tagline')).toBeVisible();

      // Verify all major sections are rendered
      await expect(page.locator('#features')).toBeVisible();
      await expect(page.locator('#quick-start')).toBeVisible();
      await expect(page.locator('#architecture')).toBeVisible();
      await expect(page.locator('#config')).toBeVisible();
      await expect(page.locator('#project-status')).toBeVisible();

      // Verify footer is present
      await expect(page.locator('footer.footer')).toBeVisible();

      // Verify CSS is loaded and applied
      // The hero section uses a gradient background, so we check backgroundImage instead
      const cssApplied = await page.evaluate(() => {
        const hero = document.querySelector('.hero');
        const styles = window.getComputedStyle(hero);
        // Check for either background gradient or min-height being set by CSS
        return {
          backgroundImage: styles.backgroundImage,
          minHeight: styles.minHeight,
          display: styles.display,
        };
      });
      // Verify CSS is applied - hero should have gradient background or flex display
      const hasGradient = cssApplied.backgroundImage && cssApplied.backgroundImage !== 'none';
      const hasFlex = cssApplied.display === 'flex';
      expect(hasGradient || hasFlex).toBe(true);
    });

    test('Page serves correctly via local file server with all content intact', async ({ page }) => {
      await page.goto('/');

      // Verify all feature cards are displayed
      const featureCards = page.locator('.feature-card');
      await expect(featureCards).toHaveCount(4);

      // Verify feature card content
      await expect(page.locator('.feature-card').nth(0)).toContainText('Memcached Protocol');
      await expect(page.locator('.feature-card').nth(1)).toContainText('Data Persistence');
      await expect(page.locator('.feature-card').nth(2)).toContainText('LSM Tree Architecture');
      await expect(page.locator('.feature-card').nth(3)).toContainText('Rust Performance');

      // Verify code examples are present
      const codeBlock = page.locator('.code-block code');
      await expect(codeBlock).toBeVisible();
      const codeContent = await codeBlock.textContent();
      expect(codeContent).toContain('git clone');
      expect(codeContent).toContain('cargo build');
    });
  });

  test.describe('Test Case 2: No API Dependencies', () => {
    test('No XHR/fetch requests to backend APIs for core content', async ({ page }) => {
      const apiCalls = [];

      // Listen for all network requests
      page.on('request', request => {
        const url = request.url();
        const resourceType = request.resourceType();

        // Track XHR and Fetch requests
        if (resourceType === 'xhr' || resourceType === 'fetch') {
          apiCalls.push({
            url,
            resourceType,
            method: request.method(),
          });
        }
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Filter out any requests to local server (which is allowed for static assets)
      const externalApiCalls = apiCalls.filter(call => {
        const url = call.url;
        // Exclude local requests and common CDN resources that are static
        return !url.includes('localhost') &&
               !url.includes('127.0.0.1') &&
               !url.startsWith('file://') &&
               // These are actual API calls, not static resources
               (url.includes('/api/') ||
                url.includes('/v1/') ||
                url.includes('/graphql') ||
                url.includes('.json'));
      });

      console.log('API calls detected:', apiCalls.length);
      if (apiCalls.length > 0) {
        console.log('All requests:', apiCalls.map(c => c.url).join('\n'));
      }

      // Verify no external API calls are made for core content
      expect(externalApiCalls.length).toBe(0);
    });

    test('Page content is fully static (no dynamic data fetching)', async ({ page }) => {
      const networkRequests = [];

      page.on('request', request => {
        networkRequests.push({
          url: request.url(),
          resourceType: request.resourceType(),
        });
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Categorize requests
      const requestTypes = networkRequests.reduce((acc, req) => {
        acc[req.resourceType] = (acc[req.resourceType] || 0) + 1;
        return acc;
      }, {});

      console.log('Request types:', requestTypes);

      // Verify that requests are only for static resources
      const allowedStaticTypes = ['document', 'stylesheet', 'image', 'font', 'script', 'other'];
      const hasOnlyStaticRequests = networkRequests.every(req =>
        allowedStaticTypes.includes(req.resourceType)
      );

      expect(hasOnlyStaticRequests).toBe(true);

      // Verify no WebSocket connections
      const wsRequests = networkRequests.filter(req =>
        req.url.startsWith('ws://') || req.url.startsWith('wss://')
      );
      expect(wsRequests.length).toBe(0);
    });
  });

  test.describe('Test Case 3: Relative Asset Paths', () => {
    test('CSS uses relative paths for portability', async ({ page }) => {
      await page.goto('/');

      // Check that stylesheet link uses relative path
      const stylesheetLink = await page.evaluate(() => {
        const link = document.querySelector('link[rel="stylesheet"]');
        return link ? link.getAttribute('href') : null;
      });

      expect(stylesheetLink).not.toBeNull();
      expect(stylesheetLink).toBe('styles.css');

      // Verify it's not an absolute URL
      expect(stylesheetLink).not.toMatch(/^https?:\/\//);
      expect(stylesheetLink).not.toMatch(/^\//);

      console.log(`CSS path: ${stylesheetLink}`);
    });

    test('All local assets use relative paths', async ({ page }) => {
      await page.goto('/');

      // Check all link elements
      const linkHrefs = await page.evaluate(() => {
        const links = document.querySelectorAll('link[href]');
        return Array.from(links).map(link => ({
          href: link.getAttribute('href'),
          rel: link.getAttribute('rel'),
        }));
      });

      // Check all script elements
      const scriptSrcs = await page.evaluate(() => {
        const scripts = document.querySelectorAll('script[src]');
        return Array.from(scripts).map(script => script.getAttribute('src'));
      });

      // Check all img elements
      const imgSrcs = await page.evaluate(() => {
        const images = document.querySelectorAll('img[src]');
        return Array.from(images).map(img => img.getAttribute('src'));
      });

      console.log('Link hrefs:', linkHrefs);
      console.log('Script srcs:', scriptSrcs);
      console.log('Image srcs:', imgSrcs);

      // Verify local resources use relative paths
      const allLocalAssets = [
        ...linkHrefs.filter(l => l.rel === 'stylesheet').map(l => l.href),
        ...scriptSrcs,
        ...imgSrcs,
      ];

      for (const asset of allLocalAssets) {
        if (asset && !asset.startsWith('http://') && !asset.startsWith('https://')) {
          // Local asset should be relative (not starting with /)
          expect(asset).not.toMatch(/^\/[^\/]/);
          console.log(`Verified relative path: ${asset}`);
        }
      }
    });

    test('Internal anchor links use relative paths', async ({ page }) => {
      await page.goto('/');

      // Check internal navigation links
      const internalLinks = await page.evaluate(() => {
        const links = document.querySelectorAll('a[href^="#"]');
        return Array.from(links).map(link => link.getAttribute('href'));
      });

      console.log('Internal anchor links:', internalLinks);

      // Verify internal links exist and work
      for (const link of internalLinks) {
        const targetId = link.replace('#', '');
        if (targetId) {
          const targetExists = await page.locator(`#${targetId}`).count();
          expect(targetExists).toBeGreaterThan(0);
        }
      }
    });
  });

  test.describe('Test Case 4: JavaScript Disabled Rendering', () => {
    test('Core content is visible even without JavaScript', async ({ browser }) => {
      // Create a context with JavaScript disabled
      const context = await browser.newContext({
        javaScriptEnabled: false,
      });

      const page = await context.newPage();
      await page.goto('/');

      // Verify core content is visible
      await expect(page.locator('h1')).toBeVisible();
      await expect(page.locator('h1')).toContainText('MirDB');

      // Verify tagline is visible
      await expect(page.locator('.tagline')).toBeVisible();
      await expect(page.locator('.tagline')).toContainText('Persistent Key-Value Store');

      // Verify hero description is visible
      await expect(page.locator('.hero-description')).toBeVisible();

      // Verify CTA buttons are visible
      await expect(page.locator('.hero-ctas')).toBeVisible();
      await expect(page.locator('.btn-primary')).toBeVisible();
      await expect(page.locator('.btn-secondary')).toBeVisible();

      // Close context
      await context.close();
    });

    test('All sections render without JavaScript', async ({ browser }) => {
      const context = await browser.newContext({
        javaScriptEnabled: false,
      });

      const page = await context.newPage();
      await page.goto('/');

      // Verify features section
      await expect(page.locator('#features h2')).toContainText('Key Features');
      const featureCards = page.locator('.feature-card');
      await expect(featureCards).toHaveCount(4);

      // Verify quick start section
      await expect(page.locator('#quick-start h2')).toContainText('Quick Start');
      await expect(page.locator('.code-block')).toBeVisible();

      // Verify architecture section
      await expect(page.locator('#architecture h2')).toContainText('How It Works');
      await expect(page.locator('.architecture-diagram')).toBeVisible();

      // Verify configuration section
      await expect(page.locator('#config h2')).toContainText('Default Configuration');
      await expect(page.locator('.config-list')).toBeVisible();

      // Verify project status section
      await expect(page.locator('#project-status h2')).toContainText('Project Status');

      // Verify footer
      await expect(page.locator('footer.footer')).toBeVisible();

      await context.close();
    });

    test('Code examples are readable without JavaScript', async ({ browser }) => {
      const context = await browser.newContext({
        javaScriptEnabled: false,
      });

      const page = await context.newPage();
      await page.goto('/');

      // Verify code block content is visible
      const codeContent = await page.locator('.code-block code').textContent();
      expect(codeContent).toContain('git clone');
      expect(codeContent).toContain('cargo build --release');
      expect(codeContent).toContain('telnet localhost 12333');
      expect(codeContent).toContain('set mykey');
      expect(codeContent).toContain('get mykey');

      await context.close();
    });

    test('Navigation links work without JavaScript', async ({ browser }) => {
      const context = await browser.newContext({
        javaScriptEnabled: false,
      });

      const page = await context.newPage();
      await page.goto('/');

      // Verify external links are present and have correct attributes
      const githubLink = page.locator('a[href="https://github.com/yetone/mirdb"]').first();
      await expect(githubLink).toBeVisible();

      // Verify internal anchor link to quick-start
      const getStartedLink = page.locator('a[href="#quick-start"]');
      await expect(getStartedLink).toBeVisible();

      await context.close();
    });
  });

  test.describe('Static File Structure Verification', () => {
    test('Page consists of only static HTML, CSS, JS, and assets', async ({ page }) => {
      const loadedResources = [];

      page.on('response', response => {
        const contentType = response.headers()['content-type'] || '';
        loadedResources.push({
          url: response.url(),
          contentType,
          status: response.status(),
        });
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Categorize resources by type
      const staticTypes = ['text/html', 'text/css', 'application/javascript', 'image/', 'font/', 'text/javascript'];

      const allStatic = loadedResources.every(resource => {
        return staticTypes.some(type => resource.contentType.includes(type)) ||
               resource.contentType === '';
      });

      console.log('Loaded resources:');
      loadedResources.forEach(r => {
        console.log(`  ${r.url} - ${r.contentType}`);
      });

      expect(allStatic).toBe(true);
    });

    test('No server-side rendering markers present', async ({ page }) => {
      await page.goto('/');

      const htmlContent = await page.content();

      // Check for common SSR markers that shouldn't be present in static files
      const ssrMarkers = [
        '__NEXT_DATA__',      // Next.js SSR
        '__NUXT__',           // Nuxt.js SSR
        '__INITIAL_STATE__',  // Common SSR hydration
        '__APOLLO_STATE__',   // Apollo GraphQL SSR
        'data-server-rendered', // Vue SSR
      ];

      for (const marker of ssrMarkers) {
        expect(htmlContent).not.toContain(marker);
      }

      console.log('Verified: No SSR markers found');
    });
  });
});
